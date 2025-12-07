# core.py
# High-Performance Email Extraction System - Core Module

import asyncio
import re
import logging
import os
from typing import List, Set, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
import aiomysql
from email_validator import validate_email, EmailNotValidError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmailExtractor:
    """Core email extraction engine with maximum concurrency."""
    
    # Common email-containing paths to prioritize
    EMAIL_PATHS = [
        '/contact', '/about', '/team', '/careers', '/jobs',
        '/faq', '/privacy', '/support', '/legal', '/terms',
        '/company', '/staff', '/people', '/leadership',
        '/contact-us', '/about-us', '/our-team', '/meet-the-team'
    ]
    
    # Email regex patterns - comprehensive
    EMAIL_PATTERN = re.compile(
        r'\b[A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Z|a-z]{2,}\b'
    )
    
    # Obfuscated email patterns
    OBFUSCATED_PATTERNS = [
        # [at] and [dot] patterns
        (re.compile(r'([A-Za-z0-9._%+-]+)\s*\[at\]\s*([A-Za-z0-9.-]+)\s*\[dot\]\s*([A-Za-z]{2,})', re.I), r'\1@\2.\3'),
        (re.compile(r'([A-Za-z0-9._%+-]+)\s*\(at\)\s*([A-Za-z0-9.-]+)\s*\(dot\)\s*([A-Za-z]{2,})', re.I), r'\1@\2.\3'),
        (re.compile(r'([A-Za-z0-9._%+-]+)\s*\[AT\]\s*([A-Za-z0-9.-]+)\s*\[DOT\]\s*([A-Za-z]{2,})'), r'\1@\2.\3'),
        # @ with spaces around it
        (re.compile(r'([A-Za-z0-9._%+-]+)\s*@\s*([A-Za-z0-9.-]+)\s*\.\s*([A-Za-z]{2,})'), r'\1@\2.\3'),
        # (a) and (dot) patterns
        (re.compile(r'([A-Za-z0-9._%+-]+)\s*\(a\)\s*([A-Za-z0-9.-]+)\s*\(dot\)\s*([A-Za-z]{2,})', re.I), r'\1@\2.\3'),
    ]
    
    def __init__(self, db_config: dict, max_depth: int = 3, 
                 timeout: int = 30, max_concurrent: int = 1000):
        """
        Initialize the email extractor.
        
        Args:
            db_config: Database configuration dict
            max_depth: Maximum crawl depth
            timeout: HTTP request timeout in seconds
            max_concurrent: Maximum concurrent requests
        """
        self.db_config = db_config
        self.max_depth = max_depth
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_concurrent = max_concurrent
        self.db_pool = None
        self.session = None
        
    async def initialize(self):
        """Initialize database pool and HTTP session."""
        try:
            self.db_pool = await aiomysql.create_pool(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['database'],
                autocommit=True,
                maxsize=100,
                charset='utf8mb4'
            )
            
            # Create HTTP session with unbounded connections
            connector = aiohttp.TCPConnector(
                limit=0,  # Unbounded connections
                limit_per_host=50,
                ttl_dns_cache=300,
                ssl=False  # Disable SSL verification for speed
            )
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
            
            logger.info(f"Email extractor initialized: max_depth={self.max_depth}, max_concurrent={self.max_concurrent}")
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup resources."""
        try:
            if self.session:
                await self.session.close()
            if self.db_pool:
                self.db_pool.close()
                await self.db_pool.wait_closed()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def normalize_url(self, url: str) -> str:
        """
        Normalize URL to standard format.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        if not url:
            return url
            
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            parsed = urlparse(url)
            # Remove www. for consistency and lowercase
            netloc = parsed.netloc.lower().replace('www.', '')
            
            # Remove trailing slash from path
            path = parsed.path.rstrip('/') or '/'
            
            return urlunparse((
                parsed.scheme,
                netloc,
                path,
                '', '', ''
            ))
        except Exception as e:
            logger.warning(f"Failed to normalize URL {url}: {e}")
            return url
    
    def extract_domain(self, url: str) -> str:
        """
        Extract domain from URL.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Domain name
        """
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower().replace('www.', '')
        except:
            return ""
    
    def is_valid_url(self, url: str, base_domain: str) -> bool:
        """
        Check if URL is valid and belongs to the same domain.
        
        Args:
            url: URL to validate
            base_domain: Base domain to compare against
            
        Returns:
            True if valid, False otherwise
        """
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Extract domain
            url_domain = parsed.netloc.lower().replace('www.', '')
            
            # Must be same domain or subdomain
            if url_domain != base_domain and not url_domain.endswith('.' + base_domain):
                return False
            
            # Exclude common non-HTML resources
            excluded_extensions = {
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', 
                '.js', '.ico', '.svg', '.zip', '.mp4', '.mp3',
                '.avi', '.mov', '.wmv', '.flv', '.webm',
                '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.exe', '.dmg', '.apk', '.deb', '.rpm'
            }
            
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in excluded_extensions):
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"URL validation failed for {url}: {e}")
            return False
    
    async def fetch_page(self, url: str) -> Tuple[Optional[str], int]:
        """
        Fetch page content asynchronously.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (html_content, status_code)
        """
        try:
            async with self.session.get(url, allow_redirects=True, ssl=False) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type or 'text/plain' in content_type:
                        text = await response.text(errors='ignore')
                        return text, response.status
                return None, response.status
                
        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching {url}")
            return None, 0
        except aiohttp.ClientError as e:
            logger.debug(f"Client error fetching {url}: {e}")
            return None, 0
        except Exception as e:
            logger.debug(f"Error fetching {url}: {e}")
            return None, 0
    
    def extract_links(self, html: str, base_url: str, base_domain: str) -> Set[str]:
        """
        Extract all valid internal links from HTML.
        
        Args:
            html: HTML content
            base_url: Base URL for relative links
            base_domain: Base domain to filter links
            
        Returns:
            Set of valid URLs
        """
        links = set()
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract from <a> and <area> tags
            for tag in soup.find_all(['a', 'area'], href=True):
                href = tag['href']
                
                # Skip mailto, tel, javascript, etc.
                if href.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                    continue
                
                # Convert to absolute URL
                try:
                    absolute_url = urljoin(base_url, href)
                except:
                    continue
                
                # Remove fragments and query parameters
                parsed = urlparse(absolute_url)
                clean_url = urlunparse((
                    parsed.scheme, parsed.netloc, parsed.path, '', '', ''
                ))
                
                # Validate and add
                if self.is_valid_url(clean_url, base_domain):
                    links.add(clean_url)
                    
        except Exception as e:
            logger.error(f"Error parsing links from {base_url}: {e}")
        
        return links
    
    def extract_emails_from_html(self, html: str) -> Set[str]:
        """
        Extract all emails from HTML content.
        
        Args:
            html: HTML content
            
        Returns:
            Set of found email addresses
        """
        emails = set()
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract from mailto links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('mailto:'):
                    email = href[7:].split('?')[0].strip()
                    if email:
                        emails.add(email)
            
            # Get text content
            text = soup.get_text()
            
            # Handle obfuscated emails
            for pattern, replacement in self.OBFUSCATED_PATTERNS:
                text = pattern.sub(replacement, text)
            
            # Find standard email patterns
            found = self.EMAIL_PATTERN.findall(text)
            emails.update(found)
            
        except Exception as e:
            logger.error(f"Error extracting emails from HTML: {e}")
        
        return emails
    
    def normalize_email(self, email: str) -> Optional[str]:
        """
        Normalize and validate email address.
        
        Args:
            email: Email address to normalize
            
        Returns:
            Normalized email or None if invalid
        """
        try:
            # Basic cleanup
            email = email.lower().strip()
            email = email.rstrip('.,;:!?')
            
            # Remove common trailing characters
            email = email.strip('<>()[]{}"\' ')
            
            # Validate using email-validator library
            validated = validate_email(email, check_deliverability=False)
            return validated.normalized
            
        except EmailNotValidError:
            return None
        except Exception as e:
            logger.debug(f"Error normalizing email {email}: {e}")
            return None
    
    async def crawl_domain(self, domain_id: int, url: str, worker_id: str):
        """
        Crawl a domain and extract all emails.
        
        Args:
            domain_id: Database ID of the domain
            url: Starting URL
            worker_id: Worker identifier
        """
        base_domain = self.extract_domain(url)
        visited_urls = set()
        emails_found = {}  # {normalized_email: (raw_email, page_url, page_id)}
        
        # Queue: list of (depth, url) tuples
        queue = [(0, url)]
        
        async def process_url(depth: int, current_url: str):
            """Process a single URL."""
            if depth > self.max_depth or current_url in visited_urls:
                return
            
            visited_urls.add(current_url)
            
            # Fetch page
            html, status_code = await self.fetch_page(current_url)
            
            # Store page record
            page_id = None
            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            """INSERT INTO pages (domain_id, url, status_code, 
                               content_type, error_message)
                               VALUES (%s, %s, %s, %s, %s)""",
                            (domain_id, current_url[:1000], status_code,
                             'text/html' if html else None,
                             None if html else 'Failed to fetch')
                        )
                        page_id = cursor.lastrowid
            except Exception as e:
                logger.error(f"Failed to store page record for {current_url}: {e}")
            
            if not html or not page_id:
                return
            
            # Extract emails
            raw_emails = self.extract_emails_from_html(html)
            for raw_email in raw_emails:
                normalized = self.normalize_email(raw_email)
                if normalized and normalized not in emails_found:
                    emails_found[normalized] = (raw_email, current_url, page_id)
            
            # Extract links for further crawling
            if depth < self.max_depth:
                links = self.extract_links(html, current_url, base_domain)
                
                # Prioritize email-containing paths
                priority_links = [
                    link for link in links 
                    if any(path in link.lower() for path in self.EMAIL_PATHS)
                ]
                other_links = [link for link in links if link not in priority_links]
                
                # Add to queue
                for link in priority_links + other_links:
                    if link not in visited_urls:
                        queue.append((depth + 1, link))
        
        try:
            # Update domain status to crawling
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE domains SET status='crawling', worker_id=%s, 
                           locked_at=NOW() WHERE id=%s""",
                        (worker_id, domain_id)
                    )
            
            logger.info(f"Starting crawl of domain {base_domain} (ID: {domain_id})")
            
            # Process URLs with concurrency
            tasks = []
            while queue:
                # Process in batches
                batch_size = min(50, len(queue))
                batch = [queue.pop(0) for _ in range(batch_size)]
                
                for depth, current_url in batch:
                    task = asyncio.create_task(process_url(depth, current_url))
                    tasks.append(task)
                
                # Wait for batch to complete
                if len(tasks) >= 50:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
            
            # Wait for remaining tasks
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Store extracted emails
            if emails_found:
                try:
                    async with self.db_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            values = [
                                (domain_id, page_id, raw[:255], normalized[:255])
                                for normalized, (raw, _, page_id) in emails_found.items()
                            ]
                            await cursor.executemany(
                                """INSERT IGNORE INTO emails 
                                   (domain_id, page_id, raw_email, normalized_email)
                                   VALUES (%s, %s, %s, %s)""",
                                values
                            )
                except Exception as e:
                    logger.error(f"Failed to store emails for domain {domain_id}: {e}")
            
            # Update domain status
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE domains SET status='completed', 
                           pages_crawled=%s, emails_found=%s, 
                           worker_id=NULL, locked_at=NULL
                           WHERE id=%s""",
                        (len(visited_urls), len(emails_found), domain_id)
                    )
            
            logger.info(
                f"Completed {base_domain}: {len(visited_urls)} pages, "
                f"{len(emails_found)} unique emails"
            )
            
        except Exception as e:
            logger.error(f"Error crawling domain {domain_id} ({base_domain}): {e}")
            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            """UPDATE domains SET status='failed', 
                               error_message=%s, worker_id=NULL, locked_at=NULL
                               WHERE id=%s""",
                            (str(e)[:500], domain_id)
                        )
            except Exception as db_error:
                logger.error(f"Failed to update domain status: {db_error}")
    
    async def process_search(self, search_id: int, worker_id: str):
        """
        Process all domains in a search.
        
        Args:
            search_id: Database ID of the search
            worker_id: Worker identifier
        """
        try:
            # Update search status
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE searches SET status='in_progress', 
                           started_at=NOW() WHERE id=%s""",
                        (search_id,)
                    )
            
            logger.info(f"Starting search {search_id} with worker {worker_id}")
            
            # Get pending domains
            domains = []
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """SELECT id, url FROM domains 
                           WHERE search_id=%s AND status='pending'
                           ORDER BY id""",
                        (search_id,)
                    )
                    domains = await cursor.fetchall()
            
            if not domains:
                logger.warning(f"No pending domains found for search {search_id}")
                return
            
            logger.info(f"Processing {len(domains)} domains for search {search_id}")
            
            # Process domains with maximum concurrency
            tasks = [
                self.crawl_domain(domain_id, url, worker_id)
                for domain_id, url in domains
            ]
            
            # Process in batches for memory efficiency
            batch_size = self.max_concurrent
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(tasks)-1)//batch_size + 1}")
                await asyncio.gather(*batch, return_exceptions=True)
            
            # Update search status
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE searches SET status='completed', 
                           completed_at=NOW() WHERE id=%s""",
                        (search_id,)
                    )
            
            logger.info(f"Search {search_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing search {search_id}: {e}")
            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(
                            """UPDATE searches SET status='failed' WHERE id=%s""",
                            (search_id,)
                        )
            except Exception as db_error:
                logger.error(f"Failed to update search status: {db_error}")


async def main():
    """Example usage and testing."""
    # Get configuration from environment
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'email_extractor'),
        'password': os.getenv('DB_PASSWORD', 'your_password'),
        'database': os.getenv('DB_NAME', 'email_extraction')
    }
    
    max_depth = int(os.getenv('MAX_DEPTH', 3))
    max_concurrent = int(os.getenv('MAX_CONCURRENT', 1000))
    
    extractor = EmailExtractor(
        db_config, 
        max_depth=max_depth, 
        max_concurrent=max_concurrent
    )
    
    await extractor.initialize()
    
    try:
        # Process search ID from environment or default to 1
        search_id = int(os.getenv('SEARCH_ID', 1))
        worker_id = os.getenv('WORKER_ID', 'worker-main')
        
        logger.info(f"Starting extraction for search {search_id}")
        await extractor.process_search(search_id=search_id, worker_id=worker_id)
        logger.info("Extraction completed")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await extractor.cleanup()


if __name__ == '__main__':
    asyncio.run(main())