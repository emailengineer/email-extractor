#!/usr/bin/env python3
# Worker script for email extraction
# Runs continuously, polling for pending searches and processing them

import asyncio
import os
import logging
import time
from core import EmailExtractor
import aiomysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'mysql'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'email_extractor'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'email_extraction')
}

# Worker configuration
MAX_DEPTH = int(os.getenv('MAX_DEPTH', 3))
TIMEOUT = int(os.getenv('TIMEOUT', 30))
MAX_CONCURRENT = int(os.getenv('MAX_CONCURRENT', 1000))
WORKER_ID = os.getenv('WORKER_ID', f'worker-{os.getpid()}')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 5))  # seconds between polls

# Global database pool
db_pool = None
extractor = None


async def initialize():
    """Initialize database pool and extractor."""
    global db_pool, extractor
    
    try:
        # Create database pool
        db_pool = await aiomysql.create_pool(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            db=DB_CONFIG['database'],
            autocommit=True,
            maxsize=50,
            charset='utf8mb4'
        )
        
        # Create extractor
        extractor = EmailExtractor(
            DB_CONFIG,
            max_depth=MAX_DEPTH,
            timeout=TIMEOUT,
            max_concurrent=MAX_CONCURRENT
        )
        await extractor.initialize()
        
        logger.info(f"Worker {WORKER_ID} initialized")
        
    except Exception as e:
        logger.error(f"Failed to initialize worker: {e}")
        raise


async def get_pending_search():
    """Get a pending search from the database."""
    try:
        async with db_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Get a pending search (prefer pending over in_progress)
                await cursor.execute(
                    """SELECT id FROM searches 
                       WHERE status='pending'
                       ORDER BY created_at ASC
                       LIMIT 1""",
                )
                result = await cursor.fetchone()
                if result:
                    return result[0]
                
                # If no pending, check for in_progress searches with domains still pending
                await cursor.execute(
                    """SELECT DISTINCT s.id FROM searches s
                       JOIN domains d ON s.id = d.search_id
                       WHERE s.status='in_progress' AND d.status='pending'
                       ORDER BY s.created_at ASC
                       LIMIT 1""",
                )
                result = await cursor.fetchone()
                return result[0] if result else None
                
    except Exception as e:
        logger.error(f"Error getting pending search: {e}")
        return None


async def process_next_search():
    """Process the next available search."""
    search_id = await get_pending_search()
    
    if not search_id:
        return False
    
    try:
        logger.info(f"Worker {WORKER_ID} processing search {search_id}")
        await extractor.process_search(search_id, WORKER_ID)
        logger.info(f"Worker {WORKER_ID} completed search {search_id}")
        return True
        
    except Exception as e:
        logger.error(f"Worker {WORKER_ID} error processing search {search_id}: {e}")
        return False


async def worker_loop():
    """Main worker loop - continuously poll and process searches."""
    logger.info(f"Worker {WORKER_ID} started - polling every {POLL_INTERVAL} seconds")
    
    while True:
        try:
            processed = await process_next_search()
            
            if not processed:
                # No work available, wait before polling again
                await asyncio.sleep(POLL_INTERVAL)
            else:
                # Work was processed, check again immediately
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info(f"Worker {WORKER_ID} interrupted by user")
            break
        except Exception as e:
            logger.error(f"Worker {WORKER_ID} error in main loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)


async def cleanup():
    """Cleanup resources."""
    global db_pool, extractor
    
    try:
        if extractor:
            await extractor.cleanup()
        if db_pool:
            db_pool.close()
            await db_pool.wait_closed()
        logger.info(f"Worker {WORKER_ID} cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


async def main():
    """Main entry point."""
    try:
        await initialize()
        await worker_loop()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await cleanup()


if __name__ == '__main__':
    asyncio.run(main())

