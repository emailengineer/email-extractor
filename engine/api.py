# email_extractor/api.py
# FastAPI REST API for Email Extraction System

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from typing import List, Optional
import asyncio
import aiomysql
from datetime import datetime
import uuid
import os
from core import EmailExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'mysql'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'email_extractor'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'email_extraction')
}

# Global database pool
db_pool = None

# Background worker instances
active_extractors = {}

app = FastAPI(
    title="Email Extraction API",
    description="High-performance email extraction system",
    version="1.0.0"
)


# Pydantic models
class DomainInput(BaseModel):
    domain: str
    
    @validator('domain')
    def validate_domain(cls, v):
        v = v.strip()
        if not v:
            raise ValueError('Domain cannot be empty')
        return v


class SearchCreate(BaseModel):
    batch_name: Optional[str] = None
    domains: List[str]
    
    @validator('domains')
    def validate_domains(cls, v):
        if not v:
            raise ValueError('At least one domain is required')
        if len(v) > 10000:
            raise ValueError('Maximum 10000 domains per batch')
        return [d.strip() for d in v if d.strip()]


class SearchResponse(BaseModel):
    search_id: int
    batch_name: Optional[str]
    total_domains: int
    status: str
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


class DomainResponse(BaseModel):
    domain_id: int
    domain: str
    status: str
    pages_crawled: int
    emails_found: int
    error_message: Optional[str]
    updated_at: datetime


class EmailResponse(BaseModel):
    email_id: int
    domain: str
    page_url: str
    raw_email: str
    normalized_email: str
    extracted_at: datetime


class SearchStatistics(BaseModel):
    search_id: int
    total_domains: int
    domains_completed: int
    domains_failed: int
    total_pages_crawled: int
    total_emails_found: int
    duration_seconds: Optional[int]


# Startup and shutdown
@app.on_event("startup")
async def startup():
    global db_pool
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
    logger.info("Database pool created")


@app.on_event("shutdown")
async def shutdown():
    global db_pool
    if db_pool:
        db_pool.close()
        await db_pool.wait_closed()
    
    # Cleanup active extractors
    for extractor in active_extractors.values():
        await extractor.cleanup()
    
    logger.info("Application shutdown complete")


# Helper functions
async def get_db():
    async with db_pool.acquire() as conn:
        yield conn


async def run_extraction_worker(search_id: int):
    """Background worker to process email extraction."""
    worker_id = f"worker-{uuid.uuid4().hex[:8]}"
    
    try:
        # Get configuration from environment variables
        max_depth = int(os.getenv('MAX_DEPTH', 3))
        timeout = int(os.getenv('TIMEOUT', 30))
        max_concurrent = int(os.getenv('MAX_CONCURRENT', 1000))
        
        extractor = EmailExtractor(
            DB_CONFIG,
            max_depth=max_depth,
            timeout=timeout,
            max_concurrent=max_concurrent
        )
        await extractor.initialize()
        active_extractors[worker_id] = extractor
        
        logger.info(f"Worker {worker_id} starting search {search_id}")
        await extractor.process_search(search_id, worker_id)
        logger.info(f"Worker {worker_id} completed search {search_id}")
        
    except Exception as e:
        logger.error(f"Worker {worker_id} error: {str(e)}")
    finally:
        if worker_id in active_extractors:
            await active_extractors[worker_id].cleanup()
            del active_extractors[worker_id]


# API Endpoints

@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "running",
        "service": "Email Extraction API",
        "version": "1.0.0"
    }


@app.post("/api/searches", response_model=SearchResponse, status_code=201)
async def create_search(
    search_data: SearchCreate,
    background_tasks: BackgroundTasks,
    conn=Depends(get_db)
):
    """Create a new email extraction search."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # Create search record
            await cursor.execute(
                """INSERT INTO searches (batch_name, total_domains, status)
                   VALUES (%s, %s, 'pending')""",
                (search_data.batch_name, len(search_data.domains))
            )
            search_id = cursor.lastrowid
            
            # Insert domains
            domain_values = [
                (search_id, domain, f"https://{domain}")
                for domain in search_data.domains
            ]
            
            await cursor.executemany(
                """INSERT INTO domains (search_id, domain, url, status)
                   VALUES (%s, %s, %s, 'pending')""",
                domain_values
            )
            
            # Get search details
            await cursor.execute(
                """SELECT id, batch_name, total_domains, status, 
                   created_at, started_at, completed_at
                   FROM searches WHERE id=%s""",
                (search_id,)
            )
            search = await cursor.fetchone()
        
        # Start background extraction
        background_tasks.add_task(run_extraction_worker, search_id)
        
        logger.info(f"Created search {search_id} with {len(search_data.domains)} domains")
        
        return SearchResponse(**search)
        
    except Exception as e:
        logger.error(f"Error creating search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/searches", response_model=List[SearchResponse])
async def list_searches(
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    conn=Depends(get_db)
):
    """List all searches with optional status filter."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            if status:
                await cursor.execute(
                    """SELECT id, batch_name, total_domains, status,
                       created_at, started_at, completed_at
                       FROM searches WHERE status=%s
                       ORDER BY created_at DESC LIMIT %s OFFSET %s""",
                    (status, limit, offset)
                )
            else:
                await cursor.execute(
                    """SELECT id, batch_name, total_domains, status,
                       created_at, started_at, completed_at
                       FROM searches
                       ORDER BY created_at DESC LIMIT %s OFFSET %s""",
                    (limit, offset)
                )
            
            searches = await cursor.fetchall()
            return [SearchResponse(**s) for s in searches]
            
    except Exception as e:
        logger.error(f"Error listing searches: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/searches/{search_id}", response_model=SearchResponse)
async def get_search(search_id: int, conn=Depends(get_db)):
    """Get details of a specific search."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(
                """SELECT id, batch_name, total_domains, status,
                   created_at, started_at, completed_at
                   FROM searches WHERE id=%s""",
                (search_id,)
            )
            search = await cursor.fetchone()
            
            if not search:
                raise HTTPException(status_code=404, detail="Search not found")
            
            return SearchResponse(**search)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/searches/{search_id}/statistics", response_model=SearchStatistics)
async def get_search_statistics(search_id: int, conn=Depends(get_db)):
    """Get statistics for a specific search."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(
                """SELECT 
                   s.id as search_id,
                   s.total_domains,
                   SUM(CASE WHEN d.status='completed' THEN 1 ELSE 0 END) as domains_completed,
                   SUM(CASE WHEN d.status='failed' THEN 1 ELSE 0 END) as domains_failed,
                   SUM(d.pages_crawled) as total_pages_crawled,
                   SUM(d.emails_found) as total_emails_found,
                   TIMESTAMPDIFF(SECOND, s.started_at, 
                                 COALESCE(s.completed_at, NOW())) as duration_seconds
                   FROM searches s
                   LEFT JOIN domains d ON s.id = d.search_id
                   WHERE s.id=%s
                   GROUP BY s.id""",
                (search_id,)
            )
            stats = await cursor.fetchone()
            
            if not stats:
                raise HTTPException(status_code=404, detail="Search not found")
            
            return SearchStatistics(**stats)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/searches/{search_id}/domains", response_model=List[DomainResponse])
async def get_search_domains(
    search_id: int,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    conn=Depends(get_db)
):
    """Get domains for a specific search."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            if status:
                await cursor.execute(
                    """SELECT id as domain_id, domain, status, 
                       pages_crawled, emails_found, error_message, updated_at
                       FROM domains WHERE search_id=%s AND status=%s
                       ORDER BY updated_at DESC LIMIT %s OFFSET %s""",
                    (search_id, status, limit, offset)
                )
            else:
                await cursor.execute(
                    """SELECT id as domain_id, domain, status,
                       pages_crawled, emails_found, error_message, updated_at
                       FROM domains WHERE search_id=%s
                       ORDER BY updated_at DESC LIMIT %s OFFSET %s""",
                    (search_id, limit, offset)
                )
            
            domains = await cursor.fetchall()
            return [DomainResponse(**d) for d in domains]
            
    except Exception as e:
        logger.error(f"Error getting domains: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/domains/{domain_id}/emails", response_model=List[EmailResponse])
async def get_domain_emails(domain_id: int, conn=Depends(get_db)):
    """Get all emails extracted from a specific domain."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(
                """SELECT 
                   e.id as email_id,
                   d.domain,
                   p.url as page_url,
                   e.raw_email,
                   e.normalized_email,
                   e.extracted_at
                   FROM emails e
                   JOIN domains d ON e.domain_id = d.id
                   JOIN pages p ON e.page_id = p.id
                   WHERE e.domain_id=%s
                   ORDER BY e.extracted_at DESC""",
                (domain_id,)
            )
            
            emails = await cursor.fetchall()
            return [EmailResponse(**e) for e in emails]
            
    except Exception as e:
        logger.error(f"Error getting emails: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/searches/{search_id}/emails", response_model=List[EmailResponse])
async def get_search_emails(
    search_id: int,
    limit: int = 1000,
    offset: int = 0,
    conn=Depends(get_db)
):
    """Get all emails extracted in a search."""
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(
                """SELECT 
                   e.id as email_id,
                   d.domain,
                   p.url as page_url,
                   e.raw_email,
                   e.normalized_email,
                   e.extracted_at
                   FROM emails e
                   JOIN domains d ON e.domain_id = d.id
                   JOIN pages p ON e.page_id = p.id
                   WHERE d.search_id=%s
                   ORDER BY e.extracted_at DESC
                   LIMIT %s OFFSET %s""",
                (search_id, limit, offset)
            )
            
            emails = await cursor.fetchall()
            return [EmailResponse(**e) for e in emails]
            
    except Exception as e:
        logger.error(f"Error getting emails: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/searches/{search_id}/pause")
async def pause_search(search_id: int, conn=Depends(get_db)):
    """Pause a running search."""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """UPDATE searches SET status='paused' 
                   WHERE id=%s AND status='in_progress'""",
                (search_id,)
            )
            
            if cursor.rowcount == 0:
                raise HTTPException(
                    status_code=400,
                    detail="Search not found or not in progress"
                )
            
            return {"message": "Search paused", "search_id": search_id}
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/searches/{search_id}/resume")
async def resume_search(
    search_id: int,
    background_tasks: BackgroundTasks,
    conn=Depends(get_db)
):
    """Resume a paused search."""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """UPDATE searches SET status='in_progress' 
                   WHERE id=%s AND status='paused'""",
                (search_id,)
            )
            
            if cursor.rowcount == 0:
                raise HTTPException(
                    status_code=400,
                    detail="Search not found or not paused"
                )
            
            # Restart background extraction
            background_tasks.add_task(run_extraction_worker, search_id)
            
            return {"message": "Search resumed", "search_id": search_id}
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/searches/{search_id}")
async def cancel_search(search_id: int, conn=Depends(get_db)):
    """Cancel a search and mark as cancelled."""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """UPDATE searches SET status='cancelled' WHERE id=%s""",
                (search_id,)
            )
            
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Search not found")
            
            # Unlock any locked domains
            await cursor.execute(
                """UPDATE domains SET worker_id=NULL, locked_at=NULL
                   WHERE search_id=%s AND status='crawling'""",
                (search_id,)
            )
            
            return {"message": "Search cancelled", "search_id": search_id}
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")