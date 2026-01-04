"""
Celery App Configuration for Pharmyrus
"""

import os
import logging

from celery import Celery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Redis URL from Railway environment variable
redis_url = os.getenv('REDIS_URL')

# Debug logging
if redis_url:
    logger.info(f"✅ REDIS_URL found: {redis_url[:50]}...")
else:
    logger.error("❌ REDIS_URL not found! Using localhost fallback")
    redis_url = 'redis://localhost:6379/0'

# Create Celery app
app = Celery(
    'pharmyrus',
    broker=redis_url,
    backend=redis_url
)

# Configure
app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_send_sent_event=True,
    task_time_limit=3600,  # 60 min
    task_soft_time_limit=3300,  # 55 min
    result_expires=86400,  # 24h
    broker_connection_retry_on_startup=True,
    worker_prefetch_multiplier=1,
)

logger.info(f"🚀 Celery configured with broker: {redis_url[:50]}...")

# CRITICAL: Define task directly here to avoid import issues
import time
import traceback

@app.task(bind=True, name='pharmyrus.search')
def search_task(self, molecule: str, countries: list = None, include_wipo: bool = False):
    """Background task for patent search"""
    start_time = time.time()
    
    try:
        self.update_state(
            state='PROGRESS',
            meta={'progress': 0, 'step': 'Initializing...', 'elapsed': 0, 'molecule': molecule}
        )
        
        logger.info(f"🚀 Starting search for: {molecule}")
        
        # Import inside task with explicit path
        import asyncio
        import sys
        import os
        
        # Add /app to path if not there
        app_path = '/app'
        if app_path not in sys.path:
            sys.path.insert(0, app_path)
        
        try:
            from main import search_patents
            logger.info("✅ Successfully imported search_patents from main")
        except Exception as e:
            logger.error(f"❌ Failed to import from main: {e}")
            raise
        
        # Create request
        class TaskRequest:
            def __init__(self, nome_molecula, paises_alvo, incluir_wo):
                self.nome_molecula = nome_molecula
                self.nome_comercial = None  # Optional field
                self.paises_alvo = paises_alvo
                self.incluir_wo = incluir_wo
                self.max_results = 100  # Default value
        
        request = TaskRequest(molecule, countries or ['BR'], include_wipo)
        
        logger.info(f"📊 Request created for {molecule}")
        
        # Define progress callback
        def progress_callback(progress: int, step: str):
            elapsed = time.time() - start_time
            self.update_state(
                state='PROGRESS',
                meta={
                    'progress': progress,
                    'step': step,
                    'elapsed': round(elapsed, 1),
                    'molecule': molecule
                }
            )
            logger.info(f"📊 [{progress}%] {step}")
        
        # Run search
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        logger.info("🔄 Running search...")
        result = loop.run_until_complete(search_patents(request, progress_callback=progress_callback))
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Search completed for {molecule} in {elapsed:.1f}s")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Search failed for {molecule}: {e}")
        logger.error(traceback.format_exc())
        raise

logger.info("✅ Task 'pharmyrus.search' registered")
