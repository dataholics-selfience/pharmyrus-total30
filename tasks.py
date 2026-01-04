"""
Celery Background Tasks
"""

import time
import logging
import traceback
from celery_app import app

logger = logging.getLogger(__name__)


@app.task(bind=True, name='pharmyrus.search')
def search_task(self, molecule: str, countries: list = None, include_wipo: bool = False):
    """
    Background task for patent search
    """
    start_time = time.time()
    
    try:
        self.update_state(
            state='PROGRESS',
            meta={
                'progress': 0,
                'step': 'Initializing search...',
                'elapsed': 0,
                'molecule': molecule
            }
        )
        
        logger.info(f"🚀 Starting async search for: {molecule}")
        
        # Import here to avoid circular dependency
        import asyncio
        import sys
        
        # Get the search function - import inside task to avoid circular import
        sys.path.insert(0, '/app')
        from main import search_endpoint, SearchRequest
        
        # Progress callback
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
            logger.info(f"📊 {molecule}: {progress}% - {step}")
        
        # Create request object
        class TaskRequest:
            def __init__(self, nome_molecula, paises_alvo, incluir_wo):
                self.nome_molecula = nome_molecula
                self.paises_alvo = paises_alvo
                self.incluir_wo = incluir_wo
        
        request = TaskRequest(
            nome_molecula=molecule,
            paises_alvo=countries or ['BR'],
            incluir_wo=include_wipo
        )
        
        # Run search
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(search_endpoint(request))
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Async search completed for {molecule} in {elapsed:.1f}s")
        
        self.update_state(
            state='PROGRESS',
            meta={
                'progress': 100,
                'step': 'Complete!',
                'elapsed': round(elapsed, 1),
                'molecule': molecule
            }
        )
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        
        logger.error(f"❌ Async search failed for {molecule}: {error_msg}")
        logger.error(error_trace)
        
        self.update_state(
            state='FAILURE',
            meta={
                'error': error_msg,
                'traceback': error_trace,
                'molecule': molecule,
                'elapsed': round(time.time() - start_time, 1)
            }
        )
        
        raise
