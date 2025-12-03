from celery import shared_task

from .ingestion import IngestionService


@shared_task(bind=True, max_retries=3)
def process_incoming_file_task(self, file_path: str, collection_id: str = None):
    """
    Celery task to process an incoming file.
    """
    service = IngestionService()
    result = service.process_incoming_file(file_path, collection_id)
    
    if not result.success and self.request.retries < self.max_retries:
        raise self.retry(countdown=60 * (self.request.retries + 1))
    
    return {
        'success': result.success,
        'datasets_processed': result.datasets_processed,
        'assets_created': result.assets_created,
        'errors': result.errors,
    }
