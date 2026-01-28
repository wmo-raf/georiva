from celery import shared_task

from georiva.core.models import Collection
from .service import IngestionService


@shared_task(bind=True, max_retries=3)
def process_incoming_file(self, collection_id: str, file_path: str, metadata: dict = None) -> dict:
    """
    Celery task to process an incoming file.
    """
    
    collection = Collection.objects.get(id=collection_id)
    
    service = IngestionService()
    result = service.process_file(file_path, catalog_slug=collection.catalog.slug, collection_slug=collection.slug)
    
    if not result.success and self.request.retries < self.max_retries:
        raise self.retry(countdown=60 * (self.request.retries + 1))
    
    return {
        'success': result.success,
        'items_created': result.items_created,
        'assets_created': result.assets_created,
        'errors': result.errors,
    }
