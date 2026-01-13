from celery import shared_task

from .ingestion import IngestionService
from .zarr_manager import get_default_zarr_manager


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


@shared_task
def update_zarr_store_task(item_id: int, raw_data_path: str):
    '''
    Async task to update Zarr store for an Item.
    
    Args:
        item_id: Item primary key
        raw_data_path: Path to temporary numpy file with raw data
    '''
    from georiva.core.models import Item
    import numpy as np
    
    item = Item.objects.get(pk=item_id)
    raw_data = np.load(raw_data_path)
    
    manager = get_default_zarr_manager()
    manager.append_timestep(
        dataset=item.dataset,
        timestamp=item.time,
        data=raw_data,
        bounds=tuple(item.bounds),
    )
    
    # Cleanup temp file
    Path(raw_data_path).unlink(missing_ok=True)
    
    
