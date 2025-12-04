"""
GeoRiva Loader Tasks

Celery tasks for automated data loading and ingestion.
"""

import logging
from datetime import timedelta

from celery import shared_task
from django.utils import timezone

from georiva.core.models import Collection

logger = logging.getLogger(__name__)


@shared_task
def run_all_scheduled_loaders():
    """
    Check and run all loaders that are due based on their schedule.
    
    This task should be called periodically (e.g., every 5 minutes)
    by Celery Beat.
    """
    collections_with_loaders = Collection.objects.filter(loader__isnull=False)
    
    for collection in collections_with_loaders:
        config = collection.loader.get_real_instance()
        
        if _should_run(config):
            logger.info(f"Triggering scheduled run for {config}")
            run_loader_for_collection.delay(collection.pk)


def _should_run(config) -> bool:
    """
    Check if a loader should run based on its cron schedule.
    
    Uses croniter to evaluate cron expressions.
    """
    if not config.interval:
        return False
    
    now = timezone.now()
    
    # If never run, run now
    if config.last_run_at is None:
        return True
    
    # Check if we're past the next scheduled time
    try:
        next_run = config.last_run_at + timedelta(minutes=config.interval)
        # Convert to timezone-aware if needed
        if timezone.is_naive(next_run):
            next_run = timezone.make_aware(next_run)
        
        return now >= next_run
    
    except Exception as e:
        logger.error(f"Failed to evaluate schedule for {config}: {e}")
        return False


@shared_task
def cleanup_old_incoming_files(days: int = 7):
    """
    Remove processed files from incoming storage older than specified days.
    """
    from georiva.core.storage import storage_manager
    
    cutoff = timezone.now() - timedelta(days=days)
    removed = 0
    
    try:
        files = storage_manager.list_files('incoming', recursive=True)
        
        for file_info in files:
            if file_info['modified'] < cutoff:
                try:
                    storage_manager.delete(file_info['path'])
                    removed += 1
                except Exception as e:
                    logger.warning(f"Failed to delete {file_info['path']}: {e}")
    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
    
    logger.info(f"Cleaned up {removed} old incoming files")
    return {'removed': removed}


@shared_task
def run_loader_for_collection(collection_id: str, dry_run: bool = False) -> dict:
    """
    Run the loader for a specific collection (if configured).
    
    Convenience task for triggering loads by collection ID.
    """
    from georiva.core.models import Collection
    from georiva.loaders import get_loader_for_config
    from georiva.loaders.models import LoaderConfig
    
    try:
        collection = Collection.objects.get(id=collection_id)
    except Collection.DoesNotExist:
        return {'error': f'Collection {collection_id} not found'}
    
    if not hasattr(collection, 'loader'):
        return {'error': 'Collection has no loader configured'}
    
    loader_config_id = collection.loader.pk
    
    try:
        config = LoaderConfig.objects.get(pk=loader_config_id)
    except LoaderConfig.DoesNotExist:
        logger.error(f"LoaderConfig {loader_config_id} not found")
        return {'error': 'Config not found'}
    
    # Get real instance (polymorphic)
    config = config.get_real_instance()
    
    # Mark as running
    config.last_run_status = 'running'
    config.save(update_fields=['last_run_status'])
    
    try:
        loader = get_loader_for_config(config, collection)
        result = loader.run(dry_run=dry_run)
        
        return {
            'status': result.status,
            'files_found': result.files_found,
            'files_fetched': result.files_fetched,
            'files_skipped': result.files_skipped,
            'files_failed': result.files_failed,
            'bytes_transferred': result.bytes_transferred,
            'duration_seconds': result.duration_seconds,
            'errors': result.errors[:5],  # Limit errors in result
        }
    
    except Exception as e:
        logger.exception(f"Loader run failed for {config}")
        config.record_run(status='failed', message=str(e))
        raise
