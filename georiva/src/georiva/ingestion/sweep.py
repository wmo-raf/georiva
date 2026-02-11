"""
GeoRiva Sweep Task

Periodic safety net that:
1. Resets stale locks (crashed workers)
2. Finds untracked files in buckets
3. Retries failed files

Schedule via Celery Beat:
    CELERY_BEAT_SCHEDULE = {
        'sweep-unprocessed': {
            'task': 'georiva.ingestion.sweep.sweep_unprocessed',
            'schedule': crontab(minute='*/5'),  # every 5 minutes
        },
    }
"""

import logging
from pathlib import Path

from celery import shared_task

from georiva.core.filename import validate_path
from georiva.core.storage import storage, BucketType
from georiva.ingestion.models import IngestionLog
from georiva.ingestion.tasks import process_incoming_file

logger = logging.getLogger(__name__)


@shared_task(queue='ingestion')
def sweep_unprocessed():
    """
    Safety net — finds files that the webhook missed and retries failures.

    Three phases:
    1. Reset stale locks (workers that crashed mid-processing)
    2. Scan incoming/sources buckets for untracked files
    3. Retry failed files that haven't exceeded max retries
    """
    
    logger.info("Starting sweep...")
    
    # -----------------------------------------------------------------
    # Phase 1: Reset stale locks
    # -----------------------------------------------------------------
    
    stale_count = IngestionLog.reset_stale_locks()
    if stale_count:
        logger.warning("Reset %d stale locks", stale_count)
    
    # -----------------------------------------------------------------
    # Phase 2: Scan buckets for untracked files
    # -----------------------------------------------------------------
    
    new_files = 0
    
    for bucket_type in [BucketType.INCOMING, BucketType.SOURCES]:
        bucket = storage.bucket(bucket_type)
        files = bucket.list_files(recursive=True)
        
        for f in files:
            path = f['path']
            filename = Path(path).name
            
            # Skip placeholder and hidden files
            if filename.startswith('.') or filename == '.keep':
                continue
            
            # Skip if path doesn't match convention
            try:
                meta = validate_path(path)
            except ValueError:
                logger.debug("Skipping non-conforming path: %s", path)
                continue
            
            # Skip if already tracked
            if IngestionLog.is_known(bucket_type, path):
                continue
            
            logger.info("Found untracked file: %s/%s", bucket_type, path)
            
            # Register
            IngestionLog.register(
                bucket=bucket_type,
                file_path=path,
                catalog_slug=meta.get('catalog', ''),
                collection_slug=meta.get('collection', ''),
                reference_time=meta.get('reference_time'),
            )
            
            # Queue for processing
            
            process_incoming_file.delay(
                collection_id=None,  # will be resolved by ingestion service
                file_path=path,
                origin_bucket=bucket_type,
                reference_time=(
                    meta['reference_time'].isoformat()
                    if meta.get('reference_time') else None
                ),
            )
            new_files += 1
    
    if new_files:
        logger.info("Queued %d untracked files", new_files)
    
    # -----------------------------------------------------------------
    # Phase 3: Retry failed files
    # -----------------------------------------------------------------
    
    retryable = IngestionLog.get_retryable(limit=50)
    retry_count = 0
    
    for log in retryable:
        logger.info(
            "Retrying (%d/%d): %s/%s — last error: %s",
            log.retry_count, IngestionLog.MAX_RETRIES,
            log.bucket, log.file_path,
            log.error[:100] if log.error else 'unknown',
        )
        
        process_incoming_file.delay(
            collection_id=None,
            file_path=log.file_path,
            origin_bucket=log.bucket,
            reference_time=(
                log.reference_time.isoformat()
                if log.reference_time else None
            ),
        )
        retry_count += 1
    
    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------
    
    # Log permanently failed files for visibility
    permanently_failed = IngestionLog.get_permanently_failed().count()
    if permanently_failed:
        logger.warning(
            "%d files permanently failed (max retries exceeded)",
            permanently_failed,
        )
    
    logger.info(
        "Sweep complete: %d stale reset, %d new files, %d retries, %d permanently failed",
        stale_count, new_files, retry_count, permanently_failed,
    )
