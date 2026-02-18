"""
GeoRiva Ingestion Celery Tasks

Tasks are queued by the webhook view or the sweep task.
Each task acquires a lock via IngestionLog before processing.
"""

import logging
from datetime import datetime
from pathlib import Path

from celery import shared_task

from georiva.core.filename import validate_path
from georiva.core.storage import storage, BucketType
from georiva.ingestion.models import IngestionLog

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    max_retries=0,
    acks_late=True,
)
def process_incoming_file(
        self,
        file_path: str,
        origin_bucket: str,
        reference_time: str = None,
):
    """
    Process a single incoming file.

    1. Acquire lock via IngestionLog
    2. Run ingestion (service resolves catalog/collection from path)
    3. Mark completed or failed
    """
    from georiva.ingestion.models import IngestionLog
    from georiva.ingestion.service import IngestionService
    
    worker_id = f"celery-{self.request.id or 'unknown'}"
    
    # Acquire lock — atomic, only one worker wins
    if not IngestionLog.acquire(origin_bucket, file_path, worker_id):
        logger.info(
            "Skipping %s/%s — already processing or completed",
            origin_bucket, file_path,
        )
        return
    
    logger.info(
        "Processing: %s/%s (worker=%s)",
        origin_bucket, file_path, worker_id,
    )
    
    # Parse reference_time if provided as ISO string
    ref_time = None
    if reference_time:
        try:
            ref_time = datetime.fromisoformat(reference_time)
        except (ValueError, TypeError):
            logger.warning("Invalid reference_time: %s", reference_time)
    
    # Run ingestion
    service = IngestionService()
    
    try:
        result = service.process_file(
            file_path=file_path,
            origin_bucket=origin_bucket,
            reference_time=ref_time,
        )
        
        if result and result.success:
            IngestionLog.mark_completed(
                bucket=origin_bucket,
                file_path=file_path,
                archive_path=result.archive_path,
                items_created=len(result.items_created),
                assets_created=len(result.assets_created),
            )
            logger.info(
                "Completed: %s/%s — %d items, %d assets",
                origin_bucket, file_path,
                len(result.items_created),
                len(result.assets_created),
            )
        else:
            error_msg = '; '.join(result.errors) if result else 'No result returned'
            IngestionLog.mark_failed(
                bucket=origin_bucket,
                file_path=file_path,
                error=error_msg,
            )
            logger.warning(
                "Failed: %s/%s — %s",
                origin_bucket, file_path, error_msg,
            )
    
    except Exception as e:
        IngestionLog.mark_failed(
            bucket=origin_bucket,
            file_path=file_path,
            error=str(e),
        )
        logger.exception(
            "Error processing %s/%s", origin_bucket, file_path,
        )


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
                logger.debug("Skipping known path: %s", path)
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
