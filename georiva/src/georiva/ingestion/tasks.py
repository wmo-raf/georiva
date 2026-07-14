"""
GeoRiva Ingestion Celery Tasks
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from django.utils import timezone as dj_timezone
from django_celery_beat.models import PeriodicTask, IntervalSchedule

from georiva.config.celery import app
from georiva.core.filename import validate_path
from georiva.core.storage import storage, BucketType
from georiva.ingestion.models import FileIngestion

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.ingestion.tasks.process_incoming_file",
    bind=True,
    max_retries=0,
    acks_late=True,
    queue="georiva-ingestion",
)
def process_incoming_file(
        self,
        file_path: str,
        origin_bucket: str,
        reference_time: str = None,  # kept for backwards compat; IngestionService resolves it
        job_id: int = None,
):
    """
    Process a single incoming file.

    Creates an FileIngestionJob for operator visibility and real-time progress
    tracking, then runs it synchronously inside this worker (no re-enqueue).

    The FileIngestionJobType handles:
      - Acquiring the FileIngestion distributed lock
      - Running IngestionService.process_file()
      - Marking the FileIngestion completed or failed
    """
    from django.contrib.contenttypes.models import ContentType
    
    from task_ferry.handler import JobHandler
    
    from georiva.ingestion.models import FileIngestionJob
    
    logger.info("process_incoming_file: %s/%s", origin_bucket, file_path)

    if job_id is not None:
        job = FileIngestionJob.objects.get(pk=job_id)
    else:
        ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
        job = FileIngestionJob.objects.create(
            user=None,
            content_type=ct,
            file_path=file_path,
            bucket=origin_bucket,
        )
    
    # Run in-place — we are already inside a Celery worker, so bypass the
    # executor and call JobHandler.run() directly.  This gives us the full
    # state machine (pending → started → finished/failed) and Redis progress
    # without spawning a second task.
    try:
        JobHandler.run(job)
    except Exception:
        # JobHandler.run() already marked the job failed and re-raised.
        # Let the exception propagate so Celery records task failure too.
        raise


@app.task(
    name="georiva.ingestion.tasks.process_staging_file",
    bind=True,
    max_retries=0,
    acks_late=True,
    queue="georiva-ingestion",
)
def process_staging_file(self, bucket: str, key: str):
    """
    Register a raw file landed in the STAGING bucket as one StagingItem.

    Store-only path: this does NOT materialize any served layers — it holds
    the file as a raw input for later derivation, then fires the staging event
    so derivation recipes that consume this input are triggered.
    """
    from georiva.ingestion.staging_consumer import register_staging_file

    item = register_staging_file(bucket=bucket, key=key)
    if item is not None:
        # Product-driven invocation (ADR-0008): route the arriving input to the
        # enabled DerivedProducts that consume it, not to every recipe.
        from georiva.processing.invocation import staging_item_trigger
        from georiva.sources.derivation_invocation import dispatch_for_input

        dispatch_for_input(staging_item_trigger(item))


@app.task(name="georiva.ingestion.tasks.sweep_unprocessed", queue="georiva-default")
def sweep_unprocessed(sync: bool = False):
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
    
    stale_count = FileIngestion.reset_stale_locks()
    if stale_count:
        logger.warning("Reset %d stale locks", stale_count)
    
    # -----------------------------------------------------------------
    # Phase 2: Scan buckets for untracked files
    # -----------------------------------------------------------------
    
    new_files = 0

    dispatch = process_incoming_file.delay if not sync else process_incoming_file.run

    from georiva.ingestion.unprocessed import find_unprocessed

    for unprocessed in find_unprocessed():
        reference_time_iso = (
            unprocessed.reference_time.isoformat()
            if unprocessed.reference_time else None
        )

        if unprocessed.reason == "reingest":
            logger.warning(
                "Re-ingesting %s/%s", unprocessed.bucket, unprocessed.file_path,
            )
            FileIngestion.reset_for_reingest(unprocessed.bucket, unprocessed.file_path)
            dispatch(
                file_path=unprocessed.file_path,
                origin_bucket=unprocessed.bucket,
                reference_time=reference_time_iso,
            )
        elif unprocessed.reason == "untracked":
            logger.info(
                "Found untracked file: %s/%s",
                unprocessed.bucket, unprocessed.file_path,
            )
            FileIngestion.register(
                bucket=unprocessed.bucket,
                file_path=unprocessed.file_path,
                reference_time=unprocessed.reference_time,
            )
            dispatch(
                file_path=unprocessed.file_path,
                origin_bucket=unprocessed.bucket,
                reference_time=reference_time_iso,
            )
            new_files += 1
        # 'pending' records already await a dispatched task — the sweep
        # leaves them alone (unchanged behavior).
    
    if new_files:
        logger.info("Queued %d untracked files", new_files)
    
    # -----------------------------------------------------------------
    # Phase 3: Retry failed files
    # -----------------------------------------------------------------
    
    retryable = FileIngestion.get_retryable(limit=50)
    retry_count = 0
    
    for log in retryable:
        logger.info(
            "Retrying (%d/%d): %s/%s — last error: %s",
            log.retry_count, FileIngestion.MAX_RETRIES,
            log.bucket, log.file_path,
            log.error[:100] if log.error else 'unknown',
        )
        
        dispatch(
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
    permanently_failed = FileIngestion.get_permanently_failed().count()
    if permanently_failed:
        logger.warning(
            "%d files permanently failed (max retries exceeded)",
            permanently_failed,
        )
    
    logger.info(
        "Sweep complete: %d stale reset, %d new files, %d retries, %d permanently failed",
        stale_count, new_files, retry_count, permanently_failed,
    )


@app.task(name="georiva.ingestion.tasks.sweep_staging", queue="georiva-default")
def sweep_staging(sync: bool = False):
    """
    Reconcile the STAGING bucket against StagingItems — the staging-tier mirror
    of sweep_unprocessed.

    The staging path is event-driven (MinIO PUT → Redis → register_staging_file),
    so an object that landed while the consumer was down, or that survives a DB
    reset with the bucket intact, has no StagingItem. This scans the bucket and
    registers exactly those objects (no re-download), firing derivation for each.

    An object is "known" if a StagingAsset already references its key as href.
    Returns the number of files dispatched for registration.
    """
    from georiva.staging.models import StagingAsset

    dispatch = process_staging_file.run if sync else process_staging_file.delay

    bucket = storage.bucket(BucketType.STAGING)
    known = set(StagingAsset.objects.values_list("href", flat=True))

    dispatched = 0
    for f in bucket.list_files(recursive=True):
        key = f["path"]
        name = Path(key).name
        if name.startswith(".") or name == ".keep":
            continue
        if key in known:
            continue
        try:
            validate_path(key)
        except ValueError:
            logger.debug("sweep_staging: skipping non-conforming path: %s", key)
            continue
        dispatch(bucket=BucketType.STAGING, key=key)
        dispatched += 1

    logger.info("sweep_staging: dispatched %d new staging file(s)", dispatched)
    return dispatched


@app.task(name="georiva.ingestion.tasks.cleanup_archives", queue="georiva-default")
def cleanup_archives(max_age_days: int = 5):
    from georiva.core.storage import storage, BucketType
    from georiva.ingestion.models import FileIngestion
    
    cutoff = dj_timezone.now() - timedelta(days=max_age_days)
    archive = storage.bucket(BucketType.ARCHIVE)
    
    ingestion_logs = FileIngestion.objects.filter(
        status=FileIngestion.Status.COMPLETED,
        completed_at__lt=cutoff,
    ).exclude(archive_path='')
    
    deleted, failed = 0, 0
    
    for log in ingestion_logs.iterator():
        try:
            archive.delete(log.archive_path)
            log.archive_path = ''
            log.save(update_fields=['archive_path'])
            deleted += 1
        except Exception as e:
            logger.warning("Failed to delete archive %s: %s", log.archive_path, e)
            failed += 1
    
    logger.info("Archive cleanup: deleted=%d failed=%d", deleted, failed)
    return {"deleted": deleted, "failed": failed}


@app.task(name="georiva.ingestion.tasks.prune_ingestion_logs", queue="georiva-default")
def prune_ingestion_logs(max_age_days: int = 30):
    from georiva.ingestion.models import FileIngestion
    result = FileIngestion.prune_old_records(max_age_days)
    logger.info("Ingestion log pruned: %s", result)
    return result


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    try:
        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        schedule_1day, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.DAYS
        )
        schedule_7days, _ = IntervalSchedule.objects.get_or_create(
            every=7, period=IntervalSchedule.DAYS
        )
        
        PeriodicTask.objects.update_or_create(
            name="georiva.ingestion.sweep_unprocessed",
            defaults={
                "task": "georiva.ingestion.tasks.sweep_unprocessed",
                "interval": schedule_5min,
                "enabled": True,
            }
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.ingestion.cleanup_archives",
            defaults={
                "task": "georiva.ingestion.tasks.cleanup_archives",
                "interval": schedule_1day,
                "enabled": True,
            }
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.ingestion.prune_ingestion_logs",
            defaults={
                "task": "georiva.ingestion.tasks.prune_ingestion_logs",
                "interval": schedule_7days,
                "enabled": True,
            }
        )
    except Exception as e:
        logger.warning("Could not register periodic tasks: %s", e)
