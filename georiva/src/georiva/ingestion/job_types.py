"""
GeoRiva FileIngestionJobType — wraps IngestionService.process_file() so that every
file ingestion run is tracked as a task-ferry Job with real-time progress.

FileIngestion owns distributed locking and retry logic.
FileIngestionJob is the operator-visible layer on top.
"""

import logging

from task_ferry.registry import JobType

from .models import FileIngestion, FileIngestionJob

logger = logging.getLogger(__name__)


def _ingest_again_if_rewritten(bucket: str, file_path: str, *, locked_at) -> None:
    """Queue ``file_path`` again when a sender wrote its key while it was being ingested (#548).

    The write's notification found the key locked and started nothing, though its delivery
    was accepted. Whatever this run's outcome, that newer write is still in ``incoming``
    (the cleanup deletes only the write it read), so it is ingested next, in its own chain.
    A write landing before the run read the file was ingested and deleted with it, so an
    empty key queues nothing. A repeated notification for the write being read records no
    delivery, so starts nothing either.
    """
    from georiva.core.storage import BucketType, storage
    from georiva.events.context import use_context
    from georiva.ingestion.chains import context_for_arrival
    from georiva.ingestion.models import Delivery
    from georiva.ingestion.tasks import process_incoming_file

    if bucket != BucketType.INCOMING or locked_at is None:
        return
    if not Delivery.accepted_since(file_path, locked_at) or not storage.incoming.exists(file_path):
        return
    if not FileIngestion.reset_for_reingest(bucket, file_path):
        return

    logger.info("Written again while it was ingested, ingesting the newer write: %s", file_path)
    log = FileIngestion.objects.get(bucket=bucket, file_path=file_path)
    with use_context(context_for_arrival(file_path, actor="system:ingestion")):
        process_incoming_file.delay(
            file_path=file_path,
            origin_bucket=bucket,
            reference_time=log.reference_time.isoformat() if log.reference_time else None,
        )


class FileIngestionJobType(JobType):
    type = "file_ingestion"
    model_class = FileIngestionJob
    max_count = 50

    def prepare_values(self, values: dict, user) -> dict:
        for field in ("file_path", "bucket"):
            if not values.get(field):
                raise ValueError(f"'{field}' is required to start an ingestion job.")
        return {
            "file_path": values["file_path"],
            "bucket": values["bucket"],
        }

    def run(self, job: FileIngestionJob, progress) -> None:
        worker_id = f"task-ferry-job-{job.id}"

        progress.increment(5, state="Acquiring processing lock…")

        if not FileIngestion.acquire(job.bucket, job.file_path, worker_id):
            progress.increment(95, state="Skipped — already being processed")
            logger.info(
                "FileIngestionJob %d: skipping %s/%s — lock not acquired",
                job.id,
                job.bucket,
                job.file_path,
            )
            return

        fi = None
        try:
            fi = FileIngestion.objects.get(bucket=job.bucket, file_path=job.file_path)
            job.file_ingestion = fi
            job.save(update_fields=["file_ingestion"])
        except FileIngestion.DoesNotExist:
            pass

        try:
            self._ingest(job, progress)
        except Exception as exc:
            # Released here rather than in on_error, which runs after the check below:
            # a write waiting on this lock must find it free.
            FileIngestion.fail_if_held_by(job.bucket, job.file_path, worker_id, str(exc))
            raise
        finally:
            _ingest_again_if_rewritten(job.bucket, job.file_path, locked_at=fi.locked_at if fi else None)

    def _ingest(self, job: FileIngestionJob, progress) -> None:
        """Run the pipeline on a file this job holds the lock for, and record how it ended."""
        from georiva.ingestion.service import IngestionService

        progress.increment(10, state="Lock acquired — starting ingestion")

        from georiva.ingestion.progress import PublishingProgress

        pub_progress = PublishingProgress(total=100, job_id=job.pk)
        service = IngestionService()
        result = service.process_file(
            file_path=job.file_path,
            origin_bucket=job.bucket,
            progress=pub_progress,
        )

        progress.increment(75, state="Pipeline complete")

        if result and result.success:
            FileIngestion.mark_completed(
                bucket=job.bucket,
                file_path=job.file_path,
                archive_path=result.archive_path,
                items_created=len(result.items_created),
                assets_created=len(result.assets_created),
                variables_discovered=result.variables_discovered,
                valid_time_start=result.valid_time_start,
                valid_time_end=result.valid_time_end,
                timestep_count=result.timestep_count,
            )
            job.items_created = len(result.items_created)
            job.assets_created = len(result.assets_created)
            job.save(update_fields=["items_created", "assets_created"])
            progress.increment(
                10, state=(f"Done — {len(result.items_created)} items, {len(result.assets_created)} assets created")
            )
            logger.info(
                "FileIngestionJob %d: completed %s/%s — %d items, %d assets",
                job.id,
                job.bucket,
                job.file_path,
                len(result.items_created),
                len(result.assets_created),
            )
        else:
            error_msg = "; ".join(result.errors) if result else "No result returned"
            FileIngestion.mark_failed(
                bucket=job.bucket,
                file_path=job.file_path,
                error=error_msg,
            )
            raise RuntimeError(error_msg)

    def on_error(self, job: FileIngestionJob, exc: Exception) -> None:
        logger.exception(
            "FileIngestionJob %d failed for %s/%s: %s",
            job.id,
            job.bucket,
            job.file_path,
            exc,
        )
        FileIngestion.fail_if_held_by(
            bucket=job.bucket,
            file_path=job.file_path,
            locked_by=f"task-ferry-job-{job.id}",
            error=str(exc),
        )

    def on_cancelled(self, job: FileIngestionJob) -> None:
        if job.file_ingestion_id:
            FileIngestion.mark_failed(
                bucket=job.bucket,
                file_path=job.file_path,
                error="Cancelled by operator",
            )

    def before_delete(self, job: FileIngestionJob) -> None:
        if job.file_ingestion_id:
            job.file_ingestion = None
            job.save(update_fields=["file_ingestion"])
