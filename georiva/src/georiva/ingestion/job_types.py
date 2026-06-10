"""
GeoRiva FileIngestionJobType — wraps IngestionService.process_file() so that every
file ingestion run is tracked as a task-ferry Job with real-time progress.

FileIngestion owns distributed locking and retry logic.
FileIngestionJob is the operator-visible layer on top.
"""

import logging

from task_ferry.registry import JobType
from .models import FileIngestionJob, FileIngestion

logger = logging.getLogger(__name__)


class FileIngestionJobType(JobType):
    type = "file_ingestion"
    model_class = FileIngestionJob
    max_count = 50  # many files can be in-flight in parallel

    def prepare_values(self, values: dict, user) -> dict:
        for field in ("file_path", "bucket"):
            if not values.get(field):
                raise ValueError(f"'{field}' is required to start an ingestion job.")
        return {
            "file_path": values["file_path"],
            "bucket": values["bucket"],
        }

    def run(self, job: FileIngestionJob, progress) -> None:
        """
        Execute a single file ingestion.

        Steps and their approximate share of the 100-point progress budget:
            5  — acquire FileIngestion lock
            10 — file registered / lock confirmed
            75 — IngestionService.process_file()  (via a child Progress)
            10 — mark completed / record counts
        """
        from georiva.ingestion.service import IngestionService

        worker_id = f"task-ferry-job-{job.id}"

        # ── Step 1: acquire the distributed lock ──────────────────────────────
        progress.increment(5, state="Acquiring processing lock…")

        if not FileIngestion.acquire(job.bucket, job.file_path, worker_id):
            progress.increment(95, state="Skipped — already being processed")
            logger.info(
                "FileIngestionJob %d: skipping %s/%s — lock not acquired",
                job.id, job.bucket, job.file_path,
            )
            return

        # Link the Job to the FileIngestion record now that we hold the lock.
        try:
            fi = FileIngestion.objects.get(bucket=job.bucket, file_path=job.file_path)
            job.file_ingestion = fi
            job.save(update_fields=["file_ingestion"])
        except FileIngestion.DoesNotExist:
            pass  # very unlikely; carry on without the FK

        progress.increment(10, state="Lock acquired — starting ingestion")

        # ── Step 2: run the ingestion pipeline ────────────────────────────────
        from georiva.ingestion.progress import PublishingProgress

        pub_progress = PublishingProgress(total=100)
        service = IngestionService()
        result = service.process_file(
            file_path=job.file_path,
            origin_bucket=job.bucket,
            progress=pub_progress,
        )

        progress.increment(75, state="Pipeline complete")

        # ── Step 3: record result ─────────────────────────────────────────────
        if result and result.success:
            FileIngestion.mark_completed(
                bucket=job.bucket,
                file_path=job.file_path,
                archive_path=result.archive_path,
                items_created=len(result.items_created),
                assets_created=len(result.assets_created),
            )
            job.items_created = len(result.items_created)
            job.assets_created = len(result.assets_created)
            job.save(update_fields=["items_created", "assets_created"])
            progress.increment(10, state=(
                f"Done — {len(result.items_created)} items, "
                f"{len(result.assets_created)} assets created"
            ))
            logger.info(
                "FileIngestionJob %d: completed %s/%s — %d items, %d assets",
                job.id, job.bucket, job.file_path,
                len(result.items_created), len(result.assets_created),
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
            job.id, job.bucket, job.file_path, exc,
        )

    def on_cancelled(self, job: FileIngestionJob) -> None:
        # Release the FileIngestion lock so sweep can retry the file.
        if job.file_ingestion_id:
            FileIngestion.mark_failed(
                bucket=job.bucket,
                file_path=job.file_path,
                error="Cancelled by operator",
            )

    def before_delete(self, job: FileIngestionJob) -> None:
        # Unlink before deleting — the FileIngestion record should outlive the job.
        if job.file_ingestion_id:
            job.file_ingestion = None
            job.save(update_fields=["file_ingestion"])
