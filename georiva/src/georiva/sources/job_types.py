"""
GeoRiva LoaderJobType — wraps Loader.run() so every data-source fetch is
tracked as a task-ferry Job with real-time per-file progress.

Dispatch via JobHandler.create_and_start() from a view, management command,
or periodic Celery task.  The LoaderRun aggregate record is still created by
LoaderProfile.record_run() at the end, keeping existing reporting intact.
"""

import logging

from task_ferry.registry import JobType

from .models import LoaderJob

logger = logging.getLogger(__name__)


class LoaderJobType(JobType):
    type = "data_source_load"
    model_class = LoaderJob
    max_count = 5  # don't hammer external APIs with parallel runs

    def prepare_values(self, values: dict, user) -> dict:
        if not values.get("loader_profile_id"):
            raise ValueError("'loader_profile_id' is required.")
        if not values.get("collection_id"):
            raise ValueError("'collection_id' is required.")
        return {
            "loader_profile_id": values["loader_profile_id"],
            "collection_id": values["collection_id"],
        }

    def run(self, job: LoaderJob, progress) -> None:
        """
        Execute a full Loader run for one (profile, collection) pair.

        Progress budget (100 points):
             5 — load profile + collection from DB
             5 — generate file requests (count total)
            85 — fetch files (one tick per file via on_file_fetched callback)
             5 — record LoaderRun aggregate + update profile stats
        """
        from georiva.core.models import Collection

        from .models import LoaderProfile

        # ── Setup ─────────────────────────────────────────────────────────────
        progress.increment(5, state="Loading profile and collection…")

        try:
            profile = LoaderProfile.objects.get_real_instance(
                pk=job.loader_profile_id
            )
        except LoaderProfile.DoesNotExist:
            raise ValueError(f"LoaderProfile {job.loader_profile_id} not found.")

        try:
            collection = Collection.objects.get(pk=job.collection_id)
        except Collection.DoesNotExist:
            raise ValueError(f"Collection {job.collection_id} not found.")

        # Populate FK fields now that we have the objects.
        job.loader_profile = profile
        job.collection = collection
        job.save(update_fields=["loader_profile", "collection"])

        # ── Count expected files ───────────────────────────────────────────────
        progress.increment(5, state="Planning fetch requests…")

        data_source = profile.get_data_source()
        requests = list(data_source.generate_requests_for_collection(collection))
        job.files_total = len(requests)
        job.save(update_fields=["files_total"])

        logger.info(
            "LoaderJob %d: %d files to fetch for %s / %s",
            job.id, len(requests), profile.name, collection.slug,
        )

        if not requests:
            progress.increment(90, state="No files to fetch")
            return

        # ── Fetch ─────────────────────────────────────────────────────────────
        fetch_stage = progress.create_child(
            represents=85,
            total=len(requests),
        )

        def on_file_fetched(request, fetch_result):
            """Called by Loader after each individual file completes."""
            job.files_fetched += 1
            job.bytes_transferred += fetch_result.bytes_transferred or 0
            job.save(update_fields=["files_fetched", "bytes_transferred"])
            fetch_stage.increment(
                state=f"[{job.files_fetched}/{job.files_total}] {request.filename}"
            )

        from .loader import Loader

        loader = Loader(
            data_source=data_source,
            collection=collection,
            loader_profile=profile,
            on_file_fetched=on_file_fetched,
        )
        result = loader.run()

        job.files_skipped = result.files_skipped
        job.files_failed = result.files_failed
        job.save(update_fields=["files_skipped", "files_failed"])

        # ── Record aggregate ───────────────────────────────────────────────────
        progress.increment(5, state="Recording run statistics…")
        # profile.record_run() creates the LoaderRun row and updates the
        # profile's last_run_at / statistics — keep that behaviour intact.
        profile.record_run(result, collection)

        if result.files_failed > 0 and result.files_fetched == 0:
            raise RuntimeError(
                f"All {result.files_failed} fetch(es) failed. "
                f"Errors: {'; '.join(result.errors[:3])}"
            )

        logger.info(
            "LoaderJob %d complete: %s",
            job.id, result.summary(),
        )

    def on_error(self, job: LoaderJob, exc: Exception) -> None:
        logger.exception(
            "LoaderJob %d failed (profile=%s, collection=%s): %s",
            job.id, job.loader_profile_id, job.collection_id, exc,
        )

    def before_delete(self, job: LoaderJob) -> None:
        # Nullify FKs so the referenced LoaderProfile / Collection rows
        # aren't blocked from deletion by this job record.
        job.loader_profile = None
        job.collection = None
        job.save(update_fields=["loader_profile", "collection"])
