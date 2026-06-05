"""
GeoRiva DataFeedJobType — wraps Loader.run() so every data-source fetch is
tracked as a task-ferry Job with real-time per-file progress.

If collection_id is provided, only that collection is run.
If collection_id is None, all collections linked to the DataFeed are run
in sequence (enabling the cross-collection copy dedup in the Loader).
"""

import logging

from task_ferry.registry import JobType

from .models import DataFeedJob

logger = logging.getLogger(__name__)


class DataFeedJobType(JobType):
    type = "data_source_load"
    model_class = DataFeedJob
    max_count = 5

    def prepare_values(self, values: dict, user) -> dict:
        if not values.get("data_feed_id"):
            raise ValueError("'data_feed_id' is required.")
        return {
            "data_feed_id": values["data_feed_id"],
            "collection_id": values.get("collection_id"),  # None → all collections
        }

    def run(self, job: DataFeedJob, progress) -> None:
        """
        Execute Loader runs for one or all collections in the DataFeed.

        Progress budget (100 points) is split evenly across collections.
        Within each collection's slice:
          - small portion for request planning
          - rest for file fetches (one tick per file)
          - small portion for recording the DataFeedRun
        """
        from georiva.core.models import Collection
        from .loader import Loader
        from .models import DataFeed

        progress.increment(5, state="Loading data feed…")

        try:
            data_feed = DataFeed.objects.get(pk=job.data_feed_id)
        except DataFeed.DoesNotExist:
            raise ValueError(f"DataFeed {job.data_feed_id} not found.")

        job.data_feed = data_feed
        job.save(update_fields=["data_feed"])

        if job.collection_id:
            try:
                collections = [Collection.objects.get(pk=job.collection_id)]
            except Collection.DoesNotExist:
                raise ValueError(f"Collection {job.collection_id} not found.")
        else:
            collections = [
                link.collection
                for link in data_feed.collection_links.select_related('collection__catalog')
            ]

        if not collections:
            progress.increment(95, state="No collections linked to this feed")
            return

        # Remaining 95 points split evenly across collections
        per_col_budget = 95 // len(collections)

        for i, collection in enumerate(collections, 1):
            col_label = collection.name
            prefix = f"[{i}/{len(collections)}] {col_label}"

            job.collection = collection
            job.save(update_fields=["collection"])

            progress.increment(0, state=f"{prefix} — planning…")

            data_source = data_feed.get_data_source(collection=collection)
            requests = list(data_source.generate_requests_for_collection(collection))

            job.files_total += len(requests)
            job.save(update_fields=["files_total"])

            logger.info(
                "DataFeedJob %d (%s): %d files to fetch",
                job.id, col_label, len(requests),
            )

            if not requests:
                progress.increment(per_col_budget, state=f"{prefix} — no files to fetch")
                continue

            fetch_stage = progress.create_child(
                represents=per_col_budget,
                total=len(requests),
            )

            def on_file_fetched(request, fetch_result, _stage=fetch_stage, _label=col_label):
                job.files_fetched += 1
                job.bytes_transferred += fetch_result.bytes_transferred or 0
                job.save(update_fields=["files_fetched", "bytes_transferred"])
                _stage.increment(
                    state=f"[{_label}] {request.filename}"
                )

            loader = Loader(
                data_source=data_source,
                collection=collection,
                data_feed=data_feed,
                on_file_fetched=on_file_fetched,
            )
            result = loader.run()

            job.files_skipped += result.files_skipped
            job.files_failed += result.files_failed
            job.save(update_fields=["files_skipped", "files_failed"])

            data_feed.record_run(result, collection)

            logger.info(
                "DataFeedJob %d (%s): %s",
                job.id, col_label, result.summary(),
            )

            if result.files_failed > 0 and result.files_fetched == 0:
                logger.error(
                    "DataFeedJob %d (%s): all fetches failed — %s",
                    job.id, col_label, "; ".join(result.errors[:3]),
                )

    def on_error(self, job: DataFeedJob, exc: Exception) -> None:
        logger.exception(
            "DataFeedJob %d failed (data_feed=%s, collection=%s): %s",
            job.id, job.data_feed_id, job.collection_id, exc,
        )

    def before_delete(self, job: DataFeedJob) -> None:
        job.data_feed = None
        job.collection = None
        job.save(update_fields=["data_feed", "collection"])
