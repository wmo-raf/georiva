import logging

from celery import shared_task
from django.utils import timezone as dj_timezone
from django_celery_beat.models import IntervalSchedule

from georiva.config.celery import app
from georiva.core.models import Collection, Item
from georiva.events.tasks import EntryTask, ReadSideTask
from georiva.utils.periodic import PeriodicSpec, register_periodic_tasks

logger = logging.getLogger(__name__)


@shared_task(
    name="georiva.core.tasks.prune_forecast_items",
    base=EntryTask,
    queue="georiva-default",
)
def prune_forecast_items():
    now = dj_timezone.now()
    total_deleted = 0

    collections = Collection.objects.filter(
        is_forecast=True,
        retain_past_forecasts=False,
        is_active=True,
    )

    for collection in collections:
        # 1. Prune items whose valid_time is in the past
        past_count, _ = Item.objects.filter(
            collection=collection,
            time__lt=now,
        ).delete()
        total_deleted += past_count

        # 2. Prune items from stale forecast runs (independent of valid_time)
        if collection.retain_latest_run_only:
            latest_ref = (
                Item.objects.filter(
                    collection=collection,
                    reference_time__isnull=False,
                )
                .order_by("-reference_time")
                .values_list("reference_time", flat=True)
                .first()
            )
            if latest_ref:
                stale_count, _ = Item.objects.filter(
                    collection=collection,
                    reference_time__isnull=False,
                    reference_time__lt=latest_ref,
                ).delete()
                total_deleted += stale_count

    logger.info("prune_forecast_items: removed %d items", total_deleted)
    return f"Pruned {total_deleted} past forecast items"


@app.task(
    name="georiva.core.tasks.delete_asset_objects",
    base=ReadSideTask,
    bind=True,
    acks_late=True,
    max_retries=3,
    # Nothing a reader observes waits on freeing storage: the rows are already
    # gone. Idempotent — deleting a missing key is a success, and the guards
    # re-run on every attempt — so a retry never deletes what came back.
    queue="georiva-default",
)
def delete_asset_objects(self, hrefs: list[str], deleted_at: str) -> dict:
    """Delete the assets-bucket objects of Asset rows whose delete committed at
    ``deleted_at`` (ADR 0032). One chunk of ``asset_deletion.CHUNK_SIZE`` keys."""
    from datetime import datetime

    from georiva.core.storage.asset_deletion import delete_objects

    try:
        summary = delete_objects(hrefs, datetime.fromisoformat(deleted_at))
        if hrefs and summary["failed"] == len(hrefs):
            raise RuntimeError(f"every one of {len(hrefs)} deletions failed; storage looks unreachable")
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            logger.error(
                "delete_asset_objects: gave up on %d object(s) after %d retries (%s); "
                "run cleanup_asset_orphans to reclaim them",
                len(hrefs),
                self.max_retries,
                exc,
            )
            raise
        raise self.retry(exc=exc, countdown=30 * 2**self.request.retries) from exc

    if summary["failed"]:
        logger.error(
            "delete_asset_objects: %d object(s) could not be deleted; run cleanup_asset_orphans to reclaim them",
            summary["failed"],
        )
    logger.info("delete_asset_objects: %s", summary)
    return summary


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    register_periodic_tasks(
        [
            PeriodicSpec(
                "georiva.core.tasks.prune_forecast_items",
                "georiva.core.tasks.prune_forecast_items",
                60,
                IntervalSchedule.MINUTES,
            )
        ]
    )
