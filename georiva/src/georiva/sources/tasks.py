import json
import logging

from celery import shared_task
from django.db.models import Min
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.events.tasks import ChainedTask, EntryTask, SweepTask
from georiva.utils.periodic import PeriodicSpec, interval_schedule, register_periodic_tasks

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.sources.tasks.sweep_scheduled_products",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_scheduled_products():
    """
    The scheduled-product beat (ADR-0008): fire every enabled scheduled
    DerivedProduct whose interval has elapsed, via the product-driven path. Runs
    on a short fixed cadence; each product's own is_due() gates its interval
    (mirrors sweep_derivations + the feed scheduler).
    """
    from georiva.sources.derivation_invocation import dispatch_due_scheduled_products

    return dispatch_due_scheduled_products()


@app.task(
    name="georiva.sources.tasks.sweep_stale_fetch_runs",
    base=SweepTask,
    sweep_name="stale_fetch_runs",
    queue="georiva-default",
)
def sweep_stale_fetch_runs():
    """
    Crash-recovery sweep: mark FetchRuns stuck in RUNNING past the staleness
    threshold as INTERRUPTED, reap wedged LoaderJobs, and auto-resume (capped).
    See acquisition_recovery for the full policy.
    """
    from georiva.sources.acquisition_recovery import sweep_stale_fetch_runs

    return sweep_stale_fetch_runs()


@app.task(
    name="georiva.sources.tasks.retry_fetched_file",
    base=ChainedTask,
    queue="georiva-ingestion",
    bind=True,
    acks_late=True,
    max_retries=0,
)
def retry_fetched_file(self, fetched_file_id):
    """
    Per-file re-fetch (PRD #217): re-fetch one FetchedFile in place. An
    impossible retry (record gone, no stored request) is logged and dropped —
    max_retries=0 per the ingestion-task convention.
    """
    from georiva.sources.acquisition_retry import RetryNotPossible, retry_fetch
    from georiva.sources.models import FetchedFile

    try:
        fetched_file = FetchedFile.objects.get(pk=fetched_file_id)
    except FetchedFile.DoesNotExist:
        logger.warning("retry_fetched_file: record %s gone", fetched_file_id)
        return

    try:
        retry_fetch(fetched_file)
    except RetryNotPossible as exc:
        logger.warning("retry_fetched_file: %s not retried: %s", fetched_file_id, exc)


@app.on_after_finalize.connect
def setup_scheduled_product_beat(sender, **kwargs):
    """Register the periodic scheduled-product beat (mirror of the derivation sweep)."""
    register_periodic_tasks(
        [
            PeriodicSpec(
                "georiva.sources.sweep_scheduled_products",
                "georiva.sources.tasks.sweep_scheduled_products",
                5,
                IntervalSchedule.MINUTES,
            )
        ],
        # DB may be unavailable at import/finalize time
        log=logger.debug,
    )


@app.on_after_finalize.connect
def setup_stale_fetch_run_sweep_beat(sender, **kwargs):
    """Register the crash-recovery sweep (mirror of the scheduled-product beat)."""
    register_periodic_tasks(
        [
            PeriodicSpec(
                "georiva.sources.sweep_stale_fetch_runs",
                "georiva.sources.tasks.sweep_stale_fetch_runs",
                30,
                IntervalSchedule.MINUTES,
            )
        ],
        # DB may be unavailable at import/finalize time
        log=logger.debug,
    )


@shared_task(
    bind=True,
    name="georiva.sources.tasks.run_data_feed_loader",
    base=EntryTask,
    queue="georiva-ingestion",
)
def run_data_feed_loader(self, data_feed_id):
    """
    Run the Loader for all due collections linked to a DataFeed in one task.

    Each collection link carries its own interval_minutes and last_run_at, so
    different collections in the same feed can run at different cadences.  The
    task fires at the feed's global interval (the shortest cadence), and
    link.is_due() gates each individual collection.

    Running all collections sequentially keeps cross-collection copy dedup in
    Loader._find_existing_catalog_path() working correctly.
    """
    from georiva.events.context import update_context
    from georiva.sources.models import DataFeed, DataFeedCollectionLink

    data_feed = DataFeed.objects.get(pk=data_feed_id)

    if not data_feed.is_active:
        return

    # An entry point: the chain this fetch starts belongs to the feed's organisation.
    update_context(organisation_id=data_feed.catalog.organisation_id)

    links = DataFeedCollectionLink.objects.filter(data_feed=data_feed).select_related("collection", "data_feed")

    results = []
    for link in links:
        real_link = link.get_real_instance()
        if not real_link.is_due():
            logger.debug("Skipping collection %s (not due yet)", real_link.collection.slug)
            continue

        collection = real_link.collection
        loader = data_feed.get_loader(collection)
        result = loader.run()
        results.append(result.to_dict())

    return results


def create_or_update_data_feed_periodic_task(data_feed):
    """Create/update one PeriodicTask per DataFeed (not per collection)."""
    name = f"georiva.sources.tasks.run_data_feed_loader:{data_feed.pk}"

    options = {
        "task": run_data_feed_loader.name,
        "enabled": False,
        "args": json.dumps([data_feed.pk]),
        "interval": None,
    }

    data_feed_collections = (
        data_feed.get_collection_link_model().objects.filter(data_feed=data_feed).select_related("collection")
    )

    if data_feed_collections.exists():
        # Fire at the shortest effective interval so no collection is starved.
        # Per-collection overrides can be shorter than the feed's global interval.
        min_override = data_feed_collections.aggregate(m=Min("interval_minutes"))["m"]
        min_interval = min(x for x in [data_feed.interval_minutes, min_override] if x is not None)

        options["interval"] = interval_schedule(min_interval, IntervalSchedule.MINUTES)

        if data_feed.is_active:
            options["enabled"] = True

    if options.get("interval"):
        PeriodicTask.objects.update_or_create(name=name, defaults=options)


def update_collection_data_feed_periodic_task(sender, instance, **kwargs):
    from georiva.core.models import Collection
    from georiva.sources.models import DataFeed

    if isinstance(instance, Collection):
        for link in instance.feed_links.select_related("data_feed"):
            create_or_update_data_feed_periodic_task(link.data_feed)
    elif isinstance(instance, DataFeed):
        create_or_update_data_feed_periodic_task(instance)


def update_link_data_feed_periodic_task(sender, instance, **kwargs):
    """Recalculate the PeriodicTask interval when a collection link is saved or deleted."""
    from georiva.sources.models import DataFeed

    try:
        data_feed = DataFeed.objects.get(pk=instance.data_feed_id)
    except DataFeed.DoesNotExist:
        return  # parent DataFeed is being cascade-deleted; nothing to update
    create_or_update_data_feed_periodic_task(data_feed)
