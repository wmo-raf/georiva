import json
import logging

from celery import shared_task
from django.utils import timezone as dj_timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Collection, Item
from georiva.sources.models import DataFeed

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name='georiva.core.tasks.run_data_feed_loader',
    queue="georiva-ingestion",
)
def run_data_feed_loader(self, data_feed_id):
    """
    Run the Loader for all collections linked to a DataFeed in one task.

    Running all collections sequentially in the same task means each file is
    still in PENDING state when the next collection's Loader runs — enabling
    the cross-collection copy dedup in Loader._find_existing_catalog_path().
    """
    data_feed = DataFeed.objects.get(pk=data_feed_id)

    if not data_feed.is_active:
        return

    collections = list(data_feed.collections.all())
    results = []

    for collection in collections:
        loader = data_feed.get_loader(collection)
        result = loader.run()
        data_feed.record_run(result, collection)
        results.append(result.to_dict())

    return results



def create_or_update_data_feed_periodic_task(data_feed):
    """Create/update one PeriodicTask per DataFeed (not per collection)."""
    name = f"georiva.core.tasks.run_data_feed_loader:{data_feed.pk}"

    options = {
        'task': run_data_feed_loader.name,
        'enabled': False,
        'args': json.dumps([data_feed.pk]),
        'interval': None,
    }

    if data_feed.collections.exists():
        schedule = (
            IntervalSchedule.objects
            .filter(every=data_feed.interval_minutes, period=IntervalSchedule.MINUTES)
            .first()
        )
        if schedule is None:
            schedule = IntervalSchedule.objects.create(
                every=data_feed.interval_minutes,
                period=IntervalSchedule.MINUTES,
            )
        options['interval'] = schedule

        if data_feed.is_active:
            options['enabled'] = True

    if options.get("interval"):
        PeriodicTask.objects.update_or_create(name=name, defaults=options)


def update_collection_data_feed_periodic_task(sender, instance, **kwargs):
    if isinstance(instance, Collection):
        for feed in instance.data_feeds.all():
            create_or_update_data_feed_periodic_task(feed)
    elif isinstance(instance, DataFeed):
        create_or_update_data_feed_periodic_task(instance)
    else:
        return


@shared_task(
    name='georiva.core.tasks.prune_forecast_items',
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
                .order_by('-reference_time')
                .values_list('reference_time', flat=True)
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


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    try:
        from georiva.core.models import Collection
        for feed in DataFeed.objects.filter(is_active=True, collections__isnull=False).distinct():
            create_or_update_data_feed_periodic_task(feed)
    except Exception as e:
        logger.warning("Could not register collection loader plugin periodic tasks: %s", e)
    
    try:
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=60,
            period=IntervalSchedule.MINUTES,
        )
        PeriodicTask.objects.update_or_create(
            name='georiva.core.tasks.prune_forecast_items',
            defaults={
                'task': 'georiva.core.tasks.prune_forecast_items',
                'interval': schedule,
                'enabled': True,
            }
        )
    except Exception as e:
        logger.warning("Could not register prune_forecast_items periodic task: %s", e)
