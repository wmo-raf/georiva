import logging

from celery import shared_task
from django.utils import timezone as dj_timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Collection, Item

logger = logging.getLogger(__name__)


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
