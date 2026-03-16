import json
import logging

from celery import shared_task
from django.utils import timezone as dj_timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Collection, Item
from georiva.sources.models import LoaderProfile

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name='georiva.core.tasks.run_collection_loader',
    queue="georiva-ingestion",
)
def run_collection_loader(self, collection_id):
    from georiva.core.models import Collection
    collection = Collection.objects.get(id=collection_id)
    
    if not collection.loader_profile:
        return
    
    loader_profile = collection.loader_profile
    
    if not loader_profile.is_active:
        return
    
    loader = loader_profile.get_loader(collection)
    
    result = loader.run()
    
    return result.to_dict()


def create_or_update_collection_loader_plugin_periodic_tasks(collection):
    name = f"georiva.core.tasks.run_collection_loader:{collection.slug}"
    
    options = {
        'task': run_collection_loader.name,
        'enabled': False,
        'args': json.dumps([collection.id]),
        'interval': None,
    }
    
    loader_profile = collection.loader_profile
    
    if loader_profile:
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=loader_profile.interval_minutes,
            period=IntervalSchedule.MINUTES,
        )
        options['interval'] = schedule
        
        if loader_profile.is_active:
            options['enabled'] = True
    
    if options.get("interval"):
        PeriodicTask.objects.update_or_create(name=name, defaults=options)


def update_collection_loader_plugin_periodic_task(sender, instance, **kwargs):
    if isinstance(instance, Collection):
        collections = [instance]
    elif isinstance(instance, LoaderProfile):
        collections = instance.collection_set.all()
    else:
        return
    
    for collection in collections:
        create_or_update_collection_loader_plugin_periodic_tasks(collection)


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
        collections = Collection.objects.filter(loader_profile__isnull=False)
        
        for collection in collections:
            create_or_update_collection_loader_plugin_periodic_tasks(collection)
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
