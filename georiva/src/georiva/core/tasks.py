import json
import logging
from datetime import timedelta

from celery import shared_task

from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Collection
from georiva.sources.models import LoaderProfile

logger = logging.getLogger(__name__)


@app.on_after_finalize.connect
def setup_network_plugin_processing_tasks(sender, **kwargs):
    try:
        from georiva.core.models import Collection
        collections = Collection.objects.filter(loader_profile__isnull=False)
        
        for collection in collections:
            create_or_update_collection_loader_plugin_periodic_tasks(collection)
    except Exception as e:
        logger.warning("Could not register collection loader plugin periodic tasks: %s", e)


@shared_task(
    bind=True,
    name='georiva.core.tasks.run_collection_loader',
    queue="georiva-ingestion",
)
def run_collection_loader(self, catalog_id):
    from georiva.core.models import Collection
    collection = Collection.objects.get(id=catalog_id)
    
    if not collection.loader_profile:
        return
    
    loader_profile = collection.loader_profile
    
    if not loader_profile.is_active:
        return
    
    loader = loader_profile.get_loader(collection)
    
    result = loader.run()
    
    return result.to_dict()


def create_or_update_collection_loader_plugin_periodic_tasks(collection):
    sig = run_collection_loader.s(collection.slug)
    name = repr(sig)
    
    options = {
        'task': sig.name,
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
        # Create or update the periodic task
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
