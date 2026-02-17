import json

from celery import shared_task
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app


@app.on_after_finalize.connect
def setup_network_plugin_processing_tasks(sender, **kwargs):
    from georiva.core.models import Collection
    collections = Collection.objects.filter(loader_profile__isnull=False, is_loader_active=True)
    
    for collection in collections:
        create_or_update_collection_loader_plugin_periodic_tasks(collection)


@shared_task(bind=True, name='georiva.core.tasks.run_collection_loader')
def run_collection_loader(self, catalog_id):
    from georiva.core.models import Collection
    collection = Collection.objects.get(id=catalog_id)
    
    if not collection.loader_profile or collection.is_loader_active:
        return
    
    loader_profile = collection.loader_profile
    loader = loader_profile.get_loader(collection)
    
    result = loader.run()
    
    return result.to_dict()


def create_or_update_collection_loader_plugin_periodic_tasks(collection):
    is_loader_active = collection.is_loader_active
    sig = run_collection_loader.s(collection.id)
    name = repr(sig)
    
    options = {
        'task': sig.name,
        'enabled': is_loader_active,
        'args': json.dumps([collection.id]),
        'interval': None,
    }
    
    loader_profile = collection.loader_profile
    if loader_profile and is_loader_active:
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=loader_profile.interval_minutes,
            period=IntervalSchedule.MINUTES,
        )
        options['interval'] = schedule
    else:
        options['enabled'] = False
    
    # Create or update the periodic task
    PeriodicTask.objects.update_or_create(name=name, defaults=options)


def update_collection_loader_plugin_periodic_task(sender, instance, **kwargs):
    create_or_update_collection_loader_plugin_periodic_tasks(instance)
