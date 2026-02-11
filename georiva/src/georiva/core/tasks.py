import json

from celery import shared_task
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app


@app.on_after_finalize.connect
def setup_network_plugin_processing_tasks(sender, **kwargs):
    from georiva.core.models import Catalog
    catalogs = Catalog.objects.all()
    
    for catalog in catalogs:
        create_or_update_catalog_loader_plugin_periodic_tasks(catalog)


@shared_task(bind=True, name='georiva.core.tasks.run_catalog_loader')
def run_catalog_loader(self, catalog_id):
    from georiva.core.models import Catalog
    catalog = Catalog.objects.get(id=catalog_id)
    
    loader_profile = catalog.loader_profile
    
    if not loader_profile:
        return
    
    loader = loader_profile.get_loader(catalog)
    
    result = loader.run()
    
    return result.to_dict()


def create_or_update_catalog_loader_plugin_periodic_tasks(catalog):
    is_active = catalog.is_active
    
    sig = run_catalog_loader.s(catalog.id)
    name = repr(sig)
    
    options = {
        'task': sig.name,
        'enabled': is_active,
        'args': json.dumps([catalog.id]),
        'interval': None,
    }
    
    loader_profile = catalog.loader_profile
    if loader_profile:
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=loader_profile.interval_minutes,
            period=IntervalSchedule.MINUTES,
        )
        options['interval'] = schedule
    else:
        options['enabled'] = False
    
    # Create or update the periodic task
    PeriodicTask.objects.update_or_create(name=name, defaults=options)


def update_catalog_loader_plugin_periodic_task(sender, instance, **kwargs):
    create_or_update_catalog_loader_plugin_periodic_tasks(instance)
