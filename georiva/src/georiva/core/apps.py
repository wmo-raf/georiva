from django.apps import AppConfig
from django.db.models.signals import post_save


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"
    
    def ready(self):
        from .models import Catalog
        from .tasks import update_catalog_loader_plugin_periodic_task
        
        # update plugin periodic task when a network connection plugin is saved
        catalogs = Catalog.objects.all()
        for catalog in catalogs:
            post_save.connect(update_catalog_loader_plugin_periodic_task, sender=Catalog)
