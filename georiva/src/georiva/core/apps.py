from django.apps import AppConfig
from django.db.models.signals import post_save


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"
    
    def ready(self):
        from .models import Collection
        from .tasks import update_collection_loader_plugin_periodic_task
        
        post_save.connect(update_collection_loader_plugin_periodic_task, sender=Collection)
