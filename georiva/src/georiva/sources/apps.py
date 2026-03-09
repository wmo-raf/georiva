from django.apps import AppConfig
from django.db.models.signals import post_save


class SourcesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.sources'
    label = 'georivasources'
    verbose_name = "GeoRIVA Sources"
    
    def ready(self):
        from .models import LoaderProfile
        from georiva.core.tasks import update_collection_loader_plugin_periodic_task
        
        post_save.connect(update_collection_loader_plugin_periodic_task, sender=LoaderProfile)
