from django.apps import AppConfig


class ChirpsDataSourceConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.sample_plugins.chirps'
    
    def ready(self):
        from georiva.sources.registry import data_source_registry
        from .source import CHIRPSDataSource
        
        data_source_registry.register(CHIRPSDataSource)
