from django.apps import AppConfig


class EcmwfAifsSourceConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.ecmwf_aifs_source'
    
    def ready(self):
        from georiva.sources.registry import data_source_registry
        from .source import ECMWFAIFSDataSource
        
        data_source_registry.register(ECMWFAIFSDataSource)
