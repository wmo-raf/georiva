from django.apps import AppConfig


class SourcesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.sources'
    label = 'georivasources'
    verbose_name = "GeoRIVA Sources"
    
    def ready(self):
        from .registry import fetch_strategy_registry
        from .fetch import HTTPFetchStrategy, FTPFetchStrategy
        
        fetch_strategy_registry.register(HTTPFetchStrategy)
        fetch_strategy_registry.register(FTPFetchStrategy)
