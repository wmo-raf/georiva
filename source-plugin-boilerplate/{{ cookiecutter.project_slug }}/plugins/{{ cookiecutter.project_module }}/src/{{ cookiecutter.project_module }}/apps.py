from django.apps import AppConfig

from georiva.sources.registry import data_source_registry


class PluginNameConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = "{{ cookiecutter.project_module }}"
    
    def ready(self):
        from .source import SourceNamePlugin
        
        data_source_registry.register(SourceNamePlugin())
