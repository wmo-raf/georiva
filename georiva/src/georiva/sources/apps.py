from django.apps import AppConfig


class SourcesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.sources'
    label = 'georivasources'
    verbose_name = "GeoRIVA Sources"

    def ready(self):
        from task_ferry.registry import job_type_registry
        from .job_types import DataFeedJobType

        job_type_registry.register(DataFeedJobType())
