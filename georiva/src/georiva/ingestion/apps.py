from django.apps import AppConfig


class IngestionConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.ingestion'
    label = 'georivaingestion'
    verbose_name = "GeoRIVA Ingestion"

    def ready(self):
        from task_ferry.registry import job_type_registry
        from .job_types import FileIngestionJobType

        job_type_registry.register(FileIngestionJobType())

        import georiva.ingestion.signals  # noqa: F401 — registers signal handlers
