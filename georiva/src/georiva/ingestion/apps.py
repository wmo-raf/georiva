from django.apps import AppConfig


class IngestionConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.ingestion'
    label = 'georivaingestion'
    verbose_name = "GeoRIVA Ingestion"

    def ready(self):
        import georiva.ingestion.signals  # noqa: F401
