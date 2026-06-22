from django.apps import AppConfig


class ProcessingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.processing'
    label = 'georivaprocessing'
    verbose_name = "GeoRIVA Processing"

    def ready(self):
        # Register the built-in recipe families on the engine.
        from georiva.processing import recipes  # noqa: F401
