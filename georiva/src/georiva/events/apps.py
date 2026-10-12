from django.apps import AppConfig


class EventsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "georiva.events"
    label = "georivaevents"
    verbose_name = "GeoRIVA Domain events"

    def ready(self):
        from .registry import populate

        populate()
