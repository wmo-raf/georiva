from django.apps import AppConfig


class HarvestConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "georiva.harvest"
    label = "georivaharvest"
    verbose_name = "GeoRIVA Harvest"
