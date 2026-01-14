from django.apps import AppConfig


class StacConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.stac'
    label = 'georivastac'
    verbose_name = 'GeoRIVA STAC'
