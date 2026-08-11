from django.apps import AppConfig


class WMTSConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.wmts'
    label = 'georivawmts'
    verbose_name = 'GeoRiva WMTS'
