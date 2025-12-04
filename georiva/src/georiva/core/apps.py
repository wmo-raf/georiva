from django.apps import AppConfig


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"
