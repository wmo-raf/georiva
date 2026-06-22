from django.apps import AppConfig


class StagingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.staging'
    label = 'georivastaging'
    verbose_name = "GeoRIVA Staging"
