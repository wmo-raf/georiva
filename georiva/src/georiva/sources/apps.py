from django.apps import AppConfig


class SourcesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.sources'
    label = 'georivasources'
    verbose_name = "GeoRIVA Sources"
