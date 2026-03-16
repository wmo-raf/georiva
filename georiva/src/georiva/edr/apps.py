from django.apps import AppConfig


class EDRConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.edr'
    label = 'georivaedr'
    verbose_name = 'GeoRiva EDR'
