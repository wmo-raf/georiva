from django.apps import AppConfig


class LoadersConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.loaders'
    label = 'georivaloaders'
    verbose_name = "GeoRIVA Loaders"
