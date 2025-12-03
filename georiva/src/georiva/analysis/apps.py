from django.apps import AppConfig


class AnalysisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.analysis'
    label = 'georiva_analysis'
    verbose_name = "GeoRIVA Analysis"
