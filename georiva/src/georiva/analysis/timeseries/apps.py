from django.apps import AppConfig


class TimeseriesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.analysis.timeseries'
    label = 'georiva_analysis_timeseries'
    verbose_name = "GeoRIVA Timeseries Analysis"
