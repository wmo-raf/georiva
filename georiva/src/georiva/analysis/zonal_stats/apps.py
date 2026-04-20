from django.apps import AppConfig


class ZonalStatsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.analysis.zonal_stats'
    label = 'georiva_analysis_zonal_stats'
    verbose_name = 'GeoRIVA Zonal Stats'
