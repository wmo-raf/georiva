from django.apps import AppConfig


class FormatsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.formats'
    label = 'georivaformats'
    verbose_name = "GeoRIVA Formats"
    
    def ready(self):
        # Import format plugins to register them
        from .geotiff import GeoTIFFFormatPlugin
        from .netcdf import NetCDFFormatPlugin
        from .grib import GRIBFormatPlugin
        
        from .registry import format_registry
        
        format_registry.register(GeoTIFFFormatPlugin)
        format_registry.register(NetCDFFormatPlugin)
        format_registry.register(GRIBFormatPlugin)
