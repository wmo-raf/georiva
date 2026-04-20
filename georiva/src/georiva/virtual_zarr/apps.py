from django.apps import AppConfig


class VirtualZarrConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.virtual_zarr'
    label = 'georivavirtualzarr'
    
    def ready(self):
        import georiva.virtual_zarr.signals  # noqa: F401
