from django.db import models
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import (
    FieldPanel,
    MultiFieldPanel
)
from wagtail.snippets.models import register_snippet


@register_snippet
class Catalog(TimeStampedModel):
    """
    A data source that produces multiple collections.
    
    Examples: GFS, CHIRPS, ERA5, MSG
    
    This is an organizational grouping - it defines how data is ingested
    """
    name = models.CharField(max_length=255)
    slug = models.SlugField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    
    # Ingestion configuration
    data_source = models.ForeignKey(
        "georivaloaders.LoaderConfig",  # or rename to DataSource
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    
    # Provider information
    provider = models.CharField(max_length=255, blank=True)
    provider_url = models.URLField(blank=True)
    license = models.CharField(max_length=255, blank=True)
    
    # Source file format
    class FileFormat(models.TextChoices):
        GRIB = 'grib', 'GRIB/GRIB2'
        NETCDF = 'netcdf', 'NetCDF'
        GEOTIFF = 'geotiff', 'GeoTIFF'
        ZARR = 'zarr', 'ZARR'
    
    file_format = models.CharField(max_length=20, choices=FileFormat.choices)
    archive_source_files = models.BooleanField(default=True)
    is_active = models.BooleanField(default=True)
    boundary = models.ForeignKey("adminboundarymanager.AdminBoundary", on_delete=models.SET_NULL, null=True,
                                 blank=True, help_text="Optional boundary to clip data to")
    
    class Meta:
        ordering = ['name']
        verbose_name_plural = 'Catalogs'
    
    def __str__(self):
        return self.name
    
    panels = [
        MultiFieldPanel([
            FieldPanel('name'),
            FieldPanel('slug'),
            FieldPanel('description'),
        ], heading="Basic Information"),
        MultiFieldPanel([
            FieldPanel('provider'),
            FieldPanel('provider_url'),
            FieldPanel('license'),
        ], heading="Provider"),
        MultiFieldPanel([
            FieldPanel('data_source'),
            FieldPanel('file_format'),
            FieldPanel('archive_source_files'),
            FieldPanel('boundary'),
        ], heading="Ingestion Configuration"),
        FieldPanel('is_active'),
    ]
