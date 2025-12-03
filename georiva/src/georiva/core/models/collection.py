from django.db import models
from django_extensions.db.models import TimeStampedModel
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import (
    FieldPanel,
    InlinePanel,
    MultiFieldPanel
)


class Collection(TimeStampedModel, ClusterableModel):
    """
    A data source that produces multiple datasets.
    
    Examples: GFS Forecast, CHIRPS Rainfall, ERA5 Reanalysis
    
    This is an organizational grouping - it defines how data is ingested
    """
    slug = models.SlugField(max_length=100, unique=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    
    # Provider information
    provider = models.CharField(
        max_length=255,
        blank=True,
        help_text="Data provider, e.g., NOAA, UCSB, ECMWF"
    )
    provider_url = models.URLField(blank=True)
    license = models.CharField(max_length=255, blank=True)
    
    # Source file format
    class FileFormat(models.TextChoices):
        GRIB = 'grib', 'GRIB/GRIB2'
        NETCDF = 'netcdf', 'NetCDF'
        GEOTIFF = 'geotiff', 'GeoTIFF'
        ZARR = 'zarr', 'ZARR'
    
    file_format = models.CharField(
        max_length=20,
        choices=FileFormat.choices
    )
    
    # Archive configuration
    archive_source_files = models.BooleanField(
        default=True,
        help_text="Whether to archive source files after processing"
    )
    
    # Status
    is_active = models.BooleanField(default=True)
    
    class Meta:
        ordering = ['name']
    
    def __str__(self):
        return self.name
    
    panels = [
        MultiFieldPanel([
            FieldPanel('id'),
            FieldPanel('name'),
            FieldPanel('description'),
        ], heading="Basic Information"),
        MultiFieldPanel([
            FieldPanel('provider'),
            FieldPanel('provider_url'),
            FieldPanel('license'),
        ], heading="Provider"),
        MultiFieldPanel([
            FieldPanel('file_format'),
            FieldPanel('archive_source_files'),
        ], heading="Ingestion Configuration"),
        FieldPanel('is_active'),
        InlinePanel('datasets', label="Datasets"),
    ]
