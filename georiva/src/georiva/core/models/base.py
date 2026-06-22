"""
Abstract STAC base models shared by the two data tiers.

The data layer has two tiers that mirror the same STAC spec but play different
roles (see docs/adr/0004-staging-tier-and-abstract-stac-models.md):

- Published — the concrete ``Collection`` / ``Item`` / ``Asset`` in this app
  (product-grained, served, ``Item`` is a TimescaleDB hypertable).
- Staging — ``StagingCollection`` / ``StagingItem`` / ``StagingAsset`` in the
  ``staging`` app (source-grained, not served, no hypertable).

These abstract bases hold only the **non-relational** fields the two tiers
share. The relational fields differ per tier (different ``collection`` targets,
different ``item`` parents, different ``variable`` reverse accessors) and so
stay on the concrete models, as do tier-specific ``Meta``, panels, and methods.
"""

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils.translation import gettext_lazy as _


class AbstractCollection(models.Model):
    """Non-relational fields shared by Published and Staging collections."""
    
    slug = models.SlugField(max_length=100)
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    
    # Spatial extent (auto-updated)
    bounds = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True,
    )
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    class Meta:
        abstract = True
    
    def __str__(self):
        return self.slug


class AbstractSpatialItem(models.Model):
    """Non-relational spatial/raster fields shared by Published and Staging items."""
    
    # Source tracking — convention: "{bucket}:{file_path}".
    source_file = models.CharField(
        max_length=500,
        blank=True,
        db_index=True,
        help_text=_("Original source file path"),
    )
    
    # Spatial metadata
    bounds = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True,
        help_text=_("Bounding box [west, south, east, north]"),
    )
    geometry = models.JSONField(
        null=True,
        blank=True,
        help_text=_("GeoJSON geometry for non-rectangular footprints"),
    )
    
    # Raster dimensions
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    resolution_x = models.FloatField(null=True, blank=True)
    resolution_y = models.FloatField(null=True, blank=True)
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    # Flexible metadata
    properties = models.JSONField(default=dict, blank=True)
    
    class Meta:
        abstract = True


class AbstractAsset(models.Model):
    """
    Non-relational fields shared by Published and Staging assets.

    The ``item`` parent and ``variable`` FK differ per tier (different parent
    models, different reverse accessors) and stay on the concrete models.
    """
    
    class Role(models.TextChoices):
        DATA = 'data', _('Data')
        VISUAL = 'visual', _('Visual')
        THUMBNAIL = 'thumbnail', _('Thumbnail')
        OVERVIEW = 'overview', _('Overview')
        METADATA = 'metadata', _('Metadata')
        SOURCE = 'source', _('Source')  # raw acquisition artifact (staging)
    
    class Format(models.TextChoices):
        COG = 'cog', _('Cloud-Optimized GeoTIFF')
        ZARR = 'zarr', _('Zarr')
        GEOTIFF = 'geotiff', _('GeoTIFF')
        NETCDF = 'netcdf', _('NetCDF')
        GRIB2 = 'grib2', _('GRIB2')
        PNG = 'png', _('PNG')
        WEBP = 'webp', _('WebP')
        JPEG = 'jpeg', _('JPEG')
        JSON = 'json', _('JSON')
    
    # File location
    href = models.CharField(
        max_length=500,
        help_text=_("Storage path or URL"),
    )
    media_type = models.CharField(
        max_length=100,
        blank=True,
        help_text=_("MIME type"),
    )
    
    # Classification
    roles = ArrayField(
        models.CharField(max_length=20, choices=Role.choices),
        default=list,
    )
    format = models.CharField(
        max_length=20,
        choices=Format.choices,
        blank=True,
    )
    
    # File metadata
    file_size = models.BigIntegerField(null=True, blank=True)
    checksum = models.CharField(max_length=64, blank=True)
    
    # Raster info (if different from Item)
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    bands = models.IntegerField(null=True, blank=True, default=1)
    
    # Statistics (computed during processing)
    stats_min = models.FloatField(null=True, blank=True)
    stats_max = models.FloatField(null=True, blank=True)
    stats_mean = models.FloatField(null=True, blank=True)
    stats_std = models.FloatField(null=True, blank=True)
    
    # Format-specific fields
    extra_fields = models.JSONField(default=dict, blank=True)
    
    class Meta:
        abstract = True
