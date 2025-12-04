"""
GeoRiva Item and Asset Models

Item: TimescaleDB hypertable for time-series raster data
Asset: Individual files associated with an Item (PNG, COG, metadata, etc.)
"""

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from timescale.db.models.models import TimescaleModel
from wagtail.models import Orderable
from wagtail.snippets.models import register_snippet


@register_snippet
class Item(TimescaleModel, TimeStampedModel, ClusterableModel):
    """
    A single timestep of raster data in the system.
    
    Uses TimescaleDB hypertable for efficient time-series queries.
    The 'time' field is provided by TimescaleModel.
    """
    
    dataset = models.ForeignKey(
        'georivacore.Dataset',
        on_delete=models.CASCADE,
        related_name='items',
    )
    
    # For forecast data: when the model was run
    run_time = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Model run time for forecast data"),
    )
    
    # Source tracking
    source_file = models.CharField(
        max_length=500,
        help_text=_("Original source file path"),
    )
    
    # Spatial metadata
    bounds = ArrayField(
        models.FloatField(),
        size=4,
        help_text=_("Bounding box [minx, miny, maxx, maxy]"),
    )
    width = models.IntegerField()
    height = models.IntegerField()
    resolution_x = models.FloatField(help_text=_("Pixel size in X direction"))
    resolution_y = models.FloatField(help_text=_("Pixel size in Y direction"))
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    # Statistics (computed during processing)
    stats_min = models.FloatField(null=True, blank=True)
    stats_max = models.FloatField(null=True, blank=True)
    stats_mean = models.FloatField(null=True, blank=True)
    stats_std = models.FloatField(null=True, blank=True)
    
    # Flexible metadata
    metadata = models.JSONField(default=dict, blank=True)
    
    class Meta:
        ordering = ['-time']
        constraints = [
            models.UniqueConstraint(
                fields=['time', 'dataset'],
                name='unique_time_per_dataset'
            ),
            models.UniqueConstraint(
                fields=['time', 'dataset', 'run_time'],
                name='unique_time_dataset_runtime',
                condition=models.Q(run_time__isnull=False),
            ),
        ]
        indexes = [
            models.Index(fields=['dataset', 'time']),
            models.Index(fields=['dataset', '-time']),
            models.Index(fields=['run_time', 'time']),
        ]
    
    def __str__(self):
        return f"{self.dataset.slug} @ {self.time}"
    
    # =========================================================================
    # Asset Access Helpers
    # =========================================================================
    
    def get_asset(self, key: str) -> 'Asset':
        """Get a specific asset by key."""
        return self.assets.filter(key=key).first()
    
    @property
    def visual_asset(self) -> 'Asset':
        """Get the visual (PNG/WebP) asset."""
        return self.get_asset('visual')
    
    @property
    def data_asset(self) -> 'Asset':
        """Get the data (COG) asset."""
        return self.get_asset('data')
    
    @property
    def thumbnail_asset(self) -> 'Asset':
        """Get the thumbnail asset."""
        return self.get_asset('thumbnail')
    
    @property
    def metadata_asset(self) -> 'Asset':
        """Get the metadata (JSON) asset."""
        return self.get_asset('metadata')
    
    def get_visual_url(self) -> str:
        """Get URL to visual asset."""
        asset = self.visual_asset
        return asset.href if asset else None
    
    def get_data_url(self) -> str:
        """Get URL to data asset."""
        asset = self.data_asset
        return asset.href if asset else None


@register_snippet
class Asset(Orderable):
    """
    An individual file associated with an Item.
    
    Uses a flat structure with format enum + extra_fields for flexibility.
    Format-specific behavior is provided via properties and methods.
    """
    
    class Role(models.TextChoices):
        DATA = 'data', _('Data')  # Raw/processed data (COG, GeoTIFF)
        VISUAL = 'visual', _('Visual')  # Rendered visualization (PNG)
        THUMBNAIL = 'thumbnail', _('Thumbnail')  # Small preview
        OVERVIEW = 'overview', _('Overview')  # Reduced resolution
        METADATA = 'metadata', _('Metadata')  # Sidecar metadata (JSON)
    
    class Format(models.TextChoices):
        COG = 'cog', _('Cloud-Optimized GeoTIFF')
        GEOTIFF = 'geotiff', _('GeoTIFF')
        PNG = 'png', _('PNG')
        WEBP = 'webp', _('WebP')
        JPEG = 'jpeg', _('JPEG')
        JSON = 'json', _('JSON')
    
    class DataType(models.TextChoices):
        FLOAT32 = 'float32', _('Float32')
        FLOAT16 = 'float16', _('Float16')
        UINT16 = 'uint16', _('UInt16')
        UINT8 = 'uint8', _('UInt8 (0-255)')
        INT16 = 'int16', _('Int16')
        RGBA = 'rgba', _('RGBA Color')
    
    # Parent relationship
    item = ParentalKey(
        'Item',
        on_delete=models.CASCADE,
        related_name='assets',
        db_constraint=False,
    )
    
    # Identity
    key = models.CharField(
        max_length=100,
        help_text=_("Unique key within item: 'visual', 'data', 'thumbnail', etc."),
    )
    title = models.CharField(max_length=255, blank=True)
    description = models.TextField(blank=True)
    
    # File info
    href = models.CharField(
        max_length=500,
        help_text=_("Storage path or URL to the asset"),
    )
    media_type = models.CharField(
        max_length=100,
        help_text=_("MIME type"),
    )
    file_size = models.BigIntegerField(
        null=True,
        blank=True,
        help_text=_("File size in bytes"),
    )
    
    # Classification
    roles = ArrayField(
        models.CharField(max_length=20, choices=Role.choices),
        default=list,
    )
    format = models.CharField(max_length=20, choices=Format.choices)
    data_type = models.CharField(
        max_length=20,
        choices=DataType.choices,
        null=True,
        blank=True,
    )
    
    # Dimensions (for raster assets)
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    bands = models.IntegerField(null=True, blank=True, default=1)
    
    # Projection info
    proj_epsg = models.IntegerField(null=True, blank=True)
    proj_bbox = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True,
    )
    
    # Format-specific fields stored as JSON
    # Examples:
    #   PNG: {"imageUnscale": [0, 100], "palette": "viridis"}
    #   COG: {"overviews": [2,4,8,16], "blocksize": 512, "compression": "deflate"}
    extra_fields = models.JSONField(default=dict, blank=True)
    
    class Meta:
        ordering = ['sort_order']
        constraints = [
            models.UniqueConstraint(
                fields=['item', 'key'],
                name='unique_asset_key_per_item'
            ),
        ]
        indexes = [
            models.Index(fields=['item', 'format']),
            models.Index(fields=['item', 'key']),
        ]
    
    def __str__(self):
        return f"{self.item} / {self.key} ({self.format})"
    
    # =========================================================================
    # Format-Specific Properties
    # =========================================================================
    
    @property
    def is_visual(self) -> bool:
        return self.format in (self.Format.PNG, self.Format.WEBP, self.Format.JPEG)
    
    @property
    def is_data(self) -> bool:
        return self.format in (self.Format.COG, self.Format.GEOTIFF)
    
    @property
    def is_cog(self) -> bool:
        return self.format == self.Format.COG
    
    # -------------------------------------------------------------------------
    # PNG-specific
    # -------------------------------------------------------------------------
    
    @property
    def image_unscale(self) -> tuple:
        """For PNG: [min, max] values to unscale byte values back to data range."""
        return self.extra_fields.get('imageUnscale')
    
    @property
    def palette(self) -> str:
        """For PNG: color palette name."""
        return self.extra_fields.get('palette')
    
    @property
    def scale_type(self) -> str:
        """For PNG: scaling type (linear, log, sqrt, diverging)."""
        return self.extra_fields.get('scale', 'linear')
    
    # -------------------------------------------------------------------------
    # COG-specific
    # -------------------------------------------------------------------------
    
    @property
    def overviews(self) -> list:
        """For COG: overview levels [2, 4, 8, 16]."""
        return self.extra_fields.get('overviews', [])
    
    @property
    def blocksize(self) -> int:
        """For COG: internal tile size."""
        return self.extra_fields.get('blocksize', 512)
    
    @property
    def compression(self) -> str:
        """For COG: compression method."""
        return self.extra_fields.get('compression', 'deflate')
    
    @property
    def nodata(self):
        """For COG/GeoTIFF: nodata value."""
        return self.extra_fields.get('nodata')
    
    # =========================================================================
    # URL Generation
    # =========================================================================
    
    def get_url(self) -> str:
        """Get public URL to this asset."""
        from georiva.core.storage import storage_manager
        return storage_manager.url(self.href)


# =============================================================================
# Manager for common queries
# =============================================================================

class ItemManager(models.Manager):
    """Custom manager for Item with common query patterns."""
    
    def for_dataset(self, dataset):
        """Get items for a dataset, ordered by time descending."""
        return self.filter(dataset=dataset).order_by('-time')
    
    def latest_for_dataset(self, dataset):
        """Get the most recent item for a dataset."""
        return self.for_dataset(dataset).first()
    
    def in_time_range(self, start, end):
        """Get items within a time range."""
        return self.filter(time__gte=start, time__lte=end)
    
    def with_assets(self):
        """Prefetch assets for efficiency."""
        return self.prefetch_related('assets')


# Add manager to Item
Item.objects = ItemManager()
Item.objects.contribute_to_class(Item, 'objects')
