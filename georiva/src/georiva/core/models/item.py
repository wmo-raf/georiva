"""
GeoRiva Item and Asset Models

Item: TimescaleDB hypertable for time-series raster data
Asset: Individual files associated with an Item, linked to Variables
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
    A single spatiotemporal entry in a Collection.
    
    Uses TimescaleDB hypertable for efficient time-series queries.
    The 'time' field from TimescaleModel serves as valid_time.
    """
    
    collection = models.ForeignKey(
        'georivacore.Collection',
        on_delete=models.CASCADE,
        related_name='items',
    )
    
    # Time dimensions
    # 'time' from TimescaleModel = valid_time (what time data represents)
    reference_time = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text=_("When data was produced (model run time for forecasts)"),
    )
    
    # Source tracking
    source_file = models.CharField(
        max_length=500,
        blank=True,
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
        ordering = ['-time']
        constraints = [
            models.UniqueConstraint(
                fields=['collection', 'time'],
                name='unique_time_per_collection',
                condition=models.Q(reference_time__isnull=True),
            ),
            models.UniqueConstraint(
                fields=['collection', 'time', 'reference_time'],
                name='unique_time_collection_reference',
                condition=models.Q(reference_time__isnull=False),
            ),
        ]
        indexes = [
            models.Index(fields=['collection', 'time']),
            models.Index(fields=['collection', '-time']),
            models.Index(fields=['collection', 'reference_time', 'time']),
        ]
    
    def __str__(self):
        return f"{self.collection.slug} @ {self.time}"
    
    # =========================================================================
    # Time Properties
    # =========================================================================
    
    @property
    def valid_time(self):
        """Alias for TimescaleModel's time field."""
        return self.time
    
    @property
    def horizon(self):
        """Forecast horizon as timedelta."""
        if self.reference_time:
            return self.time - self.reference_time
        return None
    
    @property
    def horizon_hours(self):
        """Forecast horizon in hours."""
        if self.horizon:
            return self.horizon.total_seconds() / 3600
        return None
    
    @property
    def is_forecast(self):
        """True if this is forecast data (has reference_time)."""
        return self.reference_time is not None
    
    # =========================================================================
    # Asset Access Helpers
    # =========================================================================
    
    def get_asset(self, variable_slug: str) -> 'Asset':
        """Get asset for a specific variable."""
        return self.assets.filter(variable__slug=variable_slug).first()
    
    def get_asset_by_role(self, role: str) -> 'Asset':
        """Get asset by role."""
        return self.assets.filter(roles__contains=[role]).first()
    
    @property
    def data_assets(self):
        """Get all data assets."""
        return self.assets.filter(roles__contains=['data'])
    
    @property
    def visual_assets(self):
        """Get all visual assets."""
        return self.assets.filter(roles__contains=['visual'])
    
    @property
    def thumbnail(self) -> 'Asset':
        """Get thumbnail asset."""
        return self.assets.filter(roles__contains=['thumbnail']).first()


@register_snippet
class Asset(TimeStampedModel, Orderable):
    """
    A stored data file for a specific Variable within an Item.
    
    Links Item (when/where) to Variable (what) with the actual file (href).
    """
    
    class Role(models.TextChoices):
        DATA = 'data', _('Data')
        VISUAL = 'visual', _('Visual')
        THUMBNAIL = 'thumbnail', _('Thumbnail')
        OVERVIEW = 'overview', _('Overview')
        METADATA = 'metadata', _('Metadata')
    
    class Format(models.TextChoices):
        COG = 'cog', _('Cloud-Optimized GeoTIFF')
        ZARR = 'zarr', _('Zarr')
        GEOTIFF = 'geotiff', _('GeoTIFF')
        PNG = 'png', _('PNG')
        WEBP = 'webp', _('WebP')
        JPEG = 'jpeg', _('JPEG')
        JSON = 'json', _('JSON')
    
    # Parent Item
    item = ParentalKey(
        Item,
        on_delete=models.CASCADE,
        related_name='assets',
        db_constraint=False,
    )
    
    # Link to Variable (carries units, palette, visualization config)
    variable = models.ForeignKey(
        'georivacore.Variable',
        on_delete=models.CASCADE,
        related_name='assets',
    )
    
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
        ordering = ['sort_order']
        constraints = [
            models.UniqueConstraint(
                fields=['item', 'variable', 'format'],
                name='unique_format_per_variable_per_item'
            ),
        ]
        indexes = [
            models.Index(fields=['item']),
            models.Index(fields=['item', 'variable']),
        ]
    
    def __str__(self):
        return f"{self.item} / {self.variable.slug}"
    
    # =========================================================================
    # Properties from Variable (convenience accessors)
    # =========================================================================
    
    @property
    def name(self):
        return self.variable.name
    
    @property
    def units(self):
        return self.variable.units
    
    @property
    def palette(self):
        return self.variable.palette
    
    @property
    def value_range(self):
        """Get value range from variable or computed stats."""
        return (
            self.variable.value_min or self.stats_min,
            self.variable.value_max or self.stats_max,
        )
    
    # =========================================================================
    # Format checks
    # =========================================================================
    
    @property
    def is_visual(self) -> bool:
        return self.format in (self.Format.PNG, self.Format.WEBP, self.Format.JPEG)
    
    @property
    def is_data(self) -> bool:
        return self.format in (self.Format.COG, self.Format.GEOTIFF, self.Format.ZARR)
    
    @property
    def is_cog(self) -> bool:
        return self.format == self.Format.COG
    
    @property
    def is_zarr(self) -> bool:
        return self.format == self.Format.ZARR
    
    # =========================================================================
    # Extra fields accessors
    # =========================================================================
    
    @property
    def image_unscale(self):
        """For PNG: [min, max] to unscale bytes back to data range."""
        return self.extra_fields.get('imageUnscale')
    
    @property
    def overviews(self):
        """For COG: overview levels."""
        return self.extra_fields.get('overviews', [])
    
    @property
    def compression(self):
        """For COG: compression method."""
        return self.extra_fields.get('compression')
    
    @property
    def nodata(self):
        """Nodata value."""
        return self.extra_fields.get('nodata')
    
    # =========================================================================
    # URL Generation
    # =========================================================================
    
    def get_url(self) -> str:
        """Get public URL to this asset."""
        from georiva.core.storage import storage_manager
        return storage_manager.url(self.href)


# =============================================================================
# Custom Manager
# =============================================================================

class ItemManager(models.Manager):
    """Custom manager for Item with common query patterns."""
    
    def for_collection(self, collection):
        """Get items for a collection, ordered by time descending."""
        return self.filter(collection=collection).order_by('-time')
    
    def latest(self, collection=None):
        """Get the most recent item."""
        qs = self.all()
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by('-time').first()
    
    def latest_forecast_run(self, collection):
        """Get items from the latest forecast run."""
        latest_ref = (
            self.filter(collection=collection, reference_time__isnull=False)
            .order_by('-reference_time')
            .values('reference_time')
            .first()
        )
        if latest_ref:
            return self.filter(
                collection=collection,
                reference_time=latest_ref['reference_time']
            ).order_by('time')
        return self.none()
    
    def in_time_range(self, start, end, collection=None):
        """Get items within a time range."""
        qs = self.filter(time__gte=start, time__lte=end)
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by('time')
    
    def valid_at(self, valid_time, collection=None):
        """Get items valid at a specific time."""
        qs = self.filter(time=valid_time)
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by('-reference_time')
    
    def with_assets(self):
        """Prefetch assets for efficiency."""
        return self.prefetch_related('assets', 'assets__variable')


Item.objects = ItemManager()
Item.objects.contribute_to_class(Item, 'objects')
