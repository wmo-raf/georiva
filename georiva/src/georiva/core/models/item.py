from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from timescale.db.models.models import TimescaleModel
from wagtail.models import Orderable


class Item(TimescaleModel, TimeStampedModel, ClusterableModel):
    """
    A single raster file in the system.
    
    Uses TimescaleDB hypertable for efficient time-series queries.
    """
    dataset = models.ForeignKey(
        'core.Dataset',
        on_delete=models.CASCADE,
        related_name='raster_file_assets'
    )
    
    # For forecast data
    run_time = models.DateTimeField(null=True, blank=True)
    
    # Storage
    source_file = models.CharField(max_length=500)  # Original file path
    
    # Spatial metadata
    bounds = ArrayField(models.FloatField(), size=4)
    width = models.IntegerField()
    height = models.IntegerField()
    resolution_x = models.FloatField()
    resolution_y = models.FloatField()
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    # Statistics (computed during processing)
    stats_min = models.FloatField(null=True, blank=True)
    stats_max = models.FloatField(null=True, blank=True)
    stats_mean = models.FloatField(null=True, blank=True)
    stats_std = models.FloatField(null=True, blank=True)
    
    # Metadata
    metadata = models.JSONField(default=dict, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-time']
        constraints = [
            models.UniqueConstraint(
                fields=['time', 'dataset'],
                name='unique_time_dataset_item'
            )
        ]
        indexes = [
            models.Index(fields=['dataset', 'time']),
        ]
    
    def __str__(self):
        return f"{self.dataset.name}/ @ {self.time}"


class Asset(Orderable):
    class Role(models.TextChoices):
        DATA = 'data', 'Data'
        THUMBNAIL = 'thumbnail', 'Thumbnail'
        OVERVIEW = 'overview', 'Overview'
        METADATA = 'metadata', 'Metadata'
        VISUAL = 'visual', 'Visual'
    
    item = ParentalKey('Item', on_delete=models.CASCADE, related_name='assets')
    key = models.CharField(max_length=100)
    title = models.CharField(max_length=255, blank=True)
    description = models.TextField(blank=True)
    media_type = models.CharField(max_length=100)  # MIME type
    
    roles = ArrayField(
        models.CharField(max_length=20),
        default=list
    )
    
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    
    proj_epsg = models.IntegerField(null=True, blank=True)
    proj_bbox = ArrayField(models.FloatField(), size=4, null=True, blank=True)
    
    class Format(models.TextChoices):
        TIFF = 'tiff', 'GeoTIFF'
        COG = 'cog', 'Cloud-Optimized GeoTIFF'
        PNG = 'png', 'PNG'
        WEBP = 'webp', 'WebP'
        JPEG = 'jpeg', 'JPEG'
        JSON = 'json', 'JSON'
    
    format = models.CharField(max_length=20, choices=Format.choices)
    
    class DataType(models.TextChoices):
        FLOAT32 = 'float32', 'Float32'
        FLOAT16 = 'float16', 'Float16'
        UINT16 = 'uint16', 'UInt16'
        BYTE = 'byte', 'Byte (0-255)'
        COLOR = 'color', 'Color (palette applied)'
    
    data_type = models.CharField(max_length=20, choices=DataType.choices, null=True, blank=True)
    
    extra_fields = models.JSONField(default=dict, blank=True)
    
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['item', 'key'],
                name='unique_asset_key_per_item'
            )
        ]
        indexes = [
            models.Index(fields=['item', 'format']),
        ]
