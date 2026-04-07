from django import forms
from django.db import models
from django_extensions.db.fields import AutoSlugField
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import (
    FieldPanel,
    MultiFieldPanel, TitleFieldPanel, TabbedInterface, ObjectList
)


class Topic(TimeStampedModel):
    """
    Thematic topic for classifying Catalogs
    """
    name = models.CharField(max_length=100, unique=True)
    slug = AutoSlugField(populate_from='name', unique=True, editable=False)
    description = models.TextField(blank=True)
    icon = models.CharField(
        max_length=50,
        blank=True,
        help_text="Bootstrap Icons class e.g. bi-thermometer-half"
    )
    sort_order = models.PositiveIntegerField(default=0)
    
    class Meta:
        ordering = ['sort_order', 'name']
        verbose_name = "Topic"
        verbose_name_plural = "Topics"
    
    def __str__(self):
        return self.name
    
    panels = [
        FieldPanel('name'),
        FieldPanel('description'),
        FieldPanel('icon'),
        FieldPanel('sort_order'),
    ]


class Catalog(TimeStampedModel):
    """
    A data source that produces multiple collections.
    
    Examples: GFS, CHIRPS, ERA5, MSG
    
    This is an organizational grouping - it defines how data is ingested
    """
    name = models.CharField(max_length=255)
    slug = models.SlugField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    
    # Provider information
    provider = models.CharField(max_length=255, blank=True)
    provider_url = models.URLField(blank=True)
    license = models.CharField(max_length=255, blank=True)
    
    topics = models.ManyToManyField(
        'georivacore.Topic',
        blank=True,
        related_name='catalogs',
        help_text="Thematic topics for this catalog."
    )
    
    # Source file format
    class FileFormat(models.TextChoices):
        GRIB = 'grib2', 'GRIB/GRIB2'
        NETCDF = 'netcdf', 'NetCDF'
        GEOTIFF = 'geotiff', 'GeoTIFF'
        ZARR = 'zarr', 'ZARR'
    
    class ClipMode(models.TextChoices):
        NONE = 'none', 'No clipping'
        BBOX = 'bbox', 'Bounding box only'
        MASK = 'mask', 'Precise geometry mask'
    
    file_format = models.CharField(max_length=20, choices=FileFormat.choices)
    archive_source_files = models.BooleanField(default=False, help_text="Should archive source files")
    is_active = models.BooleanField(default=True)
    
    boundary = models.ForeignKey(
        "adminboundarymanager.AdminBoundary",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="Boundary to clip data to"
    )
    clip_mode = models.CharField(
        max_length=20,
        choices=ClipMode.choices,
        default=ClipMode.MASK,
        help_text="How to apply boundary clipping"
    )
    
    class Meta:
        ordering = ['name']
        verbose_name_plural = 'Catalogs'
    
    def __str__(self):
        return self.name
    
    panels = [
        MultiFieldPanel([
            TitleFieldPanel('name', placeholder=False),
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
        MultiFieldPanel([
            FieldPanel('boundary'),
            FieldPanel('clip_mode'),
        ], heading="Clipping Configuration"),
        FieldPanel('is_active'),
        MultiFieldPanel([
            FieldPanel('topics', widget=forms.CheckboxSelectMultiple),
        ], heading="Topics"),
    ]
    
    slug_panels = [
        FieldPanel('slug'),
    ]
    
    edit_handler = TabbedInterface([
        ObjectList(panels, heading='Details'),
        ObjectList(slug_panels, heading='Slug'),
    ])
