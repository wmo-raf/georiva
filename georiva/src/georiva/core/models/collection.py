from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_extensions.db.models import TimeStampedModel
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, MultiFieldPanel, InlinePanel


class Collection(TimeStampedModel, ClusterableModel):
    """
    Groups one or more Variables.
    
    Examples:
        - gfs-temperature-2m (single variable)
        - gfs-wind-10m (wind_speed + wind_direction)
        - sentinel-vegetation (ndvi + nir + red)
    """
    
    class TimeResolution(models.TextChoices):
        HOURLY = 'hourly', 'Hourly'
        THREE_HOURLY = '3hourly', '3-Hourly'
        SIX_HOURLY = '6hourly', '6-Hourly'
        DAILY = 'daily', 'Daily'
        DEKADAL = 'dekadal', 'Dekadal'
        MONTHLY = 'monthly', 'Monthly'
        YEARLY = 'yearly', 'Yearly'
    
    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE,
        related_name='collections'
    )
    
    # Identity
    slug = models.SlugField(max_length=100)
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    
    # Spatial extent (auto-updated)
    bounds = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True
    )
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    # Temporal extent (auto-updated)
    time_resolution = models.CharField(
        max_length=20,
        choices=TimeResolution.choices,
        blank=True
    )
    time_start = models.DateTimeField(null=True, blank=True, editable=False)
    time_end = models.DateTimeField(null=True, blank=True, editable=False)
    item_count = models.PositiveIntegerField(default=0, editable=False)
    
    # Status
    is_active = models.BooleanField(default=True)
    sort_order = models.PositiveIntegerField(default=0)
    
    class Meta:
        unique_together = ['catalog', 'slug']
        ordering = ['catalog', 'sort_order', 'name']
    
    def __str__(self):
        return f"{self.catalog.slug}/{self.slug}"
    
    panels = [
        MultiFieldPanel([
            FieldPanel('name'),
            FieldPanel('slug'),
            FieldPanel('catalog'),
            FieldPanel('description'),
        ], heading="Identity"),
        MultiFieldPanel([
            FieldPanel('bounds'),
            FieldPanel('crs'),
            FieldPanel('time_resolution'),
        ], heading="Extent"),
        MultiFieldPanel([
            FieldPanel('is_active'),
            FieldPanel('sort_order'),
        ], heading="Status"),
        InlinePanel('variables', label="Variables"),
    ]
