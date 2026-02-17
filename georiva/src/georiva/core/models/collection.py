from datetime import datetime
from typing import Optional

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
    
    # Ingestion configuration
    loader_profile = models.ForeignKey(
        "georivasources.LoaderProfile",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        help_text="Loader profile to use for ingesting data for this catalog",
    )
    is_loader_active = models.BooleanField(default=True, help_text="Loader active status")
    
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
            FieldPanel('loader_profile'),
            FieldPanel('is_loader_active'),
        ]),
        MultiFieldPanel([
            FieldPanel('is_active'),
            FieldPanel('sort_order'),
        ], heading="Status"),
        InlinePanel('variables', label="Variables"),
    ]
    
    def get_loader(self):
        """Get the loader instance for this catalog."""
        if not self.loader_profile:
            return None
        
        return self.loader_profile.get_loader(self)
    
    def source_variables_list(self):
        """Return a list of source variable names in this collection."""
        source_vars = []
        for variable in self.variables.all():
            variable_sources_params = variable.sources_param_list
            source_vars.extend(variable_sources_params)
        return source_vars
    
    def get_latest_item_date(self) -> Optional[datetime]:
        """
        Latest valid_time in this collection (Item.time).
        """
        latest = self.items.order_by("-time").first()  # uses related_name='items'
        return latest.time if latest else None
