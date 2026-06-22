from datetime import datetime
from typing import Optional

from django import forms
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_extensions.db.models import TimeStampedModel
from modelcluster.models import ClusterableModel
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.panels import (
    FieldPanel,
    MultiFieldPanel,
    InlinePanel,
    TitleFieldPanel,
    TabbedInterface,
    ObjectList
)

from georiva.core.widget import ConditionalCheckbox
from .base import AbstractCollection

ADM_LEVEL_CHOICES = [
    (1, 'Level 1'),
    (2, 'Level 2'),
    (3, 'Level 3'),
]


def default_adm_levels():
    return [1, 2]


class CollectionForm(WagtailAdminModelForm):
    boundary_stats_levels = forms.MultipleChoiceField(
        choices=ADM_LEVEL_CHOICES,
        widget=forms.CheckboxSelectMultiple,
        required=False,
        initial=[1, 2],
    )
    
    def clean_boundary_stats_levels(self):
        # Convert list of strings → list of ints for the ArrayField
        return [int(v) for v in self.cleaned_data.get("boundary_stats_levels", [])]


class Collection(AbstractCollection, TimeStampedModel, ClusterableModel):
    """
    Groups one or more Variables.

    Examples:
        - gfs-temperature-2m (single variable)
        - gfs-wind-10m (wind_speed + wind_direction)
        - sentinel-vegetation (ndvi + nir + red)
    """
    
    base_form_class = CollectionForm
    
    class TimeResolution(models.TextChoices):
        SUB_HOURLY = 'sub_hourly', 'Sub-Hourly'
        HOURLY = 'hourly', 'Hourly'
        THREE_HOURLY = '3hourly', '3-Hourly'
        SIX_HOURLY = '6hourly', '6-Hourly'
        TWELVE_HOURLY = '12hourly', '12-Hourly'
        DAILY = 'daily', 'Daily'
        PENTADAL = 'pentadal', 'Pentadal'
        DEKADAL = 'dekadal', 'Dekadal'
        MONTHLY = 'monthly', 'Monthly'
        SUB_SEASONAL = 'sub_seasonal', 'Sub-Seasonal'
        SEASONAL = 'seasonal', 'Seasonal'
        ANNUAL = 'annual', 'Annual'
        CLIMATOLOGY = 'climatology', 'Climatology'
    
    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE,
        related_name='collections'
    )
    
    # Identity (slug, name, description), spatial extent (bounds, crs)
    # are inherited from AbstractCollection.
    
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
    
    # --- Forecast config ---
    is_forecast = models.BooleanField(
        default=False,
        help_text="Check if items represent forecast (future) data with a reference_time"
    )
    forecast_horizon_hours = models.PositiveIntegerField(
        null=True, blank=True,
        help_text="Max forecast horizon in hours (e.g. 240 for GFS)"
    )
    retain_past_forecasts = models.BooleanField(
        default=False,
        help_text="If checked, keep items whose valid_time is in the past. "
                  "If False, a cleanup task prunes them."
    )
    retain_latest_run_only = models.BooleanField(
        default=False,
        help_text="If checked, only keep items from the most recent reference_time run."
    )
    
    sort_order = models.PositiveIntegerField(default=0)
    
    boundary_stats_levels = ArrayField(
        models.IntegerField(choices=ADM_LEVEL_CHOICES),
        blank=True,
        null=True,
        default=default_adm_levels,
        help_text=(
            "Administrative boundary levels for zonal statistics. "
            "1 = region, 2 = district, 3 = sub-district. "
            "Leave blank to disable. Stats are computed for all selected levels."
        ),
    )
    
    class Meta:
        unique_together = ['catalog', 'slug']
        ordering = ['catalog', 'sort_order', 'name']
    
    def __str__(self):
        return f"{self.catalog.slug}/{self.slug}"
    
    panels = [
        MultiFieldPanel([
            TitleFieldPanel('name', placeholder=False),
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
        FieldPanel(
            'is_forecast',
            widget=ConditionalCheckbox(
                target_panel_id='panel-child-details-forecast_configuration-section'
            )),
        MultiFieldPanel([
            FieldPanel('forecast_horizon_hours'),
            FieldPanel('retain_past_forecasts'),
            FieldPanel('retain_latest_run_only'),
        ], heading="Forecast Configuration"),
        FieldPanel('boundary_stats_levels', ),
        InlinePanel('variables', label="Variables"),
    ]
    
    slug_panels = [
        FieldPanel('slug'),
    ]
    
    edit_handler = TabbedInterface([
        ObjectList(panels, heading='Details'),
        ObjectList(slug_panels, heading='Slug'),
    ])
    
    @property
    def spatial_extent(self) -> list | None:
        """
        Authoritative spatial extent for this collection.
    
        If the catalog has a boundary configured, use its bbox — it's
        always correct regardless of what's stored in self.bounds.
    
        Falls back to self.bounds for unclipped collections.
        """
        boundary = self.catalog.boundary
        if boundary and self.catalog.clip_mode != 'none':
            extent = boundary.geom.extent  # (west, south, east, north) from GEOS
            return list(extent)
        return self.bounds
    
    @property
    def date_picker_type(self) -> str:
        """
        HTML input type for the date filter, based on time resolution.
        'number'  → year only   (annual)
        'month'   → year+month  (monthly, seasonal, climatology)
        'date'    → full date   (everything else)
        """
        if self.time_resolution == self.TimeResolution.ANNUAL:
            return 'number'
        if self.time_resolution in (
                self.TimeResolution.MONTHLY,
                self.TimeResolution.SEASONAL,
                self.TimeResolution.CLIMATOLOGY,
        ):
            return 'month'
        return 'date'
    
    @property
    def date_picker_min(self) -> str:
        """Formatted time_start for use as the date input min attribute."""
        if not self.time_start:
            return ''
        if self.date_picker_type == 'number':
            return str(self.time_start.year)
        if self.date_picker_type == 'month':
            return self.time_start.strftime('%Y-%m')
        return self.time_start.strftime('%Y-%m-%d')
    
    @property
    def date_picker_max(self) -> str:
        """Formatted time_end for use as the date input max attribute."""
        if not self.time_end:
            return ''
        if self.date_picker_type == 'number':
            return str(self.time_end.year)
        if self.date_picker_type == 'month':
            return self.time_end.strftime('%Y-%m')
        return self.time_end.strftime('%Y-%m-%d')
    
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
