from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel
from wagtail.snippets.models import register_snippet


class ColorPalette(models.Model):
    """
    Color palette for data visualization.
    """
    id = models.SlugField(primary_key=True, max_length=100)
    name = models.CharField(max_length=255)
    
    class PaletteType(models.TextChoices):
        SEQUENTIAL = 'sequential', 'Sequential'
        DIVERGING = 'diverging', 'Diverging'
        CATEGORICAL = 'categorical', 'Categorical'
    
    palette_type = models.CharField(
        max_length=20,
        choices=PaletteType.choices,
        default=PaletteType.SEQUENTIAL
    )
    
    # Colors as hex values
    colors = ArrayField(
        models.CharField(max_length=7),
        help_text="List of hex colors, e.g., ['#f7fbff', '#08306b']"
    )
    
    # For diverging palettes
    center_value = models.FloatField(
        null=True, blank=True,
        help_text="Center value for diverging palettes"
    )
    
    class Meta:
        ordering = ['name']
    
    def __str__(self):
        return self.name
    
    panels = [
        FieldPanel('id'),
        FieldPanel('name'),
        FieldPanel('palette_type'),
        FieldPanel('colors'),
        FieldPanel('center_value'),
    ]


@register_snippet
class Dataset(ClusterableModel, TimeStampedModel):
    """A Dataset represents a single data product within a Collection."""
    
    class VariableType(models.TextChoices):
        """Type of variable in the dataset."""
        SCALAR = 'scalar', _('Scalar')
        VECTOR = 'vector', _('Vector')
    
    class ScaleType(models.TextChoices):
        """Scaling method for encoding values."""
        LINEAR = 'linear', _('Linear')
        LOG = 'log', _('Logarithmic')
        SQRT = 'sqrt', _('Square Root')
        DIVERGING = 'diverging', _('Diverging (centered on zero)')
    
    class UnitConversion(models.TextChoices):
        NONE = '', 'None'
        K_TO_C = 'K_to_C', 'Kelvin to Celsius'
        PA_TO_HPA = 'Pa_to_hPa', 'Pascal to hectoPascal'
        M_TO_MM = 'm_to_mm', 'Meters to Millimeters'
        MS_TO_KMH = 'ms_to_kmh', 'Meters/second to km/h'
        KGM2S_TO_MM = 'kgm2s_to_mm', 'kg/m²/s to mm (precipitation rate)'
    
    class TimeResolution(models.TextChoices):
        HOURLY = 'hourly', 'Hourly'
        THREE_HOURLY = '3hourly', '3-Hourly'
        SIX_HOURLY = '6hourly', '6-Hourly'
        DAILY = 'daily', 'Daily'
        DEKADAL = 'dekadal', 'Dekadal (10-day)'
        MONTHLY = 'monthly', 'Monthly'
        YEARLY = 'yearly', 'Yearly'
    
    collection = ParentalKey('georivacore.Collection', on_delete=models.CASCADE, related_name='datasets')
    name = models.CharField(max_length=200)
    slug = models.SlugField(unique=True, max_length=200)
    description = models.TextField(blank=True)
    
    # Variable extraction
    variable_type = models.CharField(max_length=20, choices=VariableType.choices, default=VariableType.SCALAR)
    primary_variable = models.CharField(max_length=100)
    secondary_variable = models.CharField(max_length=100, blank=True)
    
    vertical_dimension = models.CharField(
        max_length=50,
        blank=True,
        help_text=_(
            "Name of vertical coordinate (e.g., 'isobaricInhPa', 'level', 'depth'). Leave empty for 2D/Surface data.")
    )
    vertical_value = models.FloatField(
        null=True,
        blank=True,
        help_text=_("Value to select (e.g., 850, 500, 10).")
    )
    
    # Units
    source_units = models.CharField(max_length=50, blank=True)
    units = models.CharField(max_length=50, blank=True)
    unit_conversion = models.CharField(max_length=50, blank=True, choices=UnitConversion.choices)
    
    # Visualization
    value_min = models.FloatField(null=True, blank=True)
    value_max = models.FloatField(null=True, blank=True)
    scale_type = models.CharField(max_length=20, choices=ScaleType.choices, default=ScaleType.LINEAR)
    palette = models.ForeignKey('georivacore.ColorPalette', null=True, blank=True, on_delete=models.SET_NULL)
    
    # Spatial
    bounds = ArrayField(models.FloatField(), size=4, null=True, blank=True)
    crs = models.CharField(max_length=50, default="EPSG:4326")
    
    # Temporal
    time_resolution = models.CharField(max_length=20, choices=TimeResolution.choices, blank=True)
    time_start = models.DateTimeField(null=True, blank=True, editable=False)
    time_end = models.DateTimeField(null=True, blank=True, editable=False)
    item_count = models.PositiveIntegerField(default=0, editable=False)
    
    # Status
    is_active = models.BooleanField(default=True)
    sort_order = models.PositiveIntegerField(default=0)
    
    @property
    def is_vector(self):
        """Check if this dataset represents vector data."""
        return self.variable_type == self.VariableType.VECTOR
    
    @property
    def zarr_store(self) -> str:
        """Path to Zarr store."""
        return f"zarr/{self.slug}.zarr"
