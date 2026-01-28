from django.core.exceptions import ValidationError
from django.db import models
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, InlinePanel, MultiFieldPanel
from wagtail.models import Orderable


class Variable(TimeStampedModel, ClusterableModel, Orderable):
    """
    A user-facing variable
    
    Examples:
        - temperature_2m (passthrough from TMP_2m)
        - wind_speed (derived from UGRD + VGRD)
        - ndvi (derived from B04 + B08)
    """
    
    class TransformType(models.TextChoices):
        PASSTHROUGH = 'passthrough', 'Passthrough (direct read)'
        UNIT_CONVERT = 'unit_convert', 'Unit Conversion'
        VECTOR_MAGNITUDE = 'vector_magnitude', 'Vector Magnitude (√(u² + v²))'
        VECTOR_DIRECTION = 'vector_direction', 'Vector Direction (atan2)'
        BAND_MATH = 'band_math', 'Band Math (expression)'
        RGB_COMPOSITE = 'rgb_composite', 'RGB Composite'
        THRESHOLD = 'threshold', 'Threshold (mask)'
    
    class ScaleType(models.TextChoices):
        LINEAR = 'linear', 'Linear'
        LOG = 'log', 'Logarithmic'
        SQRT = 'sqrt', 'Square Root'
        DIVERGING = 'diverging', 'Diverging'
    
    class UnitConversion(models.TextChoices):
        NONE = '', 'None'
        K_TO_C = 'K_to_C', 'Kelvin to Celsius'
        PA_TO_HPA = 'Pa_to_hPa', 'Pascal to hectoPascal'
        M_TO_MM = 'm_to_mm', 'Meters to Millimeters'
        MS_TO_KMH = 'ms_to_kmh', 'm/s to km/h'
        KGM2S_TO_MM = 'kgm2s_to_mm', 'kg/m²/s to mm'
    
    collection = ParentalKey(
        'georivacore.Collection',
        on_delete=models.CASCADE,
        related_name='variables'
    )
    
    # Identity
    slug = models.SlugField(max_length=100)
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    
    # Transform
    transform_type = models.CharField(
        max_length=30,
        choices=TransformType.choices,
        default=TransformType.PASSTHROUGH
    )
    transform_expression = models.TextField(
        blank=True,
        help_text="For band_math: e.g., '(nir - red) / (nir + red)'"
    )
    
    # Units
    unit_conversion = models.CharField(
        max_length=50,
        choices=UnitConversion.choices,
        blank=True
    )
    units = models.CharField(max_length=50, blank=True)
    
    # Visualization
    value_min = models.FloatField()
    value_max = models.FloatField()
    scale_type = models.CharField(
        max_length=20,
        choices=ScaleType.choices,
        default=ScaleType.LINEAR
    )
    palette = models.ForeignKey(
        'georivacore.ColorPalette',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    
    # Status
    is_active = models.BooleanField(default=True)
    sort_order = models.PositiveIntegerField(default=0)
    
    def __str__(self):
        return f"{self.collection.slug}:{self.slug}"
    
    def clean(self):
        super().clean()
        
        if not (self.palette and self.value_min is not None and self.value_max is not None):
            return
        
        palette_min, palette_max = self.palette.min_max_from_stops()
        
        if abs(palette_min - self.value_min) > 0.01 or abs(palette_max - self.value_max) > 0.01:
            raise ValidationError({
                'palette': f"Palette range ({palette_min}–{palette_max}) must match value range ({self.value_min}–{self.value_max})"
            })
    
    @property
    def encoding_range(self) -> tuple[float | None, float | None]:
        """
        Get (min, max) for encoding and viewing.
        """
        return self.value_min, self.value_max
    
    @property
    def is_derived(self):
        return self.transform_type != self.TransformType.PASSTHROUGH
    
    panels = [
        MultiFieldPanel([
            FieldPanel('slug'),
            FieldPanel('name'),
            FieldPanel('description'),
        ], heading="Identity"),
        MultiFieldPanel([
            FieldPanel('transform_type'),
            FieldPanel('transform_expression'),
        ], heading="Transform"),
        InlinePanel('sources', label="Source Parameters"),
        MultiFieldPanel([
            FieldPanel('unit_conversion'),
            FieldPanel('units'),
        ], heading="Units"),
        MultiFieldPanel([
            FieldPanel('value_min'),
            FieldPanel('value_max'),
            FieldPanel('palette'),
        ], heading="Visualization"),
        MultiFieldPanel([
            FieldPanel('is_active'),
            FieldPanel('sort_order'),
        ], heading="Status"),
    ]


class VariableSource(Orderable):
    """
    Links a Variable to its source
    
    Examples:
        - wind_speed: UGRD (role=u_component), VGRD (role=v_component)
        - ndvi: B04 (role=red), B08 (role=nir)
        - temperature: TMP_2m (role=primary)
    """
    
    variable = ParentalKey(
        Variable,
        on_delete=models.CASCADE,
        related_name='sources'
    )
    
    source_name = models.CharField(
        max_length=100,
        help_text="Name in source file, e.g., 'TMP_2maboveground', 'B04'"
    )
    
    # Dimensions
    vertical_dimension = models.CharField(max_length=50, blank=True)
    vertical_value = models.FloatField(null=True, blank=True)
    band_index = models.PositiveIntegerField(null=True, blank=True)
    
    # Source metadata
    source_units = models.CharField(max_length=50, blank=True)
    source_dtype = models.CharField(max_length=20, blank=True)
    source_nodata = models.FloatField(null=True, blank=True)
    
    role = models.CharField(
        max_length=20,
        default='primary'
    )
    
    def __str__(self):
        return f"{self.variable.slug} ← {self.source_name} ({self.role})"
    
    panels = [
        FieldPanel('role'),
        MultiFieldPanel([
            FieldPanel('source_name'),
            FieldPanel('vertical_dimension'),
            FieldPanel('vertical_value'),
            FieldPanel('band_index'),
        ], heading="Source Extraction"),
        MultiFieldPanel([
            FieldPanel('source_units'),
            FieldPanel('source_dtype'),
            FieldPanel('source_nodata'),
        ], heading="Source Metadata"),
    ]
