from django.core.exceptions import ValidationError
from django.db import models
from django.utils.functional import cached_property
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import (
    FieldPanel,
    TitleFieldPanel
)
from wagtail.blocks import (
    StructBlock,
    CharBlock,
    FloatBlock,
    StreamBlock
)
from wagtail.fields import StreamField
from wagtail.models import Orderable


class SourceBlock(StructBlock):
    source_name = CharBlock(
        help_text=(
            "Exact variable name as it appears in the source file. "
            "For GRIB: use the shortName (e.g. '2t', 'u10', 'tp'). "
            "For NetCDF: use the variable name (e.g. 'air_temperature'). "
            "For GeoTIFF: use 'band_1', 'band_2', etc."
        )
    )
    vertical_dimension = CharBlock(
        required=False,
        help_text=(
            "Vertical coordinate name, e.g. 'heightAboveGround', 'isobaricInhPa'. "
            "Leave blank for surface or single-level data."
        )
    )
    vertical_value = FloatBlock(
        required=False,
        help_text=(
            "Value along the vertical dimension, e.g. 2 for 2m, 850 for 850 hPa. "
            "U and V components must be at the same level."
        )
    )
    
    class Meta:
        icon = 'pick'
        label = 'Source'


class VariableSourceStreamBlock(StreamBlock):
    primary = SourceBlock(
        label='Primary Source',
        help_text="Direct source for PASSTHROUGH variables. Exactly one allowed."
    )
    u_component = SourceBlock(
        label='U Component',
        help_text="East-west wind component (positive = eastward). Required for VECTOR transforms."
    )
    v_component = SourceBlock(
        label='V Component',
        help_text="North-south wind component (positive = northward). Required for VECTOR transforms."
    )
    
    class Meta:
        min_num = 1
        block_counts = {
            'primary': {'max_num': 1},
            'u_component': {'max_num': 1},
            'v_component': {'max_num': 1},
        }


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
        VECTOR_MAGNITUDE = 'vector_magnitude', 'Vector Magnitude (√(u² + v²))'
        VECTOR_DIRECTION = 'vector_direction', 'Vector Direction (atan2)'
    
    class ScaleType(models.TextChoices):
        LINEAR = 'linear', 'Linear'
        LOG = 'log', 'Logarithmic'
        SQRT = 'sqrt', 'Square Root'
        DIVERGING = 'diverging', 'Diverging'
    
    collection = ParentalKey(
        'georivacore.Collection',
        on_delete=models.CASCADE,
        related_name='variables'
    )
    
    # Identity
    slug = models.SlugField(
        max_length=100,
        help_text=(
            "URL-safe identifier for this variable, used in API endpoints and file paths. "
            "Use lowercase with hyphens, e.g. 'temperature-2m', 'wind-speed-10m'. "
            "Cannot be changed after data has been ingested against this variable."
        )
    )
    name = models.CharField(max_length=200)
    description = models.TextField(
        blank=True,
        help_text=(
            "Human-readable description shown in the data catalog and API responses. "
            "Include the physical quantity, level, and any relevant processing notes, "
            "e.g. 'Air temperature at 2 metres above ground, converted from Kelvin to Celsius.'"
        )
    )
    
    # Transform
    transform_type = models.CharField(
        max_length=30,
        choices=TransformType.choices,
        default=TransformType.PASSTHROUGH,
        help_text=(
            "How source data is transformed into this variable's output array. "
            "PASSTHROUGH: reads one source band directly with no computation. "
            "VECTOR MAGNITUDE: computes wind speed as √(u² + v²) from U and V components. "
            "VECTOR DIRECTION: computes meteorological wind direction (where wind comes FROM) "
            "as atan2(u, v) + 180°, ranging 0–360° clockwise from North. "
            "Changing this after ingestion will not reprocess existing assets."
        )
    )
    
    source_unit = models.ForeignKey(
        'georivacore.Unit',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Units of the raw data as it comes from the source file, e.g. Kelvin, Pa, m/s."
    )
    
    unit = models.ForeignKey(
        'georivacore.Unit',
        on_delete=models.PROTECT,
        related_name='+',
        help_text="Units of this variable's output after any conversion."
    )
    
    # Visualization
    value_min = models.FloatField(
        help_text=(
            "Minimum expected data value in the variable's output units. "
            "Used for palette mapping, COG encoding range, and legend display. "
            "Values below this will be clipped to the palette minimum. "
            "Must match the minimum stop value in the selected palette."
        )
    )
    value_max = models.FloatField(
        help_text=(
            "Maximum expected data value in the variable's output units. "
            "Used for palette mapping, COG encoding range, and legend display. "
            "Values above this will be clipped to the palette maximum. "
            "Must match the maximum stop value in the selected palette."
        )
    )
    scale_type = models.CharField(
        max_length=20,
        choices=ScaleType.choices,
        default=ScaleType.LINEAR,
        help_text=(
            "Scale used for mapping data values to palette colors. "
            "LINEAR: uniform spacing — suitable for most variables. "
            "LOG: useful for variables with large dynamic range like precipitation. "
            "SQRT: moderate compression for skewed distributions. "
            "DIVERGING: for variables with a meaningful midpoint, e.g. temperature anomaly."
        )
    )
    
    palette = models.ForeignKey(
        'georivacore.ColorPalette',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        help_text=(
            "Color palette used for rendering PNG tiles and legend display. "
            "The palette's min/max stop values must exactly match this variable's "
            "value_min and value_max. If no palette is selected, a grayscale "
            "fallback is used."
        )
    )
    
    # Status
    is_active = models.BooleanField(
        default=True,
        help_text=(
            "Inactive variables are skipped during ingestion. "
            "Use this to temporarily disable a variable without deleting it. "
            "Existing assets for this variable are retained."
        )
    )
    
    sources = StreamField(
        VariableSourceStreamBlock(),
        use_json_field=True,
        null=True,
        blank=True,
        verbose_name='Sources',
    )
    
    panels = [
        TitleFieldPanel('name', placeholder=False),
        FieldPanel('slug'),
        FieldPanel('is_active'),
        FieldPanel('description'),
        FieldPanel('source_unit'),
        FieldPanel('unit'),
        FieldPanel('value_min'),
        FieldPanel('value_max'),
        FieldPanel('palette'),
        FieldPanel('transform_type'),
        FieldPanel('sources'),
    ]
    
    class Meta:
        indexes = [
            models.Index(
                fields=['slug', 'is_active'],
                name='variable_slug_active_idx',
            ),
        ]
    
    def __str__(self):
        return f"{self.collection.slug}:{self.slug}"
    
    @property
    def output_unit(self):
        return self.unit
    
    def clean(self):
        super().clean()
        errors = {}
        
        # Unit conversion compatibility
        if self.source_unit and self.unit and self.source_unit != self.unit:
            try:
                self.source_unit.pint_unit.to(self.unit.pint_unit)
            except Exception:
                errors['unit'] = (
                    f"Cannot convert from {self.source_unit} to {self.unit} — "
                    f"incompatible dimensions."
                )
        
        # Sources / transform consistency
        if self.sources:
            block_types = [block.block_type for block in self.sources]
        else:
            block_types = []
        
        if not block_types:
            errors['sources'] = "At least one source must be defined."
        else:
            if self.transform_type == self.TransformType.PASSTHROUGH:
                if set(block_types) != {'primary'}:
                    errors['sources'] = "Passthrough requires exactly one primary source."
            
            elif self.transform_type in (
                    self.TransformType.VECTOR_MAGNITUDE,
                    self.TransformType.VECTOR_DIRECTION,
            ):
                missing = {'u_component', 'v_component'} - set(block_types)
                if missing:
                    errors['sources'] = (
                        f"{self.get_transform_type_display()} requires "
                        f"{', '.join(sorted(missing))} source(s)."
                    )
        
        # Palette range
        if self.palette and self.value_min is not None and self.value_max is not None:
            palette_min, palette_max = self.palette.min_max_from_stops()
            if abs(palette_min - self.value_min) > 0.01 or abs(palette_max - self.value_max) > 0.01:
                errors['palette'] = (
                    f"Palette range ({palette_min}–{palette_max}) must match "
                    f"value range ({self.value_min}–{self.value_max})"
                )
        
        if errors:
            raise ValidationError(errors)
    
    @property
    def encoding_range(self) -> tuple[float | None, float | None]:
        """
        Get (min, max) for encoding and viewing.
        """
        return self.value_min, self.value_max
    
    @property
    def is_derived(self):
        return self.transform_type != self.TransformType.PASSTHROUGH
    
    @cached_property
    def sources_param_list(self):
        """Return a list of source variable names for this variable."""
        return [block.value['source_name'] for block in self.sources]
    
    @property
    def weather_layers_palette(self):
        """Get palette for WeatherLayers, with grayscale fallback."""
        if self.palette:
            return self.palette.as_weatherlayers_palette()
        
        # Fallback: grayscale
        return self._generate_grayscale_palette(self.value_min, self.value_max)
    
    @property
    def palette_value_range(self) -> tuple:
        """
        Get (min, max) for legend display.
        - Variable has range defined: use it (consistent across all assets)
        - No range defined: use asset stats (grayscale fallback per-asset)
        """
        return self.value_min, self.value_max
    
    @staticmethod
    def _generate_grayscale_palette(min_val: float, max_val: float, steps: int = 11, inverted: bool = False) -> list:
        """
        Generate grayscale palette with positions matching data value range.
        
        Args:
            min_val: Minimum data value
            max_val: Maximum data value
            steps: Number of color stops
            inverted: If True, goes white→black instead of black→white
        """
        palette = []
        val_range = max_val - min_val
        
        for i in range(steps):
            t = i / (steps - 1)
            position = min_val + (t * val_range)
            gray = round((1 - t if inverted else t) * 255)
            palette.append([position, [gray, gray, gray]])
        
        return palette
    
    @property
    def value_range(self):
        return self.value_min, self.value_max
