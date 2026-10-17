from django.core.exceptions import ValidationError
from django.db import models
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, TitleFieldPanel
from wagtail.blocks import CharBlock, FloatBlock, StreamBlock, StructBlock
from wagtail.fields import StreamField
from wagtail.models import Orderable

from georiva.core.panels import StylingSummaryPanel


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
        ),
    )
    vertical_value = FloatBlock(
        required=False,
        help_text=(
            "Value along the vertical dimension, e.g. 2 for 2m, 850 for 850 hPa. "
            "U and V components must be at the same level."
        ),
    )

    class Meta:
        icon = "pick"
        label = "Source"


class VariableSourceStreamBlock(StreamBlock):
    primary = SourceBlock(
        label="Primary Source", help_text="Direct source for PASSTHROUGH variables. Exactly one allowed."
    )
    u_component = SourceBlock(
        label="U Component", help_text="East-west wind component (positive = eastward). Required for VECTOR transforms."
    )
    v_component = SourceBlock(
        label="V Component",
        help_text="North-south wind component (positive = northward). Required for VECTOR transforms.",
    )

    class Meta:
        min_num = 1
        block_counts = {
            "primary": {"max_num": 1},
            "u_component": {"max_num": 1},
            "v_component": {"max_num": 1},
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
        PASSTHROUGH = "passthrough", "Passthrough (direct read)"
        VECTOR_MAGNITUDE = "vector_magnitude", "Vector Magnitude (√(u² + v²))"
        VECTOR_DIRECTION = "vector_direction", "Vector Direction (atan2)"

    class ScaleType(models.TextChoices):
        LINEAR = "linear", "Linear"
        LOG = "log", "Logarithmic"
        SQRT = "sqrt", "Square Root"
        DIVERGING = "diverging", "Diverging"

    ORGANISATION_LOOKUP = "collection__catalog__organisation"

    collection = ParentalKey("georivacore.Collection", on_delete=models.CASCADE, related_name="variables")

    # Identity
    slug = models.SlugField(
        max_length=100,
        help_text=(
            "URL-safe identifier for this variable, used in API endpoints and file paths. "
            "Use lowercase with hyphens, e.g. 'temperature-2m', 'wind-speed-10m'. "
            "Cannot be changed after data has been ingested against this variable."
        ),
    )
    name = models.CharField(max_length=200)
    description = models.TextField(
        blank=True,
        help_text=(
            "Human-readable description shown in the data catalog and API responses. "
            "Include the physical quantity, level, and any relevant processing notes, "
            "e.g. 'Air temperature at 2 metres above ground, converted from Kelvin to Celsius.'"
        ),
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
        ),
    )

    source_unit = models.ForeignKey(
        "georivacore.Unit",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text="Units of the raw data as it comes from the source file, e.g. Kelvin, Pa, m/s.",
    )

    unit = models.ForeignKey(
        "georivacore.Unit",
        on_delete=models.PROTECT,
        related_name="+",
        help_text="Units of this variable's output after any conversion.",
    )

    # Visualization. The 0–1 defaults are the seeding fallback (ADR 0022): a
    # variable provisioned with no declared range comes up grayscale over 0–1
    # until the Styling surface tunes it — provisioning surfaces may seed a
    # better range on create, but only the Styling page edits it after.
    value_min = models.FloatField(
        default=0.0,
        help_text=(
            "Minimum expected data value in the variable's output units. "
            "Used for color mapping, COG encoding range, and legend display. "
            "Values below this will be clipped to the style's minimum color."
        ),
    )
    value_max = models.FloatField(
        default=1.0,
        help_text=(
            "Maximum expected data value in the variable's output units. "
            "Used for color mapping, COG encoding range, and legend display. "
            "Values above this will be clipped to the style's maximum color."
        ),
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
        ),
    )

    # Status
    is_active = models.BooleanField(
        default=True,
        help_text=(
            "Inactive variables are skipped during ingestion. "
            "Use this to temporarily disable a variable without deleting it. "
            "Existing assets for this variable are retained."
        ),
    )

    sources = StreamField(
        VariableSourceStreamBlock(),
        use_json_field=True,
        null=True,
        blank=True,
        verbose_name="Sources",
    )

    panels = [
        TitleFieldPanel("name", placeholder=False),
        FieldPanel("slug"),
        FieldPanel("is_active"),
        FieldPanel("description"),
        FieldPanel("source_unit"),
        FieldPanel("unit"),
        # Range and styling are read-only here (issue #323): the Styling
        # surface is the one place that tunes them (ADR 0022).
        StylingSummaryPanel(heading="Styling"),
        FieldPanel("transform_type"),
        FieldPanel("sources"),
    ]

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["collection", "slug"],
                name="unique_variable_slug_per_collection",
            ),
        ]
        indexes = [
            models.Index(
                fields=["slug", "is_active"],
                name="variable_slug_active_idx",
            ),
        ]

    def __str__(self):
        return f"{self.collection.slug}:{self.slug}"

    @property
    def units(self):
        return self.unit.symbol

    @staticmethod
    def validate_value_range(value_min, value_max):
        """
        The single min < max validator (ADR 0022). Model clean() runs it for
        every ModelForm path; non-model paths (upload wizard, plain forms)
        delegate to it instead of carrying their own copy.
        """
        if value_min is not None and value_max is not None and value_min >= value_max:
            raise ValidationError({"value_max": _("Maximum value must be greater than minimum value.")})

    def clean(self):
        super().clean()
        errors = {}

        try:
            self.validate_value_range(self.value_min, self.value_max)
        except ValidationError as e:
            errors.update(e.error_dict)

        # Unit conversion compatibility
        if self.source_unit and self.unit and self.source_unit != self.unit:
            try:
                self.source_unit.pint_unit.to(self.unit.pint_unit)
            except Exception:
                errors["unit"] = f"Cannot convert from {self.source_unit} to {self.unit} — incompatible dimensions."

        # Sources / transform consistency
        if self.sources:
            block_types = [block.block_type for block in self.sources]
        else:
            block_types = []

        if not block_types:
            errors["sources"] = "At least one source must be defined."
        else:
            if self.transform_type == self.TransformType.PASSTHROUGH:
                if set(block_types) != {"primary"}:
                    errors["sources"] = "Passthrough requires exactly one primary source."

            elif self.transform_type in (
                self.TransformType.VECTOR_MAGNITUDE,
                self.TransformType.VECTOR_DIRECTION,
            ):
                missing = {"u_component", "v_component"} - set(block_types)
                if missing:
                    errors["sources"] = (
                        f"{self.get_transform_type_display()} requires {', '.join(sorted(missing))} source(s)."
                    )

        if errors:
            raise ValidationError(errors)

    @property
    def is_derived(self):
        return self.transform_type != self.TransformType.PASSTHROUGH

    @cached_property
    def sources_param_list(self):
        """Return a list of source variable names for this variable."""
        return [block.value["source_name"] for block in self.sources]

    @property
    def default_style(self):
        """The style served when none is asked for by name — the one
        ``is_default`` row, or ``None`` for a still-grayscale variable.

        Iterates ``styles.all()`` rather than filtering so a queryset that
        prefetched the styles pays no extra query per variable.
        """
        for style in self.styles.all():
            if style.is_default:
                return style
        return None

    @property
    def weather_layers_palette(self):
        """The default style's snapshot for WeatherLayers, with grayscale fallback."""
        style = self.default_style
        if style:
            return style.as_weatherlayers_palette()

        # Fallback: grayscale
        return self._generate_grayscale_palette(self.value_min, self.value_max)

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
        """The canonical (value_min, value_max) accessor — encoding, legend
        display, and previews all read this one (ADR 0022)."""
        return self.value_min, self.value_max
