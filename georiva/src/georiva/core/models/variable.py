import logging
from dataclasses import dataclass
from datetime import timedelta

from django.core.exceptions import ObjectDoesNotExist, ValidationError
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
from georiva.core.time_statistic import Declared
from georiva.core.unit_utils import Conversion, ConversionRefused

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class VectorComponents:
    """What a vector Variable is computed from: the names of its eastward (`u`)
    and northward (`v`) parts in its source, and which of the two quantities it
    is (`transform`, a ``Variable.TransformType``). In a store collection the
    names are field keys: an array's path in the store, with its Selection
    after it where it has one (ADR 0048, #830)."""

    u: str
    v: str
    transform: str

    @property
    def arrays(self) -> tuple[str, str]:
        """Its two parts, by name: in a store collection, their field keys."""
        return (self.u, self.v)

    @property
    def paths(self) -> tuple[str, str]:
        """The paths in the store of the arrays its two parts are read from."""
        from georiva.core.selection import split_field_key

        return tuple(split_field_key(name)[0] for name in self.arrays)

    @property
    def selection(self):
        """The Selection both its parts are read at: u's, which a clean vector shares with v (#830)."""
        from georiva.core.selection import split_field_key

        return split_field_key(self.u)[1]

    def published(self) -> dict:
        """What a reader outside GeoRiva is told of it — Titiler, STAC: the paths
        of its two arrays and which quantity of them it is."""
        u, v = self.paths
        return {"u": u, "v": v, "transform": str(self.transform)}

    @property
    def one_selection(self) -> bool:
        """Whether its u and v are read at the same Selection, as one vector's parts are."""
        from georiva.core.selection import split_field_key

        return split_field_key(self.u)[1] == split_field_key(self.v)[1]


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

    class TimeStatistic(models.TextChoices):
        INSTANT = "instant", "Instantaneous (the value at its time)"
        SUM = "sum", "Sum (an accumulation)"
        MEAN = "mean", "Mean"
        MAX = "max", "Maximum"
        MIN = "min", "Minimum"

    class StatisticWindow(models.TextChoices):
        RUN_START = "run_start", "Since the run started"
        PRECEDING_INTERVAL = "preceding_interval", "Over the interval before each step"

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
    source_long_name = models.CharField(
        max_length=255,
        blank=True,
        editable=False,
        help_text=(
            "The variable's own display name as the source file named it, kept when 'name' is renamed. "
            "Set once at provisioning; empty for variables provisioned before it was recorded."
        ),
    )
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
        on_delete=models.PROTECT,
        related_name="+",
        help_text=(
            "The unit this variable's data is stored in, as the source produces it, e.g. Kelvin, Pa, m/s. "
            "Locked once the variable has data; a wrong label is fixed with Correct source unit."
        ),
    )

    unit = models.ForeignKey(
        "georivacore.Unit",
        on_delete=models.PROTECT,
        related_name="+",
        help_text="The unit this variable is served in. Changing it rewrites no data.",
    )

    # The positive affine conversion source_unit → unit (ADR 0046 §5): served =
    # raw·scale + offset. Recomputed on every save, so readers outside Django —
    # Martin's SQL, tile-config's clients — convert by arithmetic alone.
    scale = models.FloatField(default=1.0, editable=False)
    offset = models.FloatField(default=0.0, editable=False)
    water = models.BooleanField(
        default=False,
        help_text=(
            "This variable's values are water, so kg/m² and mm (and their rates) are the same quantity. "
            "Leave unset for a column load of anything else, such as dust or ozone."
        ),
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

    # Time statistic (ADR 0051): what one value is over time. The source sets
    # it when the Variable is created; after that the operator's value wins.
    time_statistic = models.CharField(
        max_length=10,
        choices=TimeStatistic.choices,
        default=TimeStatistic.INSTANT,
        help_text=(
            "Whether a value is the field at its time, or a statistic over a window. "
            "A statistic has no value at a forecast's first step: its window is empty there, "
            "so that step is never written."
        ),
    )
    statistic_window = models.CharField(
        max_length=20,
        choices=StatisticWindow.choices,
        blank=True,
        default="",
        help_text=(
            "The window a statistic is over: since the forecast run started (a running total), "
            "or the interval before each step. Left empty for an instantaneous value."
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
        FieldPanel("water"),
        FieldPanel("time_statistic"),
        FieldPanel("statistic_window"),
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

    @staticmethod
    def display_order(through: str = "") -> tuple:
        """The order Variables are shown in: the operator's `sort_order`, then
        by name, then by slug. `sort_order` is often unset (a harvested
        collection's Variables had none before #763), and a tie would otherwise
        fall to the rows' physical order, which a dump and restore changes.

        `through` is the path to the Variable when ordering another model by
        it, e.g. ``"variable__"`` for Assets.
        """
        return (
            models.F(f"{through}sort_order").asc(nulls_last=True),
            f"{through}name",
            f"{through}slug",
        )

    @property
    def units(self):
        return self.unit.symbol

    # ── Time statistic (ADR 0051) ───────────────────────────────────────────

    @property
    def declared(self) -> Declared:
        """Its time statistic and window."""
        return Declared(self.time_statistic, self.statistic_window)

    @property
    def is_instant(self) -> bool:
        return self.time_statistic == self.TimeStatistic.INSTANT

    def has_step(self, reference_time, time) -> bool:
        """Whether a step of this Variable exists at `time`: the one rule every
        writer and every expectation asks (ADR 0051). A statistic's window is
        empty at its run's first step, so a non-instant Variable has no lead-0
        step. A step of no run — `reference_time` `None` — always exists."""
        return reference_time is None or self.is_instant or time != reference_time

    def expected_leads(self, leads):
        """Of a run's lead times (`timedelta`s), those this Variable is expected at."""
        return frozenset(lead for lead in leads if self.is_instant or lead != timedelta(0))

    @property
    def zero_at_run_start(self) -> bool:
        """Whether it is a running total since its run started, so zero at the
        run's reference time by definition — where it has no step."""
        return self.time_statistic == self.TimeStatistic.SUM and self.statistic_window == self.StatisticWindow.RUN_START

    def agrees_with(self, declared: Declared | None, *, source: str) -> bool:
        """Whether what `source` declares of this Variable agrees with its own
        declaration. The stored value wins: a disagreement is logged, naming the
        Variable, and nothing is changed. A source that says nothing agrees."""
        if declared is None or declared == self.declared:
            return True
        collection = self.collection if self.collection_id is not None else None
        logger.warning(
            "Variable %s%s is declared %s, but %s says %s. Kept %s; if the source is right, change the "
            "Variable's time statistic in the admin.",
            f"{collection.slug}/" if collection is not None else "",
            self.slug,
            self.declared,
            source,
            declared,
            self.declared,
        )
        return False

    @staticmethod
    def validate_value_range(value_min, value_max):
        """
        The single min < max validator (ADR 0022). Model clean() runs it for
        every ModelForm path; non-model paths (upload wizard, plain forms)
        delegate to it instead of carrying their own copy.
        """
        if value_min is not None and value_max is not None and value_min >= value_max:
            raise ValidationError({"value_max": _("Maximum value must be greater than minimum value.")})

    @classmethod
    def validate_scale_type(cls, scale_type, value_min):
        """A ``log`` encoding curve needs a stored minimum above zero (#787):
        it encodes ``log(v / vmin)``, which has no meaning otherwise."""
        if scale_type == cls.ScaleType.LOG and value_min is not None and value_min <= 0:
            raise ValidationError({"scale_type": _("A logarithmic encoding needs a minimum above zero.")})

    def clean(self):
        super().clean()
        errors = {}

        if not self.is_instant and not self.statistic_window:
            errors["statistic_window"] = _("Say which window this statistic is over.")

        for validate in (
            lambda: self.validate_value_range(self.value_min, self.value_max),
            lambda: self.validate_scale_type(self.scale_type, self.value_min),
        ):
            try:
                validate()
            except ValidationError as e:
                errors.update(e.error_dict)

        if self.source_unit_id is None:
            errors["source_unit"] = _("Every variable states the unit its data is stored in.")
        elif self.unit_id is not None:
            try:
                self.conversion()
            except ConversionRefused as e:
                errors["unit"] = str(e)

        # Sources / transform consistency — only for a Variable read out of
        # ingested files; a harvested or recipe-written one has no bands to name.
        if self._reads_from_files():
            errors.update(self._sources_errors())
        else:
            errors.update(self._store_errors())

        if errors:
            raise ValidationError(errors)

    def conversion(self) -> Conversion:
        """The conversion ``source_unit → unit`` worked out from the units now
        set, or :class:`ConversionRefused` (ADR 0046 §4)."""
        return Conversion.between(self.source_unit.symbol, self.unit.symbol, water=self.water)

    @property
    def served_conversion(self) -> Conversion:
        """The conversion into the served unit as stored (``scale``, ``offset``)."""
        return Conversion(self.scale, self.offset, self.unit.symbol if self.unit_id else "")

    def save(self, *args, **kwargs):
        """Recompute ``scale`` and ``offset`` (ADR 0046 §5). A conversion that
        is refused is never stored: the save raises, as ``clean()`` would.

        A source unit is locked once the Variable has an asset (ADR 0046 §8):
        whatever saves it, a change raises. :meth:`correct_source_unit` is the
        one way past the lock."""
        if self.is_instant:
            # An instantaneous value is over no window.
            self.statistic_window = ""
        update_fields = kwargs.get("update_fields")
        if not getattr(self, "_correcting_source_unit", False) and (
            update_fields is None or "source_unit" in update_fields
        ):
            self._refuse_relabelling_held_bytes()
        if self.source_unit_id is not None and self.unit_id is not None:
            try:
                self._take(self.conversion())
            except ConversionRefused as e:
                raise ValidationError({"unit": str(e)}) from None
        if update_fields is not None and {"source_unit", "unit", "water"} & set(update_fields):
            kwargs["update_fields"] = {*update_fields, "scale", "offset"}
        super().save(*args, **kwargs)

    def source_unit_locked(self) -> bool:
        """Whether bytes this Variable describes exist, so its source unit says
        what they hold and only :meth:`correct_source_unit` may change it."""
        return self.pk is not None and self.assets.exists()

    def _refuse_relabelling_held_bytes(self):
        if self.pk is None or self.source_unit_id is None:
            return
        stored = type(self).objects.filter(pk=self.pk).values_list("source_unit_id", flat=True).first()
        if stored is None or stored == self.source_unit_id or not self.source_unit_locked():
            return
        raise ValidationError(
            {
                "source_unit": _(
                    "This variable already has data, which its source unit describes. "
                    "Use Correct source unit to change it."
                )
            }
        )

    def correct_source_unit(self, source_unit):
        """Say that the bytes this Variable holds are in ``source_unit`` — the
        Correct source unit action (ADR 0046 §8). Nothing is converted: every
        stored value, range, stop and statistic is reinterpreted."""
        self.source_unit = source_unit
        self._correcting_source_unit = True
        try:
            self.save(update_fields=["source_unit"])
        finally:
            self._correcting_source_unit = False

    def _reads_from_files(self) -> bool:
        try:
            collection = self.collection
        except ObjectDoesNotExist:
            return True
        return collection.reads_variables_from_files

    def _sources_errors(self) -> dict:
        errors = {}
        block_types = [block.block_type for block in self.sources] if self.sources else []

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
        return errors

    def _store_errors(self) -> dict:
        """A Variable of a store collection (ADR 0048): one its feed maps shows
        its array as it is, and a vector names two of the arrays the feed maps,
        both stored in one unit — a speed's own. Of any other collection not
        read from files there is nothing to check."""
        try:
            mapping = self.collection.store_mapping()
        except ObjectDoesNotExist:
            return {}
        if mapping is None:
            return {}
        if self.pk is not None and any(variable.pk == self.pk for variable in mapping.values()):
            if self.transform_type != self.TransformType.PASSTHROUGH:
                return {
                    "transform_type": _(
                        "This Variable shows an array of the store as it is. Add a new Variable to compute "
                        "something from it."
                    )
                }
            return {}
        if self.transform_type not in VECTOR_TRANSFORMS:
            return {}
        vector = self.vector_components
        if vector is None:
            missing = {"u_component", "v_component"} - {kind for kind, name in self._named_sources().items() if name}
            return {"sources": f"{self.get_transform_type_display()} requires {', '.join(sorted(missing))} source(s)."}
        components = vector.arrays
        unmapped = [name for name in components if name not in mapping]
        if unmapped:
            return {
                "sources": _(
                    "%(names)s is not an array this collection's harvest feed maps. Map it first, "
                    "then name it here by its path in the store."
                )
                % {"names": ", ".join(unmapped)}
            }
        if not vector.one_selection:
            from georiva.core.selection import described, split_field_key

            said = [described(split_field_key(name)[1]) or _("the whole array") for name in components]
            return {
                "sources": _("Its two parts are read at different values (%(u)s; %(v)s), so they are not one vector's.")
                % {"u": said[0], "v": said[1]}
            }
        u, v = (mapping[name] for name in components)
        if u.source_unit_id != v.source_unit_id:
            return {
                "sources": _("%(u)s and %(v)s are stored in different units, so they are not one vector's parts.")
                % {"u": components[0], "v": components[1]}
            }
        if self.transform_type == self.TransformType.VECTOR_MAGNITUDE and self.source_unit_id != u.source_unit_id:
            return {"source_unit": _("A speed is stored in its parts' unit: %(unit)s.") % {"unit": u.source_unit}}
        return {}

    @property
    def is_derived(self):
        return self.transform_type != self.TransformType.PASSTHROUGH

    def _named_sources(self) -> dict[str, str]:
        """Each of its sources' kind (``primary``, ``u_component``, …) with the name it gives."""
        return {block.block_type: block.value["source_name"] for block in self.sources or []}

    @property
    def vector_components(self) -> VectorComponents | None:
        """Its u and v and which quantity of them it is, for a vector Variable
        that names both; ``None`` for any other."""
        if self.transform_type not in VECTOR_TRANSFORMS:
            return None
        named = self._named_sources()
        u, v = named.get("u_component"), named.get("v_component")
        if not u or not v:
            return None
        return VectorComponents(u=u, v=v, transform=self.transform_type)

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
    def new_style_is_default(self) -> bool:
        """Whether a style added now becomes the default: only the first does,
        since the first style is what serves; later ones stage until promoted.
        So only that style may also set the range and curve every style
        shares (#791, #800). Reads prefetched styles without a query."""
        return not self.styles.exists()

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
        display, and previews all read this one (ADR 0022). In the source unit,
        like every number stored about values (ADR 0046 §3)."""
        return self.value_min, self.value_max

    # -------- served numbers (ADR 0046 §2, §15) --------

    def served(self, value):
        """A stored (source-unit) number in the served unit; ``None`` stays ``None``.

        Rounded to 12 significant digits, so 300 K reads 26.85 °C rather than
        26.850000000000023: these are numbers people read.
        """
        return self.served_conversion.forward(value)

    def served_spread(self, value):
        """A stored spread (a standard deviation) in the served unit: by the scale
        alone, never the offset (ADR 0046 §7); ``None`` stays ``None``."""
        return self.served_conversion.spread(value)

    @property
    def served_range(self) -> tuple:
        """:attr:`value_range` in the served unit."""
        return self.served(self.value_min), self.served(self.value_max)

    def served_palette(self, palette: list) -> list:
        """``[[value, color], ...]`` stops with each value in the served unit."""
        return [[self.served(value), color] for value, color in palette]

    # -------- the form boundary (ADR 0046 §3) --------
    #
    # Operators type and read ranges and stops in the served unit; they are
    # stored in the source unit. Forms convert both ways through these.

    def stored(self, value):
        """A served-unit number as stored (in the source unit); ``None`` stays ``None``.

        The inverse of :meth:`served`, rounded the same way, so 0 °C stores
        273.15 K rather than 273.15000000000003.
        """
        return self.served_conversion.inverse(value)

    def served_stops(self, stops: list) -> list:
        """``[{"value", "color"}, ...]`` stops with each value in the served unit."""
        return [{**stop, "value": self.served(stop["value"])} for stop in stops]

    def stored_stops(self, stops: list) -> list:
        """Served-unit ``[{"value", "color"}, ...]`` stops as stored."""
        return [{**stop, "value": self.stored(stop["value"])} for stop in stops]

    def refresh_conversion(self):
        """Recompute ``scale`` and ``offset`` from the units now set, before
        :meth:`stored` is asked of a Variable not yet saved with them. A refused
        conversion raises as :meth:`save` would."""
        try:
            self._take(self.conversion())
        except ConversionRefused as e:
            raise ValidationError({"unit": str(e)}) from None

    def _take(self, conversion: Conversion):
        self.scale, self.offset = conversion.scale, conversion.offset

    def set_served_range(self, value_min, value_max):
        """Set the range from served-unit numbers, stored in the source unit."""
        self.refresh_conversion()
        self.value_min, self.value_max = self.stored(value_min), self.stored(value_max)


#: The transforms that compute a Variable from a u and a v.
VECTOR_TRANSFORMS = (Variable.TransformType.VECTOR_MAGNITUDE, Variable.TransformType.VECTOR_DIRECTION)
