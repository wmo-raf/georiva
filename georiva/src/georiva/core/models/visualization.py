import math

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.db import models, transaction
from django.utils.html import format_html
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, InlinePanel
from wagtail.models import Orderable

from georiva.core.unit_utils import significant
from georiva.utils.slugs import free_slug

from .variable import Variable

#: '#RRGGBB' or '#RRGGBBAA', with the '#' and shorthand '#RGB'/'#RGBA' allowed —
#: the same shapes :func:`hex_to_rgba_list` accepts.
HEX_COLOR_VALIDATOR = RegexValidator(
    regex=r"^#?(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$",
    message="Enter a hex color such as '#RRGGBB' or '#RRGGBBAA'.",
)


def css_hex(color: str) -> str:
    """A stored hex colour as CSS reads it: with its ``#``, which storage may omit."""
    color = color.strip()
    return color if color.startswith("#") else f"#{color}"


def hex_to_rgba_list(hex_color: str):
    """
    '#RRGGBB' -> [r,g,b]
    '#RRGGBBAA' -> [r,g,b,a]    (alpha is 0..255, exactly what WeatherLayers expects)
    Also accepts 'RRGGBB' / 'RRGGBBAA' without '#', and shorthand '#RGB' / '#RGBA'.
    """
    if not hex_color:
        raise ValueError("Empty hex color")

    h = hex_color.strip().lstrip("#")

    if len(h) in (3, 4):
        h = "".join([c * 2 for c in h])

    if len(h) not in (6, 8):
        raise ValueError(f"Invalid hex color length: {hex_color}")

    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)

    if len(h) == 8:
        a = int(h[6:8], 16)
        return [r, g, b, a]

    return [r, g, b]


def _rgba_to_hex(rgba) -> str:
    """[r,g,b] or [r,g,b,a] -> '#rrggbb' / '#rrggbbaa' (alpha only when it says
    something — a fully opaque channel is the default and stays implicit)."""
    r, g, b = rgba[0], rgba[1], rgba[2]
    if len(rgba) == 4 and rgba[3] != 255:
        return f"#{r:02x}{g:02x}{b:02x}{rgba[3]:02x}"
    return f"#{r:02x}{g:02x}{b:02x}"


def _spread_positions(stops) -> list:
    """0–1 positions for a ramp's stops, one reading shared by previews and
    snapshot generation — so what a preview shows is what an application
    materializes.

    Stops without explicit positions are spread evenly. Positions are forced
    non-decreasing left to right, because CSS silently clamps out-of-order
    gradient stops — a preview that let that happen would misrepresent the
    ramp it is previewing.
    """
    if len(stops) == 1:
        return [0.0]
    last = len(stops) - 1
    positions = [stop.position if stop.position is not None else i / last for i, stop in enumerate(stops)]
    floor = 0.0
    for i, position in enumerate(positions):
        floor = positions[i] = max(floor, position)
    return positions


def _swatch_html(gradient: str):
    """A gradient swatch for admin listings, or "" for nothing to show."""
    if not gradient:
        return ""
    return format_html(
        '<span style="display: inline-block; '
        "width: 120px; height: 14px; border-radius: 3px; "
        'border: 1px solid var(--w-color-border-furniture); background: {};">'
        "</span>",
        gradient,
    )


def _ramp_colors_and_positions(ramp):
    """The ramp as parallel (rgba colors, 0–1 positions) lists."""
    stops = list(ramp.stops.all())
    if not stops:
        return [], []
    colors = [hex_to_rgba_list(stop.hex_value) for stop in stops]
    return colors, _spread_positions(stops)


def _sample_ramp(colors, positions, t: float):
    """The ramp's color at fraction ``t`` (0–1), interpolating linearly
    between neighbouring stops. Clamps outside the positioned span."""
    if t <= positions[0]:
        return list(colors[0])
    if t >= positions[-1]:
        return list(colors[-1])
    for j in range(len(positions) - 1):
        if positions[j] <= t <= positions[j + 1]:
            span = positions[j + 1] - positions[j]
            frac = (t - positions[j]) / span if span > 0 else 0.0
            a = colors[j] + [255] * (4 - len(colors[j]))
            b = colors[j + 1] + [255] * (4 - len(colors[j + 1]))
            return [round(a[k] + frac * (b[k] - a[k])) for k in range(4)]
    return list(colors[-1])


def _scaled_position(value_min: float, value_max: float, t: float, scale_type: str) -> float:
    """The value at fraction ``t`` (0–1) of the range, spaced by ``scale_type``.

    ``log`` spaces geometrically (a range that does not sit wholly above zero
    has no logarithm, so it spaces linearly); ``sqrt`` packs stops towards the
    low end the way a square-root colour scale stretches it; ``linear`` and
    ``diverging`` space evenly — a diverging range is centred by the operator.
    Rounded to 12 significant digits: these are numbers people read.
    """
    if scale_type == "log" and value_min > 0:
        value = value_min * (value_max / value_min) ** t
    elif scale_type == "sqrt":
        value = value_min + t * t * (value_max - value_min)
    else:
        value = value_min + t * (value_max - value_min)
    return significant(value)


def generate_stops(
    ramp,
    value_min: float,
    value_max: float,
    mode: str = "continuous",
    steps: int = None,
    scale_type: str = "linear",
) -> list:
    """Apply ``ramp`` over a value range: the snapshot-generation seam of
    ADR 0022. Returns ``[{"value": float, "color": "#rrggbb(aa)"}, ...]``.

    Continuous mode stretches the ramp's own colors over the range, one stop
    per color at its (evenly spread or declared) position. Stepped mode cuts
    the range into ``steps`` equal classes and gives each one flat color —
    sampled along the ramp, or cycled verbatim for a qualitative ramp, whose
    colors are categories that must never blend — expressing each class as two
    stops sharing its boundaries so the edges stay hard through any linear
    interpolation downstream.

    ``scale_type`` spaces the stops (or class boundaries) over the range in
    whatever unit the range is given in — the served unit, when applied for a
    Variable (:func:`served_ramp_stops`).
    """
    colors, positions = _ramp_colors_and_positions(ramp)
    if not colors:
        return []

    def at(t):
        return _scaled_position(value_min, value_max, t, scale_type)

    if mode == VariableStyle.Mode.STEPPED:
        count = max(int(steps or 0), 1)
        if ramp.ramp_type == ColorRamp.RampType.QUALITATIVE:
            class_colors = [colors[i % len(colors)] for i in range(count)]
        else:
            class_colors = [
                _sample_ramp(colors, positions, i / (count - 1) if count > 1 else 0.5) for i in range(count)
            ]
        stops = []
        for i, color in enumerate(class_colors):
            hex_color = _rgba_to_hex(color)
            stops.append({"value": at(i / count), "color": hex_color})
            stops.append({"value": at((i + 1) / count), "color": hex_color})
        return stops

    return [
        {"value": at(position), "color": _rgba_to_hex(color)} for color, position in zip(colors, positions, strict=True)
    ]


def reversed_colors(stops: list) -> list:
    """``stops`` with their colors in reverse order against fixed values — what
    reversing a ramp means once it is laid over a range. A stepped snapshot
    stays clean class pairs: a doubled list reversed is still doubled."""
    colors = [stop["color"] for stop in reversed(stops)]
    return [{**stop, "color": color} for stop, color in zip(stops, colors, strict=True)]


def ramp_stops_over(
    ramp, value_min: float, value_max: float, *, mode="continuous", steps=None, scale_type="linear", reverse=False
) -> list:
    """``ramp`` laid over ``value_min``–``value_max`` (:func:`generate_stops`),
    its colours run the other way when ``reverse``. Pure: it reads the ramp's
    stops and type only, so a migration can call it with a historical ramp."""
    stops = generate_stops(ramp, value_min, value_max, mode=mode, steps=steps, scale_type=scale_type)
    return reversed_colors(stops) if reverse else stops


def served_ramp_stops(ramp, variable, mode: str = "continuous", steps: int = None, reverse: bool = False) -> list:
    """``ramp`` applied over ``variable``'s range in its served unit (ADR 0046 §3).

    ``scale_type`` spaces stops as the operator sees them, so the ramp is laid
    over the served range; the stops come back in the served unit, for a form.
    Store them with ``variable.stored_stops(...)``. ``reverse`` runs the ramp's
    colors the other way over the same values.
    """
    low, high = variable.served_range
    return ramp_stops_over(ramp, low, high, mode=mode, steps=steps, scale_type=variable.scale_type, reverse=reverse)


def band_stops(levels: list, colors: list, convert) -> list:
    """A Style template's bands as stepped stops: each band two stops, at its
    lower and upper level (``convert``-ed) in the band's colour. Pure, so a
    migration can call it."""
    stops = []
    for low, high, color in zip(levels[:-1], levels[1:], colors, strict=True):
        stops.append({"value": convert(low), "color": css_hex(color)})
        stops.append({"value": convert(high), "color": css_hex(color)})
    return stops


def _same_stops(a: list, b: list) -> bool:
    """Whether two snapshots in one unit are the same stops: values equal to
    well within what any form displays (a unit round trip may move the last
    float digit), colors equal ignoring case."""
    if len(a) != len(b):
        return False
    for x, y in zip(a, b, strict=True):
        if x["color"].lower() != y["color"].lower():
            return False
        if not math.isclose(x["value"], y["value"], rel_tol=1e-9, abs_tol=1e-9):
            return False
    return True


def recorded_origin(stops: list, *, template_stops=None, ramp_stops=None) -> tuple:
    """``(origin, reverse)`` for a style made before styles recorded how they
    were made (#806): ``template`` when ``stops`` are ``template_stops``,
    ``ramp`` when they are ``ramp_stops`` forwards (``reverse`` False) or
    reversed (True), ``custom`` otherwise. All three in the same unit; ``None``
    for a link the style does not have. What the backfill migration decides by.
    """
    if template_stops is not None and stops and _same_stops(stops, template_stops):
        return "template", False
    if ramp_stops is not None and stops:
        if _same_stops(stops, ramp_stops):
            return "ramp", False
        if _same_stops(stops, reversed_colors(ramp_stops)):
            return "ramp", True
    return "custom", False


class ColorRamp(ClusterableModel):
    """A value-free color ramp: pure aesthetics, reusable across variables.

    The catalog half of the two-layer styling model (ADR 0022): ordered colors
    with optional 0–1 positions and a type — deliberately **no physical
    values**. Applying a ramp over a variable's range is what produces
    concrete value→color stops; the ramp itself stays reusable because it
    never mentions anybody's range.

    Tiered like other shared reference data (ADR 0011): a ramp with no
    organisation is the instance-wide tier every organisation draws on and
    only the instance admin edits; a ramp with one belongs to that institution
    alone. A data migration seeds the instance-wide tier with a curated,
    colorblind-aware catalog under matplotlib-compatible names.
    """

    ORGANISATION_LOOKUP = "organisation"
    ORGANISATION_GLOBAL_TIER = True

    organisation = models.ForeignKey(
        "organisations.Organisation",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="color_ramps",
        help_text=(
            "The organisation this ramp belongs to. Left empty, it is part of "
            "the instance-wide ramp catalog every organisation can use."
        ),
    )
    name = models.CharField(max_length=255)

    class RampType(models.TextChoices):
        SEQUENTIAL = "sequential", "Sequential"
        DIVERGING = "diverging", "Diverging"
        QUALITATIVE = "qualitative", "Qualitative"

    ramp_type = models.CharField(
        max_length=20,
        choices=RampType.choices,
        default=RampType.SEQUENTIAL,
    )

    panels = [
        FieldPanel("name"),
        FieldPanel("ramp_type"),
        InlinePanel("stops", heading="Colors", label="Color"),
    ]

    class Meta:
        ordering = ["name"]
        verbose_name = "Color Ramp"
        verbose_name_plural = "Color Ramps"
        constraints = [
            # Names are how operators (and plugins) refer to ramps in a catalog
            # that mixes both tiers, so a name is unique within its tier —
            # while different organisations may each have a "Rainfall". Two
            # constraints because the first cannot cover the instance-wide
            # tier: Postgres treats NULLs as distinct, so an unqualified pair
            # would let any number of ownerless "viridis"es through.
            models.UniqueConstraint(
                fields=["organisation", "name"],
                name="unique_ramp_name_per_organisation",
            ),
            models.UniqueConstraint(
                fields=["name"],
                condition=models.Q(organisation__isnull=True),
                name="unique_global_ramp_name",
            ),
        ]

    def __str__(self):
        return self.name

    def owner_label(self):
        """Which tier this ramp is in, for a listing that shows both."""
        return self.organisation.name if self.organisation_id else "Instance-wide"

    owner_label.short_description = "Owner"

    # -------- presentation helpers --------

    def css_gradient(self):
        """The ramp as a CSS ``linear-gradient()``, for previews.

        Stops without explicit positions are spread evenly (see
        :func:`_spread_positions`); a qualitative ramp renders as hard-edged
        blocks rather than a blend, because its colors are categories, not
        points on a continuum.
        """
        stops = list(self.stops.all())
        if not stops:
            return ""
        if len(stops) == 1:
            color = stops[0].css_color()
            return f"linear-gradient(to right, {color}, {color})"

        if self.ramp_type == self.RampType.QUALITATIVE:
            width = 100 / len(stops)
            parts = [f"{stop.css_color()} {i * width:g}% {(i + 1) * width:g}%" for i, stop in enumerate(stops)]
        else:
            parts = [
                f"{stop.css_color()} {position * 100:g}%"
                for stop, position in zip(stops, _spread_positions(stops), strict=True)
            ]
        return f"linear-gradient(to right, {', '.join(parts)})"

    def preview(self):
        """A gradient swatch for the admin listing."""
        return _swatch_html(self.css_gradient())

    preview.short_description = "Preview"


class ColorRampStop(Orderable):
    # Owned by whoever owns the ramp — including nobody, for the instance-wide
    # tier. See ColorRamp.
    ORGANISATION_LOOKUP = "ramp__organisation"
    ORGANISATION_GLOBAL_TIER = True

    ramp = ParentalKey(ColorRamp, related_name="stops", on_delete=models.CASCADE)
    hex_value = models.CharField(
        max_length=9,
        validators=[HEX_COLOR_VALIDATOR],
        help_text="Hex '#RRGGBB' or '#RRGGBBAA' (alpha optional)",
    )
    position = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text=("Where along the ramp this color sits, 0–1. Left empty, colors are spread evenly."),
    )

    panels = [
        FieldPanel("hex_value"),
        FieldPanel("position"),
    ]

    def css_color(self):
        return css_hex(self.hex_value)

    def __str__(self):
        if self.position is not None:
            return f"{self.hex_value} @ {self.position}"
        return self.hex_value


class OutOfRange(models.TextChoices):
    """What a style does with a value outside its stops (#784): draw it in
    the end colour, as every style always has, or leave it undrawn."""

    CLAMP = "clamp", "Clamp to the end colour"
    TRANSPARENT = "transparent", "Transparent"


class StyleTemplateQuerySet(models.QuerySet):
    def in_catalogue_order(self):
        """Grouped by quantity in the order the picker lists them (Temperature
        first, Other last) rather than alphabetically by key, then by name."""
        rank = models.Case(
            *(models.When(quantity=value, then=i) for i, value in enumerate(StyleTemplate.Quantity.values)),
            default=len(StyleTemplate.Quantity.values),
            output_field=models.IntegerField(),
        )
        return self.annotate(_quantity_rank=rank).order_by("_quantity_rank", "name")


class StyleTemplate(TimeStampedModel):
    """A reusable, value-anchored set of levels and band colours in a declared unit.

    Where a :class:`ColorRamp` is value-free, a template *is* values: n+1
    ascending level boundaries and one colour per band between them, in the
    unit the template declares — precipitation bands at 0.5 / 1 / 2 / 5 mm,
    temperature in 4 °C steps. It is applied into a :class:`VariableStyle` as a
    stops snapshot, converted into the Variable's source unit, and never
    live-linked (ADR 0022's snapshot semantics). Stepped only.

    Tiered like the ramp catalog (ADR 0011): no organisation is the
    instance-wide tier every organisation reads and only the instance admin
    writes. One rule goes further: a template with an ``origin`` was shipped by
    GeoRiva (the earthkit-plots or the Magics catalogue) and is read-only to everyone, the
    instance admin included, so a later release can refresh it. Anyone adapts
    one by copying it into their organisation.
    """

    ORGANISATION_LOOKUP = "organisation"
    ORGANISATION_GLOBAL_TIER = True

    class Quantity(models.TextChoices):
        TEMPERATURE = "temperature", "Temperature"
        PRECIPITATION = "precipitation", "Precipitation"
        WIND = "wind", "Wind"
        PRESSURE = "pressure", "Pressure"
        HUMIDITY = "humidity", "Humidity"
        CLOUD = "cloud", "Cloud"
        CONVECTION = "convection", "Convection"
        OCEAN = "ocean", "Ocean"
        AIR_QUALITY = "air_quality", "Air quality"
        FIRE = "fire", "Fire"
        ANOMALY = "anomaly", "Anomaly"
        PROBABILITY = "probability", "Probability"
        OTHER = "other", "Other"

    organisation = models.ForeignKey(
        "organisations.Organisation",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="style_templates",
        help_text=(
            "The organisation this template belongs to. Left empty, it is part of "
            "the instance-wide catalogue every organisation can use."
        ),
    )
    name = models.CharField(max_length=255)
    slug = models.SlugField(
        max_length=255,
        help_text="How plugins and the catalogue refer to this template. Unique within its tier.",
    )
    quantity = models.CharField(
        max_length=32,
        choices=Quantity.choices,
        default=Quantity.OTHER,
        help_text="The group this template is listed under in pickers.",
    )
    #: n+1 strictly ascending band boundaries, in ``unit``.
    levels = models.JSONField(default=list)
    #: n hex colours, one per band between consecutive levels.
    colors = models.JSONField(default=list)
    unit = models.ForeignKey(
        "georivacore.Unit",
        on_delete=models.PROTECT,
        related_name="+",
        help_text="The unit the levels are in.",
    )
    under = models.CharField(
        max_length=16,
        choices=OutOfRange.choices,
        default=OutOfRange.CLAMP,
        help_text="What a value below the first level is drawn as.",
    )
    over = models.CharField(
        max_length=16,
        choices=OutOfRange.choices,
        default=OutOfRange.CLAMP,
        help_text="What a value above the last level is drawn as.",
    )
    scale_type = models.CharField(
        max_length=20,
        choices=Variable.ScaleType.choices,
        default=Variable.ScaleType.LINEAR,
        help_text="The encoding curve recommended for a Variable styled with this template.",
    )
    origin = models.CharField(
        max_length=64,
        blank=True,
        default="",
        help_text="Where a shipped template comes from, e.g. 'earthkit-plots'. Empty for one an organisation authored.",
    )
    source_name = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The upstream style this template was converted from.",
    )

    objects = StyleTemplateQuerySet.as_manager()

    class Meta:
        ordering = ["name"]  # the catalogue lists through in_catalogue_order()
        verbose_name = "Style Template"
        verbose_name_plural = "Style Templates"
        constraints = [
            # As for ramp names: unique within a tier, and two constraints
            # because Postgres treats the instance-wide tier's NULLs as distinct.
            models.UniqueConstraint(
                fields=["organisation", "slug"],
                name="unique_style_template_slug_per_organisation",
            ),
            models.UniqueConstraint(
                fields=["slug"],
                condition=models.Q(organisation__isnull=True),
                name="unique_global_style_template_slug",
            ),
        ]

    def __str__(self):
        return self.name

    @property
    def is_shipped(self):
        """Whether GeoRiva shipped this template — and so nobody edits it."""
        return bool(self.origin)

    def copy_to(self, organisation):
        """An editable copy of this template in ``organisation``'s tier.

        The copy is the organisation's own: it has no ``origin``, so nothing
        about it is read-only. Its ``source_name`` stays, as lineage. The slug
        is kept when that tier has it free, and suffixed ``-copy``,
        ``-copy-2``… when it does not.
        """
        taken = set(type(self).objects.filter(organisation=organisation).values_list("slug", flat=True))
        slug = free_slug(self.slug, taken, stem=f"{self.slug}-copy")
        copy = type(self)(
            organisation=organisation,
            name=self.name,
            slug=slug,
            quantity=self.quantity,
            levels=list(self.levels),
            colors=list(self.colors),
            unit=self.unit,
            under=self.under,
            over=self.over,
            scale_type=self.scale_type,
            origin="",
            source_name=self.source_name,
        )
        copy.full_clean()
        copy.save()
        return copy

    def clean(self):
        super().clean()
        errors = {}
        levels = self.levels
        if not isinstance(levels, list) or not all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in levels
        ):
            errors["levels"] = "Levels must be a list of numbers."
        elif len(levels) < 3:
            # A template becomes a stepped style, and a stepped style is at least two classes.
            errors["levels"] = "A template needs at least three levels: two bands."
        elif any(b <= a for a, b in zip(levels, levels[1:], strict=False)):
            errors["levels"] = "Levels must be strictly ascending."

        colors = self.colors
        if not isinstance(colors, list) or not all(isinstance(c, str) for c in colors):
            errors["colors"] = "Colours must be a list of hex colours."
        else:
            for color in colors:
                try:
                    HEX_COLOR_VALIDATOR(color)
                except ValidationError:
                    errors["colors"] = f"Not a hex colour: {color!r}"
                    break
            if "colors" not in errors and "levels" not in errors and len(colors) != len(levels) - 1:
                errors["colors"] = (
                    f"{len(levels) - 1} bands need exactly {len(levels) - 1} colours, one per band; got {len(colors)}."
                )
        if errors:
            raise ValidationError(errors)

    def owner_label(self):
        """Which tier this template is in, for a listing that shows both."""
        if self.organisation_id:
            return self.organisation.name
        return "Instance-wide (shipped)" if self.is_shipped else "Instance-wide"

    owner_label.short_description = "Owner"

    #: How a shipped catalogue is named to an operator, by ``origin``.
    ORIGIN_LABELS = {"earthkit-plots": "earthkit-plots", "magics": "Magics"}

    def source_label(self):
        """Where a shipped template comes from and the upstream style it was
        converted from, "Magics · sh_blured_f05t300lst"; empty for one an
        organisation authored (#811)."""
        if not self.is_shipped:
            return ""
        origin = self.ORIGIN_LABELS.get(self.origin, self.origin)
        return f"{origin} · {self.source_name}" if self.source_name else origin

    source_label.short_description = "Source"

    # -------- presentation helpers --------

    def css_gradient(self):
        """The bands as equal-width hard-edged blocks, for previews: a band is
        a class, so a narrow one is drawn as readably as a wide one."""
        colors = [css_hex(c) for c in (self.colors or [])]
        if not colors:
            return ""
        width = 100 / len(colors)
        parts = [f"{color} {i * width:g}% {(i + 1) * width:g}%" for i, color in enumerate(colors)]
        return f"linear-gradient(to right, {', '.join(parts)})"

    def preview(self):
        """A band swatch for the admin listing."""
        return _swatch_html(self.css_gradient())

    preview.short_description = "Bands"

    def levels_label(self):
        """The levels and unit, as an operator reads them: "0.5, 1, 2, 5 mm"."""
        levels = ", ".join(f"{v:g}" for v in self.levels or [])
        return f"{levels} {self.unit.symbol}" if levels else ""

    levels_label.short_description = "Levels"


class VariableStyle(TimeStampedModel):
    """An applied style: one variable's value→color contract, snapshotted.

    The semantic half of the two-layer styling model (ADR 0022). Where a
    :class:`ColorRamp` is value-free aesthetics, a style is that ramp *applied*
    over one variable's range into absolute value→color stops — and then owned
    by the operator, who may fine-tune any stop, including pinning physical
    thresholds like 0 °C. The snapshot never live-links back to the ramp or the
    range: editing either leaves the stops untouched until the style is saved.

    Each style records its :class:`Origin` (#806): a ``ramp`` style's stops are
    regenerated from its ramp on every save, a ``template`` style's are its
    template's bands until another is picked in place (#807), and a
    ``custom`` style's — the only ones that can be fine-tuned — are what the
    operator saved. A ramp or
    template style becomes custom only through "Customise stops".

    A variable may carry several named styles — an official public palette
    beside an analyst's — with exactly one default, enforced by a partial
    unique constraint so a second default is impossible to write, not merely
    impolite.
    """

    ORGANISATION_LOOKUP = "variable__collection__catalog__organisation"

    class Mode(models.TextChoices):
        CONTINUOUS = "continuous", "Continuous gradient"
        STEPPED = "stepped", "Stepped classes"

    class Origin(models.TextChoices):
        """How a style was made, and so what drives its stops (#806).

        A ``template`` style's stops are its template's bands, never changed by
        a save unless another template was picked in its editor (#807); a
        ``ramp`` style's are always its ramp, mode, steps and reverse over the
        Variable's range, regenerated on every save; a ``custom``
        style's are whatever the operator saved. Set when the style is made,
        and changed only by :meth:`customise` or :meth:`start_over`."""

        TEMPLATE = "template", "From a template"
        RAMP = "ramp", "From a ramp"
        CUSTOM = "custom", "Custom"

    variable = models.ForeignKey(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="styles",
    )
    name = models.CharField(max_length=255)
    slug = models.SlugField(
        max_length=255,
        help_text=(
            "URL-safe identifier for this style, used to select it on tile and config URLs. Unique within the variable."
        ),
    )
    is_default = models.BooleanField(
        default=False,
        help_text="The style served when no style is asked for by name.",
    )
    ramp = models.ForeignKey(
        ColorRamp,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text=(
            "The ramp a ramp style's stops are generated from on every save; "
            "for a custom style, history only. The stops below are what renders."
        ),
    )
    template = models.ForeignKey(
        StyleTemplate,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text=(
            "The Style template this style was applied from (#791) — lineage "
            "only; the stops below are a snapshot and never follow the template."
        ),
    )
    origin = models.CharField(
        max_length=16,
        choices=Origin.choices,
        default=Origin.CUSTOM,
        help_text=(
            "How this style was made: from a template, from a ramp, or custom. "
            "A custom style keeps any ramp or template link as history only."
        ),
    )
    mode = models.CharField(
        max_length=20,
        choices=Mode.choices,
        default=Mode.CONTINUOUS,
    )
    reverse = models.BooleanField(
        default=False,
        help_text="A ramp style: whether its ramp's colours run the other way over the range.",
    )
    steps = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(2)],
        help_text="Number of classes for stepped mode.",
    )
    #: The materialized snapshot: ``[{"value": float, "color": "#rrggbb(aa)"},
    #: ...]`` in ascending value order. Absolute physical values — this is the
    #: one place styling and the variable's semantics meet.
    stops = models.JSONField(default=list, blank=True)
    #: What values beyond the stops are drawn as (#788): the end colour, or
    #: nothing. Carried into the style's colour table, so every renderer agrees.
    under = models.CharField(
        max_length=16,
        choices=OutOfRange.choices,
        default=OutOfRange.CLAMP,
        help_text="What a value below the first stop is drawn as.",
    )
    over = models.CharField(
        max_length=16,
        choices=OutOfRange.choices,
        default=OutOfRange.CLAMP,
        help_text="What a value above the last stop is drawn as.",
    )

    class Meta:
        ordering = ["-is_default", "name"]
        verbose_name = "Variable Style"
        verbose_name_plural = "Variable Styles"
        constraints = [
            models.UniqueConstraint(
                fields=["variable"],
                condition=models.Q(is_default=True),
                name="unique_default_style_per_variable",
            ),
            models.UniqueConstraint(
                fields=["variable", "slug"],
                name="unique_style_slug_per_variable",
            ),
        ]

    def __str__(self):
        return f"{self.variable}:{self.slug}"

    def clean(self):
        super().clean()
        errors = {}
        if self.mode == self.Mode.STEPPED and not self.steps:
            errors["steps"] = "Stepped mode needs a number of classes."
        for entry in self.stops or []:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("value"), (int, float))
                or not isinstance(entry.get("color"), str)
            ):
                errors["stops"] = 'Each stop must be {"value": <number>, "color": "#RRGGBB"}.'
                break
            try:
                HEX_COLOR_VALIDATOR(entry["color"])
            except ValidationError:
                errors["stops"] = f"Not a hex color: {entry['color']!r}"
                break
        if "stops" not in errors and self.stops:
            values = [entry["value"] for entry in self.stops]
            # strict=False, unlike every other zip here: this is the pairwise
            # idiom, so the two sides differ in length by one by construction.
            if any(b < a for a, b in zip(values, values[1:], strict=False)):
                # Equal neighbours stay legal: stepped snapshots share their
                # class-boundary values by construction.
                errors["stops"] = "Stop values must be in ascending order."
        if errors:
            raise ValidationError(errors)

    # -------- default management --------

    def promote_to_default(self):
        """Make this style the variable's default, demoting the current one.

        The partial unique constraint makes a second default impossible to
        write, so the demote and the promote have to land together — anything
        else either raises or leaves the variable defaultless mid-flight.
        """
        with transaction.atomic():
            type(self).objects.filter(variable=self.variable, is_default=True).exclude(pk=self.pk).update(
                is_default=False
            )
            if not self.is_default:
                self.is_default = True
                self.save(update_fields=["is_default", "modified"])

    # -------- snapshot generation --------

    def ramp_stops(self) -> list:
        """The stops the ramp, mode, steps and reverse give over the
        variable's current range — laid out in the served unit, stored in the
        source unit (ADR 0046 §3). Computed only; nothing is set."""
        if self.ramp is None:
            raise ValueError("This style has no ramp to apply.")
        variable = self.variable
        return variable.stored_stops(
            served_ramp_stops(self.ramp, variable, mode=self.mode, steps=self.steps, reverse=self.reverse)
        )

    def apply_ramp(self):
        """Regenerate the stops snapshot from the ramp (:meth:`ramp_stops`).
        What a ramp style's stops always are (#806): every save of one runs
        this."""
        self.stops = self.ramp_stops()

    def laid_over_old_range(self) -> bool:
        """Whether this ramp style's saved stops are not what its ramp gives
        over the current range — the range changed since it was saved, and
        saving a range never touches stops (ADR 0022). Its next save lays the
        ramp over the current range; the editor shows that before it happens."""
        if self.origin != self.Origin.RAMP or self.ramp is None:
            return False
        return not _same_stops(self.stops or [], self.ramp_stops())

    def customise(self):
        """Make this a custom style with the stops it has now (#806), unsaved:
        the caller saves. The ramp or template link stays, as history only:
        nothing it says drives the stops again."""
        self.origin = self.Origin.CUSTOM
        self.reverse = False

    def start_over(self, origin, *, ramp=None, template=None):
        """Remake this style by another path (#807), unsaved: its origin and
        its ramp or template link replaced, the other link and any reverse
        dropped. Its name, slug and default status stay — starting over never
        makes a new style. The caller sets the stops the new path gives, and
        saves."""
        self.origin = origin
        self.ramp = ramp
        self.template = template
        self.reverse = False

    # -------- runtime conversion helpers --------

    def as_weatherlayers_palette(self):
        """The snapshot as ``[[value, [r,g,b(,a)]], ...]`` — the shape the
        tile-config colormap builder and WeatherLayers clients consume."""
        return [[s["value"], hex_to_rgba_list(s["color"])] for s in self.stops]

    def min_max_from_stops(self):
        """(min, max) of the snapshot's values, or (None, None) when empty."""
        if not self.stops:
            return None, None
        values = [s["value"] for s in self.stops]
        return min(values), max(values)

    # -------- presentation helpers --------

    def css_gradient(self):
        """The snapshot as a CSS ``linear-gradient()``, for swatches. Stop
        values are normalized over the snapshot's own span."""
        if not self.stops:
            return ""
        low, high = self.min_max_from_stops()
        span = high - low
        if span == 0 or len(self.stops) == 1:
            color = self.stops[0]["color"]
            return f"linear-gradient(to right, {color}, {color})"
        parts = [f"{s['color']} {(s['value'] - low) / span * 100:g}%" for s in self.stops]
        return f"linear-gradient(to right, {', '.join(parts)})"

    def preview(self):
        """A gradient swatch for admin listings."""
        return _swatch_html(self.css_gradient())

    preview.short_description = "Preview"
