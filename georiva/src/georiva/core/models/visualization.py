from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.db import models
from django.utils.html import format_html
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, InlinePanel
from wagtail.models import Orderable

#: '#RRGGBB' or '#RRGGBBAA', with the '#' and shorthand '#RGB'/'#RGBA' allowed —
#: the same shapes ``ColorPalette.hex_to_rgba_list`` accepts.
HEX_COLOR_VALIDATOR = RegexValidator(
    regex=r"^#?(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$",
    message="Enter a hex color such as '#RRGGBB' or '#RRGGBBAA'.",
)


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
        'organisations.Organisation',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name='color_ramps',
        help_text=(
            "The organisation this ramp belongs to. Left empty, it is part of "
            "the instance-wide ramp catalog every organisation can use."
        ),
    )
    name = models.CharField(max_length=255)

    class RampType(models.TextChoices):
        SEQUENTIAL = 'sequential', 'Sequential'
        DIVERGING = 'diverging', 'Diverging'
        QUALITATIVE = 'qualitative', 'Qualitative'

    ramp_type = models.CharField(
        max_length=20,
        choices=RampType.choices,
        default=RampType.SEQUENTIAL,
    )

    panels = [
        FieldPanel('name'),
        FieldPanel('ramp_type'),
        InlinePanel('stops', heading="Colors", label="Color"),
    ]

    class Meta:
        ordering = ['name']
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
                fields=['organisation', 'name'],
                name='unique_ramp_name_per_organisation',
            ),
            models.UniqueConstraint(
                fields=['name'],
                condition=models.Q(organisation__isnull=True),
                name='unique_global_ramp_name',
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

        Stops without explicit positions are spread evenly; a qualitative ramp
        renders as hard-edged blocks rather than a blend, because its colors
        are categories, not points on a continuum.
        """
        stops = list(self.stops.all())
        if not stops:
            return ""
        if len(stops) == 1:
            color = stops[0].css_color()
            return f"linear-gradient(to right, {color}, {color})"

        if self.ramp_type == self.RampType.QUALITATIVE:
            width = 100 / len(stops)
            parts = [
                f"{stop.css_color()} {i * width:g}% {(i + 1) * width:g}%"
                for i, stop in enumerate(stops)
            ]
        else:
            last = len(stops) - 1
            parts = [
                f"{stop.css_color()} "
                f"{(stop.position if stop.position is not None else i / last) * 100:g}%"
                for i, stop in enumerate(stops)
            ]
        return f"linear-gradient(to right, {', '.join(parts)})"

    def preview(self):
        """A gradient swatch for the admin listing."""
        gradient = self.css_gradient()
        if not gradient:
            return ""
        return format_html(
            '<span style="display: inline-block; '
            'width: 120px; height: 14px; border-radius: 3px; '
            'border: 1px solid var(--w-color-border-furniture); background: {};">'
            '</span>',
            gradient,
        )

    preview.short_description = "Preview"


class ColorRampStop(Orderable):
    # Owned by whoever owns the ramp — including nobody, for the instance-wide
    # tier. See ColorRamp.
    ORGANISATION_LOOKUP = "ramp__organisation"
    ORGANISATION_GLOBAL_TIER = True

    ramp = ParentalKey(ColorRamp, related_name='stops', on_delete=models.CASCADE)
    hex_value = models.CharField(
        max_length=9,
        validators=[HEX_COLOR_VALIDATOR],
        help_text="Hex '#RRGGBB' or '#RRGGBBAA' (alpha optional)",
    )
    position = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
        help_text=(
            "Where along the ramp this color sits, 0–1. Left empty, colors "
            "are spread evenly."
        ),
    )

    panels = [
        FieldPanel('hex_value'),
        FieldPanel('position'),
    ]

    def css_color(self):
        value = self.hex_value.strip()
        return value if value.startswith('#') else f"#{value}"

    def __str__(self):
        if self.position is not None:
            return f"{self.hex_value} @ {self.position}"
        return self.hex_value


class ColorPalette(ClusterableModel):
    """
    Palette definition: numeric stops + hex colors.
    At runtime we convert hex -> [r,g,b] or [r,g,b,a] for WeatherLayers.

    Global-with-org-overrides (decision #269): a palette with no organisation is
    the instance-wide tier every organisation draws on and only the instance
    admin edits; a palette with one belongs to that institution alone. Both tiers
    are offered wherever a palette is chosen, which is the point — an operator
    reuses the shipped rainfall ramp and adds their own beside it.
    """

    ORGANISATION_LOOKUP = "organisation"
    ORGANISATION_GLOBAL_TIER = True

    organisation = models.ForeignKey(
        'organisations.Organisation',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name='color_palettes',
        help_text=(
            "The organisation this palette belongs to. Left empty, it is part of "
            "the instance-wide palette library every organisation can use."
        ),
    )
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
    
    center_value = models.FloatField(null=True, blank=True)
    
    panels = [
        FieldPanel('name'),
        FieldPanel('palette_type'),
        FieldPanel('center_value'),
        InlinePanel('stops', heading="Stops", label="Stop"),
    ]
    
    class Meta:
        ordering = ['name']
        verbose_name = "Color Palette"
        verbose_name_plural = "Color Palettes"
        constraints = [
            # Names are how operators tell palettes apart in a chooser that mixes
            # both tiers, so a name is unique within its tier — while different
            # organisations may of course each have a "Rainfall". Two constraints
            # because the first cannot cover the instance-wide tier: Postgres
            # treats NULLs as distinct, so an unqualified pair would let any
            # number of ownerless "Rainfall"s through.
            models.UniqueConstraint(
                fields=['organisation', 'name'],
                name='unique_palette_name_per_organisation',
            ),
            models.UniqueConstraint(
                fields=['name'],
                condition=models.Q(organisation__isnull=True),
                name='unique_global_palette_name',
            ),
        ]
    
    def __str__(self):
        return self.name

    def owner_label(self):
        """Which tier this palette is in, for a listing that shows both."""
        return self.organisation.name if self.organisation_id else "Instance-wide"

    owner_label.short_description = "Owner"

    # -------- runtime conversion helpers --------
    
    @staticmethod
    def hex_to_rgba_list(hex_color: str):
        """
        '#RRGGBB' -> [r,g,b]
        '#RRGGBBAA' -> [r,g,b,a]    (alpha is 0..255, exactly what WeatherLayers expects)
        Also accepts 'RRGGBB' / 'RRGGBBAA' without '#'.
        """
        if not hex_color:
            raise ValueError("Empty hex color")
        
        h = hex_color.strip().lstrip('#')
        
        # allow shorthand #RGB / #RGBA if you want
        if len(h) in (3, 4):
            h = ''.join([c * 2 for c in h])
        
        if len(h) not in (6, 8):
            raise ValueError(f"Invalid hex color length: {hex_color}")
        
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        
        if len(h) == 8:
            a = int(h[6:8], 16)
            return [r, g, b, a]
        
        return [r, g, b]
    
    def as_weatherlayers_palette(self):
        """
        Returns:
          [[value, [r,g,b]], [value, [r,g,b,a]], ...]
        """
        stops = self.stops.all().order_by('sort_order', 'pk')
        return [[s.value, self.hex_to_rgba_list(s.hex_value)] for s in stops]
    
    def min_max_from_stops(self):
        """
        Extract min/max automatically from stop values.
        """
        stops = list(self.stops.all().order_by('sort_order', 'pk'))
        if not stops:
            return None, None
        values = [s.value for s in stops]
        return min(values), max(values)


class PaletteStop(Orderable):
    # Owned by whoever owns the palette — including nobody, for the instance-wide
    # tier. See ColorPalette.
    ORGANISATION_LOOKUP = "palette__organisation"
    ORGANISATION_GLOBAL_TIER = True

    palette = ParentalKey(ColorPalette, related_name='stops', on_delete=models.CASCADE)
    value = models.FloatField(help_text="Numeric value at this stop (e.g. 11.5749)")
    hex_value = models.CharField(
        max_length=9,
        help_text="Hex '#RRGGBB' or '#RRGGBBAA' (alpha optional)"
    )
    
    panels = [
        FieldPanel('value'),
        FieldPanel('hex_value'),
    ]
    
    def __str__(self):
        return f"{self.value}: {self.hex_value}"
