from django.db import models
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.panels import FieldPanel, InlinePanel
from wagtail.models import Orderable
from wagtail.snippets.models import register_snippet


@register_snippet
class ColorPalette(ClusterableModel):
    """
    Palette definition: numeric stops + hex colors.
    At runtime we convert hex -> [r,g,b] or [r,g,b,a] for WeatherLayers.
    """
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
    
    def __str__(self):
        return self.name
    
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
