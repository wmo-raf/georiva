from django.contrib.postgres.fields import ArrayField
from django.db import models
from wagtail.admin.panels import FieldPanel


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
