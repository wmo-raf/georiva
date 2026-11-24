"""One render contract per variable, for every map and API surface (ADR 0022 §4).

A texture is encoded against the variable's range, so it decodes over that
same range — never over the span of the default style's stops, which is a
display concern. Each surface used to assemble the pair itself, and one got it
wrong. They all ask here instead.

The browser colours the texture in code space (#785): its WeatherLayers layer
unscales by identity (``layer_unscale``) and looks each code up in the default
style's colour table (``layer_palette``), the same table Titiler colours tiles
with. ``image_unscale`` is what turns a code back into a value — the hover
readout's decode, and what an external client of the texture reads.

Every value in it is in the variable's served unit (ADR 0046 §15). The texture
is encoded over the raw range; decoded over the served one it gives served
values straight away, because the conversion is positive and affine.
"""

from dataclasses import dataclass

from .colour_table import LAYER_UNSCALE, as_layer_palette, colour_table


@dataclass(frozen=True)
class RenderContract:
    image_unscale: list  # [value_min, value_max], served: what a code decodes over, always the range
    palette: list  # [[value, [r, g, b(, a)]], ...] the default style's stops, served: legends and choropleths
    legend_range: tuple  # (min, max) of the stops, served, for display only
    colour_table: list  # 256 [r, g, b, a], the default style's, one per code: what the map is coloured with
    layer_unscale: list  # [0, 255]: the browser layer reads codes, not values

    @property
    def layer_palette(self) -> list:
        """The colour table as the browser layer's palette, ``[[code, [r, g, b, a]], ...]``."""
        return as_layer_palette(self.colour_table)


def render_contract(variable) -> RenderContract:
    style = variable.default_style
    if style is not None and style.stops:
        legend_range = tuple(variable.served(v) for v in style.min_max_from_stops())
    else:
        legend_range = variable.served_range
    return RenderContract(
        image_unscale=list(variable.served_range),
        palette=variable.served_palette(variable.weather_layers_palette),
        legend_range=legend_range,
        colour_table=colour_table(variable, style),
        layer_unscale=list(LAYER_UNSCALE),
    )
