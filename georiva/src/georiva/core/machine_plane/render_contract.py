"""One render contract per variable, for every map and API surface (ADR 0022 §4).

A texture is encoded against the variable's range, so the browser must unscale
it over that same range — never over the span of the default style's stops,
which is a display concern. Each surface used to assemble the pair itself, and
one got it wrong. They all ask here instead.

Every number in it is in the variable's served unit (ADR 0046 §15). The texture
is encoded over the raw range; unscaled over the served one it decodes straight
into served values, because the conversion is positive and affine.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RenderContract:
    image_unscale: list  # [value_min, value_max], served: the encoding contract, always the range
    palette: list  # [[value, [r, g, b(, a)]], ...] from the default style, values served
    legend_range: tuple  # (min, max) of the stops, served, for display only


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
    )
