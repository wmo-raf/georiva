"""One render contract per variable, for every map and API surface (ADR 0022 §4).

A texture is encoded against the variable's range, so the browser must unscale
it over that same range — never over the span of the default style's stops,
which is a display concern. Each surface used to assemble the pair itself, and
one got it wrong. They all ask here instead.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RenderContract:
    image_unscale: list  # [value_min, value_max]: the encoding contract, always the range
    palette: list  # [[value, [r, g, b(, a)]], ...] from the default style
    legend_range: tuple  # (min, max) of the stops, for display only


def render_contract(variable) -> RenderContract:
    style = variable.default_style
    if style is not None and style.stops:
        legend_range = style.min_max_from_stops()
    else:
        legend_range = variable.value_range
    return RenderContract(
        image_unscale=list(variable.value_range),
        palette=variable.weather_layers_palette,
        legend_range=legend_range,
    )
