"""A style's colour table: the one answer to which colour a code gets (#785).

Values reach a map as 8-bit codes, encoded over the Variable's range. Every
renderer then colours a code, and they used to do it each with its own code:
Titiler from a 256-entry colormap, the browser from the raw stops. They agreed
only by coincidence. Now each style has one table of 256 RGBA entries, built
here, and every renderer looks its codes up in it:

- Titiler, tiles and WMTS, reads it as the ``colormap`` of its palette payload.
- The browser reads it as a WeatherLayers palette over the codes 0–255, with an
  identity unscale (:data:`LAYER_UNSCALE`), and decodes a code back to a value
  only for its hover readout.
- STAC render entries and EDR's ``colour_table`` carry it as they find it.

``table[code] = colour_at(decode(code))``: the code is decoded to the value it
stands for, and the stops are interpolated in value space. A stepped style
keeps its hard edges through its doubled boundary stops. Every number here is
in the Variable's source unit (ADR 0046 §14), what a tile is drawn with.

The decode is the inverse of the Variable's encoding curve (#787): ``linear``,
``sqrt`` or ``log``, chosen by its ``scale_type`` (:func:`encoding_curve`). A
curve spends more codes where it is steep, so a narrow low band of a ``sqrt``
or ``log`` Variable gets enough codes to be seen.
"""

import math

#: How many codes an encoded value can take: an 8-bit texture.
CODES = 256
TOP_CODE = CODES - 1

#: What the browser unscales a texture with: none at all. Its palette is over
#: codes, so a pixel's code is what it looks up.
LAYER_UNSCALE = [0, TOP_CODE]


#: The encoding curves (#787): how a value becomes a code. Titiler's
#: ``app/curve.py`` encodes with the same three.
LINEAR, SQRT, LOG = "linear", "sqrt", "log"


def encoding_curve(variable) -> str:
    """The curve ``variable``'s values are encoded with, from its ``scale_type``.

    ``sqrt`` and ``log`` are themselves; ``diverging`` encodes linearly, so a
    centred range stays symmetric. ``log`` has no meaning over a range that does
    not start above zero: the range form refuses one, and one declared before it
    did encodes linearly.
    """
    scale_type = variable.scale_type
    if scale_type == SQRT:
        return SQRT
    if scale_type == LOG and variable.value_min is not None and variable.value_min > 0:
        return LOG
    return LINEAR


def _fraction(value: float, vmin: float, vmax: float, curve: str) -> float:
    """How far along the codes ``value`` sits, 0–1: ``f(t)``."""
    value = min(max(value, vmin), vmax)
    if curve == LOG:
        return math.log(value / vmin) / math.log(vmax / vmin)
    t = (value - vmin) / (vmax - vmin)
    return math.sqrt(t) if curve == SQRT else t


def encode(value: float, vmin: float, vmax: float, curve: str = LINEAR) -> int:
    """The code Titiler encodes ``value`` as over ``vmin``…``vmax``, through ``curve``.

    ``int(255 · f(t))`` with ``t`` clipped to the range: scaled to 0–255 and
    truncated, as rio-tiler's rescale does.
    """
    if vmax == vmin:
        return 0
    return int(_fraction(value, vmin, vmax, curve) * TOP_CODE)


def decode(code: float, vmin: float, vmax: float, curve: str = LINEAR) -> float:
    """The value a code stands for over ``vmin``…``vmax``: :func:`encode`'s inverse through ``curve``."""
    fraction = code / TOP_CODE
    if curve == LOG:
        return vmin * (vmax / vmin) ** fraction
    t = fraction * fraction if curve == SQRT else fraction
    return vmin + t * (vmax - vmin)


def _rgba(colour) -> list:
    colour = list(colour)
    return colour + [255] if len(colour) == 3 else colour[:4]


def colour_at(stops: list, value: float) -> list:
    """``value``'s colour from ``[[value, [r, g, b(, a)]], ...]`` stops, sorted.

    Linear between neighbouring stops, the end colour beyond them. On a value
    two stops share, a stepped style's boundary, the lower band's colour wins.
    """
    if value <= stops[0][0]:
        return _rgba(stops[0][1])
    for (low, low_colour), (high, high_colour) in zip(stops, stops[1:], strict=False):
        if value <= high:
            span = high - low
            t = (value - low) / span if span > 0 else 0.0
            a, b = _rgba(low_colour), _rgba(high_colour)
            return [round(a[k] + t * (b[k] - a[k])) for k in range(4)]
    return _rgba(stops[-1][1])


def table_from_stops(stops: list, vmin: float, vmax: float, curve: str = LINEAR) -> list:
    """The 256-entry table of ``[[value, [r, g, b(, a)]], ...]`` stops over ``vmin``…``vmax``,
    each code decoded through ``curve``.

    No stops is grayscale, what a styleless Variable renders as; an empty range
    is the first stop's colour throughout.
    """
    if not stops:
        return [[code, code, code, 255] for code in range(CODES)]
    stops = sorted(stops, key=lambda stop: stop[0])
    if vmax == vmin:
        return [_rgba(stops[0][1])] * CODES
    return [colour_at(stops, decode(code, vmin, vmax, curve)) for code in range(CODES)]


def style_stops(style) -> list:
    """``style``'s snapshot as ``[[value, [r, g, b(, a)]], ...]``; none for no style."""
    if style is None:
        return []
    return style.as_weatherlayers_palette()


def colour_table(variable, style=None) -> list:
    """``style``'s table over ``variable``'s range, or the default style's when
    none is named; grayscale for a Variable with no style. Decoded through the
    Variable's encoding curve."""
    style = style if style is not None else variable.default_style
    return table_from_stops(style_stops(style), variable.value_min, variable.value_max, encoding_curve(variable))


def as_colormap(table: list) -> dict:
    """The table as a ``{code: [r, g, b, a]}`` colormap, Titiler's and STAC's shape."""
    return dict(enumerate(table))


def as_layer_palette(table: list) -> list:
    """The table as a WeatherLayers palette over codes, ``[[code, [r, g, b, a]], ...]``."""
    return [[code, colour] for code, colour in enumerate(table)]
