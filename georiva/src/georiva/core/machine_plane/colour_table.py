"""A style's colour table: the one answer to which colour a code gets (#785).

Values reach a map as 8-bit codes, encoded over the Variable's range. Every
renderer then colours a code, and they used to do it each with its own code:
Titiler from a 256-entry colormap, the browser from the raw stops. They agreed
only by coincidence. Now each style has one table of 256 RGBA entries, built
here, and every renderer looks its codes up in it:

- Titiler, tiles and WMTS, reads it as the ``colormap`` of its palette payload.
- The browser reads it as a WeatherLayers palette over the codes 0–255, with an
  identity unscale (``colour_table.js``), and decodes a code back to a value
  only for its hover readout.
- STAC render entries and EDR's ``colour_table`` carry it as they find it.

``table[code] = colour_at(decode(code))``: the code is decoded to the value it
stands for, and the stops are interpolated in value space. A stepped style
keeps its hard edges through its doubled boundary stops. A style's under and
over (#788) make the codes beyond its stops transparent, alpha 0, so the basemap
shows through them in every renderer alike. Every number here is
in the Variable's source unit (ADR 0046 §14), what a tile is drawn with.

The decode is the inverse of the Variable's encoding curve (#787): ``linear``,
``sqrt`` or ``log``, chosen by its ``scale_type`` (:func:`encoding_curve`). A
curve spends more codes where it is steep, so a narrow low band of a ``sqrt``
or ``log`` Variable gets enough codes to be seen.
"""

import math

from georiva.core.models import OutOfRange

#: How many codes an encoded value can take: an 8-bit texture.
CODES = 256
TOP_CODE = CODES - 1


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


def fraction(value: float, vmin: float, vmax: float, curve: str = LINEAR) -> float:
    """How far along the codes ``value`` sits, 0–1: ``f(t)``, untruncated.

    What a legend lays a continuous style out along (#790), so it stretches
    where the map does.
    """
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
    return int(fraction(value, vmin, vmax, curve) * TOP_CODE)


def render_expression(vmin: float, vmax: float, curve: str = LINEAR) -> str | None:
    """:func:`fraction` as a Titiler ``expression`` over band ``b1``, or None for ``linear``.

    What a STAC Render entry carries so an outside client encodes as Titiler
    does: the expression takes a value to ``f(t)`` and a ``rescale`` of 0–1
    then makes it ``int(255 · f(t))``. The value is clamped to the range first,
    so nothing outside it becomes NaN under ``sqrt`` or ``log``; ``where``, not
    ``clip``, because rio-tiler evaluates with numexpr, which has no ``clip``.
    """
    if curve == LINEAR:
        return None
    lo, hi = repr(float(vmin)), repr(float(vmax))
    clamped = f"where(b1 < {lo}, {lo}, where(b1 > {hi}, {hi}, b1))"
    if curve == LOG:
        return f"log({clamped} / {lo}) / log({hi} / {lo})"
    return f"sqrt(({clamped} - {lo}) / ({hi} - {lo}))"


def decode(code: float, vmin: float, vmax: float, curve: str = LINEAR) -> float:
    """The value a code stands for over ``vmin``…``vmax``: :func:`encode`'s inverse through ``curve``."""
    fraction = code / TOP_CODE
    if curve == LOG:
        return vmin * (vmax / vmin) ** fraction
    t = fraction * fraction if curve == SQRT else fraction
    return vmin + t * (vmax - vmin)


def rgba(colour) -> list:
    """``colour`` as four channels: ``[r, g, b]`` gains an opaque alpha."""
    colour = list(colour)
    return colour + [255] if len(colour) == 3 else colour[:4]


def colour_at(stops: list, value: float) -> list:
    """``value``'s colour from ``[[value, [r, g, b(, a)]], ...]`` stops, sorted.

    Linear between neighbouring stops, the end colour beyond them. On a value
    two stops share, a stepped style's boundary, the lower band's colour wins.
    """
    if value <= stops[0][0]:
        return rgba(stops[0][1])
    for (low, low_colour), (high, high_colour) in zip(stops, stops[1:], strict=False):
        if value <= high:
            span = high - low
            t = (value - low) / span if span > 0 else 0.0
            a, b = rgba(low_colour), rgba(high_colour)
            return [round(a[k] + t * (b[k] - a[k])) for k in range(4)]
    return rgba(stops[-1][1])


#: An undrawn code: what a transparent under or over gives (#788).
TRANSPARENT = [0, 0, 0, 0]


def _is_under(code: int, first: float, vmin: float, vmax: float, curve: str) -> bool:
    """Whether ``code`` stands for values below the first stop.

    Code 0 also holds every value clipped below the range, all of them below
    any first stop that is not itself below the range, so it is under whenever
    the stops start at or above the range's bottom. On a ``log`` Variable that
    is every value at or below its minimum, zeros included.
    """
    if code == 0:
        return first >= vmin
    return decode(code, vmin, vmax, curve) < first


def _is_over(code: int, last: float, vmin: float, vmax: float, curve: str) -> bool:
    """Whether ``code`` stands for values above the last stop.

    The top code holds the range's top and every value clipped above it, so it
    is over whenever the stops end at or below the range's top.
    """
    if code == TOP_CODE:
        return last <= vmax
    return decode(code, vmin, vmax, curve) > last


def table_from_stops(
    stops: list,
    vmin: float,
    vmax: float,
    under: str = OutOfRange.CLAMP,
    over: str = OutOfRange.CLAMP,
    curve: str = LINEAR,
) -> list:
    """The 256-entry table of ``[[value, [r, g, b(, a)]], ...]`` stops over ``vmin``…``vmax``,
    each code decoded through ``curve``.

    No stops is grayscale, what a styleless Variable renders as; an empty range
    is the first stop's colour throughout. ``under`` and ``over`` (``clamp`` or
    ``transparent``, :class:`~georiva.core.models.OutOfRange`) say what the
    codes beyond the stops get: the end colour, or nothing (alpha 0).
    """
    if not stops:
        return [[code, code, code, 255] for code in range(CODES)]
    stops = sorted(stops, key=lambda stop: stop[0])
    if vmax == vmin:
        return [rgba(stops[0][1])] * CODES
    first, last = stops[0][0], stops[-1][0]
    table = []
    for code in range(CODES):
        if under != OutOfRange.CLAMP and _is_under(code, first, vmin, vmax, curve):
            table.append(list(TRANSPARENT))
        elif over != OutOfRange.CLAMP and _is_over(code, last, vmin, vmax, curve):
            table.append(list(TRANSPARENT))
        else:
            table.append(colour_at(stops, decode(code, vmin, vmax, curve)))
    return table


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
    if style is None:
        return table_from_stops([], variable.value_min, variable.value_max)
    return table_from_stops(
        style_stops(style),
        variable.value_min,
        variable.value_max,
        style.under,
        style.over,
        curve=encoding_curve(variable),
    )


def as_colormap(table: list) -> dict:
    """The table as a ``{code: [r, g, b, a]}`` colormap, Titiler's and STAC's shape."""
    return dict(enumerate(table))
