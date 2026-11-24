"""A legend laid out like the map (#790).

A legend is a bar from 0 to 1. This module decides where along it each colour
and each tick goes, and every legend (the dataset item page, the admin item
preview) draws what it is given, so they cannot lay a style out differently:

- A **stepped** style, its stops in bands (each colour twice, at its two
  edges), is drawn as equal-width blocks, one per band, with a tick at each
  level. A 0.5–1 mm band reads as clearly as a 100–200 mm one.
- A **continuous** style is laid out along the Variable's encoding curve
  (:func:`~.colour_table.fraction`) between its first and last stop, so a
  ``sqrt`` or ``log`` legend stretches the low end where the map does. A tick
  sits at each stop.
- A Variable with **no style** renders grayscale in code space (its colour
  table is the code itself), so its bar is black to white straight along, with
  ticks at even steps labelled with the values they decode to.

Positions are reckoned in the source unit, where the encoding is; tick values
are served (ADR 0046 §15). ``under`` and ``over`` say whether each end is
clamped, drawn with an arrow, or transparent, drawn without one.
"""

from .colour_table import CLAMP, LINEAR, TOP_CODE, decode, encoding_curve, fraction, style_stops

#: How many even steps a styleless Variable's bar is ticked at.
STYLELESS_STEPS = 4

_PLACES = 6


def _position(value: float) -> float:
    return round(value, _PLACES)


def _rgba(colour) -> list:
    colour = list(colour)
    return colour + [255] if len(colour) == 3 else colour[:4]


def _bands(stops: list):
    """``stops`` as ``[(low, high, colour), ...]`` when they are bands, else ``None``.

    Bands are pairs of stops sharing a colour, each pair starting where the one
    before it ends.
    """
    if len(stops) < 2 or len(stops) % 2:
        return None
    bands = []
    for (low, low_colour), (high, high_colour) in zip(stops[::2], stops[1::2], strict=True):
        if _rgba(low_colour) != _rgba(high_colour) or (bands and bands[-1][1] != low):
            return None
        bands.append((low, high, low_colour))
    return bands


def _stepped(bands: list, served) -> dict:
    count = len(bands)
    gradient, ticks = [], [[0.0, served(bands[0][0])]]
    for index, (_low, high, colour) in enumerate(bands):
        start, end = _position(index / count), _position((index + 1) / count)
        gradient += [[start, _rgba(colour)], [end, _rgba(colour)]]
        ticks.append([end, served(high)])
    return {"gradient": gradient, "ticks": ticks}


def _continuous(stops: list, vmin: float, vmax: float, curve: str, served) -> dict:
    first, last = stops[0][0], stops[-1][0]

    def along(value):
        return fraction(value, vmin, vmax, curve) if vmax > vmin else 0.0

    low, high = along(first), along(last)
    if high <= low:
        # The range holds none of the stops' span (or has none): spread them
        # out linearly, so the legend still names them.
        def along(value):
            return fraction(value, first, last, LINEAR) if last > first else 0.0

        low, high = 0.0, 1.0

    def position(value):
        return _position((along(value) - low) / (high - low))

    gradient = [[position(value), _rgba(colour)] for value, colour in stops]
    ticks = []
    for value, _colour in stops:
        tick = [position(value), served(value)]
        if tick not in ticks:
            ticks.append(tick)
    return {"gradient": gradient, "ticks": ticks}


def _styleless(vmin: float, vmax: float, curve: str, served) -> dict:
    ticks = []
    for step in range(STYLELESS_STEPS + 1):
        at = step / STYLELESS_STEPS
        value = decode(at * TOP_CODE, vmin, vmax, curve) if vmax > vmin else vmin
        ticks.append([_position(at), served(value)])
    return {"gradient": [[0.0, [0, 0, 0, 255]], [1.0, [255, 255, 255, 255]]], "ticks": ticks}


def legend(variable, style=None) -> dict:
    """``style``'s legend for ``variable``, or the default style's when none is named.

    ``{"gradient": [[position, [r, g, b, a]], ...], "ticks": [[position, value], ...],
    "under": "clamp" | "transparent", "over": ...}``, every position 0–1 along the
    bar and every tick value served.
    """
    style = style if style is not None else variable.default_style
    vmin, vmax = variable.value_min, variable.value_max
    curve = encoding_curve(variable)
    stops = sorted(style_stops(style), key=lambda stop: stop[0])
    if not stops:
        return {**_styleless(vmin, vmax, curve, variable.served), "under": CLAMP, "over": CLAMP}
    bands = _bands(stops)
    if bands:
        layout = _stepped(bands, variable.served)
    else:
        layout = _continuous(stops, vmin, vmax, curve, variable.served)
    return {**layout, "under": style.under, "over": style.over}
