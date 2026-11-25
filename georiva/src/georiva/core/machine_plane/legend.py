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

``labels`` says which ticks are labelled (#812): a stepped legend every class
boundary, up to :data:`STEPPED_LABELS` of them; a continuous one at most
:data:`CONTINUOUS_LABELS`; beyond that they thin at an even stride, both ends
kept. That is a count; whether they fit is the drawing's to judge, since only it
knows how wide the bar is — a narrow one thins them further, the same way.
"""

from georiva.core.models import OutOfRange

from .colour_table import LINEAR, TOP_CODE, decode, encoding_curve, fraction, rgba, style_stops

#: How many even steps a styleless Variable's bar is ticked at.
STYLELESS_STEPS = 4

#: At most this many labels on a stepped legend: every class boundary up to here (#812).
STEPPED_LABELS = 16

#: At most this many labels on a continuous legend, picked from its stops.
CONTINUOUS_LABELS = 6

_PLACES = 6


def _labelled(count: int, most: int) -> list:
    """Which of ``count`` ticks are labelled: at most ``most``, picked at an even
    stride, both ends always. A pick closer than half a stride to the last tick
    gives way to it, so the end is never crowded."""
    if count <= most:
        return list(range(count))
    stride = -(-(count - 1) // (most - 1))
    picked = list(range(0, count, stride))
    if picked[-1] != count - 1:
        if (count - 1 - picked[-1]) * 2 < stride:
            picked.pop()
        picked.append(count - 1)
    return picked


def _position(value: float) -> float:
    return round(value, _PLACES)


def _bands(stops: list):
    """``stops`` as ``[(low, high, colour), ...]`` when they are bands, else ``None``.

    Bands are pairs of stops sharing a colour, each pair starting where the one
    before it ends.
    """
    if len(stops) < 2 or len(stops) % 2:
        return None
    bands = []
    for (low, low_colour), (high, high_colour) in zip(stops[::2], stops[1::2], strict=True):
        if rgba(low_colour) != rgba(high_colour) or (bands and bands[-1][1] != low):
            return None
        bands.append((low, high, low_colour))
    return bands


def _stepped(bands: list, served) -> dict:
    count = len(bands)
    gradient, ticks = [], [[0.0, served(bands[0][0])]]
    for index, (_low, high, colour) in enumerate(bands):
        start, end = _position(index / count), _position((index + 1) / count)
        gradient += [[start, rgba(colour)], [end, rgba(colour)]]
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

    gradient = [[position(value), rgba(colour)] for value, colour in stops]
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
    "labels": [tick index, ...], "under": "clamp" | "transparent", "over": ...}``,
    every position 0–1 along the bar, every tick value served, and ``labels`` the
    ticks that carry their value as text, in order, both ends always.
    """
    style = style if style is not None else variable.default_style
    vmin, vmax = variable.value_min, variable.value_max
    curve = encoding_curve(variable)
    stops = sorted(style_stops(style), key=lambda stop: stop[0])
    if not stops:
        layout, most = _styleless(vmin, vmax, curve, variable.served), STYLELESS_STEPS + 1
        under = over = OutOfRange.CLAMP
    else:
        bands = _bands(stops)
        if bands:
            layout, most = _stepped(bands, variable.served), STEPPED_LABELS
        else:
            layout, most = _continuous(stops, vmin, vmax, curve, variable.served), CONTINUOUS_LABELS
        under, over = style.under, style.over
    return {**layout, "labels": _labelled(len(layout["ticks"]), most), "under": under, "over": over}
