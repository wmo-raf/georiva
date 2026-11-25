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

``labels`` says which ticks are labelled, and is the only pick there is: every
drawing shows these and no others. A stepped legend labels every class boundary,
up to :data:`STEPPED_LABELS` of them, thinned at an even stride beyond that with
both ends kept (#812). A continuous or styleless one labels what every legend
labelled before #812: at most :data:`CONTINUOUS_LABELS`, picked as legend.js
once picked them. ``stepped`` says which kind it is, since only a stepped
legend's labels may thin further where the bar is too narrow for them, which
only the drawing can judge, knowing the bar's width.
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


def _capped(count: int) -> list:
    """Which of a continuous or styleless legend's ``count`` ticks are labelled:
    every ``ceil((count - 1) / 5)``-th and the last, as before #812, so at most
    :data:`CONTINUOUS_LABELS`, the last two however close."""
    every = max(1, -(-(count - 1) // (CONTINUOUS_LABELS - 1)))
    return sorted({*range(0, count, every), count - 1}) if count else []


def _stepped_labels(count: int) -> list:
    """Which of a stepped legend's ``count`` boundaries are labelled: all of them
    up to :data:`STEPPED_LABELS`, else at most that many, picked at an even
    stride, both ends always. A pick closer than half a stride to the last
    boundary gives way to it, so the end is never crowded."""
    most = STEPPED_LABELS
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
    "labels": [tick index, ...], "stepped": bool, "under": "clamp" | "transparent",
    "over": ...}``, every position 0–1 along the bar, every tick value served,
    ``labels`` the ticks that carry their value as text, in order, both ends
    always, and ``stepped`` whether the style is drawn as equal-width bands.
    """
    style = style if style is not None else variable.default_style
    vmin, vmax = variable.value_min, variable.value_max
    curve = encoding_curve(variable)
    stops = sorted(style_stops(style), key=lambda stop: stop[0])
    bands = _bands(stops) if stops else None
    if not stops:
        layout = _styleless(vmin, vmax, curve, variable.served)
        under = over = OutOfRange.CLAMP
    elif bands:
        layout = _stepped(bands, variable.served)
        under, over = style.under, style.over
    else:
        layout = _continuous(stops, vmin, vmax, curve, variable.served)
        under, over = style.under, style.over
    count = len(layout["ticks"])
    labels = _stepped_labels(count) if bands else _capped(count)
    return {**layout, "labels": labels, "stepped": bool(bands), "under": under, "over": over}
