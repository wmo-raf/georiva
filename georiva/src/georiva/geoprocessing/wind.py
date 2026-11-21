"""Wind from its components: speed and the meteorological direction.

``u`` is the eastward component and ``v`` the northward one, in any one unit.
Both functions are element-wise over numpy arrays; NaN propagates.
"""

from __future__ import annotations

import numpy as np


def wind_speed(u, v):
    """The vector magnitude, in the components' unit."""
    return np.hypot(u, v)


def wind_from_direction(u, v):
    """The bearing the wind blows *from*, in degrees in [0, 360), 0 north and
    clockwise — the meteorological convention.

    Calm (both components zero) has no direction worth reading; it comes out as
    180, which is what ``arctan2(0, 0) + 180`` gives.

    The modulo can round a bearing a hair below 360 up to 360 itself in floating
    point; that one value is folded back to 0, so the result stays inside the
    half-open circle a reader expects.
    """
    direction = np.mod(np.degrees(np.arctan2(u, v)) + 180.0, 360.0)
    return np.where(direction >= 360.0, 0.0, direction)
