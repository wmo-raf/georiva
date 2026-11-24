"""A Variable's encoding curve: how its values become the 256 codes (#787).

Django names the curve in the render config as ``curve`` beside ``vmin`` and
``vmax``, already resolved: ``linear``, ``sqrt`` or ``log``, and ``log`` only
over a range above zero. With ``t = (v − vmin) / (vmax − vmin)`` clipped to 0–1,
the code is ``int(255 · f(t))``:

- ``linear``: ``f(t) = t``, rio-tiler's own rescale.
- ``sqrt``: ``f(t) = √t``.
- ``log``: ``f(t) = log(v / vmin) / log(vmax / vmin)``.

Core's ``colour_table.encode`` is the same function, value by value; a code's
colour comes from a table Django built by decoding each code through the curve's
inverse. A config from before curves names none and encodes linearly.
"""

import numpy as np
from rio_tiler.models import ImageData

LINEAR, SQRT, LOG = "linear", "sqrt", "log"


def curve_of(config: dict) -> str:
    """The curve a render config encodes with: ``linear`` unless it names another that applies."""
    curve = config.get("curve") or LINEAR
    if curve == LOG and not config["vmin"] > 0:
        return LINEAR
    return curve if curve in (SQRT, LOG) else LINEAR


def encode(img: ImageData, config: dict) -> ImageData:
    """``img``'s values as codes 0–255, over the config's range and through its curve. In place."""
    vmin, vmax = config["vmin"], config["vmax"]
    curve = curve_of(config)
    if curve == LINEAR:
        img.rescale(in_range=((vmin, vmax),))
        return img

    with np.errstate(invalid="ignore", divide="ignore"):
        clipped = np.clip(img.array.data.astype("float64"), vmin, vmax)
        if curve == SQRT:
            fraction = np.sqrt((clipped - vmin) / (vmax - vmin))
        else:
            fraction = np.log(clipped / vmin) / np.log(vmax / vmin)
    img.array = np.ma.masked_array(fraction, mask=np.ma.getmaskarray(img.array))
    img.rescale(in_range=((0.0, 1.0),))
    return img
