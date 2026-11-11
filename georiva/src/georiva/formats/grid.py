"""Placing a grid by bounds plus one resolution: what that needs, shared by the handlers.

A handler publishes a grid as its bounds and one cell size per axis. That only
holds for 1D, evenly spaced axes laid out west to east within -180..180; the
helpers here check the first and arrange the second, raising
UnsupportedGridError for a grid they cannot make placeable (#543, #616).
"""

from __future__ import annotations

import numpy as np

from .base import UnsupportedGridError

# How far, in cells, any coordinate may sit from the evenly spaced axis running
# between the axis's two ends. Float32 jitter and decimal-rounded coordinates
# stay under 0.01 of a cell. A 9 km Mercator grid drifts ~1 cell over -12..15 deg
# and ~3.6 over -35..-20; a +-5 deg equatorial strip drifts 0.03 (~270 m) and passes.
SPACING_TOLERANCE_CELLS = 0.1

REPROJECT_ADVICE = (
    "Only regular grids can be placed — reproject the file onto a regular latitude-longitude grid before ingesting it."
)


def require_even_spacing(coords: np.ndarray, name: str) -> None:
    """Refuse a 1D axis with missing values or spacing that drifts past the tolerance."""
    coords = np.asarray(coords, dtype=np.float64)
    if not np.isfinite(coords).all():
        raise UnsupportedGridError(
            f"The grid's '{name}' axis has missing (NaN or infinite) coordinates, so its "
            f"cells cannot be placed. {REPROJECT_ADVICE}"
        )
    if coords.size < 3:
        return
    cell = (coords[-1] - coords[0]) / (coords.size - 1)
    even = np.linspace(coords[0], coords[-1], coords.size)
    drift = float(np.max(np.abs(coords - even))) / abs(cell) if cell else np.inf
    if drift > SPACING_TOLERANCE_CELLS:
        raise UnsupportedGridError(
            f"The grid's '{name}' axis is not evenly spaced: its coordinates drift up to "
            f"{drift:.2f} cells from an even spacing (tolerance "
            f"{SPACING_TOLERANCE_CELLS} cells), so placing it by one resolution would "
            f"misplace the data. {REPROJECT_ADVICE}"
        )


def recentre_longitudes(var, lon_name: str):
    """Lay a 1D 0..360 longitude axis out from -180 to 180, west to east.

    Wrapping the coordinate values alone would leave the columns in their stored
    order, so a global grid starting at Greenwich would be placed 180 degrees
    off; the columns are reordered with it. A global grid that repeats its seam
    column (0..360 inclusive) loses the repeat, and a grid lying wholly at or
    east of 180 moves west whole, so its 180 column is -180. A regional grid
    that straddles 180 splits in two under -180..180 and is refused. Run it on
    the whole grid, before any window is cut from it.
    """
    lons = np.asarray(var.coords[lon_name].values, dtype=np.float64)
    if lons.max() <= 180:
        return var
    dim = var.coords[lon_name].dims[0]
    repeat = np.isclose(lons, lons.min() + 360)
    if repeat.any():
        var = var.isel({dim: np.flatnonzero(~repeat)})
        lons = lons[~repeat]
    wrapped = lons - 360 if lons.min() >= 180 else np.where(lons > 180, lons - 360, lons)
    order = np.argsort(wrapped, kind="stable")
    try:
        require_even_spacing(wrapped[order], lon_name)
    except UnsupportedGridError:
        raise UnsupportedGridError(
            f"The grid's '{lon_name}' axis crosses the antimeridian ({lons.min():g}..{lons.max():g} "
            "degrees east) without covering the globe, so it splits in two under -180..180. "
            "Split the file at 180 degrees, or reproject it onto -180..180, before ingesting it."
        ) from None
    var = var.assign_coords({lon_name: (dim, wrapped, var.coords[lon_name].attrs)})
    if (order != np.arange(order.size)).any():
        var = var.isel({dim: order})
    return var
