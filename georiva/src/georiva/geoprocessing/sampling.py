"""One value from one open raster, read by range.

For a point, the cell containing it; for a polygon, an aggregate over the cells
whose centres fall inside it. Only the blocks under the point or the polygon's
bounding box are read, so the raster may as well be a COG on someone else's
server (ADR 0043 §7) — which is what timeseries use this for: a Remote asset is
not in a Variable's virtual store, so its timestep is read from the asset.

Both take an open rasterio dataset and WGS84 input, and answer ``nan`` where the
raster holds nothing: outside its footprint, or on nodata. A read that fails
raises. That is the difference from ``zonal.mask_and_aggregate``, which reads
the whole raster for a boundary set and reports a failed mask as empty
statistics.
"""

from __future__ import annotations

import math

import numpy as np

from .zonal import reproject_geometry

WGS84 = "EPSG:4326"


def _in_wgs84(src) -> bool:
    return src.crs is None or src.crs.to_epsg() == 4326


def _scaled(src, values):
    """Apply band 1's scale and offset, which a masked read leaves unapplied."""
    scale, offset = src.scales[0], src.offsets[0]
    if scale == 1.0 and offset == 0.0:
        return values
    return values * scale + offset


def point_value(src, lon: float, lat: float) -> float:
    """The value of the cell containing ``(lon, lat)``."""
    from rasterio.warp import transform
    from rasterio.windows import Window

    x, y = lon, lat
    if not _in_wgs84(src):
        xs, ys = transform(WGS84, src.crs, [lon], [lat])
        x, y = xs[0], ys[0]

    row, col = src.index(x, y)
    if not (0 <= row < src.height and 0 <= col < src.width):
        return math.nan

    cell = src.read(1, window=Window(col, row, 1, 1), masked=True)
    cell = np.ma.masked_invalid(cell)
    if cell.count() == 0:
        return math.nan
    return float(_scaled(src, cell)[0, 0])


def area_value(src, geometry: dict, aggregation: str) -> float:
    """``aggregation`` over the cells whose centres fall inside ``geometry``."""
    from rasterio.mask import mask

    shape = geometry if src.crs is None else reproject_geometry(geometry, src.crs)
    try:
        cells, _ = mask(src, [shape], crop=True, filled=False, indexes=1)
    except ValueError:
        # rasterio's way of saying the geometry does not overlap the raster.
        return math.nan

    cells = np.ma.masked_invalid(cells)
    if cells.count() == 0:
        return math.nan
    return float(getattr(_scaled(src, cells), aggregation)())
