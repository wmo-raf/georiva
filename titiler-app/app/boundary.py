"""Cropping a map to a boundary, at the moment it is drawn (ADR 0038).

A catalog in ``display`` clip mode stores its data uncropped, so every consumer
but the map gets complete values, and hands this service the outline to crop the
map to. The outline arrives in the tile config as GeoJSON in EPSG:4326, already
simplified; nothing here measures with it.

Two operations, both pure: which pixels of a tile are inside, and whether one
longitude/latitude is. They have to agree, because a click on a blank part of
the map must answer "no data" exactly where the tile drew nothing.
"""

import json
from functools import lru_cache

import numpy as np
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds
from rasterio.warp import transform_geom


@lru_cache(maxsize=64)
def _in_crs(geometry_json: str, crs: str) -> dict:
    """The boundary in the tile's CRS, memoised: a map is hundreds of tiles of one outline."""
    geometry = json.loads(geometry_json)
    if crs == "EPSG:4326":
        return geometry
    return transform_geom("EPSG:4326", crs, geometry)


def _key(geometry: dict) -> str:
    return json.dumps(geometry, sort_keys=True, separators=(",", ":"))


def inside_mask(geometry: dict, bounds, crs, shape: tuple[int, int]) -> np.ndarray:
    """``(height, width)`` booleans, True where a pixel of this tile lies inside the boundary.

    ``bounds`` and ``crs`` are the tile's own, as rio-tiler reports them, so a
    tile in Web Mercator is cropped by the outline projected into it rather than
    by the outline read as if it were already there.
    """
    height, width = shape
    crs_text = crs.to_string() if hasattr(crs, "to_string") else str(crs)
    projected = _in_crs(_key(geometry), crs_text)
    transform = from_bounds(*bounds, width, height)
    # ``geometry_mask`` marks what is *outside* with True unless inverted.
    return geometry_mask([projected], out_shape=shape, transform=transform, invert=True, all_touched=True)


def contains(geometry: dict, lon: float, lat: float) -> bool:
    """Whether a longitude/latitude lies inside a (Multi)Polygon, holes honoured.

    Ray casting over the GeoJSON rings: no geometry library is needed for a
    point, and this service does not otherwise depend on one.
    """
    kind = geometry.get("type")
    if kind == "Polygon":
        polygons = [geometry["coordinates"]]
    elif kind == "MultiPolygon":
        polygons = geometry["coordinates"]
    else:
        return False

    for rings in polygons:
        if _in_ring(rings[0], lon, lat) and not any(_in_ring(hole, lon, lat) for hole in rings[1:]):
            return True
    return False


def _in_ring(ring, lon: float, lat: float) -> bool:
    inside = False
    count = len(ring)
    j = count - 1
    for i in range(count):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside
