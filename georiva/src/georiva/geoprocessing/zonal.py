"""
Zonal statistics over an in-memory raster array.

Pure compute: takes a numpy array + affine transform + CRS and a set of
geometries (GeoJSON, in EPSG:4326), and returns per-geometry stats. No Django
models — callers build the geometries (e.g. from AdminBoundary) and own that
adapter layer.
"""
from __future__ import annotations

import json
import logging

import numpy as np
import rasterio.mask
from rasterio.crs import CRS as RasterioCRS
from rasterio.io import MemoryFile
from rasterio.warp import transform_geom

logger = logging.getLogger(__name__)

_CRS_4326 = RasterioCRS.from_epsg(4326)

EMPTY_STATS = {
    "mean": None, "min": None, "max": None,
    "sum": None, "std": None, "count": None,
}


def reproject_geometry(geom: dict, dst_crs, src_crs=_CRS_4326) -> dict:
    """Reproject a GeoJSON geometry from ``src_crs`` (default EPSG:4326) to ``dst_crs``."""
    dst = RasterioCRS.from_user_input(dst_crs)
    if dst == src_crs:
        return geom
    out = transform_geom(src_crs, dst, geom)
    if isinstance(out, str):  # older rasterio returns a JSON string
        out = json.loads(out)
    return out


def mask_and_aggregate(dataset, geom: dict | None, *, label=None) -> dict:
    """
    Mask an open rasterio dataset by a GeoJSON geometry and aggregate to stats.

    Returns all-None stats if the geometry is None/empty or does not intersect
    the raster extent.
    """
    if geom is None:
        return dict(EMPTY_STATS)

    try:
        masked, _ = rasterio.mask.mask(
            dataset, [geom], crop=False, nodata=np.nan, all_touched=False,
        )
        arr = masked[0].astype(np.float32)
        arr[~np.isfinite(arr)] = np.nan
        valid = arr[~np.isnan(arr)]

        if len(valid) == 0:
            return dict(EMPTY_STATS)

        return {
            "mean": float(np.mean(valid)),
            "min": float(np.min(valid)),
            "max": float(np.max(valid)),
            "sum": float(np.sum(valid)),
            "std": float(np.std(valid)),
            "count": int(len(valid)),
        }
    except Exception as exc:
        logger.warning("Zonal mask failed for %s: %s", label, exc)
        return dict(EMPTY_STATS)


def zonal_stats_from_array(data, transform, crs, geometries) -> list[dict]:
    """
    Compute zonal statistics for many geometries against one 2-D array.

    Opens a single in-memory rasterio dataset and masks every geometry against
    it (one MemoryFile for all geometries, not one per geometry).

    Parameters
    ----------
    data : np.ndarray
        2-D float array with NaN for nodata.
    transform : affine.Affine
        Pixel → geographic transform.
    crs : str
        Raster CRS (WKT or EPSG string).
    geometries : iterable of (key, geom)
        ``key`` is any identifier echoed back on the result; ``geom`` is a
        GeoJSON geometry dict in EPSG:4326 (or None).

    Returns
    -------
    list[dict]
        One dict per geometry: ``{"key": key, mean, min, max, sum, std, count}``.
    """
    geometries = list(geometries)
    if not geometries:
        return []

    height, width = data.shape
    raster_crs = RasterioCRS.from_user_input(crs)

    # Pre-reproject all geometries before opening the MemoryFile so a bad
    # geometry is caught early without holding the file open.
    prepared = []
    for key, geom in geometries:
        if geom is None:
            prepared.append((key, None))
            continue
        try:
            prepared.append((key, reproject_geometry(geom, raster_crs)))
        except Exception as exc:
            logger.warning("Failed to reproject geometry %s: %s", key, exc)
            prepared.append((key, None))

    results = []
    with MemoryFile() as memfile:
        with memfile.open(
                driver="GTiff", height=height, width=width, count=1,
                dtype=np.float32, crs=raster_crs, transform=transform,
                nodata=np.nan,
        ) as dst:
            dst.write(np.asarray(data, dtype=np.float32), 1)

        with memfile.open() as dataset:
            for key, geom in prepared:
                stats = mask_and_aggregate(dataset, geom, label=key)
                stats["key"] = key
                results.append(stats)

    return results
