from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING

import numpy as np
import rasterio
import rasterio.mask
from rasterio.crs import CRS as RasterioCRS
from rasterio.io import MemoryFile

if TYPE_CHECKING:
    from adminboundarymanager.models import AdminBoundary
    from georiva.core.models import Item, Variable

logger = logging.getLogger(__name__)

_BOUNDARY_CRS = RasterioCRS.from_epsg(4326)
_EMPTY_STATS = {
    "mean": None, "min": None, "max": None,
    "sum": None, "std": None, "count": None,
}


# ---------------------------------------------------------------------------
# Geometry helper
# ---------------------------------------------------------------------------

def _geom_to_dict(boundary: "AdminBoundary", raster_crs: RasterioCRS) -> dict:
    """
    Convert a Django GEOSGeometry to a GeoJSON dict in the raster's CRS.

    boundary.geom.geojson returns a JSON string — parse it to dict.
    Reproject only if the raster CRS differs from EPSG:4326 (rare for COGs).
    """
    geom = json.loads(boundary.geom.geojson)
    
    if raster_crs != _BOUNDARY_CRS:
        from rasterio.warp import transform_geom
        geom = transform_geom(_BOUNDARY_CRS, raster_crs, geom)
        # transform_geom may return a string in older rasterio versions
        if isinstance(geom, str):
            geom = json.loads(geom)
    
    return geom


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_stats_from_array(
        data: np.ndarray,
        transform,
        crs: str,
        boundaries: "list[AdminBoundary]",
) -> "list[dict]":
    """
    Compute zonal statistics for all boundaries against one 2-D numpy array.

    Opens a single in-memory rasterio dataset from the array, then masks
    each boundary against that same open dataset.  This avoids the cost of
    creating and destroying a MemoryFile per boundary (previously ~4s/COG
    for 105 boundaries, now ~0.5s/COG).

    Parameters
    ----------
    data : np.ndarray
        2-D float32 array with NaN for nodata pixels.
    transform : affine.Affine
        Affine geotransform (pixel → geographic coordinates).
    crs : str
        Raster CRS as WKT or EPSG string.
    boundaries : list[AdminBoundary]
        Boundaries to compute stats for.

    Returns
    -------
    list[dict]
        One dict per boundary: boundary_id + six stat keys.
        Boundaries with no pixel intersection return all-None stats.
    """
    if not boundaries:
        return []
    
    height, width = data.shape
    raster_crs = RasterioCRS.from_user_input(crs)
    
    # Pre-parse all boundary geometries before opening the MemoryFile
    # so any geometry error is caught early without holding the file open.
    geoms = {}
    for boundary in boundaries:
        try:
            geoms[boundary.pk] = _geom_to_dict(boundary, raster_crs)
        except Exception as exc:
            logger.warning(
                "Failed to parse geometry for boundary %s: %s",
                boundary.pk, exc,
            )
            geoms[boundary.pk] = None
    
    results = []
    
    # Build one MemoryFile for the full COG array, open it once,
    # and run rasterio.mask for every boundary inside the same context.
    with MemoryFile() as memfile:
        with memfile.open(
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=np.float32,
                crs=raster_crs,
                transform=transform,
                nodata=np.nan,
        ) as dst:
            dst.write(data, 1)
        
        # Re-open read-only for masking — keeps the buffer alive
        with memfile.open() as dataset:
            for boundary in boundaries:
                geom = geoms.get(boundary.pk)
                stats = _mask_and_aggregate(dataset, geom, boundary.pk)
                stats["boundary_id"] = boundary.pk
                results.append(stats)
    
    return results


def compute_stats_from_cog_bytes(
        cog_bytes: bytes,
        boundaries: "list[AdminBoundary]",
) -> "list[dict]":
    """
    Compute zonal statistics from raw COG bytes (used by backfill task).

    Reads band 1 from the COG, replaces nodata with NaN, then delegates
    to compute_stats_from_array which handles the single-MemoryFile pattern.
    """
    with rasterio.open(io.BytesIO(cog_bytes)) as src:
        data = src.read(1).astype(np.float32)
        transform = src.transform
        crs = src.crs.to_wkt() if src.crs else "EPSG:4326"
        
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
    
    data[~np.isfinite(data)] = np.nan
    
    return compute_stats_from_array(data, transform, crs, boundaries)


# ---------------------------------------------------------------------------
# Masking + aggregation
# ---------------------------------------------------------------------------

def _mask_and_aggregate(dataset, geom: dict | None, boundary_pk) -> dict:
    """
    Apply a GeoJSON geometry mask to an open rasterio dataset and aggregate.

    Returns all-None stats if the geometry is None, empty, or does not
    intersect the raster extent.
    """
    if geom is None:
        return dict(_EMPTY_STATS)
    
    try:
        masked, _ = rasterio.mask.mask(
            dataset,
            [geom],
            crop=False,
            nodata=np.nan,
            all_touched=False,
        )
        arr = masked[0].astype(np.float32)
        arr[~np.isfinite(arr)] = np.nan
        
        valid = arr[~np.isnan(arr)]
        
        if len(valid) == 0:
            return dict(_EMPTY_STATS)
        
        return {
            "mean": float(np.mean(valid)),
            "min": float(np.min(valid)),
            "max": float(np.max(valid)),
            "sum": float(np.sum(valid)),
            "std": float(np.std(valid)),
            "count": int(len(valid)),
        }
    
    except Exception as exc:
        logger.warning(
            "Zonal stats mask failed for boundary %s: %s",
            boundary_pk, exc,
        )
        return dict(_EMPTY_STATS)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_stats(
        item: "Item",
        variable: "Variable",
        stats_rows: "list[dict]",
        overwrite: bool = False,
) -> int:
    """
    Bulk upsert BoundaryZonalStats rows for one (item, variable) pair.

    Parameters
    ----------
    overwrite : bool
        False (default) — bulk_create with update_conflicts=True using the
        unique constraint (time, item, variable, boundary).
        True — update_or_create per row (slower but works without the
        unique constraint if it hasn't been migrated yet).

    Returns the number of rows written.
    """
    from .models import BoundaryZonalStats
    
    if not stats_rows:
        return 0
    
    objects = [
        BoundaryZonalStats(
            time=item.time,  # hypertable partition key
            item=item,
            variable=variable,
            boundary_id=row["boundary_id"],
            mean=row.get("mean"),
            min=row.get("min"),
            max=row.get("max"),
            sum=row.get("sum"),
            std=row.get("std"),
            count=row.get("count"),
        )
        for row in stats_rows
        if row.get("boundary_id") is not None
    ]
    
    if not objects:
        return 0
    
    if overwrite:
        written = 0
        for obj in objects:
            BoundaryZonalStats.objects.update_or_create(
                time=obj.time,
                item_id=obj.item_id,
                variable_id=obj.variable_id,
                boundary_id=obj.boundary_id,
                defaults={
                    "mean": obj.mean, "min": obj.min, "max": obj.max,
                    "sum": obj.sum, "std": obj.std, "count": obj.count,
                },
            )
            written += 1
        return written
    
    BoundaryZonalStats.objects.bulk_create(
        objects,
        update_conflicts=True,
        unique_fields=["time", "item", "variable", "boundary"],
        update_fields=["mean", "min", "max", "sum", "std", "count"],
    )
    return len(objects)


# ---------------------------------------------------------------------------
# Boundary resolution
# ---------------------------------------------------------------------------

def get_boundaries_for_collection(collection) -> "dict[int, list[AdminBoundary]]":
    """
    Return AdminBoundary objects grouped by level for a collection.

    Returns an empty dict if boundary_stats_levels is not set or empty.
    """
    levels = getattr(collection, "boundary_stats_levels", None)
    if not levels:
        return {}
    
    from adminboundarymanager.models import AdminBoundary
    
    return {
        level: list(AdminBoundary.objects.filter(level=level))
        for level in levels
    }
