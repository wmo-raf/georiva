from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING

import numpy as np
import rasterio

from georiva.geoprocessing.zonal import zonal_stats_from_array

if TYPE_CHECKING:
    from adminboundarymanager.models import AdminBoundary

    from georiva.core.models import Item, Variable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core computation — Django adapter over geoprocessing.zonal
# ---------------------------------------------------------------------------


def compute_stats_from_array(
    data: np.ndarray,
    transform,
    crs: str,
    boundaries: list[AdminBoundary],
) -> list[dict]:
    """
    Compute zonal statistics for all boundaries against one 2-D numpy array.

    Thin Django adapter: turns each AdminBoundary into a GeoJSON geometry
    (EPSG:4326) and delegates the actual masking/aggregation to the shared
    ``geoprocessing.zonal`` library. Reprojection to the raster CRS happens
    inside the library.

    Returns one dict per boundary: ``boundary_id`` + six stat keys.
    Boundaries with no pixel intersection return all-None stats.
    """
    if not boundaries:
        return []

    geometries = []
    for boundary in boundaries:
        try:
            geometries.append((boundary.pk, json.loads(boundary.geom.geojson)))
        except Exception as exc:
            logger.warning(
                "Failed to parse geometry for boundary %s: %s",
                boundary.pk,
                exc,
            )
            geometries.append((boundary.pk, None))

    rows = zonal_stats_from_array(data, transform, crs, geometries)
    for row in rows:
        row["boundary_id"] = row.pop("key")
    return rows


def compute_stats_from_cog_bytes(
    cog_bytes: bytes,
    boundaries: list[AdminBoundary],
) -> list[dict]:
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
# Persistence
# ---------------------------------------------------------------------------


def persist_stats(
    item: Item,
    variable: Variable,
    stats_rows: list[dict],
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
                    "mean": obj.mean,
                    "min": obj.min,
                    "max": obj.max,
                    "sum": obj.sum,
                    "std": obj.std,
                    "count": obj.count,
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


def get_boundaries_for_collection(collection) -> dict[int, list[AdminBoundary]]:
    """
    Return AdminBoundary objects grouped by level for a collection.

    Returns an empty dict if boundary_stats_levels is not set or empty.
    """
    levels = getattr(collection, "boundary_stats_levels", None)
    if not levels:
        return {}

    from adminboundarymanager.models import AdminBoundary

    return {level: list(AdminBoundary.objects.filter(level=level)) for level in levels}


# ---------------------------------------------------------------------------
# Retention — one rule, read by the pruner and the recovery sweep
# ---------------------------------------------------------------------------


def retained_items_q(collection, now, latest_reference_time, prefix: str = "item__"):
    """
    The ``Q`` over an Item's time fields matching what the collection's forecast
    retention keeps; ``None`` when it keeps everything.

    ``sweep_stale_boundary_stats`` deletes what this excludes and
    ``sweep_missing_boundary_stats`` recomputes only what this keeps, so the two
    cannot fight over a row.  ``latest_reference_time`` is the caller's notion of
    "the latest run" (the pruner's: runs with stats; the sweep's: runs with items).
    """
    from django.db.models import Q

    if not collection.is_forecast:
        return None

    kept = Q()
    if not collection.retain_past_forecasts:
        kept &= Q(**{f"{prefix}time__gte": now})
    if collection.retain_latest_run_only and latest_reference_time:
        kept &= Q(**{f"{prefix}reference_time": latest_reference_time})
    return kept or None


# ---------------------------------------------------------------------------
# Availability — what a viewer can actually be shown
# ---------------------------------------------------------------------------


def available_levels_by_variable(item, collection) -> dict[str, list[int]]:
    """
    Return ``{variable_slug: [level, ...]}`` for the aggregates that exist
    for this item and are still wanted by the collection.

    A level counts as available only when it is both listed in the
    collection's ``boundary_stats_levels`` (de-configuring a level hides it,
    even if stale rows survive) and backed by at least one row with a
    non-null mean (a run that covered nothing would otherwise promise a
    blank choropleth).

    One query. Filtering on ``time`` prunes the hypertable to the item's own
    chunk, which the (boundary, variable, item) index cannot do on its own.
    """
    wanted = set(getattr(collection, "boundary_stats_levels", None) or [])
    if not wanted:
        return {}

    from .models import BoundaryZonalStats

    rows = (
        BoundaryZonalStats.objects.filter(
            item=item,
            time=item.time,
            mean__isnull=False,
            boundary__level__in=wanted,
        )
        .values_list("variable__slug", "boundary__level")
        .distinct()
    )

    by_variable: dict[str, list[int]] = {}
    for slug, level in rows:
        by_variable.setdefault(slug, []).append(level)
    return {slug: sorted(levels) for slug, levels in by_variable.items()}
