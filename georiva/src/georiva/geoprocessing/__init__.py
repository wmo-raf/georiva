"""
GeoRiva shared geoprocessing operations.

A pure, **non-Django** compute library used by both write-side processing
(compute-on-write) and read-side analysis (compute-on-read). Functions take
in-memory raster objects (numpy arrays with an affine transform + CRS, or
xarray DataArrays/Datasets) plus parameters, and return rasters or scalars.

No Django, no storage, no request layer — callers own their own I/O. This
keeps every operation unit-testable without a database and reusable on both
sides. See docs/adr/0005-generic-derivation-engine.md.

Implemented on the stack already present in the image (numpy, rasterio,
xarray, cftime). Regridding uses ``rasterio.warp.reproject`` and calendar
conversion uses xarray + cftime, so the library adds no new dependencies.
"""

from .algebra import EMPTY_STATS, raster_combine, safe_divide
from .calendar import convert_calendar
from .regrid import regrid_array
from .temporal import (
    SEASONS,
    anomaly,
    climatology,
    select_season,
    temporal_aggregate,
)
from .zonal import mask_and_aggregate, reproject_geometry, zonal_stats_from_array

__all__ = [
    "EMPTY_STATS",
    "raster_combine",
    "safe_divide",
    "convert_calendar",
    "regrid_array",
    "SEASONS",
    "anomaly",
    "climatology",
    "select_season",
    "temporal_aggregate",
    "mask_and_aggregate",
    "reproject_geometry",
    "zonal_stats_from_array",
]
