"""A pure, non-Django compute library (ADR 0005).

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
    trend,
)
from .wind import wind_from_direction, wind_speed
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
    "trend",
    "wind_from_direction",
    "wind_speed",
    "mask_and_aggregate",
    "reproject_geometry",
    "zonal_stats_from_array",
]
