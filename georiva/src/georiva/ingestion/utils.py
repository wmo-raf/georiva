from datetime import datetime
from typing import Generator, Optional

import numpy as np
import pandas as pd
import pytz

from georiva.core.unit_utils import ureg


def apply_unit_conversion(data: np.ndarray, source_unit=None, output_unit=None) -> np.ndarray:
    if not source_unit or not output_unit or source_unit == output_unit:
        return data
    quantity = ureg.Quantity(data, source_unit.pint_unit)
    return np.asarray(quantity.to(output_unit.pint_unit).magnitude, dtype=np.float32)


def iter_windows(
        width: int,
        height: int,
        block_size: int = 2048
) -> Generator[tuple[int, int, int, int], None, None]:
    """
    Yield (x_offset, y_offset, width, height) windows for chunked processing.
    """
    for y in range(0, height, block_size):
        h = min(block_size, height - y)
        for x in range(0, width, block_size):
            w = min(block_size, width - x)
            yield x, y, w, h


def normalize_bounds(bounds: list | tuple) -> list:
    """
    Normalise bounds to valid WGS84 range.

    Handles:
      - 0–360 longitude convention (common in GRIB/ERA5) → -180 to 180
      - Latitude clamping to -90/90 (guards against floating point drift)
      - Longitude clamping to -180/180
    """
    west, south, east, north = bounds
    
    if west > 180:
        west -= 360
    if east > 180:
        east -= 360
    
    south = max(-90.0, min(90.0, south))
    north = max(-90.0, min(90.0, north))
    west = max(-180.0, min(180.0, west))
    east = max(-180.0, min(180.0, east))
    
    return [west, south, east, north]


def ensure_utc(dt) -> Optional[datetime]:
    """
    Coerce any datetime-like value to a timezone-aware UTC datetime.

    Handles: str, pandas Timestamp, numpy datetime64, and Python datetime.
    Naive datetimes are assumed to be UTC.
    """
    if dt is None:
        return None
    
    if isinstance(dt, str):
        dt = pd.Timestamp(dt).to_pydatetime()
    
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    
    if isinstance(dt, np.datetime64):
        dt = pd.Timestamp(dt).to_pydatetime()
    
    if dt.tzinfo is None:
        return pytz.utc.localize(dt)
    
    return dt.astimezone(pytz.utc)


def compute_stats(data: np.ndarray) -> dict:
    """
    Compute basic descriptive statistics from a masked float array.

    Uses nanmin/nanmax/nanmean/nanstd so that NaN nodata pixels
    (introduced by clipping or source data) are excluded.

    Returns None values on failure — stats should not abort asset creation.
    """
    try:
        return {
            "min": float(np.nanmin(data)),
            "max": float(np.nanmax(data)),
            "mean": float(np.nanmean(data)),
            "std": float(np.nanstd(data)),
        }
    except Exception:
        return {"min": None, "max": None, "mean": None, "std": None}
