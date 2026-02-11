"""
GeoRiva Filename Convention

Files with a reference time (forecast/analysis) MUST use the GR-- prefix:

    GR--{YYYYMMDDTHHMM}--{original_name}.{ext}

Files without a reference time use their original name as-is:

    {original_name}.{ext}

Path convention (collection is optional):
    georiva-{bucket}/{catalog}/[{collection}/][GR--YYYYMMDDTHHMM--]{name}.{ext}
    georiva-assets/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/[GR--YYYYMMDDTHHMM--]{name}.{ext}

Examples:
    weather-models/gfs/GR--20250115T0600--gfs_025.grib2       ← catalog + collection + ref time
    satellite-imagery/ndvi/sentinel2_20250115.tif              ← catalog + collection, no ref time
    weather-models/GR--20250115T0600--gfs_025.grib2           ← catalog only + ref time
    station-data/synop_hourly.csv                              ← catalog only, no ref time
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

# Pattern: GR--YYYYMMDDTHHMM--rest_of_filename
GEORIVA_REFTIME_PATTERN = re.compile(r'^GR--(\d{8}T\d{4})--(.+)$')
GEORIVA_REFTIME_FORMAT = '%Y%m%dT%H%M'


# =============================================================================
# Filename operations
# =============================================================================

def has_reference_time(filename: str) -> bool:
    """Check if a filename carries a GR-- reference time prefix."""
    return GEORIVA_REFTIME_PATTERN.match(filename) is not None


def parse_filename(filename: str) -> dict:
    """
    Parse a filename, extracting reference time if present.

    Args:
        filename: Just the filename (not a full path).

    Returns:
        {
            'reference_time': datetime (UTC) or None,
            'original_name': str,
        }

    Examples:
        >>> parse_filename("GR--20250115T0600--gfs_025.grib2")
        {'reference_time': datetime(2025,1,15,6,0, tzinfo=utc), 'original_name': 'gfs_025.grib2'}

        >>> parse_filename("sentinel2_ndvi.tif")
        {'reference_time': None, 'original_name': 'sentinel2_ndvi.tif'}
    """
    match = GEORIVA_REFTIME_PATTERN.match(filename)
    
    if not match:
        return {
            'reference_time': None,
            'original_name': filename,
        }
    
    ref_str, original_name = match.groups()
    
    try:
        reference_time = datetime.strptime(ref_str, GEORIVA_REFTIME_FORMAT)
        reference_time = pytz.utc.localize(reference_time)
    except ValueError:
        return {
            'reference_time': None,
            'original_name': filename,
        }
    
    return {
        'reference_time': reference_time,
        'original_name': original_name,
    }


def build_filename(
        original_filename: str,
        reference_time: Optional[datetime] = None,
) -> str:
    """
    Build a filename, adding GR-- prefix only if reference_time is provided.

    Args:
        original_filename: Original filename with extension.
        reference_time: Optional forecast/analysis reference time (must be timezone-aware).

    Returns:
        Filename string.

    Raises:
        ValueError: If reference_time is naive (no timezone).

    Examples:
        >>> build_filename("gfs_025.grib2", datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc))
        'GR--20250115T0600--gfs_025.grib2'

        >>> build_filename("sentinel2_ndvi.tif")
        'sentinel2_ndvi.tif'
    """
    if reference_time is None:
        return original_filename
    
    if reference_time.tzinfo is None:
        raise ValueError(
            "reference_time must be timezone-aware (UTC). "
            "Got naive datetime. Use datetime(..., tzinfo=timezone.utc)"
        )
    
    utc_time = reference_time.astimezone(pytz.utc)
    ref_str = utc_time.strftime(GEORIVA_REFTIME_FORMAT)
    return f"GR--{ref_str}--{original_filename}"


# =============================================================================
# Full path operations
# =============================================================================

def parse_path(file_path: str) -> dict:
    """
    Parse a GeoRiva storage path.

    Handles both:
        {catalog}/{collection}/{filename}    → catalog + collection
        {catalog}/{filename}                 → catalog only, no collection

    The distinction: if the path has 3+ parts, the second part is
    treated as a collection. If only 2 parts, there's no collection.

    Args:
        file_path: Path relative to bucket root.

    Returns:
        {
            'catalog': str or None,
            'collection': str or None,
            'reference_time': datetime (UTC) or None,
            'original_name': str,
        }
    """
    parts = Path(file_path).parts
    filename = Path(file_path).name
    parsed = parse_filename(filename)
    
    if len(parts) >= 3:
        # {catalog}/{collection}/[...dirs...]/{filename}
        catalog = parts[0]
        collection = parts[1]
    elif len(parts) == 2:
        # {catalog}/{filename}
        catalog = parts[0]
        collection = None
    else:
        catalog = None
        collection = None
    
    return {
        'catalog': catalog,
        'collection': collection,
        'reference_time': parsed['reference_time'],
        'original_name': parsed['original_name'],
    }


def validate_path(file_path: str) -> dict:
    """
    Validate a file path has at minimum a catalog and a filename.

    Does NOT require collection or GR-- prefix.

    Args:
        file_path: Path relative to bucket root.

    Returns:
        Parsed metadata dict.

    Raises:
        ValueError: If path doesn't have at least catalog/filename.
    """
    parts = Path(file_path).parts
    
    if len(parts) < 2:
        raise ValueError(
            f"Invalid path: '{file_path}'. "
            f"Expected at minimum: {{catalog}}/filename.ext"
        )
    
    return parse_path(file_path)
