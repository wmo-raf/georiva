# GeoRiva Storage Architecture

> Internal reference for plugin developers and contributors.

## Overview

GeoRiva uses a multi-bucket storage architecture built on MinIO (S3-compatible). Data flows through dedicated buckets
based on its lifecycle stage, with automated ingestion triggered by bucket notifications.

```
georiva-incoming ──┐
                   ├──→ process ──→ georiva-assets
georiva-sources  ──┘
                   │
                   └──→ georiva-archive (raw copy before processing)
```

---

## Buckets

| Bucket             | Purpose                  | Who writes                 | Access      | Notifications            |
|--------------------|--------------------------|----------------------------|-------------|--------------------------|
| `georiva-incoming` | User-uploaded raw data   | Humans (MinIO Console)     | Private     | Yes — triggers ingestion |
| `georiva-sources`  | Plugin-collected data    | Automated source plugins   | Private     | Yes — triggers ingestion |
| `georiva-archive`  | Raw data preservation    | System (before processing) | Private     | No                       |
| `georiva-assets`   | Final processed datasets | Ingestion pipeline         | Public read | No                       |

**Why separate buckets instead of directories?**

MinIO treats directories as virtual — they only exist as long as objects are inside them. An empty directory disappears
on refresh. Buckets are persistent and always visible, which means:

- `georiva-incoming` is always visible in the MinIO Console, even when empty, so users always have a clear place to drop
  files.
- Each bucket can have its own access policy, retention rules, and event notifications.

---

## Path Convention

All files in GeoRiva follow a consistent path structure relative to the bucket root:

```
{catalog}/{collection}/{filename}
```

**Examples:**

```
georiva-incoming/satellite-imagery/ndvi/sentinel2_scene.tif
georiva-sources/weather-models/gfs/GR--20250115T0600--gfs_025.grib2
```

The `catalog` and `collection` segments map directly to GeoRiva's data model — the ingestion pipeline infers both from
the file path.

### Assets bucket

Processed assets add time partitioning and a variable segment:

```
georiva-assets/{catalog}/{collection}/{variable}/{year}/{month}/{day}/{filename}
```

**Example:**

```
georiva-assets/satellite-imagery/ndvi/temperature/2025/01/15/GR--20250115T0600--temp.tif
```

### Archive bucket

The archive mirrors the source path but prefixes it with the origin bucket type, so you can always trace a raw file back
to where it came from:

```
georiva-archive/{incoming|sources}/{catalog}/{collection}/{filename}
```

**Example:**

```
georiva-archive/sources/weather-models/gfs/GR--20250115T0600--gfs_025.grib2
```

---

## Filename Convention

### Standard files (no reference time)

Files without a reference time use their original name as-is:

```
sentinel2_ndvi_20250115.tif
synop_hourly.csv
station_obs.bufr
```

### Files with reference time (GR-- prefix)

Forecast and analysis data carry a reference time (the model run or analysis cycle time). Since this metadata may not
always be extractable from the file contents, GeoRiva encodes it in the filename using the `GR--` prefix:

```
GR--{YYYYMMDDTHHMM}--{original_name}.{ext}
```

**Examples:**

```
GR--20250115T0600--gfs_025.grib2
GR--20250115T1200--ecmwf_surface.grib2
GR--20250120T0000--icon_eu_temp.grib2
```

**Rules:**

- The datetime is **always UTC**. Naive datetimes are rejected; timezone-aware datetimes are converted to UTC before
  encoding.
- The format is fixed: `YYYYMMDDTHHMM` (12 characters, no seconds).
- The `GR--` prefix and `--` delimiters are reserved. We believe the combined pattern `GR--{12 chars}--` is unique
  enough to assume that no real-world file will collide with it.
- The original filename is preserved after the second `--`, untouched.

**Why `GR--` and not something simpler?**

Source data files often contain dates in their own filenames (e.g., `gfs_20250115_12z.grib2`). A simple date prefix
would be ambiguous. `GR--` is a namespace that unambiguously marks the reference time as a GeoRiva convention, not part
of the original filename.

---

## Plugin Developer Guide

### What plugins do

Plugins are responsible for:

1. Fetching data from an external source (API, FTP, HTTP, etc.)
2. Saving the data to the `georiva-sources` bucket with the correct path and filename.

**That's it.** Plugins do NOT trigger ingestion. The MinIO bucket notification handles that automatically when a file
lands in the bucket.

### How to save files

```python
from datetime import datetime, timezone
from georiva.core.filename import build_filename
from georiva.core.storage import storage

# Build the filename
filename = build_filename(
    original_filename="gfs_025.grib2",
    reference_time=datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc),
)
# → "GR--20250115T0600--gfs_025.grib2"

# Save to georiva-sources bucket
path = f"{catalog_slug}/{collection_slug}/{filename}"
storage.sources.save(path, file_data)
# → georiva-sources/weather-models/gfs/GR--20250115T0600--gfs_025.grib2
```

For files without a reference time:

```python
filename = build_filename(
    original_filename="sentinel2_ndvi_20250115.tif",
)
# → "sentinel2_ndvi_20250115.tif"  (no prefix added)

path = f"{catalog_slug}/{collection_slug}/{filename}"
storage.sources.save(path, file_data)
```

### Reference time rules

- If your data source has a reference time (forecast run time, analysis cycle), you **must** pass it to
  `build_filename`.
- The `reference_time` **must** be timezone-aware. Use `datetime.timezone.utc` or `pytz.utc`.
- If your data source does not have a reference time (satellite observations, station data), pass `None` or omit it.

```python
from datetime import datetime, timezone

# ✅ Correct — timezone-aware UTC
ref = datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc)

# ✅ Correct — will be converted to UTC
import pytz

ref = datetime(2025, 1, 15, 6, 0, tzinfo=pytz.utc)

# ❌ Wrong — naive datetime, will raise ValueError
ref = datetime(2025, 1, 15, 6, 0)
```

### What happens after you save

1. MinIO fires a webhook notification (`s3:ObjectCreated:*`).
2. The ingestion pipeline receives the event.
3. It parses the file path to determine `catalog`, `collection`, and `reference_time`.
4. The format plugin processes the file.
5. Processed assets are written to `georiva-assets`.
6. Optionally, the source file, after successful processing, is copied to `georiva-archive` and deleted from
   `georiva-sources`

```
Plugin saves file
    ↓
georiva-sources/weather-models/gfs/GR--20250115T0600--gfs_025.grib2
    ↓
MinIO webhook fires
    ↓
Ingestion pipeline:
    1. parse_path → catalog="weather-models", collection="gfs",
                    reference_time=2025-01-15T06:00Z
    2. Process → extract variables, clip, encode
    3. Save assets → georiva-assets/weather-models/gfs/temperature/2025/01/15/GR--20250115T0600--temp.tif
    4. Optionally archive original source files
```