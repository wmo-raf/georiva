# GeoRiva Storage & Ingestion Architecture

> Internal reference for developers, plugin authors, and contributors.

## Overview

GeoRiva is a geospatial data platform that ingests raw Earth observation and meteorological files, processes them into standardized assets (PNGs, COGs, metadata JSON), and serves them via STAC-compliant APIs and visualization layers.

The system is built around a multi-bucket MinIO storage architecture. Data flows through dedicated buckets based on its lifecycle stage. Ingestion is event-driven — files landing in a bucket automatically trigger processing via webhook notifications and a Celery task queue.

```
                    ┌─────────────────┐
  Users (Console) → │ georiva-incoming │──┐
                    └─────────────────┘  │
                                         │   ┌──────────────┐     ┌────────────────┐
                                         ├──→│  Ingestion    │────→│ georiva-assets │
                                         │   │  Pipeline     │     │ (public read)  │
                    ┌─────────────────┐  │   └──────┬───────┘     └────────────────┘
  Plugins ────────→ │ georiva-sources  │──┘          │
                    └─────────────────┘              │
                                                     ↓
                                              ┌──────────────┐
                                              │georiva-archive│
                                              │ (raw backup)  │
                                              └──────────────┘
```

---

## Data Model

Before diving into storage, it helps to understand GeoRiva's data hierarchy:

**Catalog** → A data source or product family (e.g., `weather-models`, `satellite-imagery`). Defines the file format, boundary for clipping, and whether to archive source files.

**Collection** → A specific dataset within a catalog (e.g., `gfs`, `ecmwf-ifs`, `ndvi`). Collections contain Variables.

**Variable** → A measurable quantity (e.g., `temperature`, `precipitation`, `wind-speed`). Variables define value ranges, units, color maps, and scale types.

**Item** → A single observation or forecast timestep in a collection. Each Item has a timestamp, optional reference time, spatial bounds, and one or more Assets.

**Asset** → A processed output file for a single variable at a single time. Assets come in three formats: PNG (visual), COG (data), and JSON (metadata).

---

## Buckets

GeoRiva uses four dedicated MinIO buckets. Each bucket has a specific role, access policy, and notification configuration.

| Bucket | Purpose | Who writes | Access | Webhook |
|---|---|---|---|---|
| `georiva-incoming` | User-uploaded raw data | Humans via MinIO Console | Private | Yes |
| `georiva-sources` | Plugin-collected data | Automated source plugins | Private | Yes |
| `georiva-archive` | Raw data preservation | Ingestion pipeline | Private | No |
| `georiva-assets` | Processed datasets for serving | Ingestion pipeline | Public read | No |

**Why separate buckets instead of directories?**

MinIO treats directories as virtual — they disappear when empty because a "directory" is just a common prefix shared by objects. There is no actual directory object. Buckets, on the other hand, are persistent and always visible even when empty. This is critical for `georiva-incoming` — novice users must always see a clear drop target in the MinIO Console.

Additionally, separate buckets allow per-bucket access policies (assets are public, everything else is private), per-bucket event notifications (only incoming and sources trigger ingestion), and independent lifecycle rules.

---

## Path Convention

### Source buckets (incoming and sources)

Files placed in `georiva-incoming` or `georiva-sources` follow this structure:

```
{catalog}/{collection}/{filename}
```

The `collection` segment is **optional**. Some data sources organize files with only a catalog:

```
{catalog}/{filename}
```

When the collection is missing from the path, the ingestion pipeline processes the file against **all active collections** under that catalog.

**Examples:**

```
weather-models/gfs/GR--20250115T0600--gfs_025.grib2     ← catalog + collection
satellite-imagery/sentinel2_20250115.tif                  ← catalog only
station-data/daily/synop_hourly.csv                       ← catalog + collection
```

The minimum valid path is `{catalog}/{filename}` (2 segments). A bare filename with no catalog is rejected.

### Assets bucket

Processed assets use a time-partitioned structure with a variable segment for efficient range queries:

```
georiva-assets/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{filename}
```

**Example:**

```
georiva-assets/weather-models/gfs/temperature/2025/01/15/temperature_060000.png
georiva-assets/weather-models/gfs/temperature/2025/01/15/temperature_060000.tif
georiva-assets/weather-models/gfs/temperature/2025/01/15/temperature_060000.json
```

Each variable at each timestep produces three files: a PNG for visualization (e.g., WeatherLayers GL), a Cloud-Optimized GeoTIFF for data access, and a JSON sidecar with metadata, stats, and color map information.

### Archive bucket

The archive preserves raw source files before processing. It mirrors the source path but adds the origin bucket type as a prefix, so you can always trace a raw file back to where it came from:

```
georiva-archive/{incoming|sources}/{catalog}/{collection}/{filename}
```

**Example:**

```
georiva-archive/sources/weather-models/gfs/GR--20250115T0600--gfs_025.grib2
georiva-archive/incoming/station-data/daily/synop_20250115.csv
```

Whether archiving happens is controlled by the `Catalog.archive_source_files` field. When enabled, the ingestion pipeline copies the raw file to the archive and deletes it from the source bucket after successful processing.

---

## Filename Convention

### Standard files (no reference time)

Files without a forecast or analysis reference time use their original name as-is:

```
sentinel2_ndvi_20250115.tif
synop_hourly.csv
station_obs.bufr
```

### Files with reference time (GR-- prefix)

Forecast and analysis data carry a reference time — the model run time or analysis cycle time. This metadata is critical for correctly cataloging the data but may not always be extractable from the file contents alone. GeoRiva encodes it in the filename using the `GR--` prefix:

```
GR--{YYYYMMDDTHHMM}--{original_name}.{ext}
```

**Examples:**

```
GR--20250115T0600--gfs_025.grib2          ← GFS 06Z run
GR--20250115T1200--ecmwf_surface.grib2    ← ECMWF 12Z run
GR--20250120T0000--icon_eu_temp.grib2     ← ICON-EU 00Z run
```

**Rules:**

The datetime is always UTC. The `build_filename()` function rejects naive datetimes and converts timezone-aware datetimes to UTC before encoding. The format is fixed at `YYYYMMDDTHHMM` — 12 characters, no seconds, no `Z` suffix (the convention guarantees UTC). The original filename is preserved after the second `--` delimiter, untouched.

**Why `GR--`?**

Source data files often contain dates in their own filenames (e.g., `gfs_20250115_12z.grib2`). A simple date prefix would be ambiguous — is `20250115` part of the original name or the reference time? The `GR--` namespace unambiguously marks the reference time as a GeoRiva convention. The pattern `GR--{12 chars}--` is unique enough that no real-world filename will collide with it.

**When to use the GR-- prefix:**

Use it for forecast data (model run time), analysis data (analysis cycle time), and reanalysis data (reference time). Do not use it for satellite observations, station data, or any data where the reference time equals the valid/observation time — those files use their original names.

---

## Ingestion Pipeline

### How it works

The ingestion pipeline is the core of GeoRiva's data processing. It takes raw files from `georiva-incoming` or `georiva-sources`, processes them into standardized assets, and writes the results to `georiva-assets`.

The pipeline is event-driven and runs asynchronously:

```
1. File lands in bucket
       ↓
2. MinIO fires webhook (s3:ObjectCreated:*)
       ↓
3. Django webhook view receives event
       ↓
4. File registered in IngestionLog (status: pending)
       ↓
5. Celery task queued
       ↓
6. Worker acquires lock (status: processing)
       ↓
7. IngestionService.process_file() runs:
   a. Parse path → catalog, collection, reference_time
   b. Resolve Catalog model
   c. Resolve Collection(s):
      - If collection in path → [that collection]
      - If no collection → all active collections under catalog
   d. Load format plugin (GRIB, NetCDF, GeoTIFF, etc.)
   e. Initialize boundary clipper (if catalog has a boundary)
   f. Download raw file to local temp
   g. Extract timestamps from file
   h. For each collection × timestamp:
      - Create/update Item record
      - For each active Variable in collection:
        · Extract raw data array
        · Apply unit conversion
        · Apply boundary clipping/masking
        · Encode to RGBA PNG (visual asset)
        · Write COG (data asset)
        · Write metadata JSON (sidecar)
        · Create/update Asset records
      - Update Collection extent (time + spatial)
   i. Archive raw file (if catalog.archive_source_files)
   j. Delete from source bucket (if archived)
   k. Clean up temp file
       ↓
8. IngestionLog updated (status: completed or failed)
```

### Webhook view

The webhook view is the entry point for all MinIO events. It is intentionally lightweight — validate, register, queue:

```
MinIO event → authenticate → parse path → validate catalog exists
           → register in IngestionLog → queue Celery task → respond 200
```

The view only accepts events from `georiva-incoming` and `georiva-sources`. Events from `georiva-archive` and `georiva-assets` are ignored. Catalog validation uses an `lru_cache` to avoid database hits on every event.

The webhook does **not** resolve collections. That responsibility belongs to the ingestion service, which has the full context to handle both explicit collection paths and catalog-wide processing.

### Celery task

The Celery task is a thin wrapper around the ingestion service. Its job is lock management:

```python
def process_incoming_file(self, file_path, origin_bucket, reference_time=None):
    # 1. Acquire lock (atomic — only one worker wins)
    if not IngestionLog.acquire(origin_bucket, file_path, worker_id):
        return  # another worker has it, or already completed

    # 2. Run ingestion
    result = service.process_file(file_path, origin_bucket, reference_time)

    # 3. Update state
    if result.success:
        IngestionLog.mark_completed(...)
    else:
        IngestionLog.mark_failed(...)
```

Tasks are configured with `acks_late=True`, meaning the message is only acknowledged after the task completes. If a worker crashes mid-processing, the message returns to the queue.

### Collection resolution

When processing a file, the ingestion service resolves which collections to process against:

**Collection in path** (e.g., `weather-models/gfs/file.grib2`) → process against the `gfs` collection only.

**No collection in path** (e.g., `weather-models/file.grib2`) → process against ALL active collections under the `weather-models` catalog. This is useful for data sources that serve multiple variables across collections from a single file.

In both cases, the file is downloaded once and timestamps are extracted once. The collection × timestamp loop then processes each combination.

### Format plugins

Format plugins handle the specifics of reading different file formats. Each Catalog has a `file_format` field that maps to a registered plugin. The plugin interface provides:

`get_timestamps(local_path)` → extract all valid timestamps from the file.

`extract(variable, local_path, timestamp, window)` → read a 2D data array for a specific variable, timestamp, and optional spatial window.

`get_metadata(variable, local_path, timestamp)` → return spatial metadata (width, height, bounds, CRS).

Plugins are registered in the format registry and looked up by name (e.g., `grib`, `netcdf`, `geotiff`).

### Boundary clipping

If a Catalog has an associated boundary geometry, the ingestion pipeline clips processed data to that boundary. This dramatically reduces asset sizes for regional datasets derived from global sources.

There are two clipping modes, configured on the Catalog:

`bbox` — clip to the bounding box of the boundary geometry. Fast, rectangular.

`mask` — clip to bbox, then apply the geometry as a mask (setting pixels outside the boundary to transparent/nodata). Produces clean edges that follow country or region borders.

Clipping is applied per-variable after extraction and unit conversion but before encoding.

---

## IngestionLog

The `IngestionLog` model tracks every file that enters the system. It serves three purposes: preventing duplicate processing, enabling crash recovery, and providing an audit trail.

### Lifecycle

```
pending → processing → completed
                     → failed → processing (retry) → completed
                                                    → failed (max retries)
```

### States

**pending** — file registered, waiting to be picked up by a worker.

**processing** — a worker has acquired the lock and is actively processing. The `locked_at` timestamp and `locked_by` worker ID are recorded.

**completed** — processing succeeded. The `archive_path`, `items_created`, and `assets_created` fields are populated.

**failed** — processing failed. The `error` field contains the error message. The file can be retried up to `MAX_RETRIES` (default: 3) times.

### Lock acquisition

Lock acquisition is atomic — it uses a SQL `UPDATE ... WHERE` that only one worker can win:

```python
# Only updates rows that are pending or retryable-failed
IngestionLog.objects.filter(
    bucket=bucket,
    file_path=file_path,
    status__in=['pending', 'failed'],
    retry_count__lt=MAX_RETRIES,
).update(
    status='processing',
    locked_at=now,
    locked_by=worker_id,
    retry_count=F('retry_count') + 1,
)
```

If the update affects 0 rows, the file is either already being processed, already completed, or has exceeded max retries. The worker moves on.

### Crash recovery

If a worker crashes (OOM kill, SIGKILL, hardware failure) while processing a file, the lock becomes stale. The sweep task (described below) detects locks older than `LOCK_TIMEOUT` (default: 30 minutes) and resets them to `pending` so they can be picked up again.

A stale lock can also be reclaimed directly by the next worker that tries to process the same file — the `acquire()` method checks for stale locks as a fallback.

---

## Sweep Task

The sweep task is a periodic safety net that runs every 5 minutes via Celery Beat. It catches anything the webhook might have missed and handles error recovery.

### Three phases

**Phase 1: Reset stale locks.** Find any file stuck in `processing` for longer than `LOCK_TIMEOUT`. Reset to `pending`. This handles crashed workers.

**Phase 2: Scan for untracked files.** List all files in `georiva-incoming` and `georiva-sources`. For each file not found in the `IngestionLog`, register it and queue a task. This catches files that landed while the webhook was down, or events that MinIO failed to deliver.

**Phase 3: Retry failed files.** Find files in `failed` status with `retry_count < MAX_RETRIES`. Queue them for reprocessing.

### Configuration

```python
# settings.py
CELERY_BEAT_SCHEDULE = {
    'sweep-unprocessed': {
        'task': 'georiva.ingestion.tasks.sweep_unprocessed',
        'schedule': 300,  # every 5 minutes
    },
}
```

The sweep interval is a balance between responsiveness and MinIO listing costs. At 5 minutes, a missed file waits at most 5 minutes before being picked up. For most deployments this is acceptable since the webhook handles the vast majority of files instantly.

---

## Celery Configuration

GeoRiva uses a single Celery queue for all tasks. The worker is configured for reliability:

```python
# settings.py

# Only acknowledge tasks after completion — survives worker crashes
CELERY_TASK_ACKS_LATE = True

# If a worker is killed by OOM, reject the task back to the queue
CELERY_TASK_REJECT_ON_WORKER_LOST = True

# Don't let one worker hoard tasks — fetch one at a time
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
```

Worker concurrency controls how many files process simultaneously. Since ingestion is memory-intensive (large raster arrays), keep concurrency low:

```bash
# 2 concurrent tasks per worker — good for most deployments
celery -A georiva worker --concurrency=2

# Scale by adding more workers, not more concurrency
```

The queue acts as a backpressure mechanism. If 1000 files land at once, they all queue instantly (the webhook is fast), but only 2 process at a time. As each completes, the next starts.

---

## Storage Configuration

### Settings

```python
# settings.py

GEORIVA_STORAGE_BACKEND = 's3'  # or 'local'

# Internal endpoint — Docker service name, used for read/write
AWS_S3_ENDPOINT_URL = "http://georiva-minio:9000"

# Public endpoint — what browsers can reach, used for asset URLs
MINIO_PUBLIC_ENDPOINT = "localhost:9000"  # or "minio.georiva.io" in production

# Bucket definitions with per-bucket overrides
GEORIVA_BUCKETS = {
    "incoming": {
        "name": "georiva-incoming",
    },
    "sources": {
        "name": "georiva-sources",
    },
    "archive": {
        "name": "georiva-archive",
    },
    "assets": {
        "name": "georiva-assets",
        "overrides": {
            "custom_domain": f"{MINIO_PUBLIC_ENDPOINT}/georiva-assets",
            "querystring_auth": False,
        },
    },
}
```

The assets bucket uses `custom_domain` and `querystring_auth=False` to generate clean, unsigned public URLs. The other three buckets use the internal Docker endpoint with signed URLs since they are private and only accessed server-side.

**URL comparison:**

```
Private buckets (internal + signed):
  http://georiva-minio:9000/georiva-sources/...?X-Amz-Signature=...

Assets bucket (public + clean):
  http://localhost:9000/georiva-assets/weather-models/gfs/temperature/2025/01/15/temp.png
```

### Management commands

```bash
# Create buckets, set policies, configure notifications
python manage.py setup_minio

# Preview changes without applying
python manage.py setup_minio --dry-run

# Verify buckets exist (lightweight — use in Docker entrypoint)
python manage.py ensure_buckets
```

---

## Plugin Developer Guide

### What plugins do

A GeoRiva plugin is responsible for two things:

1. Fetching data from an external source (API, FTP, HTTP, etc.)
2. Saving the raw data to the `georiva-sources` bucket with the correct path and filename.

That is all. Plugins do **not** trigger ingestion, resolve collections, or process data. The MinIO webhook handles everything downstream automatically when a file lands in the bucket.

### How to save files

```python
from datetime import datetime, timezone
from georiva.core.filename import build_filename
from georiva.core.storage import storage

# Forecast data — build filename with reference time
filename = build_filename(
    original_filename="gfs_025.grib2",
    reference_time=datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc),
)
# → "GR--20250115T0600--gfs_025.grib2"

# Save to georiva-sources bucket
path = f"{catalog_slug}/{collection_slug}/{filename}"
storage.sources.save(path, file_data)
```

For files without a reference time:

```python
# Observation data — no GR-- prefix
filename = build_filename(
    original_filename="sentinel2_ndvi_20250115.tif",
)
# → "sentinel2_ndvi_20250115.tif"

path = f"{catalog_slug}/{collection_slug}/{filename}"
storage.sources.save(path, file_data)
```

If your data source doesn't have a natural collection-level grouping, you can save with just a catalog:

```python
path = f"{catalog_slug}/{filename}"
storage.sources.save(path, file_data)
# The pipeline will process against all active collections under this catalog
```

### Reference time rules

The `reference_time` must be timezone-aware. Naive datetimes raise `ValueError`.

```python
from datetime import datetime, timezone

# Correct
ref = datetime(2025, 1, 15, 6, 0, tzinfo=timezone.utc)

# Also correct
import pytz
ref = datetime(2025, 1, 15, 6, 0, tzinfo=pytz.utc)

# Wrong — raises ValueError
ref = datetime(2025, 1, 15, 6, 0)
```

Use the reference time for forecast data (model run time), analysis data (analysis cycle), and reanalysis data. Do not use it for observations where the reference time is the same as the observation time.

### Avoiding duplicate fetches

Before downloading a file, check if it already exists:

```python
path = f"{catalog_slug}/{collection_slug}/{filename}"
if storage.sources.exists(path):
    logger.info("Already fetched: %s", path)
    return
```

### What happens after you save

```
Plugin saves file to georiva-sources
    ↓
MinIO webhook fires (s3:ObjectCreated:*)
    ↓
Webhook view:
    1. Validates catalog exists
    2. Registers file in IngestionLog (pending)
    3. Queues Celery task
    ↓
Celery worker:
    4. Acquires IngestionLog lock (processing)
    5. Parses path → catalog, collection, reference_time
    6. Downloads file to local temp
    7. Extracts timestamps
    8. For each collection × timestamp × variable:
       - Extract → convert → clip → encode → save assets
    9. Archives raw file (if catalog.archive_source_files)
    10. Deletes from georiva-sources (if archived)
    11. Updates IngestionLog (completed)
```

### Plugin checklist

- Save files to `storage.sources` (never `storage.incoming` — that is for human uploads)
- Use `build_filename()` to construct the filename
- Pass timezone-aware UTC `reference_time` for forecast/analysis data
- Use path structure `{catalog_slug}/{collection_slug}/{filename}` or `{catalog_slug}/{filename}`
- Do NOT call the ingestion service directly — let the bucket notification handle it
- Check `storage.sources.exists()` before fetching to skip duplicates

---

## Storage API Reference

```python
from georiva.core.storage import storage

# Access buckets
storage.incoming     # georiva-incoming
storage.sources      # georiva-sources
storage.archive      # georiva-archive
storage.assets       # georiva-assets

# File operations (same API on all buckets)
bucket.save(path, content)          # Save bytes or file-like object
bucket.read_bytes(path)             # Read file as bytes
bucket.open(path, mode='rb')        # Open file handle
bucket.exists(path)                  # Check existence
bucket.delete(path)                  # Delete file
bucket.url(path)                     # Get URL (internal for private, public for assets)
bucket.size(path)                    # File size in bytes
bucket.list_files(path)              # List files in directory
bucket.list_directories(path)        # List subdirectories

# Cross-bucket operations
storage.transfer(source, dest, path)       # Copy between buckets (S3 server-side)
storage.move_between(source, dest, path)   # Move between buckets
storage.archive_raw(origin_bucket, path)   # Archive with origin prefix

# Asset path builder (time-partitioned)
storage.build_asset_path(catalog, collection, variable, timestamp, filename)
# → "weather-models/gfs/temperature/2025/01/15/temperature_060000.tif"
```

---

## Filename API Reference

```python
from georiva.core.filename import (
    build_filename,
    parse_filename,
    parse_path,
    validate_path,
    has_reference_time,
)

# Build a filename (adds GR-- prefix if reference_time is provided)
build_filename("gfs.grib2", ref_time)   # → "GR--20250115T0600--gfs.grib2"
build_filename("obs.csv")               # → "obs.csv"

# Parse a filename
parse_filename("GR--20250115T0600--gfs.grib2")
# → {'reference_time': datetime(2025,1,15,6,0, tzinfo=UTC), 'original_name': 'gfs.grib2'}

parse_filename("obs.csv")
# → {'reference_time': None, 'original_name': 'obs.csv'}

# Parse a full path (with or without collection)
parse_path("weather-models/gfs/GR--20250115T0600--gfs.grib2")
# → {'catalog': 'weather-models', 'collection': 'gfs',
#    'reference_time': datetime(UTC), 'original_name': 'gfs.grib2'}

parse_path("weather-models/GR--20250115T0600--gfs.grib2")
# → {'catalog': 'weather-models', 'collection': None,
#    'reference_time': datetime(UTC), 'original_name': 'gfs.grib2'}

# Validate a path (minimum: catalog + filename)
validate_path("weather-models/gfs/file.grib2")    # → OK
validate_path("weather-models/file.grib2")          # → OK
validate_path("file.grib2")                          # → raises ValueError

# Check for reference time
has_reference_time("GR--20250115T0600--gfs.grib2")  # → True
has_reference_time("obs.csv")                         # → False
```

---

## IngestionLog API Reference

```python
from georiva.ingestion.models import IngestionLog

# Register a file (idempotent — returns existing if already registered)
log, created = IngestionLog.register(
    bucket='sources', file_path='weather-models/gfs/file.grib2',
    catalog_slug='weather-models', collection_slug='gfs',
)

# Acquire a processing lock (atomic — returns True if acquired)
acquired = IngestionLog.acquire(bucket, file_path, worker_id)

# Update state
IngestionLog.mark_completed(bucket, file_path, archive_path='...', items_created=5)
IngestionLog.mark_failed(bucket, file_path, error='Connection timed out')

# Queries
IngestionLog.is_known(bucket, file_path)       # Any status
IngestionLog.is_done(bucket, file_path)         # Completed only
IngestionLog.reset_stale_locks()                # Reset locks older than LOCK_TIMEOUT
IngestionLog.get_retryable(limit=50)            # Failed files under retry limit
IngestionLog.get_permanently_failed()           # Exceeded max retries
```

---

## Docker Deployment

### Services

```yaml
services:
  georiva:
    command: gunicorn
    depends_on:
      - georiva-minio
      - georiva-redis

  celery-worker:
    command: celery-worker
    depends_on:
      - georiva-minio
      - georiva-redis

  celery-beat:
    command: celery-beat
    depends_on:
      - georiva-redis

  georiva-minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
    volumes:
      - minio_data:/data

  georiva-redis:
    image: redis:7-alpine
```

### Startup sequence

```bash
# Docker entrypoint runs:
python manage.py migrate
python manage.py ensure_buckets    # Create buckets if missing
python manage.py setup_minio       # Configure policies + notifications
python manage.py collectstatic --noinput
```

### Environment variables

```bash
# MinIO
AWS_S3_ENDPOINT_URL=http://georiva-minio:9000
MINIO_PUBLIC_ENDPOINT=localhost:9000
MINIO_WEBHOOK_BEARER_TOKEN=your-secret-token

# Celery
CELERY_BROKER_URL=redis://georiva-redis:6379/0
```
