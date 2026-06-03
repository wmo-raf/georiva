# Download Deduplication & Multi-Collection DataFeed Architecture

|             |            |
|-------------|------------|
| **Status**  | Stable     |
| **Version** | 1.0        |
| **Date**    | 2026-06-03 |

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. DataFeed and Multiple Collections](#2-datafeed-and-multiple-collections)
- [3. Download Deduplication](#3-download-deduplication)
    - [3.1 Tier 1 — Same-Collection Existence Check](#31-tier-1--same-collection-existence-check)
    - [3.2 Tier 2 — Cross-Collection Copy](#32-tier-2--cross-collection-copy)
    - [3.3 Storage Path Convention](#33-storage-path-convention)
- [4. Full Run Flow](#4-full-run-flow)
- [5. Mental Model: How to Organize Plugin Collections](#5-mental-model-how-to-organize-plugin-collections)
    - [5.1 Collections by Period](#51-collections-by-period)
    - [5.2 Collections by Level](#52-collections-by-level)
    - [5.3 Decision Guide](#53-decision-guide)

---

## 1. Overview

A `DataFeed` links to **one or more** `Collection` objects. When the feed runs it processes each
collection sequentially, building a `Loader` for each one.

There are two reasons to attach multiple Collections to one DataFeed:

1. **Organization** — different Collections expose data at different temporal resolutions or
   variable groups through the STAC API. Even when the underlying files are completely different,
   grouping them under one DataFeed keeps scheduling and configuration in one place.

2. **Deduplication** — when multiple Collections need the **same raw file**, the second collection
   copies from the first instead of re-downloading. This is the cross-collection dedup path.

The dedup mechanism is filename-based. It only triggers when two Collections request a file with
the **same name**. When files have different names (different URLs, different periods), each
Collection downloads its own files independently — Tier 1 dedup still prevents re-downloads on
repeated runs within the same collection, but no cross-collection copy occurs.

```
ECMWF AIFS — one GRIB2 per forecast step, all variables inside:

DataFeed ──── Collection: ecmwf-surface
         └─── Collection: ecmwf-pressure-levels

Both collections request the same file for each step:
  aifs_20260128060000_12h_oper_fc.grib2

Run order (sequential):
  1. ecmwf-surface loader runs → downloads the GRIB2, stores it, IngestionLog → PENDING
  2. ecmwf-pressure-levels loader runs → _find_existing_catalog_path() finds the PENDING entry
     → issues a server-side MinIO copy instead of a second HTTP download
```

---

## 2. DataFeed and Multiple Collections

**Key files:**

- `georiva/src/georiva/sources/models.py:13` — `DataFeed` model
- `georiva/src/georiva/sources/models.py:61` — `collections` ManyToManyField
- `georiva/src/georiva/sources/models.py:232` — sequential collection loop in `DataFeed.run()`

`DataFeed` is a polymorphic base model. Plugin-specific subclasses (e.g. `CHIRPSDataFeed`,
`ECMWFAIFSDataFeed`) extend it with source-specific configuration fields such as period selection
or forecast run hours.

The `collections` field is a `ManyToManyField` to `Collection`. A single DataFeed can own many
collections. When `DataFeed.run()` is called without a specific collection, it iterates every
linked collection in sequence:

```python
# sources/models.py:232
collections = [collection] if collection else list(self.collections.all())
for coll in collections:
    loader = self.get_loader(coll)
    result = loader.run()
    self.record_run(result, coll)
```

Sequential (not parallel) processing is intentional: it is what makes cross-collection dedup
work. By the time Collection B runs, Collection A has already stored its files and registered
`IngestionLog` entries, so the loader for B can find them.

Each collection run produces a `DataFeedRun` record, so per-collection progress is tracked
independently even when collections share a DataFeed.

---

## 3. Download Deduplication

Dedup is implemented in `Loader` (`georiva/src/georiva/sources/loader.py`). During a loader run,
each `FileRequest` passes through two checks before a download is attempted.

### 3.1 Tier 1 — Same-Collection Existence Check

**Location:** `loader.py:285` — `Loader._already_exists()`

```python
def _already_exists(self, request) -> bool:
    storage_path = self._get_storage_path(request)
    return storage.sources.exists(storage_path)
```

If the file is already present in MinIO at the expected path for *this* collection, the request
is skipped entirely (`files_skipped` counter incremented, no I/O). This handles re-runs and
idempotent scheduling.

### 3.2 Tier 2 — Cross-Collection Copy

**Location:** `loader.py:290` — `Loader._find_existing_catalog_path()`

If the file is not in the current collection, the loader checks whether a **sibling collection**
(another collection on the same DataFeed, within the same Catalog) already has it. Two strategies
are tried in order:

**Strategy 1 — IngestionLog query (fast, no MinIO I/O):**

```python
IngestionLog.objects
.filter(
    bucket=BucketType.SOURCES,
    catalog_slug=catalog_slug,
    file_path__endswith=f"/{filename}",
    status__in=[IngestionLog.Status.PENDING, IngestionLog.Status.PROCESSING],
)
.exclude(collection_slug=collection_slug)
.values_list("file_path", flat=True)
.first()
```

Only `PENDING` and `PROCESSING` statuses are checked. `COMPLETED` is excluded because
`SourceFileManager.cleanup()` deletes source files after successful ingestion; querying for
completed entries would return paths that no longer exist.

**Strategy 2 — Direct MinIO existence check (fallback):**

```python
for sibling in self.data_feed.collections.exclude(pk=self.collection.pk):
    candidate = f"{sibling.catalog.slug}/{sibling.slug}/{filename}"
    if storage.sources.exists(candidate):
        return candidate
```

Handles files present in MinIO with no `IngestionLog` entry: dropped events, manual uploads,
or consumer restarts that left orphaned files without a corresponding log row.

**When a match is found:**

```python
storage.sources.copy(existing_path, dest_path)
```

A server-side MinIO copy is issued instead of an HTTP fetch. If the copy fails, the file is
added to the normal fetch queue as a fallback (`requests_to_fetch`).

### 3.3 Storage Path Convention

**Location:** `loader.py:426` — `Loader._get_storage_path()`

```
{catalog_slug}/{collection_slug}/{filename}
```

Filename follows `core/filename.py:build_filename()`:

| Has `reference_time`? | Filename pattern                       | Example                            |
|-----------------------|----------------------------------------|------------------------------------|
| Yes (forecast)        | `GR--{YYYYMMDDTHHMM}--{original_name}` | `GR--20250115T0600--gfs_025.grib2` |
| No (observation)      | `{original_name}` (unchanged)          | `chirps-v2.0.2025.01.01.tif`       |

Cross-collection dedup relies on **filename matching**, which is why sibling collections with the
same original filename (and the same `reference_time`, if any) are considered candidates.

---

## 4. Full Run Flow

```
DataFeed.run(collection=None)
  │
  ├─ collections = self.collections.all()   # All linked collections
  │
  ├─ FOR EACH collection (sequential):
  │    │
  │    ├─ Loader(data_source, collection, data_feed=self)
  │    │    │
  │    │    ├─ data_source.generate_requests_for_collection(collection)
  │    │    │    └─ Yields FileRequest objects (url, filename, valid_time, reference_time)
  │    │    │
  │    │    ├─ FOR EACH request:
  │    │    │    ├─ [Tier 1] _already_exists(request)
  │    │    │    │   └─ storage.sources.exists(path) → skip if True
  │    │    │    │
  │    │    │    ├─ [Tier 2] _find_existing_catalog_path(request)
  │    │    │    │   ├─ IngestionLog query (PENDING/PROCESSING in sibling collections)
  │    │    │    │   └─ Direct MinIO check on sibling paths
  │    │    │    │   → storage.sources.copy(src, dst) if found
  │    │    │    │
  │    │    │    └─ _fetch_and_store(request)  ← only if both checks miss
  │    │    │        ├─ Fetch to temp dir
  │    │    │        ├─ Validate (size, format)
  │    │    │        ├─ post_process_fetched_file() hook (e.g. gunzip)
  │    │    │        └─ storage.sources.save(path, file)
  │    │    │
  │    │    └─ IngestionLog entries created by minio-consumer on bucket notification
  │    │
  │    └─ DataFeed.record_run(result, collection)   # DataFeedRun row
  │
  └─ Returns list[LoaderRunResult]
```

---

## 5. Mental Model: How to Organize Plugin Collections

A plugin's `generate_requests_for_collection()` is called once per linked Collection. The
Collection passed in acts as a **filter/selector**: the plugin inspects the collection's slug,
name, or extra metadata to decide which files to request for that collection.

There are two distinct reasons to split a source into multiple Collections, and they behave
differently with respect to deduplication.

### 5.1 Collections by Level — same file, dedup applies

**The situation:** A single source file contains data for multiple variable groups. You want
separate Collections in the catalog so API consumers can discover them independently (surface
parameters vs pressure-level parameters), but both Collections need the **same raw file** for
each timestep.

**ECMWF AIFS** is the canonical example. The source publishes one GRIB2 per forecast step:

```
aifs_20260128060000_12h_oper_fc.grib2  ← one file, all variables inside
```

That single file contains both surface fields (2m temperature, 10m wind, MSLP, precipitation,
surface pressure) and pressure-level fields (T, U, V, Z, Q at 9 pressure levels). You expose
them as two separate Collections so they have distinct STAC endpoints and can be styled,
visualized, and managed independently:

```
Catalog: ecmwf-aifs
├── Collection: ecmwf-aifs-surface
│   ├── Variable: temperature_2m
│   ├── Variable: wind_speed_10m
│   ├── Variable: mean_sea_level_pressure
│   └── Variable: total_precipitation
└── Collection: ecmwf-aifs-pressure-levels
    ├── Variable: temperature_500hpa  (source: t, isobaricInhPa=500)
    ├── Variable: temperature_850hpa  (source: t, isobaricInhPa=850)
    ├── Variable: wind_u_850hpa
    └── Variable: geopotential_500hpa
```

Both Collections generate a `FileRequest` with the same filename for each step. The format
plugin (`FormatPlugin.extract_variable()`) selects the right layer from the GRIB2 using
`VariableSource.vertical_dimension` / `VariableSource.vertical_value`.

**How dedup works here:**

```
Step 1: ecmwf-aifs-surface loader runs
  → requests aifs_20260128060000_12h_oper_fc.grib2
  → not in MinIO yet → downloads (HTTP fetch, ~hundreds of MB)
  → stores at: ecmwf-aifs/ecmwf-aifs-surface/GR--20260128T0600--aifs_...12h_oper_fc.grib2
  → minio-consumer fires → IngestionLog row: PENDING

Step 2: ecmwf-aifs-pressure-levels loader runs
  → requests the same filename
  → Tier 1: not at its own path → miss
  → Tier 2: _find_existing_catalog_path() finds the PENDING IngestionLog entry from step 1
  → storage.sources.copy(src, dst)  ← server-side copy, no re-download
```

The file is downloaded once per forecast step, regardless of how many Collections need it.

### 5.2 Collections by Period — different files, dedup does not apply cross-collection

**The situation:** The same physical parameter is published at different temporal resolutions.
Each resolution has its own files at its own URLs. File names differ, so the cross-collection
copy path never triggers — but you still want separate Collections for catalog organization.

**CHIRPS** is the canonical example. CHIRPS publishes two products independently:

```
chirps-v2.0.2025.01.tif          ← monthly accumulation, one file per month
chirps-v2.0.2025.01.01.tif       ← pentadal accumulation, one file per 5-day period
```

These are different files. Organizing them into separate Collections is purely for catalog
clarity — monthly precipitation and pentadal precipitation are different things for a forecast
service consumer and deserve different STAC Collection identifiers, different temporal extents,
and different variable metadata.

```
Catalog: chirps
├── Collection: chirps-monthly    ←── DataFeed config: period = "monthly"
│   └── Variable: precipitation   (monthly accumulation)
└── Collection: chirps-pentadal   ←── DataFeed config: period = "pentadal"
    └── Variable: precipitation   (5-day accumulation)
```

**How dedup works here:**

- Cross-collection: does not apply. The filenames are different, so `_find_existing_catalog_path()`
  never finds a match across collections.
- Tier 1 (same-collection): still applies. On the next scheduled run, if `chirps-v2.0.2025.01.tif`
  is already stored under `chirps/chirps-monthly/`, the loader skips it. Re-runs are idempotent.

The reason to link both period collections to **one DataFeed** (rather than two separate feeds)
is operational: one schedule, one configuration form in the admin, one set of credentials, and
one job to monitor per run.

### 5.3 Decision Guide

**First question: do multiple Collections need the same file?**

```
Same filename across collections?
    │
    ├── Yes → "by level" / "by variable group" pattern
    │           Cross-collection copy dedup applies.
    │           One download per file, regardless of how many Collections consume it.
    │           Example: ECMWF AIFS surface + pressure levels
    │
    └── No  → "by period" / "by product" pattern
                No cross-collection dedup. Each Collection downloads its own files.
                Tier 1 dedup still prevents re-downloads within the same collection.
                Reason to share a DataFeed: operational convenience, not bandwidth savings.
                Example: CHIRPS monthly + pentadal
```

**Second question: why separate into Collections at all?**

| Reason | Example |
|--------|---------|
| Different variable groups that consumers discover separately | ECMWF surface vs pressure levels |
| Different temporal resolution — different meaning, different cadence | CHIRPS monthly vs pentadal |
| Different spatial grid or coverage | 0.25° vs 1.0° output |
| Different processing stage (L1C vs L2A) | Sentinel-2 reflectance levels |

**Practical rules:**

| Situation | Cross-collection dedup? | Collections |
|-----------|------------------------|-------------|
| Multiple variable groups, one file per step | Yes — copy | One per variable group |
| Same parameter, different temporal resolution | No — different filenames | One per period |
| Same parameter, different spatial grid | No — different filenames | One per grid |
| Different processing levels, different files | No — different filenames | One per level |
| All parameters together, one file per timestep | N/A | Single collection |

When a new plugin needs multiple Collections, link them all to **one** DataFeed. Sequential
processing guarantees the copy path works without coordination: the earlier collection always
runs first and registers its IngestionLog entry before the later collection begins its check.
