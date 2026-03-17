# GeoRiva Architectural Patterns

Patterns and conventions observed across multiple files in the codebase.

## 1. Registry Pattern (Plugin Discovery)

Three independent registries share the same structure: a class with `register()`, `get()`, and a module-level singleton
instance.

| Registry                | File                                         | Singleton                                     |
|-------------------------|----------------------------------------------|-----------------------------------------------|
| Format plugins          | `georiva/src/georiva/formats/registry.py:18` | `format_registry` (line 104)                  |
| Loader profile viewsets | `georiva/src/georiva/sources/registry.py:6`  | `loader_profile_viewset_registry` (line 25)   |
| Analysis operators      | `georiva/src/georiva/analysis/registry.py:7` | `OperatorRegistry` (class-level, no instance) |

**FormatRegistry** uses a class-method decorator (`@FormatRegistry.register` at line 30) that auto-indexes by name and
file extension. Lookup supports name, extension, or file-path-based detection with fallback (`get_for_file()` at line
68).

**OperatorRegistry** uses a parameterized decorator (`@OperatorRegistry.register(name=..., category=...)` at line 20)
and supports auto-discovery of external `georiva_operators_*` packages (`load_plugins()` at line 59).

**Convention**: Always access registries via the module-level singleton, never instantiate directly.

## 2. Abstract Base Class + Protocol Pattern

Extensibility points use ABC with required class attributes and optional hook methods.

### Format Plugins

- **Base**: `formats/base.py:85` — `BaseFormatPlugin(ABC)`
- **Required class attrs**: `name`, `display_name`, `extensions` (line 93-95)
- **Abstract methods**: `can_handle()`, `list_variables()`, `get_timestamps()`, `open_variable()` (lines 101-165)
- **Default implementations**: `extract_variable()` (line 167), `get_metadata_for_variable()` (line 205)
- **Implementations**: `formats/grib.py`, `formats/netcdf.py`, `formats/geotiff.py`

### Data Sources

- **Protocol**: `sources/source.py:21` — `DataSource(Protocol)` defines the interface
- **Base**: `sources/source.py:79` — `BaseDataSource(ABC)` provides common functionality
- **Required class attrs**: `type`, `label` (lines 86-87)
- **Constructor validation**: Raises `ValueError` if `type`, `label`, or `fetch_strategy` missing (lines 90-98)
- **Template methods**: `get_time_window()` (line 178) calls overridable `get_default_start_date()`,
  `advance_start_from_latest()`

**Convention**: New plugins subclass the base, set class attributes, implement abstract methods. The base handles
logging, config, and common helpers.

## 3. Singleton Pattern

Module-level singleton instances used for cross-cutting services:

| Instance                          | File:Line                 | Purpose                      |
|-----------------------------------|---------------------------|------------------------------|
| `storage`                         | `core/storage.py:498`     | Multi-bucket storage manager |
| `format_registry`                 | `formats/registry.py:104` | Format plugin lookup         |
| `loader_profile_viewset_registry` | `sources/registry.py:25`  | Loader viewset lookup        |
| `app`                             | `config/celery.py:3`      | Celery application           |

Access via import: `from georiva.core.storage import storage`

## 4. Multi-Bucket Storage Architecture

`core/storage.py:242` — `StorageManager` wraps Django's storage backends with named bucket accessors.

**Bucket types** (`BucketType` at line 32): `INCOMING`, `SOURCES`, `ARCHIVE`, `ASSETS`

**Flow**: `incoming/sources` → process → `assets` (with raw copy to `archive`)

**Key patterns**:

- Lazy bucket initialization via `@property` + `_get_bucket()` (lines 263-268, 272-290)
- Cross-bucket operations with S3 server-side copy optimization + local fallback (`transfer()` at line 303)
- Time-partitioned asset paths: `{catalog}/{collection}/{variable}/{year}/{month}/{day}/{filename}` (
  `build_asset_path()` at line 423)
- Bucket configuration from Django settings (`GEORIVA_BUCKETS` in `config/settings/base.py:210`)

## 5. Service Layer Pattern

`ingestion/service.py:68` — `IngestionService` encapsulates all business logic for file processing.

**Characteristics**:

- Stateless orchestrator (only holds a logger in `__init__`)
- Single public entry point: `process_file()` (line 111)
- Composes multiple collaborators: format plugins, clipper, encoder, asset writer
- Returns a rich result object: `IngestionResult` dataclass (line 27)

**Convention**: Views/tasks call the service; the service calls models and utilities. Models never call services.

## 6. Distributed Lock Pattern

`ingestion/models.py:28` — `IngestionLog` implements atomic locking with crash recovery.

**State machine**: `pending` → `processing` (locked) → `completed` | `failed`

**Key methods**:

- `acquire()` (line 178): Atomic lock via `filter().update()` — only one worker wins
- `mark_completed()` / `mark_failed()` (lines 232, 254): State transitions
- `is_stale` property (line 134): Detects crashed workers via `LOCK_TIMEOUT` (30 min, line 106)
- `reset_stale_locks()` (line 285): Reclaims stale locks for retry

**Convention**: No Celery retries (`max_retries=0` in `ingestion/tasks.py:23`). Retries handled by the
`sweep_unprocessed` periodic task (line 114) which resets stale locks, scans for untracked files, and retries failures.

## 7. Celery Task Conventions

**Queue routing** — Two queues separate workloads:

- `georiva-default`: Lightweight tasks (sweeps, cleanup, scheduling)
- `georiva-ingestion`: Heavy data processing tasks

**Task patterns** (seen in `ingestion/tasks.py` and `core/tasks.py`):

- `bind=True` for access to `self.request.id` (worker ID for locking)
- `acks_late=True` — message acknowledged only after completion
- Late imports inside task body to avoid circular imports (e.g., line 40-41 in `ingestion/tasks.py`)
- `sync` parameter on sweep tasks for testability (line 141): `dispatch = task.delay if not sync else task.run`

**Periodic task registration** via `@app.on_after_finalize.connect` signal (line 293), creating `IntervalSchedule` +
`PeriodicTask` records programmatically.

## 8. Lazy-First Data Access

`formats/base.py:7-14` — Data is accessed lazily by default, materialized only when needed.

**Pattern**:

1. `open_variable()` → context manager yielding `VariableInfo` with dask-backed `xr.DataArray` (line 134)
2. `extract_variable()` → convenience that calls `open_variable()` + `.compute()` (line 167)
3. `get_metadata_for_variable()` → reads bounds/CRS without touching pixel data (line 205)

**Data carriers**:

- `VariableInfo` (line 41): Lazy data + spatial metadata, with `.compute()` method
- `ExtractedVariable` (line 70): Materialized numpy array + metadata

## 9. Partial Failure Handling

`ingestion/service.py` — Multi-level try/catch that allows partial success.

**Levels** (from outer to inner):

1. File-level: Catches all exceptions, records in `IngestionResult.errors`
2. Collection-level: Per-collection processing wrapped in try/catch
3. Variable-level: Individual variable failures tracked in `failed_variables` list
4. Asset-level: Per-asset save operations independently guarded

**Convention**: Errors are captured and logged at each level without re-raising. Processing continues for remaining
items. A file with partial variable failures keeps the source file for later re-processing rather than archiving it.

## 10. Wagtail Hook Integration

Multiple apps register Wagtail admin customizations via `wagtail_hooks.py` files:

| App           | File                             | Hooks Used                                                                                                           |
|---------------|----------------------------------|----------------------------------------------------------------------------------------------------------------------|
| core          | `core/wagtail_hooks.py`          | `register_admin_urls`, `register_admin_menu_item`, `register_admin_viewset`, `construct_main_menu`, `register_icons` |
| ingestion     | `ingestion/wagtail_hooks.py`     | `register_admin_urls`, `construct_homepage_panels`                                                                   |
| sources       | `sources/wagtail_hooks.py`       | `register_admin_viewset`                                                                                             |
| visualization | `visualization/wagtail_hooks.py` | `register_admin_viewset`                                                                                             |

**Convention**: Each app owns its admin integration. ViewSets are registered via hooks, not in `INSTALLED_APPS`. Menu
items use FontAwesome SVG icons via `wagtailfontawesomesvg`.

## 11. STAC-Aligned Data Model

Core models mirror the STAC specification hierarchy:

```
Topic (thematic tag) ──M2M──→ Catalog ──1:N──→ Collection ──1:N──→ Variable ──1:N──→ Item ──1:N──→ Asset
```

- **Catalog** (`core/models/catalog.py`): Top container, maps to STAC Catalog. Owns file_format, boundary, clip_mode.
- **Collection** (`core/models/collection.py`): Dataset within catalog. Owns temporal config (time_resolution, forecast
  settings).
- **Variable** (`core/models/variable.py`): Measured quantity. Maps source variable names via `VariableSource`.
- **Item** (`core/models/item.py`): Single spatiotemporal granule. TimescaleDB hypertable for time-series optimization.
- **Asset** (`core/models/item.py`): Physical file (COG, PNG, Zarr) linked to an Item.

**Convention**: Models split into separate files under `core/models/`, re-exported from `__init__.py` (line 1-16).

## 12. Settings Split Pattern

`config/settings/base.py` → `dev.py` → `production.py`

- **base.py**: All shared config, reads `.env` via `django-environ` (line 14)
- **dev.py**: Imports `from .base import *`, sets `DEBUG=True`, permissive CORS
- **production.py**: Production-only overrides (security headers, etc.)

**Environment variables**: All prefixed with `GEORIVA_` for app-specific settings. Standard `AWS_*` prefix for S3/MinIO.
`CELERY_*` prefix auto-namespaced by Celery.

**Storage backends**: Dynamically registered per-bucket in a loop over `GEORIVA_BUCKETS` (base.py:257-267), supporting
both S3 and local filesystem.

## 13. URL Routing Convention

Hierarchical `include()` pattern:

```
config/urls.py  →  api/urls.py  →  stac/urls.py
                                →  edr/urls.py
```

- Each API app defines `app_name` for namespacing (e.g., `stac/urls.py:5`)
- RESTful nesting for STAC: `/collections/{catalog}/collections/{variable}/items/{id}/`
- Webhook endpoint at `/api/webhook/` for MinIO event notifications
- Wagtail catch-all at the end of `config/urls.py:30`

## 14. Result Dataclass Pattern

Multiple subsystems return structured result objects instead of raw tuples:

| Dataclass           | File:Line                 | Purpose                                     |
|---------------------|---------------------------|---------------------------------------------|
| `IngestionResult`   | `ingestion/service.py:27` | File processing outcome with error tracking |
| `VariableInfo`      | `formats/base.py:41`      | Lazy data + spatial metadata                |
| `ExtractedVariable` | `formats/base.py:70`      | Materialized array + metadata               |

**Convention**: Use `@dataclass` with `field(default_factory=list)` for mutable defaults. Include computed properties
for derived values (e.g., `size_reduction_percent` at `ingestion/service.py:54`).
