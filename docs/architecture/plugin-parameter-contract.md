# Source Plugin Contract & Setup Wizard

**Geospatial Raster Ingestion, Visualization & Analysis**

|             |                          |
|-------------|--------------------------|
| **Status**  | Implemented              |
| **Version** | 1.0                      |
| **Date**    | 2026-06-04               |
| **Author**  | Erick Otenyo, WMO Africa |

> **Note:** This document supersedes the v0.1 RFC (2026-05-29) which proposed a `ParameterManifest` /
> `describe_parameters()` contract. The as-built contract uses `CollectionDefinition` /
> `get_collection_definitions()` instead — a more concrete design that maps directly onto the data
> model. `SourceKey` and `Level` from `sources/parameters.py` are still used as building blocks inside
> `CollectionVariable`, but `ParameterManifest` is not the primary contract surface.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Plugin Components](#2-plugin-components)
    - [2.1 BaseDataSource — the runtime fetcher](#21-basedatasource--the-runtime-fetcher)
    - [2.2 DataFeed — the Django model](#22-datafeed--the-django-model)
    - [2.3 DataFeedCollectionLink — per-collection config (optional)](#23-datafeedcollectionlink--per-collection-config-optional)
    - [2.4 CollectionDefinition — the catalog contract](#24-collectiondefinition--the-catalog-contract)
- [3. Declaring Collections](#3-declaring-collections)
    - [3.1 CollectionDefinition dataclass](#31-collectiondefinition-dataclass)
    - [3.2 CollectionVariable and SourceKey](#32-collectionvariable-and-sourcekey)
    - [3.3 Dict shorthand via parse_collection_defs()](#33-dict-shorthand-via-parse_collection_defs)
    - [3.4 Reference: CHIRPS plugin](#34-reference-chirps-plugin)
- [4. Setup Wizard & SourceSetupService](#4-setup-wizard--sourcesetupservice)
    - [4.1 Wizard steps](#41-wizard-steps)
    - [4.2 SourceSetupService.provision()](#42-sourcesetupserviceprovision)
    - [4.3 Idempotency](#43-idempotency)
- [5. Scheduling](#5-scheduling)
- [6. Mapping onto Core Models](#6-mapping-onto-core-models)
- [7. Checklist for a new plugin](#7-checklist-for-a-new-plugin)

---

## 1. Overview

A source plugin wires a remote data source into GeoRiva by implementing four components:

| Component                                      | Base class / location               | Purpose                                                             |
|------------------------------------------------|-------------------------------------|---------------------------------------------------------------------|
| `BaseDataSource` subclass                      | `sources/source.py`                 | Knows what files to request and how to build them                   |
| `DataFeed` subclass                            | `sources/models.py`                 | Django model: operator config + scheduling + factory methods        |
| `DataFeedCollectionLink` subclass *(optional)* | `sources/models.py`                 | Per-collection config fields on the M2M through-model               |
| `CollectionDefinition` list                    | `sources/collection_definitions.py` | Declares every collection (and its variables) the plugin can create |

The **Setup Wizard** (Wagtail admin, `sources/wagtail_hooks.py`) and **`SourceSetupService`**
(`sources/setup_service.py`) consume `get_collection_definitions()` to provision the full
`Catalog → Collection → Variable + DataFeedCollectionLink` tree in one atomic transaction.

---

## 2. Plugin Components

### 2.1 BaseDataSource — the runtime fetcher

Subclass `BaseDataSource` in your plugin's `source.py`. It must set two class attributes and
implement three abstract members. Pass a `fetch_strategy` class to `super().__init__()`:

```python
from georiva.sources.source import BaseDataSource, DataSourceType
from georiva.sources.fetch import FileRequest, HTTPFetchStrategy


class CHIRPSDataSource(BaseDataSource):
    type = "chirps"  # machine key — unique across all plugins, used in logging
    label = "CHIRPS"  # human label
    
    def __init__(self, config: dict, fetch_strategy=HTTPFetchStrategy):
        super().__init__(config, fetch_strategy)
        # Unpack merged config dict here
        self.enabled_period = config.get("period")
        self.default_start_date = config.get("default_start_date")
        self.head_timeout = int(config.get("head_timeout", 20))
    
    @property
    def name(self) -> str:
        return self.label
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.DERIVED  # or FORECAST
    
    def generate_requests(
            self,
            start_time: datetime,
            end_time: datetime,
            variables: list[str] | None = None,
            **kwargs,
    ) -> Iterator[FileRequest]:
        """Yield one FileRequest per file to download in the given window."""
        ...
```

`BaseDataSource.__init__` enforces that `type`, `label`, and `fetch_strategy` are all set; it raises
`ValueError` otherwise.

**Config dict** — the `config` dict passed to `__init__` is the merge of:

1. `DataFeed.get_loader_config()` — feed-wide settings (credentials, timeouts)
2. `DataFeedCollectionLink.config` — per-collection settings (period, start date, etc.)

Per-collection values win on key collision. Unpack everything in `__init__` for clarity.

**Time-window helpers** — override these to control the backfill window:

| Method                                             | Default behaviour                   | When to override                                        |
|----------------------------------------------------|-------------------------------------|---------------------------------------------------------|
| `get_default_start_date(*, collection)`            | Today at 00:00 UTC                  | Fixed historical start date (e.g. read from config)     |
| `get_default_end_date(*, collection)`              | `now()`                             | Delay or cap end date                                   |
| `advance_start_from_latest(latest, *, collection)` | Returns `latest` unchanged          | Skip already-stored period (e.g. advance to next dekad) |
| `get_latest_from_db(*, collection)`                | `collection.get_latest_item_date()` | Only if you need a custom DB lookup strategy            |

The base `get_time_window()` calls these in sequence and returns `(start_time, end_time)`.

`generate_requests_for_collection(collection)` is the convenience entry point used by the job runner.
It calls `get_time_window()` and passes `collection.source_variables_list()` as `variables` to
`generate_requests()`.

**`post_process_fetched_file(request, local_path) -> (Path, str | None)`** is an optional hook for
format conversion or renaming before the file is stored in MinIO. Return `(new_path, new_filename)`
or `(original_path, None)` to leave unchanged.

---

### 2.2 DataFeed — the Django model

Subclass `DataFeed` (a polymorphic model) to hold operator configuration.

**Key built-in fields:**

| Field              | Type                   | Notes                                                                    |
|--------------------|------------------------|--------------------------------------------------------------------------|
| `name`             | CharField              | Display name set during wizard step 2                                    |
| `catalog`          | OneToOneField(Catalog) | Set during wizard step 1; one feed per catalog                           |
| `is_active`        | BooleanField           | Gates scheduling; inactive feeds are skipped                             |
| `interval_minutes` | PositiveIntegerField   | Global run interval (default 360); per-collection link can override this |

```python
from georiva.sources.models import DataFeed
from georiva.sources.collection_definitions import CollectionDefinition, parse_collection_defs


class CHIRPSDataFeed(DataFeed):
    # Optional extra fields (head_timeout, API key, etc.)
    head_timeout = models.IntegerField(default=20)
    
    panels = [*DataFeed.base_panels, ...]
    
    # ── Required ──────────────────────────────────────────────────────────────
    
    @property
    def data_source_cls(self):
        from .source import CHIRPSDataSource
        return CHIRPSDataSource
    
    @classmethod
    def get_collection_definitions(cls) -> list[CollectionDefinition]:
        return parse_collection_defs(COLLECTIONS)
    
    # ── Recommended ───────────────────────────────────────────────────────────
    
    @classmethod
    def get_catalog_defaults(cls) -> dict:
        """Pre-fill wizard step 1. Keys are Catalog model field names."""
        return {"name": "CHIRPS", "file_format": "geotiff", "description": "..."}
    
    @classmethod
    def get_wizard_defaults(cls) -> dict:
        """Field values applied when creating the DataFeed instance."""
        return {}
    
    def get_loader_config(self) -> dict:
        """Feed-level config dict passed to DataSource.__init__()."""
        return {"head_timeout": self.head_timeout}
    
    # ── Only needed when using a custom CollectionLink subclass ───────────────
    
    @classmethod
    def get_collection_link_model(cls):
        return CHIRPSDataFeedCollectionLink
    
    @classmethod
    def get_link_config_for_definition(cls, definition) -> dict:
        """Fields baked into the link from the definition (not operator-editable)."""
        for period in ('monthly', 'pentadal', 'dekadal'):
            if period in definition.key:
                return {'period': period}
        return {}
```

`DataFeed.base_panels` provides the standard `name`, `is_active`, and `interval_minutes` panels.
Extend it in `panels` to expose plugin-specific fields.

**`has_wizard` behaviour** — a `DataFeed` subclass enters the setup wizard only if
`get_collection_definitions()` returns a **non-empty list**. Feeds that return `[]` display a
"Manual" badge in the plugin-selection screen and open the standard edit form instead. Feeds with
wizard support display a "Wizard" badge.

---

### 2.3 DataFeedCollectionLink — per-collection config (optional)

When different collections in the same feed need their own configuration (e.g. CHIRPS where each
collection has a `period` and a `default_start_date`), subclass `DataFeedCollectionLink`:

```python
from georiva.sources.models import DataFeedCollectionLink


class CHIRPSDataFeedCollectionLink(DataFeedCollectionLink):
    # Baked from definition_key — never shown in forms
    period = models.CharField(max_length=10, choices=PERIOD_CHOICES)
    
    # Operator-configurable
    default_start_date = models.DateField(default=date(1981, 1, 1))
    
    class Meta:
        verbose_name = "CHIRPS Collection Link"
    
    @classmethod
    def get_panels(cls) -> list:
        """Panels shown in the admin link edit form (interval_minutes always appended)."""
        return [FieldPanel("default_start_date")]
    
    @property
    def config(self) -> dict:
        """Merged into the DataSource config dict at runtime."""
        return {"period": self.period, "default_start_date": self.default_start_date}
```

`get_panels()` controls which fields the operator sees. `interval_minutes` (the per-collection
schedule override) is always appended by the base `get_form_class()`. Fields returned by
`get_link_config_for_definition()` are baked in automatically and should be omitted from `get_panels()`.

---

### 2.4 CollectionDefinition — the catalog contract

`get_collection_definitions()` is the primary plugin contract. It returns the **finite, declared set
of collections** this plugin can create. The wizard presents them as a checklist; the setup service
provisions them on demand.

See [Section 3](#3-declaring-collections) for full details.

---

## 3. Declaring Collections

### 3.1 CollectionDefinition dataclass

```python
@dataclass(frozen=True)
class CollectionDefinition:
    key: str  # e.g. 'chirps-monthly' — becomes part of Collection.slug
    name: str  # e.g. 'CHIRPS Monthly'
    time_resolution: str  # Collection.TimeResolution value
    variables: tuple[CollectionVariable, ...]
    groups: tuple[VariableGroup, ...] = ()  # UX-only groupings in the wizard
    description: str = ''
    is_forecast: bool = False
    default_interval_minutes: int | None = None  # pre-fills link.interval_minutes
```

**`Collection.slug`** is derived as `slugify(f"{catalog.slug}-{definition.key}")`.

**Per-collection config fields** (e.g. `default_start_date`) live on the
`DataFeedCollectionLink` subclass, declared via `get_panels()`. The wizard renders
`DataFeed.get_collection_link_model().get_form_class()` for each checked definition.

**`groups`** (`VariableGroup`) are purely a wizard UX concern — they render variables as collapsible
sections with a "check all" checkbox. They have no effect on the data model.

**`default_interval_minutes`** pre-fills the link's `interval_minutes` override. Useful when
different collections in the same feed run at different cadences (e.g. CHIRPS monthly at 30 days,
dekadal at 10 days).

---

### 3.2 CollectionVariable and SourceKey

```python
@dataclass(frozen=True)
class CollectionVariable:
    key: str  # Variable.slug
    name: str  # Variable.name
    units: str  # Unit.symbol (created if absent)
    source: SourceKey | None = None  # passthrough
    transform: str = 'passthrough'  # 'passthrough' | 'vector_magnitude' | 'vector_direction'
    components: dict[str, SourceKey] | None = None  # derived: {'u': SourceKey, 'v': SourceKey}
    description: str = ''
    value_range: tuple[float, float] | None = None
    palette: str | None = None
```

**`SourceKey`** and **`Level`** come from `sources/parameters.py`:

```python
@dataclass(frozen=True)
class SourceKey:
    name: str  # GRIB shortName / NetCDF variable / 'band_1'
    level: Level | None = None


@dataclass(frozen=True)
class Level:
    type: str  # 'surface' | 'pressure' | 'heightAboveGround'
    value: float | None = None  # e.g. 850, 2, 10
    dimension: str | None = None  # GRIB key: 'isobaricInhPa', 'heightAboveGround'
    unit: str | None = None  # 'hPa', 'm'
```

`expand_levels()` (also in `sources/parameters.py`) generates one `Parameter` per level — useful for
pressure-level data — and can be combined with `parse_collection_defs()`.

---

### 3.3 Dict shorthand via parse_collection_defs()

For plugins that prefer plain dicts over constructing dataclasses directly:

```python
from georiva.sources.collection_definitions import parse_collection_defs

COLLECTIONS = {
    "chirps-monthly": {
        "name": "CHIRPS Monthly",
        "time_resolution": "monthly",
        "default_interval_minutes": 43200,
        "variables": [
            {
                "key": "precip",
                "name": "Precipitation",
                "units": "mm",
                "source": "band_1",  # string shorthand for SourceKey(name='band_1')
                "value_range": (0.0, 2000.0),
            }
        ],
    },
}


@classmethod
def get_collection_definitions(cls):
    return parse_collection_defs(COLLECTIONS)
```

**Variable source shorthand:** `"source": "band_1"` is equivalent to
`SourceKey(name="band_1")`. For levelled sources pass a dict:

```python
"source": {
    "name": "2t",
    "level": {"type": "heightAboveGround", "value": 2, "dimension": "heightAboveGround", "unit": "m"}
}
```

**Derived (vector) variables:**

```python
{
    "key": "wind_speed_10m",
    "name": "10m Wind Speed",
    "units": "m/s",
    "transform": "vector_magnitude",
    "components": {
        "u": {"name": "10u", "level": {"type": "heightAboveGround", "value": 10, ...}},
        "v": {"name": "10v", "level": {"type": "heightAboveGround", "value": 10, ...}},
    },
    "value_range": (0.0, 80.0),
}
```

---

### 3.4 Reference: CHIRPS plugin

The CHIRPS plugin (`sample_plugins/chirps/`) is the canonical reference implementation:

- `models.py` — `CHIRPSDataFeed` + `CHIRPSDataFeedCollectionLink` with `period` and `default_start_date`
- `source.py` — `CHIRPSDataSource` with `advance_start_from_latest()` override (skips to next dekad/pentad/month)
- Three collections declared via `parse_collection_defs()`: monthly, dekadal, pentadal, each with its own
  `default_interval_minutes`

The ECMWF AIFS plugin (`sample_plugins/ecmwf_opendata_source/`) is the reference for **forecast data**:
multi-collection (surface + pressure levels), variable groups, no custom `CollectionLink` subclass.

---

## 4. Setup Wizard & SourceSetupService

### 4.1 Wizard steps

The wizard is registered via `sources/wagtail_hooks.py`. The flow is:

**Plugin selection page** (`data_feed_add_select.html`) — lists every `DataFeed` subclass. Plugins
with a non-empty `get_collection_definitions()` show a **Wizard** badge and enter the 3-step wizard;
others show a **Manual** badge and open the standard edit form.

**Step 1 — Catalog** (`wizard_step1_catalog.html`) — create a new `Catalog` or select an existing
unclaimed one. Fields are pre-filled from `get_catalog_defaults()`. The catalog slug is set here and
cannot be changed later.

**Step 2 — Feed Details** (`wizard_step2_feed.html`) — set the feed name, global `interval_minutes`,
and any plugin-specific fields declared in `DataFeed.panels` beyond `base_panels`. Pre-filled from
`get_wizard_defaults()`.

**Step 3 — Collections** (`wizard_step3_collections.html`) — checklist rendered from
`get_collection_definitions()`. Each checked definition expands to show the variable selection (for
multi-variable collections) and the `DataFeedCollectionLink` config form from `get_form_class()`.
Submitting this step immediately calls `SourceSetupService.provision()` — there is no separate
confirm screen.

All step data is carried between steps via Django sessions (key: `georiva_setup_wizard_{model_name}`).

### 4.2 SourceSetupService.provision()

```python
from georiva.sources.setup_service import SourceSetupService

service = SourceSetupService()
data_feed, collections = service.provision(
    CHIRPSDataFeed,
    catalog=catalog,
    feed_name="CHIRPS Africa",
    feed_interval=43200,  # global DataFeed.interval_minutes
    global_config={"head_timeout": 20},
    selected_definitions=[
        (monthly_def, {"default_start_date": date(1981, 1, 1)}),
        (dekadal_def, {"default_start_date": date(1981, 1, 1)}),
    ],
)
```

For each `(definition, config_values)` pair the service:

1. Upserts a `Collection` (slug = `slugify(f"{catalog.slug}-{definition.key}")`)
2. Upserts each `Variable` in `definition.variables` (respecting `selected_variable_keys` from wizard)
3. Upserts a `DataFeedCollectionLink` with:
    - `definition_key` — stable reference back to the definition
    - fields from `get_link_config_for_definition(definition)` — baked in, not operator-editable
    - fields from `config_values` — operator-supplied; overrides baked config on collision

**Runtime config merge** — when a job runs, `DataFeed.get_data_source(collection)` builds the final
config dict as:

```python
config = {**data_feed.get_loader_config()}  # feed-level (credentials, timeouts)
config.update(link.config)  # per-collection wins on collision
source = SourceClass(config)
```

`provision_collection()` is the single-collection variant used by the "Add collection" action on the
feed detail page (for adding collections to an existing feed post-wizard).

### 4.3 Idempotency

All upserts are keyed on slug (Collection) or `(data_feed, collection)` (link). Re-running
`provision()` after a plugin adds new collection definitions simply adds those collections without
touching existing ones.

---

## 5. Scheduling

GeoRiva creates **one Celery Beat `PeriodicTask` per `DataFeed`** (not per collection). The task name
is `georiva.sources.tasks.run_data_feed_loader:{feed.pk}`.

**How the interval is chosen** (`sources/tasks.py → create_or_update_data_feed_periodic_task`):
the task fires at the minimum of `DataFeed.interval_minutes` and all `DataFeedCollectionLink.interval_minutes`
overrides. This ensures no collection is starved even if it has a shorter cadence than the feed global.

**Per-collection gating** (`DataFeedCollectionLink.is_due()`): inside each task execution, only
collections whose `last_run_at + effective_interval ≤ now()` are processed. Others are skipped.
`record_run()` stamps `last_run_at` on the individual link after each successful run.

**PeriodicTask recalculation** is triggered by:

- `DataFeed.save()`
- `Collection.save()` (iterates feed_links)
- `DataFeedCollectionLink` post_save / post_delete (via `update_link_data_feed_periodic_task` signal handler)

Collections run **sequentially within one task** to keep cross-collection file dedup in
`Loader._find_existing_catalog_path()` race-free.

---

## 6. Mapping onto Core Models

| Plugin component                        | Core model result                                                                     |
|-----------------------------------------|---------------------------------------------------------------------------------------|
| `DataFeed` instance                     | One `DataFeed` row + optional `Catalog` (OneToOne)                                    |
| `CollectionDefinition`                  | One `Collection`                                                                      |
| `CollectionVariable` (passthrough)      | `Variable(transform_type=PASSTHROUGH)` with one `primary` source block                |
| `CollectionVariable` (vector_magnitude) | `Variable(transform_type=VECTOR_MAGNITUDE)` with `u_component` + `v_component` blocks |
| `CollectionVariable` (vector_direction) | `Variable(transform_type=VECTOR_DIRECTION)` with `u_component` + `v_component` blocks |
| `SourceKey`                             | A source stream block: `source_name`, `vertical_dimension`, `vertical_value`          |
| `definition.key`                        | Stored as `DataFeedCollectionLink.definition_key` (stable reference)                  |

---

## 7. Checklist for a new plugin

**`source.py`**

- [ ] Subclass `BaseDataSource`; set `type` and `label` as class attributes
- [ ] Call `super().__init__(config, YourFetchStrategy)` in `__init__`; unpack config keys
- [ ] Implement `generate_requests(start_time, end_time, variables, **kwargs)`
- [ ] Override `get_default_start_date()` if the source has a historical start date
- [ ] Override `advance_start_from_latest()` if already-stored periods should be skipped

**`models.py`**

- [ ] Subclass `DataFeed`; set `data_source_cls`, implement `get_loader_config()`
- [ ] Implement `get_collection_definitions()` — return non-empty list to enable the wizard
- [ ] Declare `COLLECTIONS` dict (or dataclasses) and parse with `parse_collection_defs()`
- [ ] Set `get_catalog_defaults()` so wizard step 1 pre-fills format and description
- [ ] Set `get_wizard_defaults()` for any DataFeed fields with no model-level default
- [ ] If collections need per-collection config:
    - [ ] Subclass `DataFeedCollectionLink`; add fields, implement `config` property and `get_panels()`
    - [ ] Override `get_collection_link_model()` and `get_link_config_for_definition()` on the DataFeed
    - [ ] Create a Django migration for the new link model
- [ ] Decorate `DataFeed` subclass with `@register_snippet`

**`wagtail_hooks.py`**

- [ ] Register a `SnippetViewSet` (or `ModelAdmin`) for the `DataFeed` subclass so it appears in the admin

**App registration**

- [ ] Add to `INSTALLED_APPS` in `settings/base.py` (built-in plugins) or via `GEORIVA_PLUGIN_GIT_REPOS` (external
  plugins)
- [ ] Run `makemigrations` and `migrate`
