# GeoRiva — Architecture Design Document

**Geospatial Raster Ingestion, Visualization & Analysis**

|             |                                         |
|-------------|-----------------------------------------|
| **Status**  | Living document — partially implemented |
| **Version** | 0.3                                     |
| **Date**    | 2026-06-05                              |
| **Author**  | Erick Otenyo, WMO Africa                |

> **Note (v0.2):** This document began life as a pre-implementation RFC (v0.1, 2025-02-09). Much of
> the system is now built. Sections have been updated to reflect the as-built architecture; remaining
> aspirational items are called out inline. Related design: see
> [`plugin-parameter-contract.md`](./plugin-parameter-contract.md) and
> [`download-dedup.md`](./download-dedup.md).

---

## Table of Contents

- [1. Introduction](#1-introduction)
    - [1.1 Purpose & Scope](#11-purpose--scope)
    - [1.2 Design Principles](#12-design-principles)
    - [1.3 Intended Audience](#13-intended-audience)
- [2. System Architecture Overview](#2-system-architecture-overview)
    - [2.1 Layer Responsibilities](#21-layer-responsibilities)
- [3. Data Ingestion](#3-data-ingestion)
    - [3.1 Path A: Source Plugins](#31-path-a-source-plugins)
    - [3.2 Path B: MinIO Drop Zone](#32-path-b-minio-drop-zone)
    - [3.3 Common Ingestion Pipeline](#33-common-ingestion-pipeline)
- [4. Data Model](#4-data-model)
    - [4.1 Entity Descriptions](#41-entity-descriptions)
    - [4.2 Key Design Decisions](#42-key-design-decisions)
- [5. Data Serving & Visualization](#5-data-serving--visualization)
    - [5.1 STAC API](#51-stac-api)
    - [5.2 Tile Serving with Titiler](#52-tile-serving-with-titiler)
    - [5.3 Encoded PNGs for Frontend Shading Libraries](#53-encoded-pngs-for-frontend-shading-libraries)
    - [5.4 STAC Browser](#54-stac-browser)
- [6. Analysis Layer](#6-analysis-layer)
    - [6.1 Core Analysis Capabilities](#61-core-analysis-capabilities)
    - [6.2 Pluggable Analysis Modules](#62-pluggable-analysis-modules)
    - [6.3 Integration with Existing Analysis Libraries](#63-integration-with-existing-analysis-libraries)
    - [6.4 Zarr for Analysis-Ready Data](#64-zarr-for-analysis-ready-data)
    - [6.5 Mini-Dashboards](#65-mini-dashboards)
- [7. Infrastructure & Deployment](#7-infrastructure--deployment)
    - [7.1 Service Inventory](#71-service-inventory)
    - [7.2 Key Infrastructure Decisions](#72-key-infrastructure-decisions)
- [8. Technology Stack Summary](#8-technology-stack-summary)
- [9. Open Questions & Discussion Points](#9-open-questions--discussion-points)
- [10. Next Steps](#10-next-steps)

---

## 1. Introduction

This Architecture Design Document (ADD) lays out the technical vision, design principles, and system architecture for
GeoRiva — a geospatial backend platform for automated ingestion, visualization, and analysis of gridded raster data.

The document is structured as a Request for Comments (RFC). It captures the author's current thinking on how the system
should be built, and invites contributors to review, challenge, and refine the architecture before and during
implementation.

### 1.1 Purpose & Scope

GeoRiva is designed to serve as a general-purpose foundation for any system that needs to:

1. Pull gridded geospatial data from diverse sources
2. Process it for web-based visualization
3. Expose it through standards-compliant APIs
4. Enable analytical workflows — from simple time-series queries to complex domain-specific computations

The initial target domain is meteorological data for African National Meteorological Services, but the architecture is
intentionally domain-agnostic. Any field that works with gridded raster data (agriculture, hydrology, environmental
monitoring, etc.) should be able to build on GeoRiva.

### 1.2 Design Principles

- **STAC-first mental model:** We think in terms of Catalogs, Collections, and Items from the start, aligning our
  internal data model with the SpatioTemporal Asset Catalog (STAC) specification.
- **Plugin-driven extensibility:** Data sources and analysis modules are implemented as plugins (Wagtail apps) that
  conform to defined contracts, enabling community contributions without modifying the core engine.
- **Modern, client-first visualization:** Prefer browser-side rendering of encoded data tiles — enabling smooth temporal
  animation, interactive value picking, client-side color ramps, and interpolation — over legacy server-styled WMS.
  Dynamically served tiles (Titiler for raster COGs, Martin for vector) remain available where server-side rendering is
  the better fit.
- **Cloud-optimized storage:** Cloud Optimized GeoTIFF (COG) as the canonical storage format, with MinIO (S3-compatible)
  as the object store.
- **Async-first processing:** All long-running operations (ingestion, processing, analysis) run as Celery tasks, keeping
  the web layer responsive.
- **Composable via Docker:** The entire stack is orchestrated via Docker Compose for consistent development and
  deployment.

### 1.3 Intended Audience

This document is intended for developers and contributors who will participate in building GeoRiva. It assumes
familiarity with Django, Docker, and basic geospatial concepts. Domain-specific terms (STAC, COG, Zarr) are explained
where they first appear.

---

## 2. System Architecture Overview

GeoRiva is composed of six distinct architectural layers, each with well-defined responsibilities. Data flows from
ingestion sources through processing into a STAC-aligned core, then out through serving and analysis layers to frontend
consumers.

![system_architecture_overview](../images/system-architecture-overview.png)

*Figure 1: GeoRiva System Architecture Overview*

The layers are designed to be loosely coupled. The ingestion layer knows nothing about visualization. The analysis layer
produces new Items that flow back into the core, making derived products indistinguishable from raw ingested data. The
serving layer reads from the core and object storage without concern for how data arrived there.

### 2.1 Layer Responsibilities

| Layer              | Responsibility                                                                                                                                     |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| **Data Ingestion** | Source plugins and MinIO drop zones bring external data into the system. Each path handles scheduling, downloading, and pre-processing.            |
| **Processing**     | Celery workers execute the ingestion pipeline: validation, COG generation, PNG encoding, STAC indexing, and storage.                               |
| **Core Engine**    | Django/Wagtail application housing the STAC-aligned data models, admin interface, and coordination logic. PostgreSQL with TimescaleDB and PostGIS. |
| **Object Storage** | MinIO provides S3-compatible storage for all binary assets (COGs, PNGs, Zarr archives). Titiler reads directly from here.                          |
| **Data Serving**   | STAC API for discovery, Titiler for tile serving, and encoded PNG endpoints for frontend shading library integration.                              |
| **Analysis**       | Pluggable analysis modules that leverage existing Xarray-compatible libraries. Output feeds back into the core as new Items.                       |

---

## 3. Data Ingestion

GeoRiva supports two complementary ingestion paths that converge into a single processing pipeline. This dual-path
design accommodates both fully automated source integrations and manual or scripted data drops.

![ingestion_flow](../images/data-ingestion-flow.png)

*Figure 2: Data Ingestion Flow*

### 3.1 Path A: Source Plugins

Source plugins are Wagtail apps that implement a defined base class (contract). Each plugin encapsulates the complete
logic for a specific data source:

- **Scheduling** — when to check for new data (cron or interval)
- **Downloading** — fetching from the remote source, returning a local file path
- **Pre-processing** — optional source-specific transformations (format conversion, subsetting, reprojection)
- **Triggering ingestion** — handing off to the common pipeline once a file is ready

The plugin architecture means new data sources can be developed and distributed independently of the core engine. A
plugin registers itself with the system and provides its admin UI through Wagtail's admin framework.

> **Plugin Contract (As-built):** A source plugin implements a `BaseDataSource`
> (`sources/source.py`) subclass — setting `type`/`label` and implementing `generate_requests()` —
> paired with a polymorphic **`DataFeed`** model (`sources/models.py`) that holds operator
> configuration and scheduling. A pluggable `FetchStrategy` performs the actual download. The
> **collection definition contract** (`get_collection_definitions()`) lets a plugin declaratively
> describe every collection and variable (including derived products such as wind U/V components) so
> the `Catalog → Collection → Variable` hierarchy can be provisioned by the setup wizard. See
> [`plugin-parameter-contract.md`](./plugin-parameter-contract.md) for the full as-built contract.

### 3.2 Path B: MinIO Drop Zone

For cases where a full plugin is unnecessary, data can be ingested by simply placing files in a structured MinIO
directory. The directory hierarchy encodes the target metadata:

```
incoming/{catalog_slug}/{collection_slug}/{variable_slug}/filename.tif
```

When a file is uploaded to this path, MinIO publishes an `s3:ObjectCreated:*` event to a **Redis
list** using its built-in Redis notification target (`arn:minio:sqs::primary:redis`, list key
`georiva:minio:events`). Notifications are configured per-bucket on `incoming` and `sources` by the
`setup_minio` management command. A dedicated consumer process (`georiva-minio-consumer`, the
`minio_event_consumer` command) blocks on that list (`BLPOP`), validates the object key, registers an
`IngestionLog`, and enqueues the `process_incoming_file` Celery task on the `georiva-ingestion` queue.
File naming conventions are defined per-variable to encode temporal information (e.g., timestamps,
forecast hours).

> **As-built note:** The original design called for an MQTT broker (Mosquitto) to carry MinIO
> notifications. This was replaced by MinIO's native **Redis** event target consumed by
> `georiva-minio-consumer`; Mosquitto is no longer part of the stack. The `sweep_unprocessed`
> periodic task acts as a safety net for any events the consumer misses.

### 3.3 Common Ingestion Pipeline

Regardless of the ingestion path, all data passes through the same Celery-powered pipeline:

1. **Queue:** A Celery task is created and queued for async execution.
2. **Validate:** The incoming file is parsed and validated against expected formats and metadata.
3. **Generate COG:** The data is converted to a Cloud Optimized GeoTIFF, the canonical storage format.
4. **Generate Encoded PNG:** A visualization-ready encoded PNG is produced for use with frontend shading libraries.
5. **Index as STAC Item:** A STAC Item record is created in TimescaleDB with the appropriate Catalog/Collection/Variable
   linkage.
6. **Store Assets:** The COG, PNG, and any other derived assets are stored in MinIO, and Asset records are linked to the
   Item.

---

## 4. Data Model

The core data model is loosely aligned with the STAC specification. This ensures that thinking in STAC terms is natural
from the start, and that exposing a STAC API requires minimal translation.

![stac_aligned_data_model](../images/stac-aligned-data-model.png)

*Figure 3: STAC-Aligned Data Model*

The implemented hierarchy is:

```
Topic ──M2M──→ Catalog ──1:N──→ Collection ──1:N──→ Variable ──1:N──→ Item ──1:N──→ Asset
```

### 4.1 Entity Descriptions

**Topic** is a thematic tag (many-to-many with Catalog) used to group catalogs across domains for
discovery and navigation.

**Catalog** represents the top-level organizational container. A Catalog groups related Collections — for example by
thematic domain (weather forecasts, satellite observations, reanalysis products). It also owns
file-format, boundary, and clip-mode configuration.

**Collection** represents a coherent dataset within a Catalog. Collections share common spatial/temporal extents and
data characteristics. A Collection might represent "GFS Forecast Data" or "CHIRPS Rainfall Estimates."

**Variable** is a GeoRiva-specific extension to the STAC hierarchy. It represents a measured or computed quantity within
a Collection (e.g., temperature, precipitation, wind speed). The Variable model holds per-variable processing
configuration, visualization configuration (palette, value ranges, scale type), and the mapping to raw source bands. It
supports both **passthrough** variables (one source band, read directly) and **derived** variables — notably vector
products computed from U/V components (`VECTOR_MAGNITUDE` for wind speed, `VECTOR_DIRECTION` for wind direction) via the
`sources` StreamField and `transform_type`.

**Item** represents a single spatiotemporal data granule — one snapshot of one variable at one point in time. The Item
model is backed by a TimescaleDB hypertable keyed on the `time` column (the valid time), with an additional
`reference_time` column for forecast data (the model run time). Time-range queries are the dominant access pattern.

**Asset** represents a physical data object associated with an Item (following STAC conventions). An Item may have
multiple Assets: a COG for analysis, an encoded PNG for visualization, a thumbnail, metadata sidecar files, etc.

**DataFeed** (a polymorphic model, `sources/models.py`) tracks a configured data source: its global
scheduling interval (`interval_minutes`), source-specific operator configuration, and aggregate run
statistics. A DataFeed owns one `Catalog` (OneToOne) and links to one or more Collections through
**DataFeedCollectionLink** — the M2M through-model that carries per-collection configuration (e.g.
period, start date) and a per-collection `interval_minutes` override that allows different collections
in the same feed to run at different cadences. Individual executions are recorded as **DataFeedRun**
(aggregate stats) and surfaced live as **DataFeedJob** records via the async job system.

### 4.2 Key Design Decisions

- **TimescaleDB for Items:** The Item table is expected to grow rapidly and will be queried primarily by time range.
  TimescaleDB hypertables provide automatic partitioning and optimized time-range queries.
- **PostGIS for spatial:** The `bbox` field on Item uses PostGIS geometry types for spatial indexing and querying.
- **JSONB for extensibility:** `extra_fields` and `properties` columns use PostgreSQL JSONB to accommodate STAC
  extensions and custom metadata without schema migrations.
- **Variable as a first-class entity:** Standard STAC does not have a Variable concept. GeoRiva introduces it to hold
  per-variable configuration that would otherwise be duplicated across Items or encoded in Collection-level metadata.

---

## 5. Data Serving & Visualization

The serving layer is designed around a key philosophy: move rendering to the browser and serve data in formats that
enable modern, interactive visualization experiences comparable to applications like Windy.com.

### 5.1 STAC API

GeoRiva exposes a standards-compliant STAC API (`/api/stac/`) for data discovery. This allows any STAC-compatible client
(including the bundled STAC Browser) to search, filter, and browse the data catalog. The API is generated from the
internal data model with minimal translation since the models are STAC-aligned by design.

### 5.1a EDR API

Alongside STAC, GeoRiva exposes an **Environmental Data Retrieval (EDR) API** (`/api/edr/`, OGC API – EDR 1.1). EDR is
purpose-built for spatio-temporal environmental data and provides query patterns — position, area, cube — that suit
point/time-series extraction from gridded fields far better than STAC item search. An async **Jobs API** (`/api/jobs/`)
exposes the status of long-running loader and processing jobs.

> **As-built status (v0.2):** The EDR **metadata plane** is implemented: landing page, conformance
> (core, collections, oas30, html, geojson), collection list, and collection detail. The detail
> document is the GetCapabilities equivalent — it advertises spatial/temporal extents, the explicit
> timestep list, `parameter_names` with units and WeatherLayers palette hints (under an `x-georiva`
> extension), the advertised `data_queries`, and a canonical cross-link to the matching STAC
> collection. Collection ID = `Collection.slug`.
>
> **Gaps:** The EDR **data-retrieval plane is not yet implemented** — `position`, `area`, `locations`,
> `instances`, and `cube` endpoints are stubbed/commented out. CoverageJSON output is not yet offered
> (only JSON/GeoJSON). In the interim, equivalent point/area extraction is available through the
> Analysis API (§6.1), which is not yet wired under the EDR query paths.

### 5.2 Tile Serving with Titiler

Titiler serves map tiles directly from Cloud Optimized GeoTIFFs stored in MinIO. A critical design goal is to keep
Titiler stateless and database-free — it should read COGs from object storage and serve tiles without hitting
PostgreSQL.

We are exploring two strategies:

- **Encoded tiles:** Titiler returns raw data-encoded tiles that the frontend shading library interprets and styles.
  This keeps server-side logic minimal and gives the frontend full control over visualization.
- **Styled tiles:** Titiler applies a color ramp server-side and returns pre-styled image tiles. This reduces frontend
  complexity but limits interactivity.

The preferred direction is encoded tiles for maximum frontend flexibility, with styled tiles as a fallback for simpler
use cases.

### 5.3 Encoded PNGs for Frontend Shading Libraries

For integration with frontend shading libraries like WeatherLayers GL, the system generates encoded PNGs where pixel
values represent actual data values (not visual colors). The frontend library decodes these values and applies styling,
interpolation, and animation in the browser. This approach enables:

- Smooth temporal animations between time steps
- Interactive value picking (hover to see the value at any point)
- Custom color ramps applied client-side in real time
- Particle/wind animations from vector data

### 5.4 STAC Browser

A standalone STAC Browser service is included in the Docker stack, connected to GeoRiva's STAC API. This provides an
out-of-the-box discovery and preview interface without requiring custom frontend development.

### 5.5 Vector Tiles with Martin

For vector overlays — primarily administrative boundaries used in clipping and zonal statistics — the stack includes a
**Martin** vector tile server (`georiva-martin`) reading directly from PostGIS. This serves boundary geometries as
MVT/PBF tiles to the frontend, complementing Titiler's raster tiles.

---

## 6. Analysis Layer

The analysis layer is where GeoRiva moves beyond data serving into data processing. The vision is to provide
analysis-ready datasets and a pluggable framework that can leverage the broader ecosystem of scientific Python libraries
for domain-specific computation.

### 6.1 Core Analysis Capabilities

At its foundation, the system will support:

- Time-series extraction at points and over regions
- Spatial subsetting and aggregation
- Basic statistical summaries

These are the building blocks that all higher-level analysis modules can rely on.

> **As-built status (v0.2):** The analysis layer is implemented as two concrete, purpose-built modules
> under `analysis/` rather than the generic operator framework originally envisioned in §6.2.
>
> **Time-series (`analysis/timeseries/`)** — extracts series from the per-variable **virtual Zarr
> manifests** (§6.4) via xarray. Two endpoints:
> - `GET /api/analysis/timeseries/point/` — nearest-grid-cell series for a lat/lon (synchronous).
> - `POST /api/analysis/timeseries/area/` — zonal series over an arbitrary GeoJSON polygon: bbox
    > subset → `regionmask` polygon mask → `mean`/`sum`/`min`/`max`/`std` aggregation (synchronous;
    > designed to move to Celery for large areas).
>
> **Zonal statistics (`analysis/zonal_stats/`)** — *precomputed* admin-boundary statistics. A Celery
> task (`compute_boundary_zonal_stats`, `georiva-ingestion` queue) runs per COG asset using
> `rasterio.mask`, writing `mean/min/max/sum/std/count` per (Item × Variable × AdminBoundary) into the
> `BoundaryZonalStats` TimescaleDB hypertable. Levels come from `Collection.boundary_stats_levels` and
> the `adminboundarymanager` package; a `compute_boundary_stats` command backfills history. These
> stats are served to the frontend as **vector tiles via Martin** (a generated PostgreSQL function,
> `create_martin_function`, exposed at `/martin/boundary_stats/{z}/{x}/{y}`). There is no DRF endpoint
> for zonal stats yet — they are consumed through Martin/DB.

### 6.2 Pluggable Analysis Modules

Analysis modules follow the same plugin pattern as data sources. Each module implements a base contract that defines:

- **Input requirements** — which Items/Variables the module consumes
- **Computation logic** — the actual processing
- **Output specification** — new Items/Assets to generate

The output of any analysis module flows back into the core as new derived Items, fully indexed and discoverable through
the STAC API. This means derived products (climatological means, anomalies, composite indices, etc.) are first-class
citizens in the system, indistinguishable from raw ingested data.

This plugin architecture is intentionally open-ended. Contributors can build modules for any domain: climatological
analysis, agricultural indices, hydrological modeling, forecast verification — whatever the use case demands.

> **As-built gaps (v0.2):** This generic, pluggable framework is **not yet implemented**. There is no
> analysis operator registry or plugin contract today (an earlier `analysis/registry.py` /
> `OperatorRegistry` no longer exists); the two shipped modules (§6.1) are wired directly. Crucially,
> analysis output does **not** yet flow back as new derived Items — time-series are computed on demand
> and zonal stats live in their own hypertable. Integration with external libraries (Xclim, Verde,
> scikit-learn, §6.3) is also not yet present; current analysis uses xarray, `regionmask`, and
> rasterio only. Formalizing the analysis-plugin contract — ideally sharing the parameter-manifest
> vocabulary from [`plugin-parameter-contract.md`](./plugin-parameter-contract.md) — remains future
> work.

### 6.3 Integration with Existing Analysis Libraries

A core design goal is to not reinvent the wheel. The scientific Python ecosystem already has excellent libraries for
working with gridded data, and GeoRiva's data formats (COGs, Zarr, Xarray-compatible structures) are chosen specifically
to interoperate with them.

Analysis modules are expected to leverage existing packages rather than implement algorithms from scratch. Some examples
of the kinds of libraries that fit naturally:

| Library       | Domain                    | Example Use                                                   |
|---------------|---------------------------|---------------------------------------------------------------|
| Xclim         | Climate indices           | SPI, SPEI, growing degree days, heat wave detection           |
| Xarray / Dask | General array computation | Resampling, aggregation, parallel processing                  |
| Rioxarray     | Raster I/O                | Reading/writing COGs within analysis pipelines                |
| Verde         | Spatial processing        | Gridding, trend estimation, cross-validation                  |
| scikit-learn  | Machine learning          | Anomaly detection, classification, regression on gridded data |
| Regionmask    | Regional analysis         | Masking and aggregation by geographic regions                 |

The key consideration is Xarray compatibility — if a library works with Xarray datasets, it should slot into GeoRiva's
analysis framework with minimal friction. The system is not tied to any single analysis domain; the plugin contract
simply needs a module that can read Items in and produce Items out.

### 6.4 Zarr for Analysis-Ready Data

While COG is the canonical storage format for individual Items, Zarr is planned as an additional format for
analysis-ready data cubes. Zarr archives can be generated at the Catalog, Collection, or Variable level, aggregating
many Items into a single chunked, cloud-optimized array suitable for large-scale computation with tools like Xarray and
Dask.

> **As-built status (v0.2):** Implemented as **virtual Zarr** rather than materialized Zarr archives.
> Using `virtualizarr` + `virtual-tiff` (kerchunk), GeoRiva builds a manifest that references the
> existing COG assets *in place* — no data is copied or re-chunked. This is realized in the
> `virtual_zarr/` app:
> - One `VirtualZarrManifest` per **Variable** (a Wagtail snippet), tracking build state via a
    > `pending → building → ready → stale → failed` state machine with distributed locking and crash
    > recovery (same pattern as `IngestionLog`).
> - Manifests are kerchunk JSON stored in a dedicated `georiva-zarr` bucket; `open_dataset()` returns
    > a lazy, dask-backed `xarray.Dataset`.
> - Built by the `build_virtual_zarr` command and Celery tasks; a signal marks a manifest **stale**
    > when new COG assets land, and a sweep debounces rebuilds.
> - This is what the time-series analysis service reads from (§6.1).
>
> **Resolved design question:** This answers §9.2 — virtual Zarr is generated at the **Variable** level
> and maintained automatically (stale-on-write + sweep), with no data duplication.
>
> **Gaps:** Only Variable-level virtual cubes exist; Collection/Catalog-level aggregation and
> materialized Zarr (for heavy Dask workloads or external sharing) are not implemented. Manifests
> cover COG assets only.

### 6.5 Mini-Dashboards

Analysis results and key data summaries will be presentable as mini-dashboards embedded throughout the system. These are
lightweight, auto-updating views that surface insights from the analysis layer without requiring users to build custom
visualizations.

> **As-built status (v0.2):** Not yet a general capability. An operational **ingestion dashboard**
> frontend exists (`ingestion/ingestion-dashboard/`), and the `visualization/` app currently provides
> Wagtail admin hooks only (its DRF views are a stub). Map visualization is delivered via the
> `tile-config` endpoint, encoded PNGs + WeatherLayers GL, Titiler raster tiles, and Martin vector
> tiles rather than embedded analysis mini-dashboards.

---

## 7. Infrastructure & Deployment

![docker_compose_stack](../images/docker-compose-stack.png)

*Figure 4: Docker Compose Stack*

### 7.1 Service Inventory

| Service (compose name)                    | Technology                            | Purpose                                                                   |
|-------------------------------------------|---------------------------------------|---------------------------------------------------------------------------|
| Web Application (`georiva`)               | Django / Wagtail                      | Core engine, admin, STAC API, EDR API, Jobs API, plugin host              |
| Default Worker (`...-default-worker`)     | Celery                                | Lightweight tasks (sweeps, cleanup, scheduling) — `georiva-default` queue |
| Ingestion Worker (`...-ingestion-worker`) | Celery                                | Heavy data processing — `georiva-ingestion` queue                         |
| Scheduler (`...-celery-beat`)             | Celery Beat                           | Schedules periodic tasks (source polling, sweeps, maintenance)            |
| MinIO Consumer (`...-minio-consumer`)     | App process (BLPOP)                   | Consumes MinIO events from a Redis list and enqueues drop-zone ingestion  |
| Tile Server (`...-titiler-app`)           | Titiler (FastAPI)                     | Serves raster map tiles from COGs in MinIO                                |
| Vector Tiles (`...-martin`)               | Martin                                | Serves boundary/vector MVT tiles from PostGIS                             |
| STAC Browser (`...-stac-browser`)         | stac-browser                          | Standalone STAC catalog browsing UI                                       |
| Web Proxy (`...-web-proxy`)               | Nginx                                 | Reverse proxy / static & media serving, routes to all services            |
| Database (`...-db`)                       | PostgreSQL 18 + TimescaleDB + PostGIS | Models, time-series hypertables, spatial data                             |
| Connection Pooler (`...-pgbouncer`)       | PgBouncer                             | Pools DB connections for the app and Martin                               |
| Cache / Broker (`...-redis`)              | Redis                                 | Celery broker, application cache, and MinIO event bus                     |
| Object Storage (`...-minio`)              | MinIO                                 | S3-compatible storage for all binary assets                               |

### 7.2 Key Infrastructure Decisions

- **MinIO over cloud S3:** MinIO provides S3 API compatibility while keeping the entire stack self-contained for
  on-premises deployment, which is critical for many National Meteorological Services.
- **TimescaleDB over raw PostgreSQL:** The Item model's time-series nature makes TimescaleDB's hypertables and
  compression essential for performance at scale.
- **Redis-based event ingestion:** MinIO publishes bucket notifications to a Redis list via its native Redis target; a
  dedicated consumer (`georiva-minio-consumer`) drains the list and enqueues Celery tasks for the drop-zone path. This
  replaced the originally-planned MQTT/Mosquitto broker, removing an infrastructure component and reusing Redis.
- **Split Celery workers:** Two queues and two worker services separate lightweight orchestration (`georiva-default`)
  from heavy data processing (`georiva-ingestion`) so large ingest jobs cannot starve routine tasks.
- **PgBouncer connection pooling:** A pooler fronts PostgreSQL so the app workers and Martin can share a bounded set of
  database connections.
- **Redis as triple-purpose:** Redis serves as the Celery broker, the application cache, *and* the MinIO event bus,
  reducing the number of infrastructure components.

---

## 8. Technology Stack Summary

| Category         | Technology                            | Notes                                       |
|------------------|---------------------------------------|---------------------------------------------|
| Web Framework    | Django 5.x + Wagtail 7.x              | Core engine, admin, plugin host             |
| Database         | PostgreSQL 18 + TimescaleDB + PostGIS | Primary data store (TimescaleDB HA image)   |
| Connection Pool  | PgBouncer                             | DB connection pooling                       |
| Object Storage   | MinIO                                 | S3-compatible, self-hosted                  |
| Task Queue       | Celery + Redis                        | Async processing, two queues                |
| Raster Tiles     | Titiler                               | COG-native tile serving                     |
| Vector Tiles     | Martin                                | MVT tiles from PostGIS (boundaries)         |
| Discovery APIs   | STAC API + OGC API – EDR              | Catalog search + environmental retrieval    |
| STAC Browser     | Radiant Earth stac-browser            | Data discovery UI                           |
| Analysis         | Xarray-compatible ecosystem           | Pluggable, domain-agnostic                  |
| Data Formats     | COG, Zarr, Encoded PNG                | Storage and serving formats                 |
| Frontend Viz     | WeatherLayers GL                      | Browser-side rendering                      |
| Event Ingestion  | MinIO → Redis list → consumer         | Drop-zone notifications (Mosquitto dropped) |
| Reverse Proxy    | Nginx                                 | Routing, static/media serving               |
| Containerization | Docker + Docker Compose               | Full stack orchestration                    |
| Language         | Python 3.10+                          | Primary development language                |

---

## 9. Open Questions & Discussion Points

This section captures areas where the design is not yet settled and contributor input is especially welcome.

### 9.1 Titiler Strategy

Should Titiler serve encoded tiles or pre-styled tiles? The current leaning is toward encoded tiles for maximum frontend
flexibility, but this adds complexity to the frontend rendering pipeline. Contributor feedback on real-world experience
with either approach would be valuable.

> **Update (v0.2):** Largely resolved — the encoded-data approach is implemented via encoded PNGs consumed by
> WeatherLayers GL for client-side styling/animation, with Titiler available for COG-native raster tiles.

### 9.2 Zarr Generation Strategy

At which level should Zarr archives be generated — Catalog, Collection, or Variable? Should Zarr generation be triggered
automatically as Items accumulate, or on-demand? What chunking strategies make sense for the expected access patterns?

> **Update (v0.2):** Resolved for the common case — see §6.4. GeoRiva uses **virtual Zarr (kerchunk)**
> manifests at the **Variable** level, built automatically (stale-on-write + debounced sweep) and
> referencing COGs in place (no re-chunking). Collection/Catalog-level and materialized Zarr remain
> open.

### 9.3 Plugin Distribution

How should source plugins and analysis plugins be distributed? Options include: bundled with the core repository, as
separate Python packages installable via pip, or as Docker sidecar containers.

> **Update (v0.2):** Partially resolved — external plugins are pulled from Git repositories listed in
> `GEORIVA_PLUGIN_GIT_REPOS` and installed at startup (see `deploy/` install scripts). In-tree examples live in
> `sample_plugins/`, and a cookiecutter template is provided in `source-plugin-boilerplate/`.

### 9.4 Analysis Scheduling

Should analysis tasks run reactively (triggered when new Items are ingested) or on a schedule? Reactive execution
ensures derived products are always up-to-date but could create processing storms during bulk ingestion.

### 9.5 Authentication & Multi-Tenancy

The current design does not address authentication or multi-tenancy. Should the STAC API be publicly accessible? Should
different Catalogs be access-controlled? This needs further discussion based on deployment requirements.

### 9.6 Analysis Library Integration Depth

How deeply should GeoRiva integrate with external analysis libraries? Should the system provide thin wrappers that make
it easy to call libraries like Xclim or Verde from within analysis plugins, or should plugins be fully responsible for
managing their own dependencies?

---

## 10. Next Steps

> **v0.3 update:** The items listed below reflect remaining work. Steps 2–7 from earlier revisions
> are complete: plugin contracts are fully specified (see
> [`plugin-parameter-contract.md`](./plugin-parameter-contract.md)), the Docker stack is running,
> core models are implemented, the ingestion pipeline is operational, CHIRPS and ECMWF sample plugins
> are shipping, and Titiler is serving tiles. Remaining open items:

1. **Analysis plugin contract** — Formalize the base class interface for analysis modules (§6.2).
   Currently the two built-in modules (timeseries, zonal stats) are wired directly; a generic
   registry does not yet exist.
2. **EDR data retrieval plane** — Implement `position`, `area`, and `cube` endpoints (§5.1a). The
   metadata plane is complete; data queries are stubbed.
3. **Collection/Catalog-level virtual Zarr** — Variable-level manifests are working; multi-variable
   and collection-level aggregation is not yet implemented (§6.4).
4. **Authentication & access control** — The STAC API is currently public; per-catalog access control
   is not yet designed (§9.5).

---

> **How to Contribute:** This is a living document. If you have questions, suggestions, or disagreements with any aspect
> of the design, please open an issue or start a discussion. The best architectures emerge from collaborative
> refinement.
