# GeoRiva

A platform that ingests gridded raster data from external sources or manual uploads, processes it into cloud-optimized
formats, and indexes it as STAC-compliant catalogs for African National Meteorological Services.

## Language

### Data tiers

**Staging**:
Fetched or uploaded raw artifacts held as STAC-shaped, **source-grained**, **not-served** data — inputs that still
require a transform before they can be served. Mirrors the STAC *spec* (Collection/Item/Asset) but follows the
**source/acquisition** shape, not the product shape. Lives in a dedicated `staging` data app. Raw-ness is expressed
as the asset role `source`, not by the tier name. A `StagingItem` is **not** a TimescaleDB hypertable; it carries a
flexible STAC temporal extent (nullable `datetime` plus optional `start_datetime`/`end_datetime`).
_Avoid_: Raw (as a tier name), Raw tier

**Published**:
Product-grained, **served** STAC data — the existing `core` `Collection`/`Item`/`Asset`. "Served" always means
Published. A published `Item` is a TimescaleDB hypertable (one row per timestep). Reached either directly
(Direct → Published, for ready products needing only inline normalization) or via Derivation from Staging.
A Published `Collection` carries a `visibility` (`public | internal`); serving exposes only `public`.
_Avoid_: Processed tier, Analysis-ready (as a tier name — those are MinIO buckets, not tiers)

**Intermediate product**:
A derived product that is itself an input to a further Derivation (e.g. an anomaly feeding the Combined Drought
Indicator). Lives in **Published** as a normal `core.Item`/`Collection` with `visibility=internal` — **not** in
Staging (it is product-shaped and derived, not raw acquisition). Internal collections are read freely by the engine
but never served.
_Avoid_: pre-final artifact, internal staging

### Derivation & lineage

**Derivation**:
The **write-side** act of transforming Staging (and/or Published) inputs into Published products and persisting the
result. "Derivation = analysis you persist." Distinct from read-side **Analysis** (compute-on-read, not persisted).
Performed by the Derivation Engine in the `processing` app, never in `analysis`.
_Avoid_: processing (as a synonym for the phase), import, analysis (for the write-side act)

**DerivationLink**:
A lineage edge recording that one Published `Item` was derived from one input Item (Staging or Published). One row
per (output, input) edge, tagged with recipe id/version and an input hash. Item-level granularity. Lives in the
`staging` data app, written by the engine. Cross-tier and descriptive provenance — **not** an execution plan.
_Avoid_: provenance link, lineage record (as model names), derived_from (as a model name)

### Derivation engine

**Derivation Engine**:
The generic, domain-agnostic orchestrator in the `processing` app — the write-side counterpart of the Loader. Owns
the run loop: enumerate units → resolve inputs → check readiness → compute → write asset → register Published
items/assets → write DerivationLinks → emit events → idempotency/versioning. Knows nothing about climate semantics
(seasons, baselines, indices). The same primitive `run(recipe, selector)` serves event-driven, scheduled/backfill,
and manual invocation.
_Avoid_: processing engine, pipeline engine

**Recipe**:
A declarative plugin registered against the engine that describes a single family of derivation. Declares: named
**input selectors** (over Staging/Published, parameterized by a unit's coordinates), how to **enumerate units**,
a **readiness** predicate, a **pure transform**, and an **outputs** descriptor. Does **not** own its run loop (the
engine does), but may override individual steps via hooks. Recipe families (Climatology & Indices, ML/Forecast
post-processing, Impact-based) register without editing the engine.
_Avoid_: processor, operator, derivation (Recipe is the spec; Derivation is the act)

**Derived Product Definition**:
The generic, plugin-agnostic blueprint (ADR-0008) declaring one derived product a feed offers: `recipe_type`, a human
`label`/`description`, a `config_schema` (operator options), declared `inputs`/`outputs` (as `InputRef`/`OutputRef`),
and a `trigger_mode` (`event | scheduled | manual`). Pure declaration in `core` — no DB, no engine import — so both
the feed layer (`sources`) and the engine (`processing`) can read it without a backwards dependency. Returned by
`DataFeed.get_derived_products()`. The dependency graph and product readiness are computed from this declaration
**without executing the recipe**. A **product is an edge** in the chain DAG (consumes input collections, emits output
collections); a **`Collection` is a node**. A product is **not** a `Collection`: one product may emit several output
`Collection`s. Mirrors `CollectionDefinition`.
_Avoid_: DerivedCollection (a product is not a collection); conflating the blueprint with the persisted `DerivedProduct`
config

**DerivedProduct**:
The operator's **persisted config** for one derived product (ADR-0008) — the saved counterpart of a Derived Product
Definition. A `DataFeed` child (mirrors `DataFeedCollectionLink`) holding `definition_key`, `recipe_type`, a `config`
JSON validated against the definition's `config_schema`, `is_enabled` (pause without deleting), and a scheduled-trigger
`interval_minutes`. Written by the wizard's "Derived Products" step via `SourceSetupService.provision_derived_products`
(upsert on `(data_feed, definition_key)`, so a revisit edits in place). Not a `Collection`: one product may emit several
output `Collection`s.
_Avoid_: DerivedCollection; treating it as the blueprint (that is the Derived Product Definition)

**Product-driven invocation**:
The application-layer flip (ADR-0008) where an arriving input is routed to the enabled `DerivedProduct`s that *declare*
it as an input — not fanned out to every recipe. `sources.derivation_invocation.dispatch_for_input(trigger)` matches the
trigger's `(collection_slug, tier)` against each enabled product's declared `InputRef`s, builds
`selector = {**config, **trigger}`, and calls the engine's generic `run(recipe, selector)`. It is the **only** place that
joins `DerivedProduct` to the engine, so the engine never imports the feed layer (ADR-0005); the feed layer depending on
the engine is the allowed direction. Event-driven products fall out of this for free.
_Avoid_: recipe-driven dispatch (the pre-ADR-0008 fan-out); putting product routing in `processing`

**Auto-derived tier**:
A collection's storage tier is a **computed** consequence of the configured products (ADR-0008), not a stored field. The
Loader routes a fetched file to the STAGING bucket iff `sources.derivation_invocation.collection_routes_to_staging(feed,
slug)` — some enabled `DerivedProduct` of the feed consumes that collection at the staging tier; otherwise it lands in
SOURCES (published, no `StagingItem`s — "no derivation, no staging"). Replaces the manual `DataFeed.target_tier` field
and the per-plugin `get_wizard_defaults` tier override, removing the "configured to publish but a product needs staging →
silently skipped" drift class.
_Avoid_: `target_tier` (removed); a manual publish/staging toggle

**Scheduled-product beat**:
The periodic loop (ADR-0008) that keeps scheduled derivations current with no operator action:
`sources.derivation_invocation.dispatch_due_scheduled_products()` fires every enabled `DerivedProduct` whose declared
`trigger_mode` is `scheduled` **and** whose `is_due()` interval has elapsed, via the same product-driven path as a
manual run ([[run-now-backfill]]), then stamps `DerivedProduct.last_run_at`. Event-driven and manual products are never
fired here. The Celery task `sweep_scheduled_products` runs on a short fixed cadence; each product's `is_due()`
(`interval_minutes` or the feed's interval) gates its own period — mirroring `sweep_derivations` + the feed scheduler.
_Avoid_: per-product Celery PeriodicTasks (one beat + is_due gating, not N timers)

**Origin** (`DerivationRun.origin`):
An opaque, nullable, indexed grouping key the invocation layer stamps on each `DerivationRun` with the product identity
(`derived_product:{pk}`). The engine stores and indexes it but never interprets it; the tracking UI joins product → runs
by it. `NULL` = no product origin (engine-internal or manual run). An engine-internal re-run (sweep/invalidation) passes
no origin, so it never clobbers the original product stamp.
_Avoid_: a hard `FK(DerivationRun → DerivedProduct)` (would make the engine depend on the feed layer)

**Product status**:
A `DerivedProduct`'s aggregate run state for the tracking view (ADR-0008), computed by
`sources.derivation_tracking.product_status` joining its `DerivationRun`s on [[origin]]. Priority
**`running` > `failed` > `completed` > `idle`** — meaningful because runs are per-unit and overwrite in place, so a
`FAILED` row means a unit is *currently* stuck (not "failed once"). Carries per-status `counts` and `last_completed_at`.
The read-side mirror of product-driven invocation; the engine stays unaware.
_Avoid_: "failed once ever" semantics (a fixed unit's row transitions out of FAILED on re-run)

**Product readiness**:
A coarse, product-level gate (ADR-0008) computed by `sources.derivation_tracking.product_readiness` from the declared
inputs — **no recipe execution**: a product is ready iff every *required* declared input collection exists and is
non-empty. When blocked, names the first empty required input (`blocked_by` + `reason`, e.g. "normals empty"). Gates the
tracking view's **Run now** button. Distinct from and **in front of** the engine's per-unit `readiness()` + min-count
guard, which are unchanged.
_Avoid_: confusing it with the engine's fine-grained per-unit readiness

**Run now / backfill**:
The manual overlay (ADR-0008): `sources.derivation_invocation.run_product_now(product)` triggers a product on demand
with a *wide* selector built from its config and **no** event coordinate, so the recipe enumerates all the product's
units (the same path as a backfill). Reuses the engine's `run()` and the [[origin]] stamping; gated on [[product-readiness]].
_Avoid_: a bespoke backfill path (it is just a wide selector through the same `run()`)

**Production Unit**:
The atomic, opaque, hashable coordinate the engine iterates over — one unit produces one output slice. Its
**semantics are owned by the Recipe**, not the engine (e.g. climatology = `(variable, period, season, quantity,
baseline)`; CDI = `(region, month)`; promotion = the staging item, 1:1). The recipe's `outputs(unit)` maps a unit
onto the Published schema (Collection slug + Item key); the engine treats it only as an idempotency key.
_Avoid_: target slice, slice, tile (as the model name)

**DerivationRun**:
The per-Production-Unit tracking record for one engine execution — the write-side analogue of `FileIngestion`.
Serves three roles: distributed **lock** (prevents two workers computing the same unit), **state machine**
(`pending → running → completed / failed`, the only record of a failed/in-progress unit since those produce no
Published item), and **monitoring** surface. Carries `recipe_type`, `recipe_version`, serialized `unit_key`,
`input_hash`, timing, error, and FK(s) to produced item(s). Lives in the `processing` (engine) app, **not** the data
layer — it is engine bookkeeping and is removed with the engine, while `DerivationLink` and Items survive.
_Avoid_: DerivationLog, RecipeRun, ProcessingRun

**Promotion**:
The degenerate identity Derivation — a ready Staging item normalized (clip/unit-convert/COG/register) into a
Published item with no real transform. The base case proving the engine handles the 1:1 path.
_Avoid_: copy, publish (as a verb for this), passthrough

### Pipeline phases

**Fetch**:
Pulling data from an external source into MinIO. The DataFeed/Loader is responsible for this phase.
_Avoid_: ingest (for this phase)

**Ingestion**:
Processing a file already in MinIO into STAC items and assets. The IngestionService is responsible for this phase.
_Avoid_: processing, import, fetch (for this phase)

### Pipeline records

**FetchRun**:
The record of a single automated DataFeed execution. Created at the start of the run — before any files are fetched —
to enable real-time monitoring. One `FetchRun` per DataFeed execution, covering all collections in that feed.
Status: `running → completed / failed / cancelled`. Success vs partial outcome is derived from `FetchedFile` children,
not stored on the run itself.
_Avoid_: DataArrival, LoaderRun, DataFeedRun

**FetchedFile**:
Per-file acquisition record within a `FetchRun`. Created and updated incrementally as the Loader processes each file.
Status: `pending → fetching → stored / skipped / failed`. Skipped files (already exist in storage) appear as
`FetchedFile` records with `status=skipped`. Linked to `FileIngestion` via `file_path` — there is no FK.
_Avoid_: FetchResult (that is a transient in-memory dataclass, not a model)

**UploadSession**:
The record of a manual multi-file upload by an operator. Owned by `catalog` + `user`. Status:
`active → completed / failed / cancelled`. Transitions to `completed` automatically once all `UploadedFile` children
reach a terminal state (`stored` or `failed`). Not linked to a `DataFeed` — manual uploads are independent of
configured automated sources.
_Avoid_: DataArrival, upload batch, upload job

**UploadedFile**:
Per-file upload record within an `UploadSession`. Status: `pending → uploading → stored / failed`. No `skipped`
state — user-chosen files are always attempted. Linked to `FileIngestion` via `file_path` — there is no FK.
_Avoid_: UploadArrival

**FileIngestion**:
The per-file record of processing a single file from MinIO into STAC items and assets. Owns the distributed lock,
state machine, and retry logic for that file. Created directly by the MinIO consumer (bucket event) or by the
sweep task — with no reference to any acquisition record. Carries a `collections` M2M populated immediately after
collection resolution (before per-collection processing begins), so failed runs are still collection-trackable even
when no Items are created. Summary fields populated on completion: `variables_discovered` (int),
`valid_time_start` (datetime), `valid_time_end` (datetime), `timestep_count` (int), `reference_time` (datetime).
Items produced by a FileIngestion are found via `Item.source_file` (indexed, value: `"{bucket}:{file_path}"`) —
correct for all formats, including GRIB/NetCDF multi-item files.
_Avoid_: IngestionLog

### Triggers

**DataFeed**:
A configured automated data source — what to fetch, how often, and from where. Creates `FetchRun` records on a
schedule via the Loader.
_Avoid_: data source, loader config, plugin (when referring to the configured instance)

**Manual Upload**:
A file upload triggered by a human via the admin upload interface. Creates an `UploadSession` and one
`UploadedFile` per submitted file. Files never go client → MinIO directly (no presigned URLs).
_Avoid_: manual drop, manual ingest, manual ingestion

**Sweep**:
A periodic safety-net task that finds files in MinIO that have no corresponding `FileIngestion` record and
registers them for processing. Sweep is not an acquisition event — it creates `FileIngestion` records directly,
without a `FetchRun` or `UploadSession`.
_Avoid_: sweep arrival, sweep ingestion

### Manual upload setup

**Manual Uploads Menu**:
A top-level Wagtail admin sidebar item ("Manual Uploads") ordered after "Data Feeds". Links to the
`ManualUploadConfig` list page, which shows all configs across all Catalogs, each with an "Upload" button that
navigates to that config's Upload Page.

**ManualUploadConfig**:
A one-time operator-created configuration that enables manual file uploads for a Catalog. Many configs can exist per
Catalog (e.g. one for surface variables, one for pressure levels). Each config defines which Collections it populates,
whether the data is a forecast (`is_forecast`), and the `valid_time_format` (a predefined choice, e.g. `YYYYMMDD`)
used to parse valid time from uploaded filenames. File format is derived from the linked Catalog's `file_format`
field — not stored separately on the config.
_Avoid_: upload profile, upload template, feed config

**Manual Upload Setup Wizard**:
A multi-step Wagtail admin flow that creates a `ManualUploadConfig`. Steps: (1) select/create Catalog, (2) name the
config (e.g. "Surface variables") — unique per Catalog, (3) upload a Sample File (required; server calls
`list_variables()`, scans each variable's data min/max, resolves units against `Unit` records, then discards the
file), set `is_forecast` and choose `valid_time_format`, and select variables, (4) define Collections and per-variable
display name, unit, and value range, (5) review and save. Provisioning is atomic and creates, alongside the config:
any new Catalog/Collections, a core `Variable` per assignment (passthrough transform, primary source =
`variable_name`, unit, value range — via `get_or_create` so existing hand-tuned Variables are never clobbered), and
any `Unit` chosen via the "Create unit" dropdown option. Parallel to the DataFeed setup wizard.
_Avoid_: upload wizard, config wizard

**Sample File**:
A representative file uploaded during the Manual Upload Setup Wizard solely to extract variable metadata via
`list_variables()` plus a per-variable data min/max scan (lazy, via `open_variable()`). Discarded immediately after
extraction — never ingested.
_Avoid_: seed file, template file

**ManualUploadConfigVariable**:
Through model linking a `ManualUploadConfig` to a `Collection` for one variable. Created by the wizard from
`list_variables()` output. Fields: `config` (FK), `collection` (FK), `variable_name`, `long_name`, `units`.
The collection FK is what routes each variable to the right Collection at upload time.
_Avoid_: variable mapping, variable link

**Upload Page**:
The admin page where an operator submits files for ingestion, one page per `ManualUploadConfig` (reached via
the list page's "Upload" button at `/admin/manual-uploads/<pk>/upload/`). Supports multi-file upload (like
Wagtail's images/multiple/add interface). Each submission creates one `UploadSession` with one `UploadedFile`
per file. After submit, the page shows per-file progress in real time.
_Avoid_: upload form, upload interface

**Upload Flow**:
The sequence for a manual upload via the admin interface: (1) operator selects one or more files and submits;
(2) server creates `UploadSession(status=active)` and one `UploadedFile(status=pending)` per file; (3) for each
file: transition to `uploading`, stream to MinIO `incoming` bucket — on failure set `status=failed`; on success
set `status=stored`; (4) when all `UploadedFile` children reach a terminal state, `UploadSession` transitions to
`completed`. The MinIO bucket event then triggers `FileIngestion` independently. Files never go client → MinIO
directly (no presigned URLs).
_Avoid_: upload pipeline, ingest flow

**Time Extraction**:
The process of determining `reference_time` and `valid_time` from an incoming file before it is processed. Attempted
in order: (1) parse the filename using universal conventions + the config's `valid_time_format`, (2) read file content
(GRIB/NetCDF only). Universal filename conventions: `GR--{reftime}--` prefix extracts `reference_time`; the last
segment of the filename before the extension is parsed as `valid_time` using the format from `ManualUploadConfig`.
When extraction succeeds, the admin upload form pre-fills the time fields. When extraction fails, the admin shows
manual entry fields; a direct MinIO drop (outside admin) raises an error and stops ingestion.
_Avoid_: time parsing, date detection

### Operator-facing monitoring surfaces

**Collection Health Panel**:
The Wagtail admin home panel showing a per-collection health summary — sparklines, OK/Warning/Failed counts, and
last-run time. A fleet-level view across all active Collections. Entry point to the Ingestion Feed via a "View all"
link. Sparkline data (30-day binary per-day status: success / failed / empty) is derived entirely from
`FileIngestion.collections` M2M — completed runs for success days, failed runs for failure days.
_Avoid_: ingestion dashboard, activity panel

**Acquisition Feed**:
A dedicated admin page showing a live, chronologically-ordered feed of acquisition activity — both `FetchRun`
records (automated DataFeed executions) and `UploadSession` records (manual uploads). Each card shows the run/session
status with per-file detail expandable. Updated in real time via SSE.
_Avoid_: fetch feed, arrival feed, data arrival feed

**Ingestion Feed**:
A dedicated admin page showing a live, chronologically-ordered feed of `FileIngestion` records with inline
per-job step-by-step progress and summary fields (variables discovered, valid time range, timestep count). Updated
in real time via SSE. Covers files from any trigger — automated fetch, manual upload, or sweep. Accessible from
the sidebar and from the Collection Health Panel.
_Avoid_: live feed, ingestion log, activity dashboard, Ingestion Activity Feed

### Jobs

**FileIngestionJob**:
A task-ferry job providing real-time status of a single `FileIngestion` run. One job is created per
`process_incoming_file` invocation, so retries and re-ingests produce multiple jobs pointing at the same
`FileIngestion` (FK, not one-to-one).
_Avoid_: IngestionJob
