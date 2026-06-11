# GeoRiva

A platform that ingests gridded raster data from external sources or manual uploads, processes it into cloud-optimized
formats, and indexes it as STAC-compliant catalogs for African National Meteorological Services.

## Language

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
