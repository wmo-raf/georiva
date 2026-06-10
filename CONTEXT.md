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

**DataArrival**:
A batch of one or more files entering MinIO from any trigger. The top-level observable unit of work in the pipeline —
exists for both scheduled and manual upload paths.
_Avoid_: LoaderRun, DataFeedRun, IngestionRun

**FileIngestion**:
The per-file record of processing a single file from MinIO into STAC items and assets. Owns the distributed lock, state
machine, and retry logic for that file.
_Avoid_: IngestionLog

### Triggers

**DataFeed**:
A configured automated data source — what to fetch, how often, and from where. Creates `DataArrival` records on a
schedule via the Loader.
_Avoid_: data source, loader config, plugin (when referring to the configured instance)

**Manual Upload**:
A `DataArrival` triggered by a human, either via the admin upload interface (DataArrival created before the file lands)
or by dropping a file directly into MinIO (DataArrival created at the bucket event).
_Avoid_: manual drop, manual ingest, manual ingestion

**Trigger**:
The cause of a `DataArrival`. One of: `scheduled` (created by a DataFeed) or `manual_upload` (created by a human).

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

**Arrival Status Endpoint**:
A lightweight polling endpoint `GET /api/arrivals/{id}/status/` returning `{id, status, error_message}`. Used by
the admin upload interface to poll until the `DataArrival` reaches a terminal status. Separate from the heavier
collection arrivals list endpoint.

**Upload Page**:
The admin page where an operator submits a single file for ingestion, one page per `ManualUploadConfig` (reached via
the list page's "Upload" button at `/admin/manual-uploads/<pk>/upload/`). Shows: a variable dropdown (from
`ManualUploadConfigVariable`), a single time field (labelled "Model run time" when `is_forecast`, "Observation date"
otherwise) pre-filled from the filename on file pick, and a file picker. One file per submission; each submission
creates one `DataArrival`. After submit, the page polls the Arrival Status Endpoint every 2.5s and shows progress
inline until a terminal status. Incoming paths: GeoTIFF
`{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{filename}`; GRIB/NetCDF `{catalog}/[GR--{reftime}--]{filename}`.

**Upload Flow**:
The sequence for a manual upload via the admin interface: (1) operator submits the upload form; (2) server creates
`DataArrival(trigger=MANUAL_UPLOAD, status=UPLOADING)`; (3) server writes the file to MinIO `incoming` bucket — on
failure, sets `DataArrival.status=FAILED, error_message=<reason>` and returns 500; (4) on success, sets
`DataArrival.status=PENDING` and enqueues the ingestion task; (5) server returns `DataArrival.id`; (6) client polls
the Arrival Status Endpoint until terminal. The file never goes client → MinIO directly (no presigned URLs).
`DataArrival` gains an `error_message = TextField(blank=True)` field to carry upload-time failure reasons.
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
The Wagtail admin home panel showing a per-collection health summary — sparklines, OK/Warning/Failed counts, and last-run time. A fleet-level view across all active Collections. Entry point to the Ingestion Activity Feed via a "View all" link.
_Avoid_: ingestion dashboard, activity panel

**Ingestion Activity Feed**:
A dedicated admin page (`/admin/ingestion/activity/`) showing a live, chronologically-ordered feed of `DataArrival` records with inline `FileIngestion` status and per-job step-by-step progress. Updated in real time via SSE. Covers both manual and scheduled arrivals. Accessible from the sidebar and from the Collection Health Panel.
_Avoid_: live feed, ingestion log, activity dashboard

### Jobs

**DataArrivalJob**:
A task-ferry job providing real-time status of a single `DataArrival` run — covers the full batch from start to finish.
_Avoid_: DataFeedJob

**FileIngestionJob**:
A task-ferry job providing real-time status of a single `FileIngestion` run. One job is created per
`process_incoming_file` invocation, so retries and re-ingests produce multiple jobs pointing at the same
`FileIngestion` (FK, not one-to-one).
_Avoid_: IngestionJob
