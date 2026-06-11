# Acquisition Model — FetchRun, FetchedFile, UploadSession, UploadedFile

## Context

`DataArrival` was introduced to anchor pipeline visibility — it represented "data arrived for this catalog" and
served as the bridge between the acquisition phase (Loader / manual upload) and the processing phase
(`FileIngestion`). It worked for basic history, but had several structural problems:

- **No per-file fetch visibility.** `DataArrival` was a batch record; individual files fetched during a `Loader`
  run had no model-level representation. The only way to know "which files did this run fetch?" was to infer it
  from `FileIngestion` records after they were processed — too late for real-time monitoring.
- **Conflated acquisition and processing.** `DataArrival` carried both "how did data arrive?" (trigger, fetch
  metadata) and "what happened to it?" (status that mirrored `FileIngestion` state). These are separate concerns
  with different lifecycles and different owners.
- **Created after the run.** `Loader.record_run()` was called in the `finally` block — the record existed only
  after completion, making real-time feed updates impossible without a separate mechanism.
- **Sweep was not acquisition.** `sweep_unprocessed` was forced to create a `DataArrival(trigger=SWEEP)` to
  satisfy the non-null FK on `FileIngestion`. But sweep is a recovery mechanism, not an acquisition event — it
  finds files already in MinIO and registers them for processing.
- **Manual upload had no session concept.** One `DataArrival` was created per file submission, making multi-file
  upload tracking awkward and preventing a natural "upload session" UX.

## Decision

**`DataArrival` is removed.** Its responsibilities are split into two separate concerns.

**Acquisition side** — two new models track how files got into MinIO:

- `FetchRun`: one record per `DataFeed` execution, created at the start of the run (before any files are
  fetched). Status: `running → completed / failed / cancelled`. Covers all collections in the feed.
- `FetchedFile`: one record per file within a `FetchRun`, created and updated incrementally as the Loader
  processes each file. Status: `pending → fetching → stored / skipped / failed`. Skipped files (already
  exist in storage) appear as records with `status=skipped` for full audit coverage.
- `UploadSession`: one record per manual multi-file upload, owned by `catalog + user`. Status:
  `active → completed / failed / cancelled`. Transitions to `completed` automatically once all `UploadedFile`
  children reach a terminal state.
- `UploadedFile`: one record per file within an `UploadSession`. Status:
  `pending → uploading → stored / failed`. No `skipped` state — user-chosen files are always attempted.

**Processing side** — `FileIngestion` becomes self-contained:

- The `data_arrival` FK is dropped. `FileIngestion` is created directly by the MinIO consumer (bucket event)
  or by the sweep task.
- New summary fields populated on completion: `variables_discovered`, `valid_time_start`, `valid_time_end`,
  `timestep_count`.
- The join from `FileIngestion` to acquisition records is via `file_path` only — no FK. This keeps the two
  concerns independent and handles the sweep path naturally.

**Two monitoring feeds** replace the single Ingestion Activity Feed:

- `Acquisition Feed`: shows `FetchRun` and `UploadSession` records with per-file `FetchedFile` /
  `UploadedFile` drill-down. Real-time via SSE. Answers "what did we fetch / upload?"
- `Ingestion Feed`: shows `FileIngestion` records with step-by-step job progress and summary metadata.
  Real-time via SSE. Answers "what did we process?"

**`Loader` creates `FetchRun` / `FetchedFile` incrementally.** The `on_file_fetched` (and a new
`on_file_started`) callback hooks on `Loader` are used to write `FetchedFile` status transitions as they happen.

**`sweep_unprocessed` creates `FileIngestion` directly.** No acquisition record. Sweep is recovery, not fetch.

## Alternatives considered

**Keep `DataArrival` and add per-file child records to it** — rejected. `DataArrival` conflates acquisition
context with processing state; adding children would deepen that coupling rather than resolving it. Removing
it gives each concern a clean model with its own lifecycle.

**One `FetchRun` per collection per execution** — rejected. Operators think in terms of "the CHIRPS run at
06:00", not "the CHIRPS precip-daily run". One `FetchRun` per DataFeed execution (Option A) maps to the
mental model; per-file collection context is already encoded in `file_path`.

**Keep `DataArrival` as a Sweep anchor** — rejected. Sweep is a recovery mechanism for files already in
MinIO. Creating a `DataArrival(trigger=SWEEP)` just to satisfy a FK constraint misrepresents what happened.
`FileIngestion` standalone is the correct representation for sweep-registered files.

**FK from `FileIngestion` to `FetchedFile` / `UploadedFile`** — rejected. The MinIO consumer fires on
bucket events and has no knowledge of which acquisition path produced the file. A `file_path` join is the
right coupling — it decouples the two pipelines and handles sweep naturally.

## Consequences

- `DataArrival` model, migrations, and all references removed. `DataArrival.Trigger` enum removed.
- New models added: `FetchRun`, `FetchedFile`, `UploadSession`, `UploadedFile` (with clean migrations).
- `FileIngestion`: drop `data_arrival` FK; add `variables_discovered`, `valid_time_start`,
  `valid_time_end`, `timestep_count` fields.
- `DataFeed.record_run()` replaced by `FetchRun` creation + `FetchedFile` incremental updates in `Loader`.
- MinIO consumer: call `FileIngestion.register()` directly (no `DataArrival.find_or_create()`).
- `sweep_unprocessed`: remove `DataArrival` creation; call `FileIngestion.register()` directly.
- SSE events: `data_arrival.*` events replaced by `fetch_run.*`, `fetched_file.*`, `upload_session.*`,
  `uploaded_file.*`, and `file_ingestion.*` event types.
- `DataArrivalJob` removed. `FileIngestionJob` unchanged.
- Upload Page: rewritten to support multi-file upload sessions.
