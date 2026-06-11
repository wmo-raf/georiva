# Pipeline Tracking Model — DataArrival, FileIngestion, Item

## Context

The Collection Health Panel needs per-collection health data (sparklines, last-run time, failure counts) for
both automated (DataFeed) and manual upload paths. The original model could not support this for GRIB/NetCDF
manual collections because:

- `DataArrival.collection` FK was null for GRIB/NetCDF uploads (one file → N collections, no single FK target)
- `FileIngestion.collection_slug` was an empty char field for GRIB/NetCDF (collection unknown at file-arrival time)
- `FileIngestion.item` FK pointed *to* the TimescaleDB hypertable (`Item`), requiring `db_constraint=False`
  and giving the wrong directionality

## Decision

**DataArrival is catalog-scoped.** It carries a `catalog` FK and no `collection` FK. A DataArrival represents
"data arrived for this catalog" — which catalog is always known; which collection(s) are resolved during ingestion.

**FileIngestion carries a `collections` M2M.** After `_resolve_collections()` succeeds but before the
per-collection processing loop begins, the service writes the resolved collections into
`FileIngestion.collections`. This means even a run that fails before creating any Items is still
collection-trackable. The M2M is the authoritative record of which collections a file touched or attempted
to touch.

**`FileIngestion.item` FK is dropped.** It was semantically wrong for GRIB/NetCDF files — the FK was
overwritten on every item processed, ending up pointing to the last item written. For a GRIB file producing
80 items, 79 items had no link at all. `Item.source_file` (indexed, value: `"{bucket}:{file_path}"`) provides
the correct audit join for all formats. The collection items list view switches from
`prefetch_related('file_ingestions')` to a bulk lookup keyed on `source_file`.

**Collection Health Panel uses `FileIngestion.collections` M2M for both success and failure signals.**
The sparkline is binary per-day pipeline health (success / failed / empty) — not volume counts. Both signals
come from the same query shape against `FileIngestion.collections`, just filtered on `status`. Items are not
used for dashboard queries; `Item.source_file` (indexed) is the audit trail for tracing which Items a
FileIngestion produced.

**Wizard validates no duplicate source_name across collections within a catalog.** For GRIB/NetCDF, collection
resolution is unambiguous only if each `source_name` (raw variable name in the file) maps to exactly one
collection per catalog. This invariant is enforced at wizard save time, not at the DB level (source names live
in a StreamField, not a constrainable column).

## Alternatives considered

**DataArrival with a `collections` M2M** — rejected. The natural scope of an arrival event is the catalog
(one physical file, one catalog), not the collections it affects. Collections are an output of processing.

**One FileIngestion per (file × collection)** — rejected. Would change the lock granularity from file to
(file, collection), reshaping sweep, retry, and FileIngestionJob linkage. The `collections` M2M achieves
per-collection failure tracking without restructuring the lock model.

**Keep `FileIngestion.collection_slug` char field, backfill after processing** — rejected. The char field
is not a FK, cannot be queried with joins, and is only populated for the last collection processed (the
service loop overwrites it per iteration). No help for failures that abort before any collection completes.

**Use `FileIngestion.collection_slug` for dashboard sparklines** — rejected. The char field is empty for
all GRIB/NetCDF files; no amount of backfilling fixes the fundamental mismatch. The `collections` M2M
supersedes it and handles both formats uniformly.

**`FileIngestion.item` FK (kept)** — rejected. Semantically wrong for multi-item files (last-write-wins);
79 of 80 items from a GRIB file would have no link. `Item.source_file` supersedes it cleanly.

## Consequences

- `DataArrival` migration: drop `collection` FK, add `catalog` FK. Existing rows: derive catalog from
  `collection.catalog` or `data_feed` where available.
- `FileIngestion` migration: add `collections` M2M, drop `catalog_slug` / `collection_slug` char fields.
- `FileIngestion.item` FK removed.
- `Item.source_file`: add `db_index=True`. Convention: value is `"{bucket}:{file_path}"`, matching
  `FileIngestion.bucket + FileIngestion.file_path` — the audit join for "all Items from this FileIngestion".
- `IngestionService.process_file()`: write `FileIngestion.collections` immediately after `_resolve_collections()`.
- Dashboard queries: replace `FileIngestion.collection_slug` lookups with Item and FileIngestion.collections queries.
- Wizard step 4 save: validate no duplicate `source_name` across all collections in the catalog being configured.
