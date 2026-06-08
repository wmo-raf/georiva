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

### Jobs

**DataArrivalJob**:
A task-ferry job providing real-time status of a single `DataArrival` run — covers the full batch from start to finish.
_Avoid_: DataFeedJob

**FileIngestionJob**:
A task-ferry job providing real-time status of a single `FileIngestion`. Paired one-to-one with its `FileIngestion`.
_Avoid_: IngestionJob
