# GeoRiva Documentation

Geospatial Raster Ingestion, Visualization & Analysis. This index ties the documentation together and suggests a
reading order. If you're new, start at the top and work down.

## Start here

1. **[Architecture Design Document](architecture/README.md)** — the system-level picture: the six layers, how data
   flows from ingestion through a STAC-aligned core out to serving and analysis, the service inventory, and the design
   decisions (and open questions) behind them. Every other doc is a zoom-in on one part of this one.
2. **[Data Model Guide](georiva-data-model-guide.md)** — how to organize real data into Catalogs, Collections, and
   Variables, with a decision flowchart. Read this before configuring a new data source.

## Ingestion & storage

3. **[Storage & Ingestion Architecture](plugins/georiva-storage-architecture.md)** — the full reference for the
   multi-bucket MinIO layout, the event-driven pipeline (MinIO → Redis list → `minio-consumer` → Celery), the
   `IngestionLog` state machine and crash recovery, the sweep task, and storage/filename APIs.
4. **[Storage (plugin author short version)](plugins/storage.md)** — the condensed, plugin-author-focused view of
   buckets, path/filename conventions, and how to save files.
5. **[Download Deduplication & Multi-Collection Feeds](architecture/download-dedup.md)** — how a single source plugin
   feeds multiple collections without re-downloading, and how to organize plugin collections.

## Plugins & extension points

6. **[Format Plugin System](format-plugins.md)** — the `BaseFormatPlugin` contract for reading GRIB / NetCDF / GeoTIFF,
   the lazy-first data model (`open_variable` → `VariableInfo`), and a step-by-step guide to writing a new format
   plugin.
7. **[Source Plugin Parameter Contract & Setup Wizard](architecture/plugin-parameter-contract.md)** — *(RFC, draft)* a
   proposed declarative `describe_parameters()` contract so plugins can provision their Catalog/Collection/Variable
   hierarchy automatically.
8. **[Derived Products](plugins/derived-products.md)** — the contract for declaring layers a feed computes from its own
   collections (anomaly, climatology, promotion…): `get_derived_products()`, the full `DerivedProductDefinition` /
   `InputRef` / `OutputRef` / `ConfigField` reference, how the tier-aware chain and stages are computed, what core
   materialises versus what recipes create, and a worked CHIRPS example. A product's collection references are
   **feed-local keys** resolved once to catalog `Collection`s and **pinned** as binding rows, so routing, dispatch, and
   resolution match by FK (catalog-scoped, rename-safe). Decisions recorded in
   [ADR-0008](adr/0008-configurable-derivation-products.md),
   [ADR-0009](adr/0009-derived-product-chain-and-lifecycle.md), and
   [ADR-0010](adr/0010-pinned-collection-bindings-for-derived-products.md).

## Contributing

9. **[Contributing Guide](contributing.md)** — dev environment, running the stack and tests, branching model, and code
   conventions.

## Cross-cutting patterns (for maintainers)

For registry/plugin/service/locking/Celery conventions with `file:line` references across the codebase, see
[`.claude/docs/architectural_patterns.md`](../.claude/docs/architectural_patterns.md). The top-level
[`CLAUDE.md`](../CLAUDE.md) is the quick orientation map (tech stack, project structure, key conventions).

## How the pieces fit together

```
Source plugin (sources/)              MinIO drop zone (georiva-incoming)
        │  saves file to                      │  human upload
        ▼  georiva-sources                    ▼
        └──────────────► MinIO s3:ObjectCreated:* ──► Redis list (georiva:minio:events)
                                                            │
                                                  georiva-minio-consumer (BLPOP)
                                                            │  registers IngestionLog,
                                                            ▼  enqueues Celery task
                                          IngestionService.process_file()  ── formats/ plugins
                                                            │                 read GRIB/NetCDF/GeoTIFF
                                       extract → convert → clip → encode (COG / PNG / JSON)
                                                            ▼
                                                   STAC-aligned core models
                                       Catalog → Collection → Variable → Item → Asset
                                                            │
                  ┌─────────────────────────┬───────────────┴───────────────┐
                  ▼                          ▼                               ▼
            STAC API (/api/stac)     Titiler (raster tiles)        virtual_zarr/ (kerchunk
            EDR API (/api/edr)       Martin (vector tiles)         manifests per Variable)
                                     Encoded PNG + WeatherLayers GL          │
                                                                            ▼
                                                              analysis/ (time-series, zonal stats)
```

- **Ingestion & storage** is covered by docs 3–5.
- **The core data model** (the middle row) is doc 2.
- **Reading the raw files** (formats) is doc 6.
- **Serving & analysis** (the bottom rows) is the Architecture Design Document, §5–6.
