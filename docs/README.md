# GeoRiva Documentation

Geospatial Raster Ingestion, Visualization & Analysis. This index ties the documentation together and suggests a
reading order. If you're new, start at the top and work down.

## Start here

1. **[Architecture Design Document](architecture/README.md)** — the system-level picture: the layers, the two data
   tiers, how data flows from ingestion through a STAC-aligned core out to derivation, serving and analysis, the
   multi-tenancy model, the service inventory, and the design decisions (and open questions) behind them. Every other
   doc is a zoom-in on one part of this one.
2. **[Data Model Guide](georiva-data-model-guide.md)** — how to organize real data into Catalogs, Collections, and
   Variables, with a decision flowchart. Read this before configuring a new data source.

## Ingestion & storage

3. **[Storage & Ingestion Architecture](plugins/georiva-storage-architecture.md)** — the full reference for the
   multi-bucket MinIO layout, the event-driven pipeline (MinIO → Redis list → `minio-consumer` → Celery), the
   `IngestionLog` state machine and crash recovery, the sweep task, and storage/filename APIs.
4. **[Storage (plugin author short version)](plugins/storage.md)** — the condensed, plugin-author-focused view of
   buckets, path/filename conventions, and how to save files.
5. **[Runtime Flow — from schedule to served layer](architecture/runtime-flow.md)** — the end-to-end trace of one
   run: scheduling, acquisition (`Loader.run()`), the handoff, the two post-handoff paths (published vs staging →
   derivation), and crash recovery. The best single answer to "what actually happens when a feed fires?".
6. **[Download Deduplication & Multi-Collection Feeds](architecture/download-dedup.md)** — how a single source plugin
   feeds multiple collections without re-downloading, and how to organize plugin collections.

## Plugins & extension points

7. **[Format Plugin System](format-plugins.md)** — the `BaseFormatPlugin` contract for reading GRIB / NetCDF / GeoTIFF,
   the lazy-first data model (`open_variable` → `VariableInfo`), and a step-by-step guide to writing a new format
   plugin.
8. **[Source Plugin Contract & Setup Wizard](architecture/plugin-parameter-contract.md)** — the as-built
   `CollectionDefinition` / `get_collection_definitions()` contract, and the wizard that turns it into a provisioned
   Catalog/Collection/Variable hierarchy. (Supersedes the v0.1 `describe_parameters()` RFC.)
9. **[Derived Products](plugins/derived-products.md)** — the contract for declaring layers a feed computes from its own
   collections (anomaly, climatology, promotion…): `get_derived_products()`, the full `DerivedProductDefinition` /
   `InputRef` / `OutputRef` / `ConfigField` reference, how the tier-aware chain and stages are computed, what core
   materialises versus what recipes create, and a worked CHIRPS example. A product's collection references are
   **feed-local keys** resolved once to catalog `Collection`s and **pinned** as binding rows, so routing, dispatch, and
   resolution match by FK (catalog-scoped, rename-safe). Decisions recorded in
   [ADR-0008](adr/0008-configurable-derivation-products.md),
   [ADR-0009](adr/0009-derived-product-chain-and-lifecycle.md), and
   [ADR-0010](adr/0010-pinned-collection-bindings-for-derived-products.md).

## Contributing

10. **[Contributing Guide](contributing.md)** — dev environment, running the stack and tests, branching model, and code
   conventions.

## Architecture Decision Records

Every architectural decision, with the context that forced it and the alternatives rejected. The documents above
describe *what* the system does; these say *why*, and they are authoritative when the two disagree. Grouped by theme,
newest thinking last:

**Data tiers, derivation & lineage**

| ADR | Decision |
|-----|----------|
| [0004](adr/0004-staging-tier-and-abstract-stac-models.md) | Staging tier and abstract STAC models |
| [0005](adr/0005-generic-derivation-engine.md) | Generic derivation engine — recipes declare, the engine executes |
| [0007](adr/0007-chirps-rolling-anomaly-product-structure.md) | CHIRPS rolling anomaly product structure |
| [0008](adr/0008-configurable-derivation-products.md) | Configurable, trackable derivation products |
| [0009](adr/0009-derived-product-chain-and-lifecycle.md) | Derived-product chain, gates, and lifecycle |
| [0010](adr/0010-pinned-collection-bindings-for-derived-products.md) | Pinned collection bindings, matched by FK |
| [0024](adr/0024-remove-the-json-metadata-sidecar.md) | Remove the JSON metadata sidecar |

**Multi-tenancy & access**

| ADR | Decision |
|-----|----------|
| [0011](adr/0011-row-level-tenancy-choke-point.md) | Row-level tenancy through one choke point |
| [0012](adr/0012-self-contained-per-org-api-roots.md) | Self-contained per-org API roots, addressed by host |
| [0013](adr/0013-org-in-the-path-on-the-machine-plane.md) | The organisation travels in the path on the machine plane |
| [0014](adr/0014-private-tier-and-per-user-api-keys.md) | A private visibility tier, and per-user API keys |
| [0015](adr/0015-nginx-auth-request-gateway-for-tiles.md) | Tile servers are gated at the proxy, not taught who is asking |
| [0016](adr/0016-per-org-page-trees-in-the-admin.md) | Per-org page trees in the Wagtail admin |
| [0017](adr/0017-org-hopper-in-the-admin-sidebar.md) | The org-hopper in the admin sidebar |
| [0018](adr/0018-aggregates-are-scoped-like-rows.md) | Aggregates are scoped like rows |

**Serving & styling**

| ADR | Decision |
|-----|----------|
| [0021](adr/0021-on-demand-encoded-textures.md) | Encoded textures are derived on demand, never stored |
| [0022](adr/0022-two-layer-styling-ramps-and-variable-styles.md) | Two-layer styling: value-free ramps, per-variable snapshots |
| [0023](adr/0023-style-multiplicity-on-the-machine-plane.md) | Multiple styles per variable, selected by query param |

**Acquisition, tracking & operations**

| ADR | Decision |
|-----|----------|
| [0001](adr/0001-sse-for-ingestion-activity-feed.md) | SSE for the ingestion activity feed |
| [0002](adr/0002-pipeline-tracking-model.md) | Pipeline tracking model — DataArrival, FileIngestion, Item |
| [0003](adr/0003-acquisition-model-fetchrun-uploadsession.md) | Acquisition model — FetchRun, UploadSession |
| [0019](adr/0019-retire-the-monitoring-menu-and-acquisition-feed.md) | Retire the Monitoring menu and the org-wide Acquisition Feed |
| [0020](adr/0020-not-ready-runs-are-resurrected.md) | Not-ready runs are resurrected |

## Cross-cutting patterns (for maintainers)

For registry/plugin/service/locking/Celery conventions with `file:line` references across the codebase, see
[`.claude/docs/architectural_patterns.md`](../.claude/docs/architectural_patterns.md). The top-level
[`CLAUDE.md`](../CLAUDE.md) is the quick orientation map (tech stack, project structure, key conventions).

## The ingestion path, concretely

The layer picture lives in [Figure 1](architecture/README.md#2-system-architecture-overview). This block is the other
thing — the named services, buckets, queues and Redis keys you can actually grep for.

```
Source plugin (sources/)              MinIO drop zone (georiva-incoming)
        │  saves file to                      │  human upload / admin UploadSession
        ▼  georiva-sources | georiva-staging  ▼
        └──────────────► MinIO s3:ObjectCreated:* ──► Redis list (georiva:minio:events)
                                                            │
                                    georiva-minio-consumer / georiva-staging-consumer
                                                            │  registers IngestionLog,
                                                            ▼  enqueues Celery task
                                          IngestionService.process_file()  ── formats/ plugins
                                             on georiva-ingestion             read GRIB/NetCDF/GeoTIFF
                                                            │
                                        extract → convert units → clip → write COG
                                                            ▼
                                              ┌─────────────┴─────────────┐
                                              ▼                           ▼
                                   Published (core/)              Staging (staging/)
                                   Collection→Item→Asset          StagingCollection→StagingItem
                                   served                         not served, awaits derivation
                                              ▲                           │
                                              │   sources.derivation_invocation
                                              │   .dispatch_for_input(trigger)
                                              │          matches pinned bindings by FK
                                              │                           ▼
                                              └───── processing/engine.py run(recipe, selector)
                                                     on georiva-processing, writes DerivationLinks
```

Which branch a file takes is computed, not configured: it goes to Staging only if some enabled derived product
declares a staging-tier input on that collection. No derivation, no staging.

Every path above is org-first — `{org}/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/` — and every row is scoped
through `organisations/ownership.py`.

- **Ingestion & storage** is covered by docs 3–6.
- **The core data model** is doc 2; the end-to-end trace is doc 5.
- **Derivation** (the lower half of the diagram) is doc 9, and Architecture §6.
- **Serving & analysis** is the Architecture Design Document, §5 and §7.
