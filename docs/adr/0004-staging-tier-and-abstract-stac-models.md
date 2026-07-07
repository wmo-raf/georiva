# Staging Tier and Abstract STAC Models

> **Amended by [ADR-0010](0010-pinned-collection-bindings-for-derived-products.md).**
> `StagingCollection` gains a nullable `collection` FK to its published-tier
> `core.Collection` (same catalog + slug), set at registration, so an arriving
> `StagingItem`'s derivation trigger can carry a `collection_id` and dispatch can
> match pinned binding rows by FK rather than by slug.

## Context

GeoRiva's current data flow couples acquisition to materialization: a `DataFeed` fetch lands a file in the
`georiva-sources` bucket, the MinIO consumer reacts to the bucket event, and `FileIngestion` materializes it
straight into served Published `Item`/`Asset` rows. Every fetched file becomes served layers.

This breaks for inputs that require a transform before they mean anything to a user. The concrete driver is CDS /
CMIP6: the plugin downloads raw multi-temporal NetCDF (one long series per `variable × experiment`, subset to a
country). What users actually see are **derived** climatologies (`period × season × quantity × baseline`). Forcing
that raw NetCDF through the current path would shred it into thousands of useless per-timestep served layers — and
there is nowhere to *hold* the raw input while it awaits derivation.

We need a second data tier that holds raw inputs as first-class, STAC-shaped, **not-served** data — without
inflating it into the Published catalog, and without coupling its existence to the derivation engine that will
later consume it.

## Decision

**Introduce two STAC tiers that mirror the same spec but play different roles.**

- **Published** — the existing `core` `Collection`/`Item`/`Asset`. Product-grained, served. `Item` keeps
  `TimescaleModel` (hypertable, one row per timestep).
- **Staging** — new `StagingCollection`/`StagingItem`/`StagingAsset`. Source/acquisition-grained, **not served**.
  A `StagingItem` is **not** a hypertable; it carries a flexible STAC temporal extent (nullable `datetime` plus
  optional `start_datetime`/`end_datetime`) — one item per raw file, not per timestep. Its temporal fields are
  **approximate Gregorian index bounds**; the authoritative time (and calendar, e.g. CMIP6 360-day) is read from
  file content at derivation time.

Raw-ness is expressed as an **asset role** (`source`), not by the tier name — hence "Staging", not "Raw".

**Staging lives in its own `staging` data app.** The tier boundary becomes a real import boundary
(`from staging.models import StagingItem`), so "not served" is visible at the import site and `core` does not
accumulate a second item lifecycle.

**Shared structure via abstract base models, not multi-table inheritance or proxies.** Extract
`AbstractCollection`, `AbstractSpatialItem`, `AbstractAsset` into `core`. The bases hold only the
**non-relational** fields (bounds, geometry, raster dims, crs, properties; href, media_type, roles, format,
file_size, checksum, stats, extra_fields) **plus the shared `variable` FK** (both tiers reference the same
`core.Variable`). Each concrete model adds its own relations: `Item`/`StagingItem` add their `collection` FK and
their own temporal fields; `Asset`/`StagingAsset` add their own `item` ParentalKey.

**Lineage is a `DerivationLink`, written by the engine, living in the data layer.** One row per (output, input)
edge:

- `derived_item` → `core.Item` (output, always Published);
- exactly one of `source_staging_item` → `staging.StagingItem` or `source_published_item` → `core.Item`
  (enforced by a check constraint);
- provenance tags: `recipe_id`, `recipe_version`, `input_hash`.

Granularity is **item-level**. `DerivationLink` lives in the **`staging` app** — `staging` already depends on
`core` (it imports the abstract bases), so the dependency direction stays `staging → core` and `core` remains
dependency-free. (Putting it in `core`, as an early sketch did, would force `core` to depend on `staging`.)

**Intermediate products stay in Published, marked internal.** A derived product that is itself an input to a
further derivation (e.g. an anomaly feeding the Combined Drought Indicator) is product-shaped, not raw — so it does
**not** go in Staging. Instead, `Collection` gains `visibility = public | internal` (collection-grained). Serving
exposes only `public`; the engine reads `internal` collections freely. This keeps Staging meaning exactly one
thing (raw inputs awaiting derivation) and keeps lineage/versioning uniform (intermediates are `core.Item`s, so
`DerivationLink.source_published_item` and forward invalidation cover them with no special-casing).

**The dependency rule:** the data layer (`core`, `staging`) depends on nothing above it; producers (`sources`,
`processing`) and consumers (`serving`, `monitoring`) depend on the data layer; the derivation engine is removable
without touching the schema.

## Alternatives considered

**Single tier with an `is_raw` / `served` flag on `core.Item`** — rejected. Staging and Published have genuinely
different shapes: Published is a hypertable with one row per timestep; Staging is one item per file with a range
extent and no hypertable. A flag cannot reconcile those storage semantics, and it would put raw multi-temporal
inputs into the same table the serving layer queries.

**Multi-table inheritance (a shared parent table)** — rejected. MTI adds an implicit join on every query and a
shared PK space across tiers; it couples the two tiers at the database level precisely where we want them
independent.

**Proxy models** — rejected. Proxies share one table, so they cannot give Staging a different schema (no
hypertable, range extent) — they only relabel behavior on identical storage.

**Name the tier "Raw"** — rejected. Raw-ness is a property of an *asset* (role `source`), not of the tier; a
staging item can also hold non-raw companion assets. "Staging" names the role (held, awaiting derivation) without
overloading it with format/provenance meaning.

**`DerivationLink` via GenericForeignKey** — rejected. GFK loses DB-level referential integrity, cascade, and
cheap reverse queries. With only two tiers, two nullable FKs + a check constraint is the pragmatic
integrity-preserving choice; a unified `ItemRef` indirection table only pays off at three or more tiers.

**Intermediate products as Staging items** — rejected. Anomalies are derived, product-shaped time-series wanting
the hypertable and `data`-role COGs — not acquisition artifacts. Folding them into Staging would conflate "raw
input awaiting derivation" with "derived product not meant for serving" and force CDI's lineage to span both FK
types for one conceptual edge.

## Consequences

- New `staging` app: `StagingCollection`, `StagingItem`, `StagingAsset`, plus `DerivationLink`.
- `core` gains `AbstractCollection`, `AbstractSpatialItem`, `AbstractAsset`; concrete `Collection`/`Item`/`Asset`
  reparented onto them (Meta, temporal fields, and tier-specific relations kept on the concretes).
- `Asset.Role` gains `source`. `Collection` gains `visibility` (`public | internal`); serving queries filter to
  `public`.
- The data layer stays dependency-free of the engine; `DerivationLink` is written by `processing` but owned by
  the `staging` schema and survives engine removal.
- The app is pre-1.0; migrations for the reparenting are reset rather than preserved as no-ops.
