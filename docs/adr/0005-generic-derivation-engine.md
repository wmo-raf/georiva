# Generic Derivation Engine

## Context

[ADR-0004](./0004-staging-tier-and-abstract-stac-models.md) introduces a Staging tier that holds raw inputs
awaiting a transform. Something has to turn those inputs (and/or existing Published products) into served Published
products, recording lineage. GeoRiva has no first-class concept for this.

The driving requirement is an Atlas-like experience (Copernicus Interactive Climate Atlas), but the capability must
be **general**, not CDS-specific. The products span a wide space: single-input promotions, climatology families
(`period × season × quantity × baseline`), SPI/SPEI indices, and multi-input cross-collection products like the
Combined Drought Indicator (precip + soil moisture + vegetation anomalies). The hard part is making one engine
serve all of these without learning what a "season" or a "drought index" is — and without every product family
re-implementing idempotency, locking, and lineage.

This is the write-side counterpart of the existing Loader plugin system, where `DataSource.generate_requests()`
only *declares* what to fetch, `FetchStrategy` is the pluggable mechanism, and `Loader` is the generic
orchestrator.

This is **derivation** (compute-on-write, persisted) and must be kept distinct from **analysis** (compute-on-read,
not persisted). "Derivation = analysis you persist."

## Decision

**A new `processing` app owns a generic, domain-agnostic Derivation Engine.** Recipe families register against it;
adding a family never edits the engine.

**Recipes are declarative + a pure transform; the engine owns the run loop.** A `Recipe` declares — it does not
execute:

- named **input selectors** over Staging/Published, parameterized by a unit's coordinates;
- how to **enumerate units** (declarative default = cartesian product over declared dimensions, temporal dims
  clipped to the trigger range);
- a **readiness** predicate;
- a pure **`transform(resolved_inputs) → outputs`**;
- an **outputs** descriptor.

The engine does resolve → readiness → compute → write → register → link → event → idempotency, generically. A
recipe may override individual *steps* via hooks (e.g. `enumerate_units`, `resolve_inputs`) when the declarative
form can't express something — imperative *within* a declared step, never instead of the loop.

**The engine iterates over an opaque `ProductionUnit`.** A unit is a hashable coordinate whose semantics the
**recipe** owns (climatology = `(variable, period, season, quantity, baseline)`; CDI = `(region, month)`;
promotion = the staging item, 1:1). The engine has **no built-in notion of "slice = (collection, time)"** —
mapping a unit onto the Published schema (Collection slug + Item key, including categorical dims that don't fit
`Item`'s `(collection, time[, reference_time])` key) lives entirely in the recipe's `outputs(unit)`. The engine
treats the unit only as an idempotency key.

**Readiness is a join with no late-arrival state machine.** Candidate generation (`candidate_units(trigger)`) maps
an arriving input back to the units it feeds (or, for backfill, enumerates over a range). The default readiness
predicate = all `required` selectors resolve to a complete result; optional inputs pass to the transform as
present-or-`None`. Quorum/tolerance ("≥2 of 3", "wait then proceed") is an override hook, not a built-in. Late or
partial units simply stay unmaterialized and are re-evaluated on the next relevant event and by a periodic
**backfill sweep** — the write-side mirror of `sweep_unprocessed`. Cadence mismatch is absorbed by selectors
(declare a `time_window(unit)`, pull all timesteps) and the transform (aggregate), never by the engine.

**Alignment lives in recipe transforms, never the engine.** The engine hands in-memory rasters (possibly on
different grids/CRS/cadences/calendars) to the transform, which harmonizes onto a recipe-declared target grid via
the shared geoprocessing library. For multi-input recipes (CDI) we **harmonize eagerly**: upstream intermediates
are written to internal Published collections already on a common analysis grid, so the regrid cost is paid once
and the multi-input transform is trivial. Calendar conversion (e.g. CMIP6 360-day → Gregorian) is a geoprocessing
op.

**Versioning is overwrite-in-place.** An input's version is its `Asset.checksum`; a unit's `input_hash` =
`hash(sorted(input checksums) + recipe_version)`. Idempotency: if a Published item for the unit already records a
matching `input_hash` + `recipe_version`, no-op; else (re)compute and update the item's assets in place. No
parallel item history (the `Item` unique constraint forbids two items per slice anyway). Staleness propagates
**forward** by walking `DerivationLink` from a changed input to its derived items (transitively through internal
intermediates), plus the sweep comparing recorded vs current checksums.

**`DerivationRun` tracks each unit's execution** — the write-side analogue of `FileIngestion`, living in
`processing` (engine bookkeeping, not catalog data). It is the distributed **lock** (preventing two workers
computing the same unit), the **state machine** (`pending → running → completed / failed` — the only record of a
failed or in-progress unit, since those produce no Published item), and the **monitoring** surface.

**One invocation primitive: `run(recipe, selector)`.** It enumerates candidate units and fans out one per-unit
Celery task each (each task takes the `DerivationRun` lock). Event-driven, scheduled/backfill, and manual
invocation are all thin callers differing only in how wide a selector they build — backfill and streaming share
one code path. Per-unit compute runs on a **dedicated `georiva-processing` queue**, isolated from
`georiva-ingestion`, so a multi-year backfill cannot starve live ingestion.

**Compute is a shared, pure library.** `geoprocessing` is a non-Django package (no models, no migrations) holding
raster algebra, regridding, temporal aggregation, calendar conversion, and zonal stats. Functions take in-memory
raster objects in and return rasters/scalars out — no MinIO, no models, no request layer. Both write-side
processing and read-side analysis call it; callers own their own I/O. It is extracted from `analysis`
incrementally, op-by-op, as recipes need each operation.

**Recipes are code-registered for v1; DB configuration is deferred.** Like `BaseDataSource`/format plugins,
recipes register in code; invocation is event hooks + management command + Celery beat + a thin admin trigger. A
DB `DerivationConfig` + admin wizard (the `DataFeed` analogue) is deferred until 2–3 recipe families stabilize
their parameters.

## Alternatives considered

**Imperative recipes that own their run loop** — rejected. Every recipe would re-implement (and could get wrong)
idempotency, locking, lineage, and versioning — the exact cross-cutting concerns we want centralized. Declarative
recipes + an engine-owned loop are the only shape where "adding a family must not edit the engine" is structurally
enforced rather than hoped for. Override hooks recover the needed flexibility without surrendering the loop.

**Engine knows "slice = (collection, time)"** — rejected. Simpler for promotion and CDI, but it breaks on
climatology's categorical dimensions (season, quantity, baseline) which don't fit `Item`'s key, and it leaks
product semantics into the engine. An opaque unit + recipe-owned `outputs` mapping keeps the engine domain-blind.

**Put derivation in the `analysis` app** — rejected. Analysis is read-side, request-scoped, and unpersisted.
Derivation is write-side, scheduled, and persisted with lineage. Sharing the app would conflate two lifecycles;
sharing the *compute* (via the `geoprocessing` library) gives the reuse without the conflation.

**A first-class late/partial-input state machine in v1** (windows, deadlines, wait-then-proceed) — rejected as
premature. Event re-evaluation + a periodic sweep covers the common case with machinery you already trust;
deadline/quorum semantics go in an override hook for the recipes that actually need them.

**Keep history (versioned items) instead of overwrite-in-place** — rejected. No v1 product reads an old version;
retaining them multiplies storage and forces serving to choose a version. `DerivationLink` rows already preserve
*what* changed over time.

**Share the `georiva-ingestion` queue** — rejected. A heavy, long-running CMIP6 backfill on the ingestion queue
would starve newly-fetched operational data. A dedicated queue isolates the two.

**A DB `DerivationConfig` model in v1** — rejected for now. The config surface a recipe family needs is unknown
until families exist; modeling it prematurely would churn. Code registration is enough to ship the first families.

## Consequences

- New `processing` app: the engine (`run`, resolve/readiness/compute/register/link), the `Recipe` contract +
  registry, and the `DerivationRun` model.
- New `geoprocessing` package extracted incrementally from `analysis`; read-side analysis rewired to call it.
- New `georiva-processing` Celery queue and worker; a periodic backfill sweep task.
- Depends on ADR-0004: `DerivationLink`, the `source` asset role, and `Collection.visibility`.
- STAC has no recipe/derivation concept — the engine is entirely ours; `DerivationLink` is descriptive provenance,
  not an execution plan.
- The Phase-0 metadata contract (crs, resolution, bounds, exact time + calendar, nodata, **populated
  `Asset.checksum` on staging assets**) is a prerequisite: alignment and versioning both depend on it.
- Ensemble-dependent products (GWL composites, model-agreement hatching, 1-in-20-yr extremes) remain out of scope
  until CMIP6 post-processing stops collapsing to the ensemble mean; the design incurs no debt by deferring them.
