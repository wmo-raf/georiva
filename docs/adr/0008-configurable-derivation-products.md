# Configurable, trackable derivation products

## Status

proposed

## Context

The generic Derivation Engine (ADR-0005) runs recipes over a *selector*, and
plugins own recipe families (ADR-0006 Atlas, ADR-0007 CHIRPS). But everything
that configures and operates a derivation is currently **code or ad-hoc CLI**:

- A recipe's config lives in the recipe (CHIRPS bakes `CHIRPS_BASELINE`, the
  quantity list, `min_count` into the class) or is hand-typed into
  `run_recipe --selector-json`.
- Which collections a recipe consumes is **imperative**, buried inside
  `resolve_inputs` (see `test_multi_input.py`), so nothing can show the pipeline
  topology or decide whether a derivation is runnable without executing it.
- Staging vs published is a manual `DataFeed.target_tier` field plus a
  per-plugin `get_wizard_defaults` hack — easy to drift (a product needs staging
  but the feed is set to published → every unit silently skipped).
- Derivation runs are recorded per-unit in `DerivationRun` but have no UI and no
  grouping by a configured product, so operators can't see, trigger, or reason
  about the pipeline the way they can the ingestion pipeline.

We want derivation to be **configured from the wizard UI**, **trackable like
ingestion**, **manually triggerable when its inputs are ready**, and shown as an
**interconnected chain** — all behind a **generic contract usable by every
plugin**, without making the engine domain-aware.

## Decision

Introduce a **declarative derived-product contract** plugins implement, a
**persisted per-feed config** the wizard writes, **product-driven invocation**,
and a **tracking/chain UI** — the engine stays generic throughout.

### Naming

- **`DerivedProductDefinition`** — the blueprint (plugin code), mirrors
  `CollectionDefinition`.
- **`DerivedProduct`** — the persisted config (a `DataFeed` child), mirrors
  `DataFeedCollectionLink`.

A `DerivedProduct` is *not* a `Collection`: one product may emit several
output `Collection`s (e.g. the CHIRPS anomaly emits `anomaly` + `relative-anomaly`).
Its outputs are ordinary `Collection`s; in the chain DAG, **products are edges,
collections are nodes**. (Rejected `DerivedCollection`: not a collection, no 1:1
mapping, collides with `Collection`/`CollectionDefinition`.)

### Contracts (generic, in core)

`DataFeed.get_derived_products()` returns a list of `DerivedProductDefinition`,
each declaring:

| Field | Purpose |
|---|---|
| `recipe_type` | which registered recipe runs |
| `label`, `description` | "what it means", rendered in the wizard step |
| `config_schema` | option fields (type, default, choices) → the wizard form + validation |
| `inputs` | `InputRef(role, collection, tier, required)` — declared, not buried in `resolve_inputs` |
| `outputs` | the collections/roles it produces |
| `trigger_mode` | `event` / `scheduled` / `manual` — a property of the recipe, declared here |

`resolve_inputs` is refactored to **consume `inputs`** instead of hardcoding
slugs, so the DAG and readiness are computable from the declaration.

### Persisted config (sources app)

A **`DerivedProduct`** model, FK child of `DataFeed`: `definition_key`,
`recipe_type`, validated **`config` JSON**, `is_enabled`, and scheduled-trigger
params (interval). The wizard's new **"Derived Products" step** creates these
from the selected definitions + operator config. Recipes are **stripped of
embedded config** — baseline, quantities, `min_count` move into
`DerivedProduct.config` and reach the recipe via the selector. Recipe becomes a
pure `(selector) → units` transform.

### Invocation (engine stays generic)

Invocation flips from **recipe-driven** to **`DerivedProduct`-driven**: an
arriving input finds the enabled `DerivedProduct`s whose declared inputs include
that collection/tier, builds a selector from each `config`, and calls
`run(recipe, selector)`. Scheduled products run from one beat loop over enabled
scheduled products (mirrors `sweep_derivations`/the feed scheduler). The manual
button overlays all modes.

**`target_tier` is auto-derived**, not set: a collection stages iff some enabled
`DerivedProduct` consumes its staging tier; otherwise it publishes directly (no
`StagingItem`s — "no derivation → no staging"). Raw-serving is modeled as the
**promotion** `DerivedProduct`. The manual `target_tier` field and
`get_wizard_defaults` tier hack are removed.

### Tracking & UI

- **`DerivationRun` gains a generic, nullable, indexed `origin` key**, stamped
  by the invocation layer with the product/trigger identity. The engine never
  imports `DerivedProduct` (preserves the ADR-0005 layering); the UI joins
  product → runs by `origin`.
- **Product readiness** = all *required* declared input collections exist and are
  non-empty. It gates the **"Run now"** button and renders blocking reasons
  ("anomaly blocked: normals empty"). The engine's per-unit `readiness()` +
  min-count are unchanged (fine-grained skip at run time).
- **Chain UI = the planned DAG from declarations** (collections = nodes,
  products = edges labeled with recipe/status/readiness/trigger), overlaid with
  `origin`-grouped run status. `DerivationLink` is the item-level provenance
  drill-down.

## Considered Options

- **Recipe-registry-global catalog (the step lists every recipe).** Rejected: a
  generic recipe can't know which of a feed's collections it binds or sensible
  defaults; the operator would hand-wire every selector.
- **Two-layer recipe-schema + feed-binding.** Rejected: recipes are already
  plugin-specific (ADR-0006/0007), so the schema and binding live in the same
  plugin — the split is indirection with no cross-plugin reuse to harvest.
- **Keep inputs imperative in `resolve_inputs` + a separate `describe_inputs()`.**
  Rejected: two methods that drift; one declaration is the source of truth.
- **Hard `FK(DerivationRun → DerivedProduct)`.** Rejected: makes the engine
  depend on the feed layer (backwards dependency, ADR-0005 violation). An opaque
  `origin` key keeps the engine generic.
- **`DerivationLink` lineage as the primary chain view.** Rejected: can't show
  planned-but-unrun or blocked edges, so it can't host triggers/readiness.
- **Operator-chosen trigger mode.** Rejected: lets an inherently event-driven
  product be put on a redundant timer, or a 30-year backfill on per-arrival.

## Consequences

- **Migration:** existing plugin recipes (`chirps-climatology`,
  `chirps-anomaly`, the Atlas recipe) must read config from the selector instead
  of constants and declare `DerivedProductDefinition`s; the CHIRPS
  `get_wizard_defaults` tier hack is replaced by auto-derivation.
- **New models/migrations:** `DerivedProduct`; `DerivationRun.origin`.
- **Cross-feed products deferred:** a CDI spanning multiple feeds does not belong
  to one feed's `get_derived_products()`. The declared-inputs-by-slug design
  leaves room for a future "system product" home; this ADR scopes to feed-local
  products.
- **The engine is untouched** beyond the additive `origin` field — invocation
  routing, config, tracking, and UI all live in the sources/processing
  application layers, not in the generic run loop.
- **One source of truth** for tier, topology, readiness, and triggers (the
  declarations + `DerivedProduct`s) removes the `target_tier`-vs-products drift
  class entirely.

See ADR-0005 (engine), ADR-0006/0007 (plugin recipe families), and **ADR-0009**
(the chain computation, enablement/readiness gates, always-provision +
materialise-on-enable, and the orphan/upgrade lifecycle built on this contract).
