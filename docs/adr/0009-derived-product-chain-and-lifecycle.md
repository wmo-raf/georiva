# Derived-product chain, gates, and lifecycle

## Status

proposed

> **Extended by [ADR-0010](0010-pinned-collection-bindings-for-derived-products.md).**
> This ADR's orphan/upgrade lifecycle gains a sibling drift state — **unbound** (a
> bound `Collection` was deleted, so a pinned binding row cascaded away) — with a
> re-bind action, and materialise-on-enable now also *pins* the resolved
> collections as binding rows. The chain read-model and gates here are unchanged.

## Context

ADR-0008 introduced the declarative derived-product contract
(`DerivedProductDefinition` / `DerivedProduct`), product-driven invocation, and
the tracking/chain UI. It established the vocabulary — **products are edges,
collections are nodes** — and left the pipeline *topology* declared but only
lightly consumed: the wizard showed products with no opt-out, the declared
dependency between products (an anomaly needs its climatology baseline) was
never computed or enforced, output collections were created as a recipe side
effect, and nothing was operator-editable or upgrade-aware.

This ADR records the decisions taken while turning that contract into a
lifecycle every plugin can rely on, implemented with CHIRPS as the reference. It
covers how the **chain** is computed, the **two gates** that decide whether a
product may run, how products are **provisioned and materialised**, what happens
across a **plugin upgrade**, which properties are **editable**, and the **known
boundaries** we chose not to cross.

Everything here stays within the ADR-0005 layering: the generic engine remains
unaware of products; the chain math is a pure module in `core`, and its DB-bound
use lives in the `sources` application layer.

## Decision

### 1. The chain is a tier-aware, computed DAG

`core/product_chain.py` computes the product-level dependency graph as **pure
functions** over a `Sequence[DerivedProductDefinition]` — no DB, no recipe
execution — so it is importable from both the feed layer and the engine
(ADR-0005).

Product **P depends on product Q** iff a **required** input of P at the
**published** tier names a collection among Q's outputs, unioned with any
explicit `depends_on` extras P declares.

Tier awareness is the crux. A staging-tier input is fed by the loader, not by
another product; a published-tier input is another product's served output. In
CHIRPS the anomaly reads the raw slice at *staging* and the climatology baseline
at *published* — so the one true edge is **anomaly → climatology**. A tier-blind
rule would additionally fabricate **anomaly → promotion** (promotion also
outputs the raw collection at the published tier), which is wrong. The rule
therefore filters on `required and tier == "published"`.

`depends_on` exists for the rare non-data-flow dependency the rule can't infer
(a product needing another's side effect rather than its output collection). In
practice it is usually empty — a published-tier input gives the edge for free.

The module exposes `product_dependencies` / `product_dependents`, transitive
`dependencies_closure` / `dependents_closure`, `topological_stages` (Kahn
layering, stable in declaration order — the wizard and panel render lanes from
it), and `validate_chain`.

**Cycles are plugin bugs that fail loudly.** `validate_chain` raises
`ChainError` on duplicate keys or an unknown `depends_on` target, and
`ChainCycleError` on any cycle (data-flow, explicit, or a degenerate self-loop),
at first render/provision — never silently mid-sweep. The wizard degrades to a
single flat lane with the error surfaced; provisioning refuses the batch.

### 2. Two distinct gates

A product may run only if it passes **both**, and they are deliberately separate:

- **Structural enablement gate** (`product_service.enable_product`): a product
  may be enabled only if every product in its transitive `dependencies_closure`
  has a row that exists *and* is enabled. Enabling an anomaly whose climatology
  is off is refused, naming the missing dependency by its display label.
- **Runtime readiness gate** (`derivation_tracking.product_readiness`): a
  product may *run* only when every required input collection exists and is
  non-empty — the same data gate the tracking dashboard already used.

Enablement is about structure and can be satisfied ahead of any data: an
operator may enable a whole chain in one pass (climatology, then anomaly) before
a single byte has been fetched. Readiness is about data and is checked at
dispatch. Keeping them separate is what lets an operator configure the pipeline
up front and let data catch up.

### 3. Cascade-disable through a single write-path

All enable/disable flows — wizard, feed panel, tracking dashboard — route
through `product_service`, so the invariant *no enabled product may have a
disabled (or missing) dependency* holds from every surface. Disabling a product
with enabled dependents shows a confirmation listing the transitive downstream
set; the confirming request **recomputes the closure server-side** (never trusts
the submitted list) and disables the whole set atomically.

### 4. Always provision a row; materialise output collections on enable

Provisioning writes a `DerivedProduct` row for **every** declared definition,
whether the operator ticked it in the wizard or not; the opt-out lives in
`is_enabled`, not in a missing row. `is_enabled` is written **once, at row
creation** (via `update_or_create(create_defaults=…)`), so re-running the wizard
edits config but never clobbers a toggle an operator changed later. An unticked
product is fully inert (every dispatch path filters on `is_enabled`) but stays
visible, one toggle from being enabled.

A product's output **Collections materialise when it is enabled** — including at
provision time for products enabled in the wizard — from each `OutputRef`'s
`title` / `description` / `visibility` metadata. Materialisation is
**get-or-create only, never update**: an operator's later rename, re-description,
or visibility change survives every subsequent enable, upgrade, or run. The
recipes' own lazy `get_or_create` becomes an inert fallback that finds the
pre-materialised row. Everything catalog-facing about an output is therefore
**declaration**, not recipe side effect; the recipe selector binding never
carries the display fields, so a display edit can't change a recipe's unit
identity.

### 5. Orphan / upgrade lifecycle

The chain panel and diagram merge the **live declaration** with **database
state**, so both stay truthful across plugin upgrades:

- A **declared definition with no row** (added by an update) renders as a
  "New — not enabled" card/edge with an inline enable action that shows its
  config form, provisions the row, runs the structural gate, and materialises
  its collections — no wizard re-run.
- A **row whose definition key is no longer declared** (removed/renamed by an
  update) is an **orphan**: flagged distinctly, excluded from all invocation
  (its definition lookup returns nothing, so every dispatch path already skips
  it), with a delete action whose confirmation states that already-published
  collections and their data are kept — deletion removes only the config row.

### 6. Semantic vs structural editability

Editable **semantic** properties (a separate edit view): nullable
`title` / `description` overrides on `DerivedProduct` (blank falls back to the
declared label/description, so un-overridden text refreshes on upgrade), the
`name` / `description` of each materialised output Collection, and the operator
config plus the schedule interval — the last clearly flagged **"affects future
runs only"**. Read-only **structural** identity: definition key, recipe type,
collection slugs, inputs/outputs, and trigger mode are fixed by the plugin
declaration. Every surface that names a product uses the display fallback.

## Known boundaries

- **Completion chaining stays recipe-driven.** When a produced item is
  published, the engine fans it to all recipes without knowing about products
  (`processing/invocation.py`), and it cannot learn about them without a
  `processing → sources` import, which ADR-0005 forbids. CHIRPS recipes
  self-defend via their `candidate_units`. Making completion chaining
  product-aware is out of scope and would require a different layering.
- **The enable-after-ingest staging gap.** Data fetched while a
  staging-consuming product was *disabled* routed to the sources bucket, not
  staging (staging routing is auto-derived from *enabled* products). Enabling
  the product later finds its staging input empty, so readiness reports blocked.
  The chain panel surfaces a specific hint — *re-run the feed to backfill* — and
  no cache invalidation is needed (`collection_routes_to_staging` is computed
  live per loader run). Automatic backfill on enable is out of scope.

## Consequences

- **New contract fields** (all defaulted, so existing declarations compile
  unchanged): `DerivedProductDefinition.default_enabled`, `.depends_on`;
  `OutputRef.title` / `.description` / `.visibility`.
- **New model fields / migration:** `DerivedProduct.title` / `.description`
  overrides.
- **New pure module** `core/product_chain.py` and **service module**
  `sources/product_service.py` (the single enable/disable/provision/materialise
  write-path).
- **CHIRPS is the blueprint:** it declares output titles/descriptions and the
  internal climatology visibility, needs no `depends_on` (the published baseline
  gives the edge), and moved its recipe's hardcoded visibility into the
  declaration.
- **The engine is still untouched** — all of the above lives in `core` (pure
  contract + chain) and the `sources` application layer.

See ADR-0008 (the derived-product contract this builds on), ADR-0005 (the
generic engine and its layering boundary), ADR-0007 (the CHIRPS recipe family),
and `docs/plugins/derived-products.md` (the developer guide).
