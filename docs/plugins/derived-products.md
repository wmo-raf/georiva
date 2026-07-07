# Derived products — plugin developer guide

A **derived product** is a layer a feed computes from its own collections by
running a registered recipe: a promotion that serves raw data 1:1, a climatology
normal, an anomaly against that normal, and so on. This guide is the contract for
declaring them. See ADR-0008 for the original design and ADR-0009 for the chain,
gates, and lifecycle decisions.

Vocabulary (used throughout): in the chain DAG **products are edges** and
**collections are nodes**. A collection lives at a **tier** — `staging`
(loader-fed, pre-publish, never served) or `published` (served via
STAC/EDR/tiles). One product may emit several output collections.

## Declaring products

A feed declares the products it offers by implementing `get_derived_products()`.
It is an **instance** method (unlike `get_collection_definitions`, which is a
classmethod): a product's inputs and outputs bind to *this feed's actual
collections*, which is instance state. Return one `DerivedProductDefinition` per
product; the base default is an empty list (a feed that only ingests raw data
declares none).

```python
def get_derived_products(self):
    from georiva.core.derived_products import (
        ConfigField, DerivedProductDefinition, InputRef, OutputRef,
    )
    return [
        DerivedProductDefinition(
            key="rainfall-anomaly",
            recipe_type="my-anomaly",
            label="Rainfall anomaly",
            description="Departure from the climatological normal.",
            config_schema=(
                ConfigField(key="min_years", type="int", default=30),
            ),
            inputs=(
                InputRef(role="value", collection="rainfall", tier="staging"),
                InputRef(role="baseline", collection="rainfall-climatology",
                         tier="published"),
            ),
            outputs=(
                OutputRef(role="anomaly", collection="rainfall-anomaly",
                          title="Rainfall anomaly",
                          description="Absolute departure from the normal."),
            ),
            trigger_mode="event",
        ),
    ]
```

The contract is **pure declaration** — dataclasses with string enums, no DB or
engine imports — so the wizard (form + validation), invocation (selector
building), tracking (run grouping), and the chain UI all read from the same
source of truth. The DB-backed resolution of a declared input into the catalog
items it points at lives in the engine layer, not in your declaration.

## Contract reference

### `DerivedProductDefinition`

| Field | Type | Default | Purpose |
|---|---|---|---|
| `key` | str | — | Unique per feed. Identifies the product's config row and stamps its run origin. Stable — changing it orphans the old row. |
| `recipe_type` | str | — | The registered recipe this product runs (a `@recipe_registry.register` name). |
| `label` | str | — | Human name shown in the wizard, chain panel, and diagram. |
| `description` | str | — | One-line "what it means", shown alongside the label. |
| `config_schema` | tuple[`ConfigField`] | — | Operator options → the wizard form and validation. Empty tuple = no options. |
| `inputs` | tuple[`InputRef`] | — | The collections this product consumes, declared (not buried in the recipe). |
| `outputs` | tuple[`OutputRef`] | — | The collections this product produces. |
| `trigger_mode` | str | — | `event` (fire on each arriving input), `scheduled` (per-interval), or `manual` (operator-triggered only). |
| `default_enabled` | bool | `True` | Whether the product is pre-ticked in the wizard's opt-in step. |
| `depends_on` | tuple[str] | `()` | Extra product keys this one depends on, for a non-data-flow dependency the tier rule can't infer (see below). |

`validate_config(config)` (called by the wizard and the setup service) coerces
each supplied option to its declared type, fills missing options from their
defaults, constrains `choice` values, and rejects unknown keys — so a bad option
is caught before any row is written.

> **`collection` is a feed-local key, not a catalog slug (ADR-0010).** In an
> `InputRef`/`OutputRef`, `collection` names a key within *this feed's* namespace:
> one of your `CollectionDefinition.key`s (a raw collection) or an output key of
> one of your own products (a sibling output). It is **not** a global catalog
> slug. `validate_chain` rejects an input key that resolves to neither, and two
> products may not declare the same output key (a promotion serving the raw
> collection 1:1 may reuse the raw key as its output). At enable time each key is
> resolved once to a `Collection` and **pinned** as a binding row; every runtime
> joint then matches by FK, so an operator renaming a collection's slug never
> breaks routing, dispatch, or resolution.

### `InputRef`

| Field | Type | Default | Purpose |
|---|---|---|---|
| `role` | str | — | The recipe's name for this input (e.g. `value`, `baseline`). |
| `collection` | str | — | Feed-local collection **key** consumed: a `CollectionDefinition.key` or a sibling product's output key (not a catalog slug). |
| `tier` | str | — | `staging` or `published`. Determines both routing and dependency edges (below). |
| `required` | bool | `True` | An optional input never blocks readiness and never creates a dependency edge. |

### `OutputRef`

| Field | Type | Default | Purpose |
|---|---|---|---|
| `role` | str | — | The recipe's name for this output. |
| `collection` | str | — | Feed-local output collection **key** (not a catalog slug); it materialises as a `Collection` with `slug = slugify(key)`. |
| `title` | str | `""` | Catalog display name of the materialised collection. Blank → the slug. |
| `description` | str | `""` | Catalog description of the collection. |
| `visibility` | str | `"public"` | `public` (served) or `internal` (a derivation intermediate — read by the engine, never served). |

### `ConfigField`

| Field | Type | Default | Purpose |
|---|---|---|---|
| `key` | str | — | Option name (a key in the saved `config`). |
| `type` | str | — | `str`, `int`, `float`, `bool`, or `choice`. |
| `default` | any | `None` | Value used when the operator leaves it blank. |
| `choices` | tuple | `None` | Required for `choice`; the allowed values (and `default`, if set, must be one). |

## How edges and stages are computed

The chain is a **computed DAG**, pure over your declarations
(`core/product_chain.py`) — no DB, no run needed to draw it.

- **Edge rule:** product **P depends on Q** iff a **required** input of P at the
  **published** tier names a collection among Q's outputs, unioned with P's
  `depends_on`. Tier awareness is essential: a staging input is fed by the
  loader, so a required *staging* input never creates a product edge; only a
  required *published* input (another product's served output) does.
- **Stages:** products are laid out in topological stages (Kahn layering, stable
  in declaration order) — the lanes the wizard step and feed panel render. A
  product sits one stage after its dependencies.
- **Cycles fail loudly:** duplicate keys, an unknown `depends_on` target, or any
  cycle (data-flow, explicit, or self-loop) raise at first render/provision, so
  a broken declaration never runs half-applied. Keep your graph acyclic.

## Core materialises; recipes compute

Draw a clear line between the two:

- **Core materialises output collections** from your `OutputRef` metadata when a
  product is enabled (including at provision time). It uses **get-or-create
  only** — it never overwrites an existing collection's `name`, `description`, or
  `visibility`, so an operator's rename survives every later enable, upgrade, or
  run. Declare the catalog-facing strings and visibility on the `OutputRef`; do
  **not** set them in the recipe.
- **Your recipe computes the data** — items and assets — into those collections.
  Its own lazy `get_or_create` for a collection should be a bare fallback
  (`defaults={"name": slug}` at most); it will normally find the collection core
  already materialised. The recipe selector binding carries only
  `role`/`collection`/`tier`, never the display fields, so a title edit can't
  change a recipe's unit identity.

## `default_enabled` and `depends_on`

- **`default_enabled`** controls only the *pre-tick* in the wizard's opt-in step.
  Leave it `True` for products an operator will usually want; set `False` for an
  advanced or expensive product they should opt into deliberately. Either way a
  row is always provisioned — the opt-out lives in `is_enabled`, so an unticked
  product stays visible (disabled) and is one toggle from running.
- **`depends_on`** is almost always unnecessary. If product P consumes product
  Q's output at the published tier (the normal case — an anomaly reading a
  climatology baseline), the edge is inferred for free; do **not** also list it.
  Reach for `depends_on` only for a genuine dependency with no data-flow edge
  (P needs Q to have run for a side effect, not for its output collection).
  Entries must be non-empty and must not name the product itself.

## Worked example: CHIRPS

CHIRPS (`georiva-source-chirps`) is the reference implementation. Per selected
resolution (e.g. `monthly`) it declares three products:

- **Promotion** — `recipe_type="promotion"`, `trigger_mode="event"`. Input: the
  raw slice at **staging**. Output: the same slug at **published** (title
  "CHIRPS monthly rainfall"). Serves each staged slice 1:1.
- **Climatology** — `recipe_type="chirps-climatology"`, `trigger_mode="manual"`.
  Input: the raw slice at **staging**. Output:
  `chirps-monthly-climatology`, declared **`visibility="internal"`** (a baseline
  the anomaly reads, never served). Config: `baseline_start`, `baseline_end`,
  `min_count`.
- **Anomaly** — `recipe_type="chirps-anomaly"`, `trigger_mode="event"`. Inputs:
  the raw slice at **staging** *and* the climatology at **published** (role
  `baseline`, required). Outputs: `chirps-monthly-anomaly` and
  `chirps-monthly-relative-anomaly`, both public.

From these declarations alone the chain computes exactly **anomaly →
climatology** (the anomaly's required published baseline is climatology's
output) and **no** edge to promotion (the anomaly's raw input is staging-tier).
Promotion and climatology land in stage 1, anomaly in stage 2. CHIRPS needs no
`depends_on` — the published baseline gives the edge — and its climatology
recipe no longer sets visibility: the `OutputRef` owns it, materialised on
enable.

## Lifecycle, in brief

Once declared, products are managed from the feed's **Derived Products** panel
(and cross-monitored on the tracking dashboard). Enabling a product is gated on
its dependencies being enabled; running it is separately gated on its input data
being ready. Disabling one that others depend on cascades (with confirmation).
Output collections appear in the catalog the moment a product is enabled.
Adding a product in a later plugin release surfaces it as "New — not enabled"
with inline enable; removing one leaves an "orphan" row that is inert and
deletable (its published data is kept). See ADR-0009 for the full rationale.
