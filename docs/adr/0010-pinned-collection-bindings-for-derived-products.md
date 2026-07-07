# Pinned collection bindings for derived products

## Status

proposed

## Context

ADR-0008/0009 made the derived-product chain declarative: a plugin's
`DerivedProductDefinition`s name their input and output collections, and the
wizard persists a `DerivedProduct` config row per definition. But the *binding*
between a declaration and the materialised catalog `Collection`s it operates on
is never persisted and never resolved at configuration time. `InputRef.collection`
and `OutputRef.collection` are **bare catalog-slug strings**, re-matched by
string equality at four independent runtime points:

- `collection_routes_to_staging()` (auto-derived tier) compares the fetched
  collection's slug against declared staging inputs;
- `dispatch_for_input()` scans **every** enabled `DerivedProduct` in the system
  and matches the trigger's `(collection_slug, tier)`;
- `resolve_declared_inputs()` queries `filter(collection__slug=...)` with **no
  catalog scoping**, although `Collection.slug` is only unique per catalog;
- recipes read slugs back out of the injected selector binding and query by
  them again.

This string-keyed resolution has three concrete failure classes, one of them
already live:

1. **Slug-scheme mismatch (latent, live).** `SourceSetupService.
   _provision_collection` creates raw collections with
   `slug = slugify(f"{catalog.slug}-{definition.key}")` (e.g.
   `chirps-chirps-monthly`), while plugin declarations reference the bare
   definition key (`chirps-monthly`). For a wizard-provisioned feed the two
   never match: `collection_routes_to_staging()` returns False, raw files land
   in SOURCES instead of STAGING, no `StagingItem` is created, and the entire
   product chain is silently inert. Tests mask this by hand-creating
   collections with unprefixed slugs. Output collections are meanwhile
   materialised **unprefixed** — two naming schemes inside one catalog.

2. **Cross-catalog leakage.** Two feeds of the same plugin type (two catalogs)
   declare identical slugs. `dispatch_for_input` and `resolve_declared_inputs`
   match on slug alone, so an arriving item in catalog A triggers catalog B's
   products, and inputs resolve to the union of both catalogs' items.

3. **Silent decoupling on rename.** The Wagtail admin exposes `Collection.slug`
   as editable. A rename produces no error and no orphan state — routing flips
   tiers, dispatch stops matching, readiness reports "blocked" with a
   misleading hint. ADR-0009 built orphan detection for `definition_key` drift;
   collection-slug drift, the likelier accident, has none. The failure mode is
   silent misrouting, worse than a crash.

The durable join we need already half-exists: `DataFeedCollectionLink` pins
`definition_key → Collection` with a real FK, rename-safe by construction. The
derived-product path ignores it and re-derives slugs through string helpers
(`source_slug`, `climatology_slug`, `resolution_from_slug` parsing slugs with
`rsplit`). Two identity systems for the same concept; the fragile one is
load-bearing.

Scope constraint (unchanged from ADR-0008): products are **feed-local**. Every
input a product declares is either one of the feed's own raw collections or a
sibling product's output. Cross-feed products remain deferred.

## Decision

Keep the declaration layer exactly as it is — pure dataclasses in `core`, no
DB imports (ADR-0005) — and change **what the strings mean** and **when they
are resolved**. Declarations reference feed-local keys; resolution happens once,
at provision/enable time, is validated loudly, and is **pinned as FK rows** the
runtime then matches on. Slugs become cosmetic.

### 1. Declarations reference feed-local collection keys, not catalog slugs

`InputRef.collection` and `OutputRef.collection` are reinterpreted as keys in a
single **per-feed collection-key namespace**: the union of

- the feed's `CollectionDefinition.key`s (raw collections), and
- the output keys declared by the feed's own products (`OutputRef.collection`).

The dataclass shape is unchanged; only the namespace semantics change. CHIRPS's
declarations already coincide with this scheme (`chirps-monthly` *is* the
definition key; `chirps-monthly-climatology` *is* the climatology product's
output key), so plugin churn is minimal.

`core.product_chain.validate_chain` is extended: every `InputRef` key must
resolve within the feed namespace (a definition key, or a sibling product's
output key), and no two distinct products may declare the same output key
(ambiguous provenance). An output key that *equals* a raw definition key is
**allowed** — a promotion serving the raw collection 1:1 reuses the raw key as
its own served output by design, and after §5's unified slug scheme the two
resolve to the same `core.Collection`; the collision rule is therefore
output-vs-output only, never output-vs-definition-key. An unresolvable key is a
plugin bug that **fails loudly** at declaration time — this check alone would
have caught failure class 1.

### 2. Bindings are resolved and pinned at provision/enable time

Two new models in `sources`, children of `DerivedProduct`:

```
DerivedProductInput:  product FK, role, tier, required,
                      source_key (the declared key, for re-resolution/diagnostics),
                      collection FK → core.Collection
DerivedProductOutput: product FK, role,
                      output_key,
                      collection FK → core.Collection
```

`unique_together (product, role)` on each. Resolution rules:

- a key matching one of the feed's `CollectionDefinition`s resolves through
  `DataFeedCollectionLink` (`definition_key → link.collection`) — the pinning
  mechanism raw collections already have;
- a key matching a sibling product's output resolves through that product's
  `DerivedProductOutput` row. The ADR-0009 dependency gate already guarantees
  the upstream product is enabled (and therefore materialised) before a
  dependent can be enabled, so resolution order falls out for free;
- anything else raises `ProductActionError` naming the key — enabling never
  half-succeeds (same atomicity as ADR-0009's gates).

Binding rows are written inside the existing enable transaction
(`enable_product` / `provision_derived_products`), immediately after
`materialise_output_collections` — which already returns the `Collection`
objects and today throws them away. Re-running provision or enable re-resolves
idempotently (upsert on `(product, role)`), so a plugin upgrade that changes a
declaration re-pins on the next enable.

`materialise_output_collections` remains **get-or-create only** (operator
renames of titles/descriptions survive, per ADR-0009 §6); it now additionally
records the FK.

### 3. Runtime matches by FK, not by slug

Every runtime joint flips from slug equality to an indexed FK query:

- **Auto-derived tier** — `collection_routes_to_staging(feed, collection)`
  becomes `DerivedProductInput.objects.filter(product__data_feed=feed,
  product__is_enabled=True, tier="staging", collection=collection).exists()`.
- **Product-driven invocation** — the trigger carries `collection_id`;
  `dispatch_for_input` queries binding rows for `(collection_id, tier)` instead
  of scanning every enabled product and calling `get_derived_products()` per
  row. Feed and catalog scoping are automatic (the FK is the scope), and the
  scan-all-products N+1 disappears.
- **Engine-side resolution** — `resolve_declared_inputs` filters
  `collection_id=...` (via the StagingCollection link of §4 for staging-tier
  inputs). `product_readiness` reads the same rows.
- **Selector binding** — `_binding()` injects `collection_id` alongside the
  slug for each input/output. Recipes migrate to IDs; slugs stay in the binding
  transitionally for display and for recipes not yet migrated.
- **Chain panel** — `build_chain`'s `output_collections` reads
  `DerivedProductOutput` rows instead of a catalog+slug query.

Rename-safety then falls out: editing a `Collection.slug` changes URLs and
storage prefixes for *future* writes, but no binding breaks. No slug-edit
guard is needed.

### 4. StagingCollection links to its core Collection

`StagingCollection` gains a nullable FK `collection → core.Collection`.
`register_staging_file` already looks up the catalog and parses the collection
slug from the storage path (which the Loader writes from `Collection.slug`), so
it resolves `(catalog, slug) → Collection` once at registration and stores the
FK. A data migration backfills existing rows by `(catalog, slug)`. The staging
trigger then enters the system as an ID, closing the last path-parsed string
joint.

### 5. One slug scheme, derived from the key, cosmetic

`_provision_collection` drops the catalog prefix: a provisioned collection's
slug is `slugify(definition.key)`, matching how output collections are already
materialised (`slug = output key`). `Collection.slug` is unique per
`(catalog, slug)` and storage paths already carry the catalog segment
(`{catalog}/{collection}/...`), so the prefix was redundant duplication.
Existing rows keep their slugs — with FK bindings the slug is cosmetic, so no
data migration is required (one may be run for aesthetics).

### 6. Unbound is a visible lifecycle state

If a bound `Collection` is deleted outside the feed lifecycle, the binding row
cascades away. An enabled product with missing binding rows is **unbound**: it
is inert on every dispatch path (no rows to match — safe by construction) and
the chain panel surfaces it as a distinct card state with a "re-enable to
re-bind" action, mirroring ADR-0009's orphan lane. Orphans (definition gone)
and unbound products (collection gone) are the two drift states, both loud.

## Considered Options

- **Keep slug matching, add catalog scoping everywhere.** Fixes cross-catalog
  leakage only. Renames still decouple silently, the slug-scheme mismatch class
  remains, and the fix must be replicated at every current and future query
  site — the drift class this ADR exists to remove.
- **Globally-unique Collection slugs.** Breaks running two feeds of the same
  plugin; STAC/EDR URLs already namespace by catalog. Treats the symptom.
- **Persist the whole `DerivedProductDefinition` in the DB.** Rejected: the
  ADR-0009 upgrade lifecycle (orphans, "new — not enabled" cards, refreshing
  un-overridden labels) depends on the *code* declaration being live truth. A
  persisted copy is a second source of truth that goes stale. We persist only
  the **resolved binding**, which is genuinely instance state.
- **Make `InputRef` carry a model FK.** Violates the ADR-0005 layering — the
  declaration must stay importable by `core`/`processing` with no DB coupling.
- **Resolve per-dispatch via `DataFeedCollectionLink` without pinned rows.**
  Works for raw inputs, but derived inputs still need an output registry;
  every dispatch pays resolution; and there is no indexable match for
  `dispatch_for_input`, so the scan-all-products cost stays.

## Consequences

- **Fixes a live defect loudly.** The wizard-provisioned slug mismatch (failure
  class 1) becomes an enable-time `ProductActionError` / declaration-time
  `ChainError` instead of a silently inert chain, and the unified slug scheme
  removes its cause. An end-to-end provisioning test (wizard path, not
  hand-created collections) becomes possible and required.
- **New models/migrations:** `DerivedProductInput`, `DerivedProductOutput`,
  `StagingCollection.collection` FK + backfill; a backfill that pins bindings
  for existing enabled products (re-running the enable-time resolution).
- **Plugin contract is unchanged in shape.** `InputRef`/`OutputRef` keep their
  fields; keys replace slugs as their meaning. CHIRPS's helpers
  (`source_slug` → *source key*, etc.) survive as key builders;
  `resolution_from_slug`-style slug parsing goes away.
- **`dispatch_for_input` stops being O(all products).** Matching is one indexed
  query on binding rows.
- **The engine stays generic** (ADR-0005): `resolve_declared_inputs` still
  receives declared refs plus IDs via the selector; it never imports
  `DerivedProduct`.
- **Cross-feed products remain deferred**, but the binding rows are the natural
  future home: a "system product" would pin inputs across feeds with the same
  FK mechanism, without new string conventions.
- **Docs:** CONTEXT.md gains *feed-local collection key*, *pinned binding*,
  *unbound*; the derived-products plugin guide and the boilerplate skeleton
  update their `get_derived_products()` examples.

## Implementation slices

Tracer-bullet vertical slices; each lands green and shippable on its own.

1. **Fail loudly + one slug scheme.** Extend `validate_chain` with feed-namespace
   key resolution; add enable-time resolution raising `ProductActionError`;
   drop the catalog prefix in `_provision_collection`; add the end-to-end
   regression test that provisions CHIRPS through the real wizard service and
   asserts the raw collection routes to staging. Fixes failure class 1 and
   makes it impossible to reintroduce silently — before any new model exists.
2. **Pin the binding.** Add `DerivedProductInput`/`DerivedProductOutput` +
   migrations; write them in `provision_derived_products` / `enable_product` /
   `enable_new_definition` using slice 1's resolver; backfill existing enabled
   products; `build_chain` reads output collections from binding rows.
3. **Staging trigger by ID.** `StagingCollection.collection` FK, resolved in
   `register_staging_file`, backfilled by `(catalog, slug)`; staging and
   published triggers carry `collection_id`.
4. **Route and dispatch by FK.** Rewrite `collection_routes_to_staging`,
   `dispatch_for_input`, and the scheduled beat's matching as binding-row
   queries; `_binding()` injects `collection_id`s into the selector (slugs kept
   transitionally).
5. **Resolve and compute by FK.** `resolve_declared_inputs` and
   `product_readiness` filter by `collection_id`; migrate the promotion,
   climatology, and anomaly recipes to consume IDs from the binding; delete the
   remaining slug-only global queries and slug-parsing helpers.
6. **Lifecycle + docs.** Surface the *unbound* state in the chain panel with a
   re-bind action; re-pin on the upgrade path; CONTEXT.md glossary entries;
   update `docs/plugins/` derived-products guide and the
   `source-plugin-boilerplate` skeleton.
