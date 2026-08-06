# GeoRiva

A platform that ingests gridded raster data from external sources or manual uploads, processes it into cloud-optimized
formats, and indexes it as STAC-compliant catalogs for African National Meteorological Services.

## Language

### Data tiers

**Staging**:
Fetched or uploaded raw artifacts held as STAC-shaped, **source-grained**, **not-served** data — inputs that still
require a transform before they can be served. Mirrors the STAC *spec* (Collection/Item/Asset) but follows the
**source/acquisition** shape, not the product shape. Lives in a dedicated `staging` data app. Raw-ness is expressed
as the asset role `source`, not by the tier name. A `StagingItem` is **not** a TimescaleDB hypertable; it carries a
flexible STAC temporal extent (nullable `datetime` plus optional `start_datetime`/`end_datetime`).
_Avoid_: Raw (as a tier name), Raw tier

**Published**:
Product-grained, **served** STAC data — the existing `core` `Collection`/`Item`/`Asset`. "Served" always means
Published. A published `Item` is a TimescaleDB hypertable (one row per timestep). Reached either directly
(Direct → Published, for ready products needing only inline normalization) or via Derivation from Staging.
A Published `Collection` carries a `visibility` (`public | private | internal`) — see **Visibility tier**.
_Avoid_: Processed tier, Analysis-ready (as a tier name — those are MinIO buckets, not tiers)

**Intermediate product**:
A derived product that is itself an input to a further Derivation (e.g. an anomaly feeding the Combined Drought
Indicator). Lives in **Published** as a normal `core.Item`/`Collection` with `visibility=internal` — **not** in
Staging (it is product-shaped and derived, not raw acquisition). Internal collections are read freely by the engine
but never served — on any plane, to anyone, signed in or not.
_Avoid_: pre-final artifact, internal staging

### Derivation & lineage

**Derivation**:
The **write-side** act of transforming Staging (and/or Published) inputs into Published products and persisting the
result. "Derivation = analysis you persist." Distinct from read-side **Analysis** (compute-on-read, not persisted).
Performed by the Derivation Engine in the `processing` app, never in `analysis`.
_Avoid_: processing (as a synonym for the phase), import, analysis (for the write-side act)

**DerivationLink**:
A lineage edge recording that one Published `Item` was derived from one input Item (Staging or Published). One row
per (output, input) edge, tagged with recipe id/version and an input hash. Item-level granularity. Lives in the
`staging` data app, written by the engine. Cross-tier and descriptive provenance — **not** an execution plan.
_Avoid_: provenance link, lineage record (as model names), derived_from (as a model name)

### Derivation engine

**Derivation Engine**:
The generic, domain-agnostic orchestrator in the `processing` app — the write-side counterpart of the Loader. Owns
the run loop: enumerate units → resolve inputs → check readiness → compute → write asset → register Published
items/assets → write DerivationLinks → emit events → idempotency/versioning. Knows nothing about climate semantics
(seasons, baselines, indices). The same primitive `run(recipe, selector)` serves event-driven, scheduled/backfill,
and manual invocation.
_Avoid_: processing engine, pipeline engine

**Recipe**:
A declarative plugin registered against the engine that describes a single family of derivation. Declares: named
**input selectors** (over Staging/Published, parameterized by a unit's coordinates), how to **enumerate units**,
a **readiness** predicate, a **pure transform**, and an **outputs** descriptor. Does **not** own its run loop (the
engine does), but may override individual steps via hooks. Recipe families (Climatology & Indices, ML/Forecast
post-processing, Impact-based) register without editing the engine.
_Avoid_: processor, operator, derivation (Recipe is the spec; Derivation is the act)

**Derived Product Definition**:
The generic, plugin-agnostic blueprint (ADR-0008) declaring one derived product a feed offers: `recipe_type`, a human
`label`/`description`, a `config_schema` (operator options), declared `inputs`/`outputs` (as `InputRef`/`OutputRef`),
and a `trigger_mode` (`event | scheduled | manual`). Pure declaration in `core` — no DB, no engine import — so both
the feed layer (`sources`) and the engine (`processing`) can read it without a backwards dependency. Returned by
`DataFeed.get_derived_products()`. The dependency graph and product readiness are computed from this declaration
**without executing the recipe**. A **product is an edge** in the chain DAG (consumes input collections, emits output
collections); a **`Collection` is a node**. A product is **not** a `Collection`: one product may emit several output
`Collection`s. Mirrors `CollectionDefinition`.
_Avoid_: DerivedCollection (a product is not a collection); conflating the blueprint with the persisted `DerivedProduct`
config

**DerivedProduct**:
The operator's **persisted config** for one derived product (ADR-0008) — the saved counterpart of a Derived Product
Definition. A `DataFeed` child (mirrors `DataFeedCollectionLink`) holding `definition_key`, `recipe_type`, a `config`
JSON validated against the definition's `config_schema`, `is_enabled` (pause without deleting), and a scheduled-trigger
`interval_minutes`. Written by the wizard's "Derived Products" step via `SourceSetupService.provision_derived_products`
(upsert on `(data_feed, definition_key)`, so a revisit edits in place). Not a `Collection`: one product may emit several
output `Collection`s.
_Avoid_: DerivedCollection; treating it as the blueprint (that is the Derived Product Definition)

**Feed-local collection key**:
The namespace a Derived Product Definition's `InputRef`/`OutputRef` `collection` names live in (ADR-0010): a raw
`CollectionDefinition.key` of the feed **or** an output key of one of the feed's own products — *not* a global catalog
slug. `core.product_chain.validate_chain` rejects an input key outside this namespace, and two distinct products may not
declare the same output key (a promotion serving the raw collection 1:1 may reuse the raw key as its output). Products are
feed-local: every input a product declares resolves within its own feed. The provisioned `Collection.slug` is
`slugify(definition.key)` (no catalog prefix), so the key and the slug coincide for a raw collection.
_Avoid_: catalog slug (as the reference); cross-feed inputs (deferred)

**Pinned binding** (`DerivedProductInput` / `DerivedProductOutput`):
The resolved, persisted link from a `DerivedProduct` to the catalog `Collection`s it consumes and produces (ADR-0010) —
each a row with a `collection` FK, the declared `source_key`/`output_key`, `role`, and (inputs) `tier`. Written by
`product_service.pin_bindings` inside the enable transaction, once, so every runtime joint (auto-derived tier, dispatch,
input resolution, readiness) matches by **FK** rather than re-matching a slug on each event — catalog-scoped and
rename-safe. Deleting a bound `Collection` cascades the row away. Replaces the per-event slug reconstruction of ADR-0008.
_Avoid_: re-resolving the declared slug at dispatch time; treating the slug as identity

**Product-driven invocation**:
The application-layer flip (ADR-0008; FK-matched in ADR-0010) where an arriving input is routed to the enabled
`DerivedProduct`s that consume it. `sources.derivation_invocation.dispatch_for_input(trigger)` matches the trigger's
`(collection_id, tier)` against the pinned `DerivedProductInput` rows in one indexed query — feed/catalog scoping falls
out of the FK, so an item in one catalog can't trigger another's products even under a shared slug. It builds
`selector = {**config, **binding, **trigger}` (binding = the pinned rows, each carrying a `collection_id`) and calls the
engine's generic `run(recipe, selector)`. It is the **only** place that joins `DerivedProduct` to the engine, so the
engine never imports the feed layer (ADR-0005). No `get_derived_products()` on this path.
_Avoid_: recipe-driven dispatch (the pre-ADR-0008 fan-out); slug matching (the pre-ADR-0010 dispatch); putting product
routing in `processing`

**Auto-derived tier**:
A collection's storage tier is a **computed** consequence of the configured products (ADR-0008), not a stored field. The
Loader routes a fetched file to the STAGING bucket iff `sources.derivation_invocation.collection_routes_to_staging(feed,
slug)` — some enabled `DerivedProduct` of the feed has a **pinned** staging-tier input on that collection (an indexed
`DerivedProductInput` query, ADR-0010); otherwise it lands in SOURCES (published, no `StagingItem`s — "no derivation, no
staging"). Replaces the manual `DataFeed.target_tier` field and the per-plugin `get_wizard_defaults` tier override,
removing the "configured to publish but a product needs staging → silently skipped" drift class.
_Avoid_: `target_tier` (removed); a manual publish/staging toggle

**Unbound**:
An enabled, still-declared `DerivedProduct` whose pinned bindings no longer cover its declaration (ADR-0010) — usually a
bound `Collection` was deleted outside the feed lifecycle, cascading its binding row away. Inert on dispatch (no binding
row to match) and surfaced as a distinct, loud card state in the feed-detail chain panel, with a **re-bind** action
(`product_service.rebind_product`) that re-runs enable-time resolution to restore the rows, or raises `ProductActionError`
if a required input still can't resolve. The sibling drift state to **orphaned** (definition gone, not collection gone);
both are loud and inert.
_Avoid_: conflating unbound (collection gone) with orphaned (definition gone)

**Scheduled-product beat**:
The periodic loop (ADR-0008) that keeps scheduled derivations current with no operator action:
`sources.derivation_invocation.dispatch_due_scheduled_products()` fires every enabled `DerivedProduct` whose declared
`trigger_mode` is `scheduled` **and** whose `is_due()` interval has elapsed, via the same product-driven path as a
manual run ([[run-now-backfill]]), then stamps `DerivedProduct.last_run_at`. Event-driven and manual products are never
fired here. The Celery task `sweep_scheduled_products` runs on a short fixed cadence; each product's `is_due()`
(`interval_minutes` or the feed's interval) gates its own period — mirroring `sweep_derivations` + the feed scheduler.
_Avoid_: per-product Celery PeriodicTasks (one beat + is_due gating, not N timers)

**Origin** (`DerivationRun.origin`):
An opaque, nullable, indexed grouping key the invocation layer stamps on each `DerivationRun` with the product identity
(`derived_product:{pk}`). The engine stores and indexes it but never interprets it; the tracking UI joins product → runs
by it. `NULL` = no product origin (engine-internal or manual run). An engine-internal re-run (sweep/invalidation) passes
no origin, so it never clobbers the original product stamp.
_Avoid_: a hard `FK(DerivationRun → DerivedProduct)` (would make the engine depend on the feed layer)

**Product status**:
A `DerivedProduct`'s aggregate run state for the tracking view (ADR-0008), computed by
`sources.derivation_tracking.product_status` joining its `DerivationRun`s on [[origin]]. Priority
**`running` > `failed` > `completed` > `idle`** — meaningful because runs are per-unit and overwrite in place, so a
`FAILED` row means a unit is *currently* stuck (not "failed once"). Carries per-status `counts` and `last_completed_at`.
The read-side mirror of product-driven invocation; the engine stays unaware.
_Avoid_: "failed once ever" semantics (a fixed unit's row transitions out of FAILED on re-run)

**Product readiness**:
A coarse, product-level gate (ADR-0008) computed by `sources.derivation_tracking.product_readiness` from the declared
inputs — **no recipe execution**: a product is ready iff every *required* declared input collection exists and is
non-empty. When blocked, names the first empty required input (`blocked_by` + `reason`, e.g. "normals empty"). Gates the
tracking view's **Run now** button. Distinct from and **in front of** the engine's per-unit `readiness()` + min-count
guard, which are unchanged.
_Avoid_: confusing it with the engine's fine-grained per-unit readiness

**Run now / backfill**:
The manual overlay (ADR-0008): `sources.derivation_invocation.run_product_now(product)` triggers a product on demand
with a *wide* selector built from its config and **no** event coordinate, so the recipe enumerates all the product's
units (the same path as a backfill). Reuses the engine's `run()` and the [[origin]] stamping; gated on [[product-readiness]].
_Avoid_: a bespoke backfill path (it is just a wide selector through the same `run()`)

**Production Unit**:
The atomic, opaque, hashable coordinate the engine iterates over — one unit produces one output slice. Its
**semantics are owned by the Recipe**, not the engine (e.g. climatology = `(variable, period, season, quantity,
baseline)`; CDI = `(region, month)`; promotion = the staging item, 1:1). The recipe's `outputs(unit)` maps a unit
onto the Published schema (Collection slug + Item key); the engine treats it only as an idempotency key.
_Avoid_: target slice, slice, tile (as the model name)

**DerivationRun**:
The per-Production-Unit tracking record for one engine execution — the write-side analogue of `FileIngestion`.
Serves three roles: distributed **lock** (prevents two workers computing the same unit), **state machine**
(`pending → running → completed / failed`, the only record of a failed/in-progress unit since those produce no
Published item), and **monitoring** surface. Carries `recipe_type`, `recipe_version`, serialized `unit_key`,
`input_hash`, timing, error, and FK(s) to produced item(s). Lives in the `processing` (engine) app, **not** the data
layer — it is engine bookkeeping and is removed with the engine, while `DerivationLink` and Items survive.
_Avoid_: DerivationLog, RecipeRun, ProcessingRun

**Promotion**:
The degenerate identity Derivation — a ready Staging item normalized (clip/unit-convert/COG/register) into a
Published item with no real transform. The base case proving the engine handles the 1:1 path.
_Avoid_: copy, publish (as a verb for this), passthrough

### Pipeline phases

**Fetch**:
Pulling data from an external source into MinIO. The DataFeed/Loader is responsible for this phase.
_Avoid_: ingest (for this phase)

**Ingestion**:
Processing a file already in MinIO into STAC items and assets. The IngestionService is responsible for this phase.
_Avoid_: processing, import, fetch (for this phase)

### Pipeline records

**FetchRun**:
The record of a single automated DataFeed execution. Created at the start of the run — before any files are fetched —
to enable real-time monitoring. One `FetchRun` per DataFeed execution, covering all collections in that feed.
Status: `running → completed / failed / cancelled`. Success vs partial outcome is derived from `FetchedFile` children,
not stored on the run itself.
_Avoid_: DataArrival, LoaderRun, DataFeedRun

**FetchedFile**:
Per-file acquisition record within a `FetchRun`. Created and updated incrementally as the Loader processes each file.
Status: `pending → fetching → stored / skipped / failed`. Skipped files (already exist in storage) appear as
`FetchedFile` records with `status=skipped`. Linked to `FileIngestion` via `file_path` — there is no FK.
_Avoid_: FetchResult (that is a transient in-memory dataclass, not a model)

**UploadSession**:
The record of a manual multi-file upload by an operator. Owned by `catalog` + `user`. Status:
`active → completed / failed / cancelled`. Transitions to `completed` automatically once all `UploadedFile` children
reach a terminal state (`stored` or `failed`). Not linked to a `DataFeed` — manual uploads are independent of
configured automated sources.
_Avoid_: DataArrival, upload batch, upload job

**UploadedFile**:
Per-file upload record within an `UploadSession`. Status: `pending → uploading → stored / failed`. No `skipped`
state — user-chosen files are always attempted. Linked to `FileIngestion` via `file_path` — there is no FK.
_Avoid_: UploadArrival

**FileIngestion**:
The per-file record of processing a single file from MinIO into STAC items and assets. Owns the distributed lock,
state machine, and retry logic for that file. Created directly by the MinIO consumer (bucket event) or by the
sweep task — with no reference to any acquisition record. Carries a `collections` M2M populated immediately after
collection resolution (before per-collection processing begins), so failed runs are still collection-trackable even
when no Items are created. Summary fields populated on completion: `variables_discovered` (int),
`valid_time_start` (datetime), `valid_time_end` (datetime), `timestep_count` (int), `reference_time` (datetime).
Items produced by a FileIngestion are found via `Item.source_file` (indexed, value: `"{bucket}:{file_path}"`) —
correct for all formats, including GRIB/NetCDF multi-item files.
_Avoid_: IngestionLog

### Triggers

**DataFeed**:
A configured automated data source — what to fetch, how often, and from where. Creates `FetchRun` records on a
schedule via the Loader.
_Avoid_: data source, loader config, plugin (when referring to the configured instance)

**Manual Upload**:
A file upload triggered by a human via the admin upload interface. Creates an `UploadSession` and one
`UploadedFile` per submitted file. Files never go client → MinIO directly (no presigned URLs).
_Avoid_: manual drop, manual ingest, manual ingestion

**Sweep**:
A periodic safety-net task that finds files in MinIO that have no corresponding `FileIngestion` record and
registers them for processing. Sweep is not an acquisition event — it creates `FileIngestion` records directly,
without a `FetchRun` or `UploadSession`.
_Avoid_: sweep arrival, sweep ingestion

### Manual upload setup

**Data Menu**:
The single Wagtail admin sidebar group holding every data surface, in order: "Add Data" (the Add Data Front Door),
"Catalogs", "Automated Sources", "Manual Uploads", and "Derived Products". None of these exist as separate top-level
sidebar items.
_Avoid_: data submenu, sources menu

**Add Data Front Door**:
The single entry point for defining new data, at the top of the Data Menu. One screen asking "How does this data
arrive?" with two cards — "Fetched automatically from a provider" (routes to the DataFeed setup wizard) and "I will
upload files" (routes to the Manual Upload Setup Wizard). Copy uses the data manager's language; "plugin", "feed"
and "config" never appear on it.
_Avoid_: add data wizard, data entry screen

**Manual Uploads Menu**:
The "Manual Uploads" item in the Data Menu. Links to the `ManualUploadConfig` list page, which shows all configs
across all Catalogs, each with an "Upload" button that navigates to that config's Upload Page.

**ManualUploadConfig**:
A one-time operator-created configuration that enables manual file uploads for a Catalog. Many configs can exist per
Catalog (e.g. one for surface variables, one for pressure levels). Each config defines which Collections it populates,
whether the data is a forecast (`is_forecast`), and the `valid_time_format` (a predefined choice, e.g. `YYYYMMDD`)
used to parse valid time from uploaded filenames. File format is derived from the linked Catalog's `file_format`
field — not stored separately on the config.
_Avoid_: upload profile, upload template, feed config

**Manual Upload Setup Wizard**:
A multi-step Wagtail admin flow that creates a `ManualUploadConfig`. Steps: (1) select/create Catalog, (2) name the
config (e.g. "Surface variables") — unique per Catalog, (3) upload a Sample File (required; server calls
`list_variables()`, scans each variable's data min/max, resolves units against `Unit` records, then discards the
file), set `is_forecast` and choose `valid_time_format`, and select variables, (4) define Collections and per-variable
display name, unit, and value range, (5) review and save. Provisioning is atomic and creates, alongside the config:
any new Catalog/Collections, a core `Variable` per assignment (passthrough transform, primary source =
`variable_name`, unit, value range — via `get_or_create` so existing hand-tuned Variables are never clobbered), and
any `Unit` chosen via the "Create unit" dropdown option. Parallel to the DataFeed setup wizard.
_Avoid_: upload wizard, config wizard

**Sample File**:
A representative file uploaded during the Manual Upload Setup Wizard solely to extract variable metadata via
`list_variables()` plus a per-variable data min/max scan (lazy, via `open_variable()`). Discarded immediately after
extraction — never ingested.
_Avoid_: seed file, template file

**ManualUploadConfigVariable**:
Through model linking a `ManualUploadConfig` to a `Collection` for one variable. Created by the wizard from
`list_variables()` output. Fields: `config` (FK), `collection` (FK), `variable_name`, `long_name`, `units`.
The collection FK is what routes each variable to the right Collection at upload time.
_Avoid_: variable mapping, variable link

**Upload Page**:
The admin page where an operator submits files for ingestion, one page per `ManualUploadConfig` (reached via
the list page's "Upload" button at `/admin/manual-uploads/<pk>/upload/`). Supports multi-file upload (like
Wagtail's images/multiple/add interface). Each submission creates one `UploadSession` with one `UploadedFile`
per file. After submit, the page shows per-file progress in real time.
_Avoid_: upload form, upload interface

**Upload Flow**:
The sequence for a manual upload via the admin interface: (1) operator selects one or more files and submits;
(2) server creates `UploadSession(status=active)` and one `UploadedFile(status=pending)` per file; (3) for each
file: transition to `uploading`, stream to MinIO `incoming` bucket — on failure set `status=failed`; on success
set `status=stored`; (4) when all `UploadedFile` children reach a terminal state, `UploadSession` transitions to
`completed`. The MinIO bucket event then triggers `FileIngestion` independently. Files never go client → MinIO
directly (no presigned URLs).
_Avoid_: upload pipeline, ingest flow

**Time Extraction**:
The process of determining `reference_time` and `valid_time` from an incoming file before it is processed. Attempted
in order: (1) parse the filename using universal conventions + the config's `valid_time_format`, (2) read file content
(GRIB/NetCDF only). Universal filename conventions: `GR--{reftime}--` prefix extracts `reference_time`; the last
segment of the filename before the extension is parsed as `valid_time` using the format from `ManualUploadConfig`.
When extraction succeeds, the admin upload form pre-fills the time fields. When extraction fails, the admin shows
manual entry fields; a direct MinIO drop (outside admin) raises an error and stops ingestion.
_Avoid_: time parsing, date detection

### Operator-facing monitoring surfaces

**Collection Health Panel**:
The Wagtail admin home panel showing a per-collection health summary — sparklines, OK/Warning/Failed counts, and
last-run time. A fleet-level view across all active Collections. Entry point to the Ingestion Feed via a "View all"
link. Sparkline data (30-day binary per-day status: success / failed / empty) is derived entirely from
`FileIngestion.collections` M2M — completed runs for success days, failed runs for failure days.
_Avoid_: ingestion dashboard, activity panel

**Ingestion Feed**:
A dedicated admin page showing a live, chronologically-ordered feed of `FileIngestion` records with inline
per-job step-by-step progress and summary fields (variables discovered, valid time range, timestep count). Updated
in real time via SSE. Covers files from any trigger — automated fetch, manual upload, or sweep. Reached from the
Collection Health Panel's "View all" link — there is no sidebar menu entry (ADR-0019).
_Avoid_: live feed, ingestion log, activity dashboard, Ingestion Activity Feed

Acquisition monitoring (`FetchRun` / `FetchedFile`) is per-feed: each Data Feed's dashboard carries summary stat
cards with linked fetch-run and ingestion listing pages. The former org-wide live "Acquisition Feed" page was
retired (ADR-0019); "is anything failing to fetch" is answered by the health chips on the Data Feeds list.
`UploadSession` history is visible per collection through the Collection Health Panel drill-down, and the manual
upload page streams its own in-flight progress.

### Jobs

**FileIngestionJob**:
A task-ferry job providing real-time status of a single `FileIngestion` run. One job is created per
`process_incoming_file` invocation, so retries and re-ingests produce multiple jobs pointing at the same
`FileIngestion` (FK, not one-to-one).
_Avoid_: IngestionJob

### Tenancy

**Organisation**:
An institution (NMHS or regional centre) occupying one tenant of a shared GeoRiva instance. Created only by the
instance admin (provisioned tenancy — no self-service signup). Identified by an immutable, strictly lowercase slug
that enters public URLs and storage paths. Carries contact/identity settings and default provider metadata that
prefill new catalogs; carries no branding or boundary.
_Avoid_: tenant (as a model name), workspace, institution (as a model name)

**Organisation Membership**:
The row linking a user to an Organisation with a role of Admin or Member. A membership always means the person
belongs to that institution. Users can belong to multiple Organisations. Members are onboarded by direct account
creation (an org admin or the instance admin creates the account) — no email invitations.
_Avoid_: OrganizationUser, workspace user, invite

**Instance admin**:
The Django superuser. Not a separate concept or flag. Creates Organisations and their first org admins, bypasses
membership checks entirely, and may enter any Organisation without holding a membership row.
_Avoid_: platform admin, staff (as a synonym)

**Organisations Menu**:
The single Wagtail admin sidebar group holding the Instance admin's tenancy surfaces, in order: "Organisations" (the
`Organisation` list) and "Organisation members" (the `OrganisationMembership` list). Neither exists as a separate
top-level sidebar item. Superuser-only, and it hides itself whole rather than showing entries that would turn a
member away. Distinct from the "Organisation" group in the Settings menu, which is one Organisation's own admins
editing their own institution.
_Avoid_: org menu, tenancy menu

**Active organisation**:
The Organisation a request is operating in, derived from its Host and nothing else: hostname → Wagtail Site →
Organisation. There is no session-org state — the subdomain *is* the switcher. Membership is re-read on every
request and access fails closed; an unknown hostname is a 404, never a fallback to some default organisation.
Carried on the request as `active_org` (with `active_org_role`), and all admin-plane scoping derives from it.
_Avoid_: current tenant, session org, default org

**Org-hopper**:
The workspace block at the top of the admin sidebar: the Organisation's name and host, always visible, with a
popover listing the Organisations the signed-in user may cross to. Wayfinding only — each entry is a plain
cross-host link to that Organisation's `/admin/`, and clicking one is an ordinary navigation whose Host resolves the
Active organisation as usual. It holds no state of its own; there is no "last organisation" memory to go stale. A
member is listed the Organisations they hold a membership row in, the instance admin all of them, and a user in
exactly one gets a static badge with nothing to open.
_Avoid_: org switcher, workspace switcher, tenant picker

**Central organisation**:
The Organisation bootstrapped on Wagtail's default Site at the base domain during first setup. Entirely ordinary —
same storage prefix, same membership rules, no fallback status — so that a single-institution install never has to
think about tenancy.
_Avoid_: default org, root org, main tenant

**Shared reference data**:
Instance-global records that no Organisation owns and every Organisation reads: topics, units, and administrative
boundaries (with the zonal-stats geometries beneath them). Curated by the instance admin — `Topic`'s admin is
superuser-only — and deliberately exempt from org scoping: an unscoped chooser over reference data is by design, not
a missed guard. Boundaries are the clearest case for the exemption: a regional centre clips against several
countries, and the model is a third party's, so per-org curation (if ever needed) would be a mapping table, not an
FK on someone else's model.
_Avoid_: system data, common data

**Global tier**:
The ownerless rows of a model that *is* org-owned — a nullable organisation FK where null means "the instance's,
readable by every Organisation and writable only by the instance admin". Colour palettes are the case: an
institution draws on the shipped library and adds its own beside it, and a chooser offers both tiers together.
Declared by `ORGANISATION_GLOBAL_TIER` alongside the usual `ORGANISATION_LOOKUP`, which is what makes reads widen to
include the ownerless rows while writes stay narrow. Distinct from **Shared reference data**, which no organisation
can own at all.
_Avoid_: default palette, system palette, public row

**Org page tree**:
The portal an Organisation authors, being everything under the root page of its Wagtail Site. Pages are org-owned
through that link rather than through a field, so their scoping is a tree question and not an FK filter: on an
organisation's host the explorer, the page chooser, page search and every page-id admin URL are confined to its own
root. Wagtail's per-org page-permission group is the capability half of this; the host scoping is the tenancy half,
and it is what a user who belongs to two institutions — or a superuser, who effectively belongs to all of them —
runs into.
_Avoid_: site tree (ambiguous with Wagtail Site), portal pages

**Org-owned model**:
A model whose rows belong to exactly one Organisation, reached through the FK chain rooted at `Catalog` rather than
an organisation FK of its own. Says so by declaring `ORGANISATION_LOOKUP` — the ORM path from itself to
`Organisation`. A model that declares none cannot be scoped at all: scoping it raises rather than returning every
row, so an undeclared model is a loud error instead of a quiet leak.
_Avoid_: tenant model, scoped model, owned model

**Visibility tier**:
What audience a Published `Collection` is served to, and the only thing that decides it: `public` to anyone,
`private` to authenticated members of the owning Organisation, `internal` to nobody — a derivation intermediate,
read by the engine as an input and served on no plane. `private` is *not* a smaller `public` and `internal` is not a
smaller `private`; the three are separate audiences, so widening one never publishes another by accident. Defined
once as `Collection.objects.visible_to(request)` and reached by every serving surface through it. A caller who may
not see a `private` collection is not told it exists: it is absent from listings and search, and a fetch by name is
the same 404 a misspelling gets. Holds on the machine plane too since the **Tile gateway** — the raster and the
choropleth are gated as the collection is, so the tier is a security boundary and not only a listing filter.
See ADR 0014 and ADR 0015.
_Avoid_: private as "restricted public", access level, permission level

**API key**:
A per-user credential (`grv_`-prefixed, hash-only at rest, shown once at creation) that lets a script — QGIS,
`pystac-client`, a notebook, a cron job — read what its holder can already see, without a browser session. Named,
individually revocable, with an optional expiry and a `last_used_at`. Carries **no** Organisation: it establishes
*who* is asking and nothing else, and what that reaches is then decided per request by the access choke point
against the host's Organisation. Presented as `Authorization: Bearer grv_…`, or as `?api_key=grv_…` for clients that
can only take a URL. Lives in the `accounts` app, because it belongs to a person rather than to an institution.
_Avoid_: token, service account, org key, scoped key

**Access choke point**:
`organisations/access.py` — the single module every surface goes through to reach tenant rows, admin and public
plane alike (`scoped_queryset`, `get_org_object_or_404`, `require_org_object`, `require_org_member`,
`require_org_admin`, `may_see_private`). Membership itself is `resolve_org_role`, read live from the database and
shared by the middleware and the serving planes — the latter cannot use the middleware's answer, because an API-key
request is still anonymous when the middleware runs. One implementation so there is one place to audit, and one that guard tests walking the admin
and the public API can hold the instance to. Distinct from Wagtail's model permissions, which stay the *capability*
layer: what a user may do, not whose rows they may do it to.
_Avoid_: permission manager chain, scoping middleware, tenancy filter

**Per-org API root**:
One Organisation's whole public service, served at its own host: `<org-host>/api/stac/`, `/api/edr/`,
`/api/analysis/`, `/api/datasets/`. Self-contained — conformance, search, queryables and every self/root/child link
resolve on the host they were requested from, and no root ever names or reaches another Organisation's rows. Ids
stay bare (`Catalog.slug`, `Collection.slug`) and may collide across Organisations; the Organisation lives in the
hostname, never in a path segment or query parameter. Two deliberate exceptions: `/api/jobs/` is mounted on every
host and org-agnostic, guarded by unguessable job ids rather than tenancy; and the **machine plane** below, which
has no hostname to read and so carries the Organisation in the path instead.
_Avoid_: org path prefix, namespaced STAC id, tenant-qualified id

**Machine plane**:
The services that answer without a resolvable Host — Titiler and Martin — plus the one Django endpoint they call
back on (`/api/tile-config/{org}/…`). They read object storage and the database directly, so nothing resolves an
Organisation from a hostname there and nothing tries: the Organisation travels in the address instead
(`/titiler/{org}/{catalog}/{collection}/{variable}/…`, Martin's required `org`/`catalog`/`collection` params,
`georiva:palette:{org}:…`). This is the *only* place a path names the tenant, and for the one reason that justifies
it — there is no Host to disagree with. Django decides which Organisation and writes every such URL through
`core/machine_plane/addresses.py`, from a row it already resolved from the Host; Titiler and Martin concatenate and join.
Guarded by the **Tile gateway** below, which is what makes the Visibility tier mean anything here.
See ADR 0013.
_Avoid_: tile plane, internal API, server-to-server tenancy

**Tile gateway**:
The nginx `auth_request` that decides whether a machine-plane request may be proxied at all. Before serving a
Titiler or Martin URL, nginx subrequests Django's internal `/internal/tile-auth/` endpoint with the original
request line and the caller's own credentials (session cookie, `Authorization`, `?api_key=`); Django reads the
address through `machine_plane.scope_of` — the inverse of the URL builders beside it — checks the org segment
against the Host, and answers from the same `public()` / `visible_to()` vocabulary every other serving plane uses.
Titiler and Martin gain no tenancy logic, which is the property it exists to preserve. Denials are 403 on the wire
(all `auth_request` understands) and 404 to the caller; a broken key stays 401. Cached ~60s per
collection-and-credential, so revocation lags on tiles and nowhere else. See ADR 0015.
_Avoid_: tile auth middleware, tile permission check, signed tile URL
