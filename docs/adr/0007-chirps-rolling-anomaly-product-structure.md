# CHIRPS rolling anomaly product structure

## Status

accepted

## Context

CHIRPS rainfall is a primary drought-monitoring input for African NMS, and the
headline product they need is the **rainfall anomaly** — how far a period
departs from its long-term normal for the *same calendar slot* (June 2024 vs the
mean of all Junes; dekad-18-2024 vs the mean of all dekad-18s). The
`georiva-source-chirps` plugin today only *fetches* CHIRPS GeoTIFFs (monthly,
dekadal, pentadal); it produces no normals and no anomalies.

The generic `ClimatologyRecipe` (ADR-0005) does not fit. It groups by
meteorological **season** (`annual/DJF/MAM/JJA/SON`) read from a file's time
axis and produces a single **fixed-period** climatology/anomaly per 20–30-year
window. The CHIRPS product is the opposite shape on two axes:

1. **Per-calendar-slot, not per-season.** The grouping is month-of-year (12),
   dekad-of-year (36), or pentad-of-year (72) — `geoprocessing` has no such
   grouping primitive.
2. **Rolling/event-driven, not fixed-period backfill.** Every newly ingested
   slice yields one new anomaly against a *reused* baseline normal.

Additionally, CHIRPS slices are **single-timestep GeoTIFFs** carrying no internal
time axis (the date lives only in the filename → `StagingItem.datetime`), so the
generic `.nc`-based, file-axis-authoritative series reader does not apply.

This is the same situation ADR-0006 faced for the Atlas: a source whose product
space doesn't fit the generic recipe.

## Decision

Build **two new plugin-owned recipes in `georiva-source-chirps`** —
`chirps-climatology` and `chirps-anomaly` — registered on the generic engine via
`RecipeRegistry`. The engine, `geoprocessing`, and `PromotionRecipe` are **not
modified**. The generic `ClimatologyRecipe` is left intact for other uses.

**Two stages, because the baseline normal is reused.** The slot's baseline mean
is identical across every year's anomaly for that slot, so it is precomputed
once and reused, rather than re-reducing the multi-decade series on every
arriving slice (most acute at pentadal — 72 slots).

- **`chirps-climatology` (scheduled/manual).** Production Unit
  `(source_collection, baseline, slot)`. `candidate_units` returns `[]` for a
  bare trigger; invoked via `run_recipe` with a selector. `resolve_inputs`
  selects staging slices whose `StagingItem.datetime` is in the baseline years
  **and** matches the slot; `transform` is a NaN-aware mean
  (`temporal_aggregate(series, freq=None)` — no new primitive). A **minimum-count
  guard (default 20)** skips a thin slot rather than publishing a weak normal;
  the contributing count is written to `Item.properties`.
- **`chirps-anomaly` (event-driven).** Production Unit
  `(source_collection, baseline, valid_time, quantity)` with
  `quantity ∈ {anomaly, relative_anomaly}`. `candidate_units` answers
  `staging_item_id` triggers only and returns `[]` for `published_item_id`
  triggers (mirroring `PromotionRecipe`). It resolves the arriving raw slice
  (`value`, Staging tier) and the matching published climatology slot
  (`baseline`, Published tier) and computes `anomaly(value, climatology,
  relative=…)`. The engine's `readiness` cleanly skips a unit whose normal isn't
  built yet.

**Fixed baseline as a shared plugin constant.** `CHIRPS_BASELINE = (1991, 2020)`
(the WMO standard normal), surfaced via plugin settings, is the single source of
truth for both stages, so they can never join a slug that was never built.

Dimension placement (ADR-0006 encoding pattern — one unit → one Item, sub-period
in `Item.time`):

| Dimension | Lives at | Mechanism |
|---|---|---|
| resolution (monthly/dekadal/pentadal) | **Collection** | via the source staging collection → slug prefix `chirps-{res}-…` |
| baseline | **Collection** | slug suffix `…-1991-2020` |
| quantity (normal / anomaly / relative anomaly) | **Collection** | distinct collections: `chirps-{res}-climatology-…`, `chirps-{res}-anomaly-…`, `chirps-{res}-relative-anomaly-…` |
| calendar slot (normal) | **Item** | `Item.time = datetime(1991, month, slot_start_day)` (sentinel year = baseline start; reversible `(month, day)` encoding) |
| valid time (anomaly) | **Item** | the slice's real `valid_time` |
| variable (`precip`) | **Variable / Asset** | one Variable, one COG per Item |

**Both quantities from the start:** absolute (`value − climatology`) and relative
(`(value − climatology) / climatology` via `safe_divide`, which maps
divide-by-zero to NaN — correct for arid/dry-season slots).

**Slot definitions are shared.** A plugin `periods.py` owns slot-of-year,
`slot_start_day`, and the sentinel encode/decode; the existing `_dekad_num` /
`_pentad_num` methods are refactored off `CHIRPSDataSource` into it so the fetch
path and the derivation path share one definition.

## Prerequisite: CHIRPS on the staging rails

The engine auto-fires only from `register_staging_file` (staging arrivals) and
derivation-completion chaining — the legacy published-ingestion path never pokes
it. So CHIRPS `DataFeed.target_tier` must be flipped to **`staging`**. Each slice
then lands as one `StagingItem`, firing the engine once, which drives both
`PromotionRecipe` (keeping the raw layers **served**, as a passthrough copy) and
`chirps-anomaly`. This is a config/data change (the field already exists), not
code, but it is a hard prerequisite for the event-driven product.

## Considered Options

- **Reuse the generic `ClimatologyRecipe`** — rejected: it groups by season, not
  calendar slot, and is fixed-period not rolling.
- **Single on-the-fly anomaly recipe (no precomputed normal)** — rejected:
  re-reduces the whole multi-decade series on every slice; wasteful, and yields
  no queryable "normal" product.
- **Data-driven multi-baseline discovery** — deferred: a single fixed baseline
  keeps config off the per-slice event path; multiple normals can be added
  later without breaking the unit shape.
- **Auto-fire anomalies when a normal is built** — rejected for v1: adds
  cross-recipe trigger fan-out; the manual backfill covers the bootstrap case.
- **Keep CHIRPS published and read derivation inputs from published items** —
  rejected: the legacy published path sends no engine trigger, so anomalies
  would be manual-only (or require a core change to the ingestion trigger).
- **Two feeds (one published, one staging)** — rejected: doubles fetch and
  storage and can drift.

## Consequences

- **Bootstrap ordering matters.** Ingesting history fires anomaly units that are
  all *skipped* (no normal yet), and they do not auto-recompute (the staleness
  sweep only re-runs units with an existing run record). The documented runbook
  is **build climatology → run the `chirps-anomaly` backfill selector →
  steady-state events**.
- **`Item.time` for normals is an encoding,** not a valid time — it carries the
  slot via a sentinel year; the true slot/baseline live in `Item.properties`.
- **Slot selection trusts `StagingItem.datetime`,** a deliberate departure from
  the generic recipe's "re-read time from file content" (CHIRPS GeoTIFFs have no
  internal time axis). This is code-commented so it isn't "fixed" later.
- **Raw served layers become passthrough `.tif`** (promotion does not re-encode
  to COG). Optimizing them to COG is an orthogonal follow-up that would touch
  core.
- **Palettes are manual.** The recipe sets `value_min/max` + `scale_type`
  (symmetric mm for absolute, `[-1, 1]` relative, source range for the normal);
  a diverging palette is attached per anomaly collection once in Wagtail.
- **Install-everywhere.** The plugin must be installed in both the web process
  and the `georiva-processing` worker, and its `AppConfig.ready()` must import
  the recipe modules, or units drop with "Unknown recipe."
- **Terminology.** CHIRPS keeps the domain-standard "anomaly" / "relative
  anomaly" naming; the CONTEXT.md "use *change*" guidance is scoped to the CMIP6
  Atlas product language, and the engine quantity codes line up exactly.
- **Steady-state consistency** is preserved: if a baseline slice is later
  re-staged, the sweep recomputes the affected normal and `invalidate_downstream`
  recomputes its anomalies.
