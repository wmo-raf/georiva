# Item is a plain table; only zonal stats is a hypertable

## Status

accepted — see wmo-raf/georiva#703. Supersedes in part
[ADR-0004](0004-staging-tier-and-abstract-stac-models.md): its "`Item` keeps `TimescaleModel` (hypertable, one row per
timestep)". An Item is still one row per timestep; it is no longer a hypertable.

## Context

`Item` was a TimescaleDB hypertable, `time` (the valid time) its partition key, in 1-day chunks. On the dev stack
that was ~7k rows across 441 chunks reaching back to 1991, each chunk with Item's 11 indexes — 82 MB for what is
4.4 MB as a plain table.

It earned nothing:

- No Timescale feature touched Item: no compression, no retention by `drop_chunks`, no `time_bucket`, no continuous
  aggregates.
- Item's reads name a **collection**, not a time window. A query that names no time is planned across every chunk:
  0.7–6 s of *planning* per Item query, on a warm connection as on a new one, against tens of milliseconds of
  execution. The public dataset collection page spent ~4 s of its 3.8–4.6 s planning four Item queries; the WMTS
  capabilities document paid the same (#682).
- A hypertable's unique keys must include the partition column, so **Item had no primary key**: `id` was only
  `NOT NULL`. Nothing could reference it with a foreign key, and every key pointing at Item carried
  `db_constraint=False`. Orphans accumulated behind them — 774 `Asset` rows and 36,933 `BoundaryZonalStats` rows on
  dev, and still growing.

Before the switch, a copy of the dev database converted to this schema was measured against the original
(wmo-raf/georiva#703, 2026-11-21): every Item query planned in under 7 ms warm; the dataset pages went from 3–4 s to
0.06–0.15 s end to end, the credentialed WMTS capabilities from ~1 s to 0.33 s, `/api/runs/…?valid_times=1` from
0.8 s to 0.02 s. One query got slower: the dataset page's paged item list (+14 ms, from Timescale's ordered chunk
scan stopping early), tracked in #764.

## Decision

1. **`Item` is a plain table** with `PRIMARY KEY (id)`. `time` is an ordinary `DateTimeField`; Item does not inherit
   `TimescaleModel`.

2. **The foreign keys to Item are real constraints:** `Asset.item`, `DerivationLink.derived_item`,
   `DerivationLink.source_published_item`, `DerivationRun.produced_item` and `BoundaryZonalStats.item`. So is
   `StagingAsset.item`, to `StagingItem`, which was never a hypertable and had no reason to be unconstrained. Like
   all of Django's, they are `DEFERRABLE INITIALLY DEFERRED`: checked at commit.

3. **`BoundaryZonalStats` stays a hypertable, and its key to Item is constrained too.** A hypertable may reference a
   plain table; what it could not be was the referenced side. The zonal-stats rows were where most orphans came
   from. (#703 first left this key unconstrained, on the reading that a key *from* a hypertable could not be one; that
   was checked against TimescaleDB 2.32 and is not so.)

4. **The zonal-stats task tolerates a deleted Item.** Retention or a re-ingest can remove an Item between the task's
   dispatch and its write. The task writes every level in one transaction; an `IntegrityError` at commit with the
   Item gone is "nothing left to describe": logged at info, no rows, no retry. With the Item still there it is
   raised. Ingestion, harvest and derivation writers are left to fail loudly: there the Item was created moments
   earlier in the same flow, and its vanishing is a bug.

5. **Unchanged on purpose:** `ProductPublished.produced_item_id` and `DerivationAttemptFinished.produced_item_id`
   stay plain id columns — an event outlives its Item. `harvested_by` stays unconstrained (ADR-0042).

6. **Item's indexes are trimmed:** `(collection, -time)` (a backward scan of `(collection, time)` serves it), the
   `collection` key's own index (`(collection, time)` leads with it), and the index on `reference_time` (no run-list
   query regressed without it).

7. **History is rewritten, not migrated.** No instance is in production. The existing migrations are edited in
   place, under their own names, so plugin migrations depending on them stay valid.

## Consequences

- **Every existing database must be recreated.** `migrate` sees the edited migrations as applied and would silently
  keep the hypertable, the missing key and the unconstrained columns — the app runs on it, slow and still gathering
  orphans. Recreate the database *and empty the buckets*: a fresh database leaves the old objects in `assets`,
  `sources` and `archive` with no rows naming them. A harvested store loses nothing; its data is upstream. No
  conversion command is provided, and nothing checks at startup.
- Whatever produces orphaned Assets now raises in ingestion instead of leaving one behind.
- Deleting an Item still goes through Django's cascade; a raw `DELETE` of a referenced Item is refused.
- `core/tests/test_item_plain_table.py` fails CI if Item becomes a hypertable again or loses its primary key, or if
  any of the six constraints goes.

## When to reconsider

When Item is in the **tens of millions of rows and read by time window** — queries that name a time range across
collections, where chunk exclusion would pay for the planning. Neither holds now: Item is read by collection.

## Considered Options

- **Keep the hypertable, with larger chunks.** Fewer chunks plan faster, but still without a primary key, so still
  without constraints and still gathering orphans — and for no feature in use.
- **Keep the hypertable, add `time` to every referencing table and key on `(id, time)`.** Real constraints, at the
  cost of a denormalized `time` on every child, kept in step by hand, for a partitioning nothing uses.
- **Convert existing databases in place** (copy into a plain table, add the key, add the constraints `NOT VALID`,
  clean orphans, validate). Done by hand for the benchmark; not worth supporting with no production instance.
