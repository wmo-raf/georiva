# Published asset objects follow their rows out on commit

## Status

accepted

## Context

Deleting an `Item` cascades its `Asset` rows through the ORM (`Asset.item` is a
`ParentalKey` with `on_delete=CASCADE`, `db_constraint=False` because `Item` is
a hypertable), but nothing deleted the objects those rows named. Every COG in
the assets bucket outlived its row until someone ran
`cleanup_asset_orphans --apply` by hand, with a scope. The hourly
`prune_forecast_items` deletes past forecast items continuously, so for forecast
collections storage grew without bound while the catalog stayed small.

`Collection.item_count` had the same problem in the database: ingestion counted
items in and nothing counted them out.

Two facts shape the fix. Asset keys are **deterministic**
(`{org}/{catalog}/{collection}/{variable}/{y}/{m}/{d}/…`), so re-ingesting a
timestamp writes the same key again, object first and row second. And deletes
arrive in bulk: a feed or collection delete cascades to every asset in one
transaction.

## Decision

1. **The platform owns the objects published `Asset` rows name, and deletes
   them when the row goes.** Only the assets bucket. Raw inputs on the staging
   tier are evidence of what was received and keep their files (see gaps).

2. **Only after the delete commits.** An `Asset` `post_delete` receiver
   (`core/storage/asset_deletion.py`) buffers the href on the connection for
   the current transaction. The first href registers one `on_commit` flush. A
   rollback drops the flush, and a buffer whose flush is no longer pending is
   replaced rather than reused. Every ORM path is covered: single, queryset and
   cascading deletes all send `post_delete`.

3. **In chunks.** The flush stamps the commit time and dispatches
   `core.tasks.delete_asset_objects` per 500 hrefs on `georiva-default` (ADR
   0025: nothing a reader observes waits on freeing storage).

4. **Guarded at run time** by `asset_cleanup.deletion_verdict`. A key that a
   live `Asset.href` names again is spared. So is an object modified at or
   after `commit time − 60s`: a re-ingest may already have written new bytes
   under the key before its row exists, and the margin absorbs clock skew
   between the app and the object store. A missing object counts as done.

5. **Failure is tolerated per key and retried per chunk.** One bad key is
   logged and counted, and the rest proceed. A chunk where every key failed (or
   the live-href lookup raised) is treated as storage being unreachable and
   retried up to 3 times with exponential backoff. After that the task logs an
   error that points at the sweep. Each run returns
   `{deleted, skipped_live, skipped_recent, missing, failed}`.

6. **`cleanup_asset_orphans` stays the manual backstop and shares the guard.**
   It reads the live hrefs and takes its cutoff *before* listing, and spares
   objects modified within the margin. Previously, an object written between
   the two looked orphaned. After deploying, run it once with `--all --apply`
   to clear the backlog from before this change.

7. **Deleting an item counts it out.** An `Item` `post_delete` receiver
   decrements `item_count` by `collection_id`, clamped at zero. `recount_items`
   repairs counts that drifted before this change. Run it once after deploying.

## Consequences

- Forecast pruning, admin deletes, feed and collection deletes now free storage
  shortly after they commit.
- A cascade sends one small `UPDATE` per item (the count) and two storage calls
  per key, in the task rather than in the request. The collector was already
  loading every asset for the virtual-Zarr receiver, so it loads nothing new.
- Raw SQL against `Item`/`Asset` bypasses both receivers. Nothing does that
  today. Anything that starts to must free storage itself or leave it to the
  sweep.

## Known gaps

- **Staging raw inputs keep their files.** Deleting a `StagingItem` cascades
  its `StagingAsset` rows; the objects in `sources`/`incoming` stay. Removing
  them is a retention/archival decision, not a side effect of removing a row.
- **Derived items are not touched when an input is deleted.** The input's
  `DerivationLink` rows cascade away and the derived item keeps serving what
  it was computed from. Tracked in wmo-raf/georiva#495.
- **Proxy-cached renders outlive the data.** Deleting an Item removes it from
  every catalog surface at once, and from storage on commit. Tiles and previews
  already cached in nginx's `tile_content` zone, under URLs already issued, can
  keep being served for up to 30 days of inactivity. Open-source nginx has no
  purge. Retracting published data needs its own feature.

## Alternatives considered

- **ETag compare** captured at write or delete time. It closes the same window,
  but `Asset.checksum` is a provenance hash rather than one of the stored bytes
  and is blank for ingested assets. It would need a new field stamped by every
  writer plus a backfill, or a synchronous HEAD per asset inside the delete
  request.
- **A scheduled orphan sweep instead.** Simpler, but storage leaks until the
  sweep runs, and listing a whole bucket on a schedule is the expensive,
  race-prone operation the command's `--all` guard makes deliberate.
- **Explicit calls from each delete path.** Every future path (the snippet
  delete view, retention jobs) would have to remember, and cascades are where
  people forget.
- **One task per asset.** A collection delete would put tens of thousands of
  tasks on the default queue.
