# Runtime Flow — from schedule to served layer

How a file actually moves through GeoRiva at runtime: what triggers a fetch, what
fetches it, where it lands, and what turns it into a served layer or a derived
product.

The single most important structural fact:

> **Acquisition and ingestion are fully decoupled.** `Loader` never calls
> ingestion. The only thing joining the two halves is an object landing in a
> MinIO bucket.

That seam is why a human dropping a GRIB into the `incoming` bucket by hand
enters exactly the same pipeline as a scheduled plugin fetch.

Related reading: [writing-a-source-plugin.md](../plugins/writing-a-source-plugin.md),
[download-dedup.md](download-dedup.md), ADR-0003 (acquisition model),
ADR-0004 (staging tier), ADR-0008 (configurable derivation products).

---

## Overview

```
     ┌─ celery beat ─┐        ┌─ operator ─┐
     │  PeriodicTask │        │  "Run now" │
     └───────┬───────┘        └──────┬─────┘
             │                       │
             └──────────┬────────────┘
                        ▼
              run_data_feed_loader                    PHASE 1  scheduling
                        │
                        ▼
                   Loader.run()                       PHASE 2  acquisition
              (plugin code runs here)
                        │
                   bucket.save()
                        │
        ╔═══════════════▼═══════════════╗
        ║   MinIO notification → Redis  ║             PHASE 3  handoff
        ╚═══════════════╤═══════════════╝
                        │
          ┌─────────────┴──────────────┐
          ▼                            ▼
   SOURCES / INCOMING              STAGING                PHASE 4
   process_incoming_file      process_staging_file
          │                            │
   IngestionService            StagingItem (store-only)
   COG / PNG / assets                  │
   STAC Item + Assets          dispatch_for_input
          │                            │
          ▼                            ▼
     served layer              derivation recipe → outputs
```

---

## Phase 1 — Scheduling

There is **one `PeriodicTask` per `DataFeed`**, not per collection
(`sources/tasks.py:123`). It is created or updated on every `DataFeed.save()`
and on every collection-link save/delete, and it fires at the **shortest**
effective interval across the feed and all of its links, so no collection is
starved (`tasks.py:140-156`).

```
celery beat
  └─> run_data_feed_loader(data_feed_id)          queue: georiva-ingestion
        ├─ bail if not feed.is_active
        └─ for each collection_link:
             ├─ skip unless link.is_due()          ← per-collection cadence gate
             └─ data_feed.get_loader(collection).run()
```

Two design points:

- **Feed-level task, link-level gate.** The task fires often (shortest cadence);
  each `link.is_due()` decides whether that collection actually runs.
- **Links run sequentially inside one task**, deliberately — it keeps
  cross-collection dedup in `Loader._find_existing_catalog_path()` working
  (`tasks.py:92`).

Other entry points into the same machinery:

| Trigger | Path |
|---|---|
| Operator "Run now" | `DataFeed.run_now()` → task_ferry `LoaderJob` with live progress (`models.py:284`) |
| Management command / tests | `DataFeed.run_now(async_run=False)` → `Loader.run()` inline |
| Operator "Check for new files" | `DataFeed.check_new_files()` → `Loader.check_new_files()` — read-only, persists nothing (`models.py:263`) |
| Per-file retry | `retry_fetched_file` task → `Loader.fetch_one()` |
| Scheduled derived products | `sweep_scheduled_products`, a 5-min beat on `georiva-default` gated by each product's `is_due()` (`tasks.py:17`) |

---

## Phase 2 — Acquisition (`Loader.run()`)

```
Loader.run()                                        loader.py:226
  ├─ FetchRun.objects.create(status='running')
  ├─ fetch_strategy.connect()
  ├─ requests = data_source.generate_requests_for_collection(collection)
  │                                                 ← time-window logic, plugin code
  ├─ triage each request:
  │    ├─ already in this collection's tier bucket? → skip, FetchedFile.mark_skipped()
  │    ├─ exists under a sibling collection?        → bucket.copy(), no download
  │    └─ otherwise                                 → queue for fetch
  │
  ├─ for each queued request:  _fetch_and_store()
  │    ├─ fetch_strategy.fetch(request, temp_path)
  │    ├─ _validate_file()                          ← generic: exists, ≥1000 bytes
  │    ├─ data_source.post_process_fetched_file()   ← plugin hook
  │    └─ bucket.save(storage_path)                 ← THE HANDOFF
  │
  └─ finally: disconnect, rmtree temp, FetchRun.mark_completed(),
              data_feed._update_run_stats(result, collection)
```

`FetchRun` / `FetchedFile` are the acquisition audit trail (ADR-0003). Each
`FetchedFile` stores the serialized `FileRequest` as `request_payload`, which is
what makes single-file re-fetch possible later.

**Tier routing happens here, and it is computed rather than stored**
(`loader.py:151`). A collection lands in the **STAGING** bucket iff some enabled
`DerivedProduct` of this feed consumes it at the staging tier; otherwise it lands
in **SOURCES**. See `collection_routes_to_staging()`. This is the fork that
decides everything downstream — and because it is derived from the product
declarations, "publish vs. products" cannot drift out of sync.

Storage path is `{catalog.slug}/{collection.slug}/{filename}`, with
`build_filename()` prefixing `GR--<reference_time>--` when the request carries a
`reference_time` (`loader.py:551`). This is flat per collection; the
time-partitioned layout is the *asset* path written later by ingestion.

---

## Phase 3 — The handoff

The object write raises a MinIO bucket notification, which is pushed onto a Redis
list. Two independent consumers watch two separate lists:

| Bucket | Redis key | Consumer | Task dispatched |
|---|---|---|---|
| `incoming`, `sources` | `georiva:minio:events` | `minio-consumer` | `process_incoming_file` |
| `staging` | `georiva:minio:staging-events` | `staging-consumer` | `process_staging_file` |

Both are `blpop` loops run as management commands (`ingestion/consumer.py:157`,
`ingestion/staging_consumer.py:222`). There is **no HTTP webhook endpoint** — the
transport is Redis, not MQTT and not HTTP.

`incoming` is also the manual drop-zone, so operator uploads and plugin fetches
converge here.

---

## Phase 4a — Published path (SOURCES → served layers)

```
process_incoming_file(file_path, origin_bucket)     queue: georiva-ingestion
  └─ FileIngestionJob  →  JobHandler.run(job)       (in-place, not re-enqueued)
       ├─ acquire the FileIngestion distributed lock
       ├─ IngestionService.process_file()
       │    extract → clip → encode → write COG / PNG / JSON assets
       │    → create STAC Item + Assets
       └─ mark FileIngestion completed or failed
```

The job runs synchronously inside the worker that received the task — it is not
re-enqueued — so the task_ferry state machine and Redis progress work without
spawning a second task (`ingestion/tasks.py:64-73`).

---

## Phase 4b — Staging path (STAGING → derivation)

```
process_staging_file(bucket, key)                   queue: georiva-ingestion
  ├─ register_staging_file()  → one StagingItem     (store-only; materializes nothing)
  └─ dispatch_for_input(staging_item_trigger(item))            ADR-0008
       └─ route the arriving input to the enabled DerivedProducts
          that declare it → recipe runs → outputs published as served collections
```

Note the consequence: a raw collection consumed by a derived product routes to
staging and is therefore **not** served automatically. Publishing it requires an
explicit product — CHIRPS declares a `promotion` product that copies each staged
slice 1:1 to its served collection for exactly this reason.

---

## Phase 5 — Recovery

Ingestion tasks use `max_retries=0` by convention. Recovery is by periodic sweep
rather than Celery retry:

| Task | Queue | Role |
|---|---|---|
| `sweep_unprocessed` | `georiva-default` | Re-scan for files with no completed ingestion |
| `sweep_staging` | `georiva-default` | Same, for the staging tier |
| `cleanup_archives` | `georiva-default` | Age out archived files |
| `prune_ingestion_logs` | `georiva-default` | Trim ingestion history |

This also explains why the loader's time window is derived from **ingested items**
(`Collection.get_latest_item_date()`) rather than from fetched files: a file that
downloads but fails ingestion leaves the window un-advanced, so the next loader
run re-offers it. Failure is self-healing at both layers.

---

## The `Loader`, specifically

`Loader` (`sources/loader.py:116`) is core's acquisition orchestrator — one
instance per `(DataFeed, Collection)` pair, built fresh for each run and thrown
away. Plugins never construct or subclass it; `DataFeed.get_loader(collection)`
does that (`sources/models.py:212`).

It holds the three collaborators and owns the loop between them:

```python
Loader(
    data_source = feed.get_data_source(collection),  # WHAT to fetch  (plugin)
    collection  = collection,                        # WHERE it belongs
    data_feed   = feed,                              # audit trail + tier routing
)
self.fetch_strategy = self.data_source.fetch_strategy()   # HOW to fetch
```

It instantiates the fetch strategy itself (`loader.py:138`) — which is why a
`BaseDataSource` assigns the strategy **class**, not an instance.

What the Loader owns, and a plugin therefore does not:

- **Temp directory** — `mkdtemp` under `GEORIVA_TEMP_DIR`, `rmtree`'d in the
  `finally` (`loader.py:580-593`).
- **Storage paths** and the `GR--` reference-time filename prefix.
- **Tier routing** — the `_tier_bucket` property.
- **Dedup** — `_already_exists` (same collection) and
  `_find_existing_catalog_path` (sibling collections, satisfied with a
  server-side `bucket.copy()`).
- **Generic validation** — file exists and is ≥1000 bytes; an `expected_size`
  mismatch only warns (`loader.py:526`). Format-specific validation belongs in
  the plugin's `post_process_fetched_file`.
- **Persistence** — `FetchRun`, `FetchedFile`, and the feed's run statistics.

Entry points:

| Method | Behaviour |
|---|---|
| `run(dry_run=, max_files=, skip_existing=)` | Full run — triage, fetch, store, record |
| `check_new_files()` | Read-only; returns `CandidateFile(filename, storage_path, exists)`. Persists nothing, never connects the fetch strategy |
| `fetch_one(request)` | Single file, unconditional (no skip-existing). Backs per-file retry |

It is also a context manager (`__enter__`/`__exit__` connect/disconnect plus temp
cleanup), though `run()` manages that itself.

`run()` stops at `bucket.save()` and returns a `LoaderRunResult` (counts, errors,
`stored_paths`, and a `status` of `success` / `partial` / `failed` / `empty`).

> **Gotcha for plugin authors:** after `post_process_fetched_file` returns, the
> Loader mutates the request — `request.filename = new_filename or request.filename`
> (`loader.py:496`) — *before* computing the storage path. The filename you return
> is the one that lands in the bucket and the one the format plugin will parse.

---

## Where plugin code runs

Out of this entire diagram, a source plugin executes in exactly two places, both
inside Phase 2:

1. `generate_requests_for_collection()` — deciding what to fetch.
2. `post_process_fetched_file()` — massaging each file before it is stored.

Everything after the bucket write belongs to core. If a plugin finds itself
wanting to create STAC Items or dispatch tasks directly, it is on the wrong side
of the seam — the answer is almost always a derived product instead.
