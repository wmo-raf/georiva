# The ingestion queue admits only fetch and extraction

## Status

accepted

Refines ADR 0005 (generic derivation engine), which introduced
`georiva-processing` to keep long derivation runs off `georiva-ingestion`.

## Context

The queue split was, until now, stated as a split by *weight*: heavy work on
`georiva-ingestion`, lightweight work on `georiva-default`, per-unit derivation
compute on `georiva-processing`. Weight turns out to be the wrong axis.

`georiva-ingestion` defaults to a single pool process, because GRIB and raster
extraction are memory-hungry and oversubscribing them puts the worker into the
OOM killer. One pool process makes the queue a strict FIFO: whatever sits on it
delays every file behind it, in order, with no way past.

Two tasks fan out once per COG asset after extraction — boundary zonal
statistics (`analysis/zonal_stats`) and virtual-Zarr manifest builds
(`virtual_zarr`) — and both were routed to `georiva-ingestion` on the "heavy
work" reading. They are individually small (~1.3–1.9s), which is exactly why
the routing looked harmless.

It is not harmless at fan-out. The first fetch of an ECMWF IFS feed
(2 collections × 81 variables × 16 steps) enqueued roughly 1,300 of them. Files
staged *after* that fan-out — a second run of the same feed, or an unrelated
AIFS fetch — waited 30–40 minutes for their `process_staging_file` to start
(wmo-raf/georiva-source-ecmwf#9, #398). Nothing was stalled and nothing failed;
new COGs and items simply became available half an hour late, behind
bookkeeping that no reader was waiting on. Every additional many-variable feed
makes that worse for every other feed on the instance.

The distinction that matters is not how heavy a task is but **whether data
availability waits on it**. Fetching a file and extracting it into COGs and
STAC items is the critical path: until they run, the data does not exist to a
user. Zonal statistics and virtual-Zarr manifests are derived from an item that
is already published and already servable; both are re-dispatched by a periodic
sweep if dropped, which is what makes them deferrable by construction.

## Decision

**`georiva-ingestion` carries fetch and extraction, and nothing else.** Four
tasks: `process_incoming_file`, `process_staging_file`, `run_data_feed_loader`,
`retry_fetched_file`. The queue's admission rule is a closed set, asserted in
`ingestion/tests/test_queue_routing.py`, so adding a fifth is a deliberate edit
rather than a default.

**Deferrable derived work goes to `georiva-processing`.** This widens ADR
0005's queue from "per-unit derivation compute" to *deferrable derived work* in
general — per-unit compute and per-asset bookkeeping alike. The two are the
same kind of thing from the queue's point of view: derived from published data,
awaited by nobody, recoverable by sweep. They share a worker (concurrency 4 by
default) and can therefore delay each other; that is an acceptable trade,
because neither delays data availability.

**A task's queue is declared once, on the task, and callers do not override
it.** `apply_async(queue=...)` silently wins over the declaration, which is how
these two tasks stayed on the ingestion queue: all four dispatch sites named
the queue explicitly. Dispatch is now `delay(...)`, and a test walks the
package's AST to keep it that way. Routing that a caller can contradict is not
routing.

**Virtual-Zarr builds are claimed at dispatch, not at start.** Moving them to a
four-slot worker exposed a latent duplicate-dispatch bug: the build marked its
manifest BUILDING only when it began running, so a manifest queued longer than
the sweep's 5-minute cadence was still PENDING when the next sweep looked, and
was dispatched again — once per sweep for the length of the backlog. On one
pool process the copies merely ran in series, wasting a rebuild each; on four
they overlap, and two writers on one Icechunk repo raise `ConflictError` and
flip the manifest to FAILED. `VirtualZarrManifest.claim_for_build` now takes
the lock in the same conditional UPDATE that decides whether to dispatch, and
every dispatch goes through `tasks.dispatch_build`.

## Alternatives considered

**Route the follow-ups to `georiva-default`** — rejected. It is the simplest
edit and the worst outcome: `georiva-default` hosts `sweep_unprocessed` and
`sweep_staging`, the safety nets that recover files the event consumer missed.
Putting 1,300 bookkeeping tasks in front of the ingestion recovery sweeps
degrades the mechanism that exists to catch ingestion failures.

**A dedicated `georiva-bookkeeping` queue** — rejected as a distinction without
a difference. It reads well, but the isolation would be nominal unless it also
got its own worker, and a sixteenth container is real operational cost for
deployments that are already 15 services on modest national-met hardware.
Sharing a worker with derivation compute gives the same scheduling behaviour
under a name that already exists. If bookkeeping latency ever becomes a
complaint in its own right, splitting it out is an ops change (`-Q`) plus a
routing constant, not a redesign.

**Celery task priorities on the existing queue** — rejected. Redis priority
support is coarse (priority is per-message across a small number of sub-queues)
and does not give real preemption, so a long fan-out still occupies the pool.
It would also leave the ingestion queue's contents unbounded in kind, which is
the property that caused the problem.

**Raise `GEORIVA_CELERY_INGESTION_WORKER_CONCURRENCY`** — rejected as a fix,
though it remains available as a tuning knob. More slots make the queue drain
faster without changing what is on it; extraction is memory-bound, so the
headroom is small and it trades an availability delay for OOM risk.

## Consequences

- `compute_boundary_zonal_stats` and `build_virtual_zarr_manifest` declare
  `queue="georiva-processing"`; their four dispatch sites now use `delay`.
- The `georiva-processing` worker's load becomes bursty in a new way: a large
  fetch now lands ~1,300 short tasks alongside whatever derivation is running.
  `GEORIVA_CELERY_PROCESSING_WORKER_CONCURRENCY` is the knob if that matters.
- Zonal statistics are safe to run four-up because `persist_stats` upserts on
  its unique constraint; that idempotency is now load-bearing, not incidental.
- `VirtualZarrManifest.BUILDABLE_STATUSES` and `claim_for_build` are the
  supported way to take a build lock; `get_buildable`'s dead `exclude()` on
  BUILDING (unreachable — BUILDING was never in its status filter) is gone.
- A manifest claimed but never built stays BUILDING until `reset_stale_locks`
  recovers it after `LOCK_TIMEOUT` (30 minutes). The queued window now counts
  against that clock, so a backlog deeper than 30 minutes can still produce a
  duplicate — far less likely now that builds no longer queue behind ingestion,
  and the build re-stamps the lock when it actually starts.
- No new queue, no new worker, no new container.
