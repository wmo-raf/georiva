# Bookkeeping has its own queue and worker

## Status

accepted

Refines ADR 0025, which put zonal statistics and virtual-Zarr builds on `georiva-processing` and named this split as
the way out if they ever got in the way.

## Context

ADR 0025 let derived bookkeeping share `georiva-processing` with derivation units, on one premise: neither delays data
availability, so letting them delay each other is acceptable.

That premise stopped holding. A publisher plugin's publish (`georiva-publisher-forti`) is dispatched to the same
queue, and a publish *is* something a reader waits on: the Forti app reads what it writes. Derivation units produce
data. Both now sit in one FIFO with per-asset zonal statistics.

It showed on a real instance. A forecast landed, its run closed, and the publish was claimed and queued 17 ms later.
It did not run for a long time: `georiva-processing` held about 2,000 messages, every one sampled a
`compute_boundary_zonal_stats`, and the pool completes roughly eleven a minute. A 15-minute grace period plus the
sweep's 30-minute claim per asset do not keep a backlog that deep from outliving the claim and being queued again.
Nothing failed. The publish just waited behind bookkeeping.

ADR 0025 rejected a dedicated bookkeeping queue because without its own worker the isolation would be nominal, and a
sixteenth container was a real cost. It also said splitting later would be "an ops change (`-Q`) plus a routing
constant, not a redesign". This is that change, with the worker.

## Decision

**`georiva-bookkeeping` carries `compute_boundary_zonal_stats` and `build_virtual_zarr_manifest`, and nothing else.**
The set is closed and asserted in `ingestion/tests/test_queue_routing.py`, the way ADR 0025 closed `georiva-ingestion`.

**It has its own worker service**, `georiva-celery-bookkeeping-worker`, consuming only that queue, with
`GEORIVA_CELERY_BOOKKEEPING_WORKER_CONCURRENCY` (4 in production, 2 in dev, as `georiva-processing`). A queue name
alone would be isolation in name only.

**`georiva-processing` keeps work something waits on**: derivation units and publishes. A plugin's publish is free to
use it without being queued behind a fan-out.

Everything else in ADR 0025 stands: the queue is declared once on the task, dispatch is `delay(...)`, zonal statistics
and manifests are idempotent and safe four-up, and their recovery sweeps are unchanged.

## Alternatives considered

**Celery priorities on `georiva-processing`** — rejected for ADR 0025's reason: coarse on Redis, no preemption.

**Move the publishes to `georiva-default`** — rejected: it hosts the recovery sweeps, and a plugin's work should not
decide where core's safety nets queue.

**A second worker process inside the processing container** — rejected: it keeps 15 containers but couples the two
workers' memory and restarts, and no other queue is run that way.

**Fix only the sweep** — rejected as the whole answer. `sweep_missing_boundary_stats` already claims each asset once
per 30 minutes, and the first fan-out of a run is not the sweep's doing. Isolation fixes the cause; the sweep stays as it is.

## Consequences

- One more service: 16 in total, in `docker-compose.yml`, `deploy/docker-compose.yml` and the dev override. `just logs
  bookkeeping` resolves to it.
- Messages already queued on `georiva-processing` stay there after an upgrade, because a message keeps the queue it was
  published to. An install upgrading with a deep backlog either lets it drain or moves the bookkeeping messages across
  once; a transient state, so no command ships for it.
- A bookkeeping burst now only delays other bookkeeping, which nothing waits on.
