# A model run is a first-class ingestion record

## Status

accepted

## Context

GeoRiva tracks four things that arrive: a fetch (`FetchRun`), a fetched file
(`FetchedFile`), an uploaded file (`UploadedFile`), and a file being processed
into COGs and STAC items (`FileIngestion`). It does not track the thing
operators and downstream consumers actually reason about — **a model run**.
"Has the 00Z run landed for `ecmwf-ifs-surface`?" has no record to answer it.

Three consumers already pay for that gap, and they arrived at it independently:

- **`virtual_zarr`** marks a manifest STALE from `Asset.post_save`. One ECMWF
  IFS run is 68 files × 14 variables ≈ 950 asset saves, so the manifest is
  marked stale ~950 times to produce one build. The sweep collapses that into a
  single rebuild, which is why it has never been visibly wrong — but the
  granularity is off by three orders of magnitude, and there is no signal
  anywhere that says "the inputs are now whole".
- **`retain_latest_run_only`** prunes items from older reference times. It has
  no notion of a run boundary, so it cannot ask whether the newer run is
  complete enough to be worth dropping the older one for.
- **The Collection Health Panel** can say which files landed. It cannot say
  whether a run did.

The obstacle is that nothing in the pipeline announces the last file. A run's
boundary has to be *inferred*, and the three arrival routes do not have equally
good evidence to infer it from.

### What each route actually knows

**DataFeed.** The Loader enumerates every `FileRequest` before fetching any of
them (`loader.py:262`) and persists one `FetchedFile` per request — including
the ones it skips because the bytes are already in the bucket. The expected set
is therefore declared before any of it arrives. This is the only route that can
be exact, and the only one that cannot close early.

Two things about `FetchRun` that the code says and `CONTEXT.md` did not:

1. It is created **per `(feed execution, collection)`**, not per feed execution.
   `LoaderJobType.run` loops the feed's collections and calls `Loader.run()`
   once for each; `loader.py:248` creates a `FetchRun` inside that call.
2. It carries **no collection FK**. So the collection an expected file belongs
   to is recoverable only from the key — whose third segment is the collection
   slug, and whose filename carries the reference time in the `GR--` prefix.
   Both are the Loader's own output (`_get_storage_path` builds the path from
   the collection's catalog chain, deliberately never from anything a plugin or
   a remote server supplies), so reading them back is sound rather than
   incidental.

**Manual upload.** `UploadSession` is the `FetchRun` analogue and already
auto-completes when all its `UploadedFile` children are terminal
(`models.py:758`). But it declares *its own batch size*, not the run's: an
operator uploading 20 of 68 steps today and 48 tomorrow completes twice, and
means "run complete" neither time. `UploadedFile` also carries no
`reference_time` — `FetchedFile` does, inside `request_payload` — so the run has
to be identified through the `FileIngestion` rows the uploaded keys produced.

**MinIO drop and sweep.** No declaration and no batch. All that is left is
quiet: nothing in flight, nothing new for a settle window.

### Why the record cannot be written on the acquisition side

A key with no collection segment (`{org}/{catalog}/file.grib`) fans out to
*every* active collection of its catalog (`service.py::_resolve_collections`).
The `(collection, reference_time)` key therefore does not exist until collection
resolution has run — which happens during ingestion. A DataFeed file does carry
its collection in the key, but writing the record in two different places for
two routes would be two different records.

## Decision

**A `RunIngestion` model: one row per `(collection, reference_time)`**, in
`georivaingestion`. The sibling of `FileIngestion` — that is one *file*
arriving, this is one *model run* arriving into one collection. Rows exist only
for forecast collections; a collection with no reference times has no run
boundary.

**Named to avoid two glossary collisions.** `cycle` is listed under *Run hour*'s
`_Avoid_` ("cycle (in code)"), and `Arrival` under `FetchRun`'s and
`UploadSession`'s (`_Avoid_: DataArrival`). Bare *Run* in
`FetchRun`/`DerivationRun` means *our execution*; qualifying it keeps the *model
run* sense distinct.

**It opens on the first `FileIngestion` reaching COMPLETED for that key**, from
inside `FileIngestion.mark_completed` — the sole writer of COMPLETED. Not a
`post_save` receiver: `mark_completed` is a bulk `update()` and emits no
`post_save` at all. The hook is wrapped so that bookkeeping can never fail the
ingestion it is bookkeeping for.

**It closes by whichever signal its route has, and records which:**

| Route | Closer | Fires |
| --- | --- | --- |
| DataFeed | every declared key has arrived | on arrival |
| Manual upload | every feeding session is closed and all its stored files have ingested | on arrival |
| Drop / sweep | quiet period | periodic task, `georiva-default` |

The first two fire on a file *arriving*, because the last file to arrive is the
only moment either condition becomes true — and either is a stronger claim than
the quiet period that would otherwise close the run some minutes later. The
declared-set closer compares key **sets**, not counts: a re-ingest of one file
plus a missing other file give the same count, and only the set notices. FAILED
`FetchedFile`s are excluded from the expected set, because they will never
arrive and waiting for them would hold the run open forever.

The upload closer is deliberately **not** hooked on `UploadSession`
auto-completion. A session completes when its files reach a terminal *upload*
state — the bytes are in MinIO — and ingestion happens afterwards,
asynchronously; closing there would close a run before a single file of it had
been ingested.

The quiet-period closer's in-flight test is **catalog-wide, not
collection-wide**. A PENDING `FileIngestion` has no collections yet, because
resolution has not run, and a key with no collection segment fans out to all of
them. Scoping to the catalog prefix is the only test that sees those; it errs
toward holding a run open, which is the safe side.

**It reopens on any later `FileIngestion` for the same key, bumping a
revision.** This is the load-bearing decision, not a detail. Two of the three
closers can close early, and the reopen is what makes that safe: a run that
closes on weak evidence and reopens is correct; a run that closes on weak
evidence and stays closed is a silently truncated forecast. It also demotes
"has it closed" from a correctness guarantee to an optimisation — a consumer
that acted on the first close is not wrong, only out of date, and the higher
revision tells it so.

**`version = ref_epoch_seconds × 100 + revision`** is exposed as a property: one
sortable integer that orders by model time first and republishes of the same run
second. Consumers that reload only on a strictly greater version need both
halves — without the revision, a corrected republish reuses the same number and
is ignored; with the revision leading, a backfilled older run would outrank a
newer one.

**Closing and reopening emit domain signals** (`run_ingestion_closed`,
`run_ingestion_reopened`) from `ingestion/domain_signals.py`, connected by
subscribers in their own `AppConfig.ready()` — the direction ADR 0020 protects.
They live apart from `ingestion/signals.py`, which holds *receivers* (the
`post_save` hooks that fan rows out to the SSE stream). Both are "signals" in
Django's vocabulary and they are opposite halves of it, so they do not share a
module.

**The quiet sweep runs on `georiva-default`**, with the other sweeps. Nothing a
reader can observe waits on a run being marked closed, so it is deferrable work
by ADR 0025's rule and must not join the ingestion queue.

## Alternatives considered

**Infer the run at read time from `FileIngestion` rows** — rejected. A query can
count what arrived; it cannot know what was expected, and the expected set is
the whole difficulty. It would also have nowhere to put the revision, so a
consumer could not tell a republished run from the original.

**Close on `UploadSession` completion** — rejected on timing, above. It is the
obvious hook and it fires before any of the batch has been ingested.

**Key the row on `(catalog, reference_time)`** — rejected. One ECMWF run
populates a surface collection and a pressure-level collection with different
files, on independent ingestion paths; they finish at different times and a
consumer of one should not wait on the other.

**Reuse `FetchRun` as the run record** — rejected. It exists only for the
DataFeed route, it is per feed execution rather than per model run (a feed
fetching two run hours in one execution produces one `FetchRun`), and it is
written on the acquisition side where the collection key does not yet exist.

**A `no_data` or `failed` state** — deferred. A run that never opens has no row,
and a run that opens and stalls is closed by the quiet period with
`closed_by=quiet_period`, which is legible enough. Adding states before there is
a consumer that distinguishes them would be guessing.

## Consequences

- `FileIngestion.mark_completed` now does bookkeeping work. It is wrapped in
  `try/except` with a logged exception: a broken closer degrades run tracking,
  never file ingestion. A test pins that.
- The declared-set closer runs two set queries per completed file of a forecast
  collection. Both are prefix/`IN` queries on indexed columns and bounded to one
  collection and one reference time.
- `RunIngestion.file_count` is recomputed from `FileIngestion` on every arrival
  rather than incremented, so a re-ingest cannot inflate it.
- A DataFeed run whose declared set can never complete — a step the source
  stopped publishing — is closed by the quiet period instead, on weaker
  evidence. `closed_by` is what tells an operator which of the two happened.
- `GEORIVA_RUN_SETTLE_MINUTES` (default 20) is the quiet window.
- Nothing subscribes to the signals yet. `virtual_zarr`,
  `retain_latest_run_only` and the health panel are the intended first
  subscribers, each on its own decision.
- `CONTEXT.md`'s `FetchRun` entry is corrected: one per
  `(feed execution, collection)`, not one per feed execution.
