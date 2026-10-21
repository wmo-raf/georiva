# Fetch runs stop on source failures and retry their own plan

## Status

accepted — see wmo-raf/georiva#473 (slices #477, #478, #479, #480)

## Context

On 2026-10-18 the ECMWF IFS feed ran for about 20 hours and ended
`0 fetched, 0 skipped, 16 failed`: an overloaded portal (`Read timed out`) and
our own DNS (`NameResolutionError` for `data.ecmwf.int`). Nothing was ingested
for two days.

The Loader treated every failure alike. It recorded the file as failed and moved
on to the next, so an outage of minutes or hours failed every remaining file one
by one, each after its own timeout. The run then ended `COMPLETED`, stamped
`last_run_at` when it *finished*, and so pushed back its own next attempt by
however long it took to fail. Nothing retried the failed files except an
operator's button, and the next scheduled run asked the source for whatever was
latest, so the failed run's missing steps were never fetched.

#474 persisted each run's whole plan up front: one `FetchedFile` row per
request, with its `request_payload`. That makes "retry the same run" a matter of
fetching the rows that failed, with no help from the source plugin.

## Decision

1. **Every failed fetch carries a Failure scope** (#477): `file` (definitive,
   this file only: a 404, a download that fails validation), `transient`
   (ambiguous: a timeout, a dropped transfer, a 5xx that outlasted the HTTP
   retries, and anything nobody classified) or `source` (the source cannot serve
   us: the host does not resolve, refuses the connection, answers 401/403/429 or
   a 503 with `Retry-After`; or our side cannot accept the file). Rows the run
   never reached are `not_attempted`. Core strategies classify from transport
   facts; a source-plugin override hook is deferred until a plugin needs one
   (ECMWF's step-0 404 after run detection said the run exists).

2. **A run stops on the first `source` failure** (#478). Every remaining file
   would fail the same way, and continuing adds load to a source that is
   already struggling.

3. **A circuit breaker stops it on consecutive `transient` failures** (#478).
   One timeout is noise; `GEORIVA_FETCH_CIRCUIT_BREAKER_THRESHOLD` (default 3)
   in a row means the source is down. Any success, or a `file` failure, resets
   the count. Counting is per run, not per host.

4. **A stopped run ends `FAILED`**, with `error_message` reading
   `Stopped: <reason>`, and its remaining rows become `failed` /
   `not_attempted`. No new status: `FAILED` was already there and the Loader
   had never used it. An exception escaping plan generation or the fetch loop
   also ends the run `FAILED`, not `COMPLETED`. A run that walks its whole plan
   stays `COMPLETED` however many files failed, and an empty plan stays
   `COMPLETED`: those are the source's honest answer, not a stop.

5. **One stopped collection stops the feed's tick.** When a run stops because
   the source cannot serve it (a `source` failure, the breaker, or an exception
   connecting to the source or generating the plan), the feed's remaining due collections in that tick are
   not attempted against the source: each gets a `FetchRun` that ends `FAILED`
   at once, `not attempted: source unavailable — aborted in <collection>`, with
   no plan rows, because generating a plan may itself call the source. An
   exception in the fetch loop is our own failure and does not short-circuit.

6. **Wall-clock limits bound a trickling source** (#479): 15 minutes per file
   (strategy configuration; exceeding it is `transient`), 2 hours per run (a
   setting; exceeding it stops the run as above).

7. **A stopped or partially failed run retries its own plan** (#480): a new
   `FetchRun`, linked to the one before it, fetches that run's persisted failed
   requests, so the retry targets the same reference time rather than whatever
   is latest. Backoff is 5, 15, 45, then 120 minutes from when the run finished,
   with `Retry-After` as a floor; a lineage gets at most 4 retries, shared with
   crash recovery, then is parked for an operator. `last_run_at` is stamped when
   a run starts. A source with no plan to copy falls back to asking the source.

8. **Stopping a fetch is never how a run's completeness is protected.** Whether
   a model run is complete enough to close or publish stays downstream:
   `RunIngestion` closes on its declared set or its quiet period, and consumers
   (the derivation engine, the Forti planner) take only what is complete. The
   fetch gets as much as it can and reports honestly.

## Consequences

- An outage now costs a run seconds to minutes instead of hours, and the
  ingestion queue (ADR 0025) is not held by a run that cannot succeed.
- A `FAILED` fetch run is the Loader's own verdict: the run page shows
  "Stopped early: <reason>", and its `FetchRunFinished` event records `failed`.
  `DataFeed.last_run_status` reads `failed` for it.
- The breaker's threshold trades false aborts against wasted timeouts. A source
  that is genuinely flaky but serving can trip it; the retry (7) is what makes
  that cheap.
- The manual "Run now" job (`LoaderJobType`) stops each collection's run the
  same way, but does not short-circuit its remaining collections; only the
  feed's scheduled tick does.

## Out of scope

- Mirror fallback for ECMWF Open Data, and the step-0 404 hook: both belong in
  `georiva-source-ecmwf`.
- Per-host breaker counting.
- ASYNC / queued providers (CDS, MARS): their failures are not classified here.
- Alerting on parked runs.

## Alternatives considered

- **Keep going and let the next run pick up the pieces.** That is what we had:
  the next run asked for whatever was latest, and the failed run's steps were
  lost.
- **Count the breaker per host, across runs.** More precise for sources shared
  by several feeds, but it needs state outside a run; deferred until a source
  needs it.
- **A new `ABORTED` status.** It would say nothing `FAILED` plus its
  `error_message` does not, and every consumer of the status would need to
  learn it.
- **Abort to protect the model run's completeness.** Completeness is decided
  downstream already (8); stopping the fetch would only make the run less
  complete.
