# Domain events beside run rows

> **Amended by [ADR 0033](0033-dashboard-failures-roll-up-by-failure-group.md)** — the failures panel shows one line
> per failure group.

## Status

accepted — see wmo-raf/georiva#418

## Context

`FileIngestion`, `RunIngestion`, `FetchRun` and `DerivationRun` are locks and
state machines. They are overwritten in place on every retry, reopen and
re-ingest, so they can only answer "how do things stand now". Nothing ties a
fetch, the ingestion of the file it stored, the Derivations that file triggered
and a plugin's publication into one story, and a failure is only visible to
someone who already knows which row to open.

## Decision

1. **Run rows stay as locks and the truth for current state. Domain events are
   the truth for history.** Where a row and an event could disagree, the row
   wins — it is the lock. No second `IngestionRun` / `DerivationRun` is created.
2. **Events are written inside the state-changing transition, atomically with
   it, and only by the writer that won the conditional UPDATE.** Never from a
   `post_save` receiver: a receiver cannot know which racing writer won, and
   cannot share the transition's transaction by contract.
3. **Events carry their own snapshot of identifying facts** (slugs, recipe
   version, file path, attempt, revision …). Links to subjects are convenience
   foreign keys, nullable and `SET_NULL`; references to Items are plain id
   columns. Deleting a collection leaves its history readable.
4. **Organisation lives on the event and is nullable.** It is the design's only
   `PROTECT` reference: an organisation with events cannot be deleted. Null
   means *unattributed* — declared as a plain organisation path, which the
   tenancy dispatcher reads as "nobody's", so only the instance administrator's
   unscoped listing shows it.
5. **One Correlation per chain.** The first event sets it; downstream events
   copy it. At fan-in the *trigger* wins; non-triggering inputs are reached
   through `DerivationLink` lineage. A sweep reviving a run reuses the
   Correlation and organisation of that run's most recent event.
6. **Append-only is enforced in the database** — a row trigger rejecting DELETE
   and any UPDATE other than setting a nullable reference to NULL — as well as
   by a model/queryset guard. The one deletion path is an audited purge command.
7. **No backfill.** Every event is a fact observed when it happened; history
   begins at deployment and the panels say so.
8. **Domain signals stay** as the consumer-notification mechanism (ADR 0020,
   0026) but fire after commit and carry the event just written.

## Consequences

- Task dispatch and signal sends inside an event-writing atomic block go
  through `transaction.on_commit`.
- Correlation travels in task headers; the fetch → ingestion hop crosses object
  storage and is bridged by lookup of the key's most recent fetched/uploaded
  file.
- Events are kept indefinitely; volume is low thousands of rows per day.
- The Timeline draws a chain as a rail along `caused_by` and folds what is wide
  or repetitive (`events/timeline.py`). It stays reachable by link only: `?from=`
  narrows it to one event's branch and `?event=` lands on one event, both read
  through the same scoped index rows as the whole chain.
- The plugin-facing surface (abstract base, `record()`, context, migration
  operation, registry, signal-carries-event) becomes public API once a plugin
  adopts it.

## Alternatives considered

- **Rewriting run rows as append-only history** — loses the lock semantics the
  rows exist for.
- **`post_save` recording** — races, and sees no winner.
- **A cross-table UNION view for the timeline** — the set of event tables
  differs by installed plugins; an index table replaces it.
- **Header-only propagation** — cuts every chain at the object-storage hop.
