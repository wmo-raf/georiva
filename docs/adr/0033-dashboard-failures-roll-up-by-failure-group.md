# Dashboard failures roll up by failure group, keyed at record time

## Status

accepted

## Context

ADR 0030 put a failures panel on the admin homepage: the newest twenty failed or
refused Domain events of the last seven days, each as a full History row. It is
the one org-wide listing the design allows, kept for silent-failure detection.

In practice one problem produces many events. A feed whose host stopped
resolving fails every scheduled run; a collection with a parsing bug fails every
file a sweep retries. Twenty multi-line rows of the same two problems filled the
dashboard and pushed the Collection Health Panel out of view, and a plain
"newest N" cut would hide exactly the quiet failure the panel exists for behind
the noisy one.

Events carry no common notion of *what* failed: `subject_url()` names the
narrowest object — one fetch run, one file — so grouping on it changes nothing.

## Decision

1. **The panel shows one line per failure group**: event type plus a per-type
   *failure group key* naming what an operator would fix. A fetch failure
   groups under its Data Feed, a file ingestion under its Collection (first
   touched, else read off the key's `{org}/{catalog}/{collection}/` segments), a
   derivation attempt under its Derived Product (else its recipe). The default,
   for every other type and every plugin type, is the blank key: one group per
   type. Each line carries the failed and refused counts apart, the newest
   event's time and one-line summary, a link to the subject and a link to its
   events. At most ten lines, newest first, with a count of the rest.

2. **The key is stored on the index row at `record()`**, as
   `EventIndex.failure_group_key`, the way `outcome_class` already is. The panel
   then groups in SQL and loads only each group's newest event. Computed at read
   time instead, every failure in the window would have to be loaded to group
   them — unbounded, on the page every user opens first. Because `record()`
   calls it on every write, `DomainEvent.failure_group_key()` reads the event's
   own columns and never queries; `failure_group()` adds the label and link at
   render time, so a renamed feed shows its new name. The migration backfills
   past failed and refused rows, lifting the append-only trigger for that one
   write as `purge_events` does for its deletes.

3. **A later success does not clear a group.** It stays for the window. For a
   feed a success would mean recovery, but for a collection it does not: a good
   file says nothing about the files that failed before it. The newest-event
   time shows a stale group as stale.

4. **The events themselves move to a Failures page** (`events/failures/`,
   `?type=&group=` narrows it to one group), paginated, under the same ADR 0030
   exception and, like the Timeline, reached by link only — no menu entry
   (ADR 0019).

## Consequences

- An event type that wants its own grouping overrides `failure_group_key()` and
  `failure_group()`; one that does nothing still appears, grouped by type.
- A key is fixed when the event is written. Changing a type's grouping applies
  to new events only, unless a migration re-keys the old ones.
- The panel's cost is two index queries plus one per event type shown, however
  many failures the window holds.

## Alternatives considered

- **Newest N events, made compact** — the noisiest problem crowds out the rest.
- **Group by event type alone** — needs no per-type code, but folds every feed
  into one line; kept as the default for types that declare nothing.
- **Grouping at read time** — unbounded, see 2.
- **Clearing a group on a later success** — right for feeds, wrong for
  collections, see 3.
