# Shared build discipline, without a writer abstraction

## Status

accepted

Refines ADR 0025 (the ingestion queue admits only fetch and extraction), whose
claim-at-dispatch rule is one of the things extracted here.

> **Amended 2026-09-10: the root is not always the tenancy boundary.** This ADR
> says a publication is rooted at `{org}/{publication-slug}/` and that the root
> *is* the boundary, a foreign reader being unable to resolve outside one
> organisation. `PublicationSink` now also offers an **instance-wide root**,
> `{root}/`, for a reader built to hold several organisations at once — see
> *Consequences* below, where the original claim is left standing as written and
> the amendment records what replaces it. The org-rooted form is unchanged and
> remains the default.

## Context

GeoRiva is about to grow a second thing that is *built* rather than ingested. The
first is `virtual_zarr`: one Icechunk repository per `Variable`, holding virtual
references to that variable's COGs, rebuilt as new COGs arrive. The second is a
point-forecast publication: one collection's model run transposed out of
per-timestep COGs into met.no's Forti binary format, written to a bucket that a
foreign Go service reads.

They look alike enough that the temptation is to abstract the *writer*. A sketch
that preceded this ADR did exactly that — a `BasePublisher` ABC with
`applies_to()` / `plan()` / `write()` / `prune()`, a registry of publishers, and
a `PublicationSink` Protocol — with `virtual_zarr` named as the second
implementation that would prove the abstraction.

It cannot be. Checked against the code, `virtual_zarr` fails the sketched
contract on four counts:

1. `applies_to(collection)` and `plan(collection, …)` are collection-grained.
   `VirtualZarrManifest` is per-`Variable` — a `OneToOneField`
   (`virtual_zarr/models.py:52`).
2. `PublicationPlan.version` is a sortable string and `prune()` keeps the N
   highest. Icechunk has no versions: one branch, and snapshots that expire by
   age.
3. `PublicationSink` wraps Django storage. Icechunk needs a raw object-store
   handle (`StorageManager.get_zarr_fs`) and deliberately bypasses Django
   storage, because zarr v3 chunked writes cannot go through it.
4. `CompletionMarker` ordering is meaningless there. The Icechunk **commit** is
   the atomicity mechanism; there is no window in which a reader can see a
   half-written repo.

A contract the only other implementation cannot satisfy is not an abstraction,
it is a guess. And the cost of guessing wrong is not neutral: a registry and an
ABC are load-bearing structure that every later writer has to be bent to fit.

What the two genuinely share is narrower and much more expensive to get right.
Both are triggered by events arriving in bursts, both take minutes, both are
idempotent, and both are wrong if two copies run at once. `virtual_zarr` has
already paid for that knowledge — #398 was a duplicate-dispatch race that only
became visible when the build moved to a four-slot worker, and the fix needed
two mechanisms, not one: a claim taken in the same UPDATE that decides to
dispatch, *and* a claim token that travels with the task, because a queue wait
longer than the lock timeout lets the sweep recycle the lock and dispatch a
replacement while the first copy is still queued.

A second builder starting from `VirtualZarrManifest`'s field list would have
reproduced the states, the sweep and the lock — and would not have reproduced
either half of that fix, because neither is visible in the field list.

## Decision

**Core gains a build discipline, not a writer abstraction.**

`core/build_discipline` holds two abstract models and four functions:

- `BuildDisciplinedModel` — the six states (`pending / building / ready / stale
  / failed / no_data`), the lock (`locked_at` / `locked_by` / `LOCK_TIMEOUT`),
  the input fingerprint, and every transition as a conditional UPDATE:
  `claim_for_build`, `reset_stale_locks`, `refresh_build_lock`, `queue_rebuild`,
  `mark_ready` / `mark_failed` / `mark_stale` / `mark_no_data`, `get_buildable`,
  `is_up_to_date`.
- `BuildAttemptLog` — the durable per-attempt record with retention, because the
  built record only ever holds the *latest* state and a build that has been
  failing all week leaves no other trace.
- `dispatch_build` / `sweep_builds` / `stand_down` / `build_attempt` — the
  claim-then-queue sequence, the sweep that recovers abandoned claims before
  looking for work, the stand-down check for a recycled claim, and the context
  manager that records a failed attempt with whatever it had learned before it
  died.

It is deliberately grain-free. `VirtualZarrManifest` hangs one off a `Variable`
and a publication hangs one off a `Collection`; nothing in the base can tell,
and nothing in it knows what a build writes.

**No `BasePublisher` ABC, no registry, no `PublicationSink` Protocol.** The Forti
writer is an installable plugin (`georiva-publisher-forti`) that declares its own
model on the base and its own Celery tasks. If a third writer arrives and wants
the same shape as the second, that is the moment to extract it — from two
working implementations rather than one and a sketch.

**`virtual_zarr` is not migrated onto the base.** The shape was extracted from
it, so it fits by construction, but adopting it means a migration that renames
`watermark` to `input_fingerprint` and changes its type — on a model whose source
of truth is deliberately the Icechunk commit, not the Django row. That is its own
decision, worth taking when there is a reason beyond symmetry.
`core/tests/test_build_discipline.py` pins the two together instead: the state
values, the buildable set, the lock window and the field list are asserted equal,
so drift is visible rather than discovered later.

**Core also gains the publications bucket and `PublicationSink`.**
`georiva-publications` is private and notification-free — a notification would
hand a publication's own output back to the ingestion consumer as a file to
ingest. Keys are org-first like every other bucket, rooted at
`{org}/{publication-slug}/`, and that root is the tenancy boundary: a foreign
reader is pointed at one organisation's prefix and cannot resolve outside it.

**The engine writes completion markers last; the writer never writes them.** A
foreign reader polls for a marker and loads whatever it names, so a marker that
appears before its bytes is a reader loading a partial dataset with nothing
anywhere reporting an error — the marker was a promise. The rule is enforced by
the sink rather than left to the writer's discipline: `write()` refuses any path
matching the publication's declared marker patterns, `publish_markers()` refuses
anything that is *not* one, writes them in the order given, and can be asked to
verify that named objects are staged first.

Verified against met.no's `rawdataforecaster` during the format spike: bytes left
staged for four seconds with the marker withheld drew no load attempt at all, and
the load began within two seconds of the marker appearing.

## Alternatives considered

**Ship the `BasePublisher` ABC and migrate `virtual_zarr` onto it** — rejected on
the four counts above. Making it fit would mean widening the contract until it
said almost nothing (a `plan()` that takes either grain, a `version` that may be
absent, a sink that may be bypassed), which is the same as not having it.

**Copy the state machine into the plugin** — rejected. It is ~150 lines of
conditional UPDATEs whose correctness is not local: the parts that matter are the
two that #398 needed, and both look like incidental detail when read out of
context. A copy would have been made from the field list.

**Put the build discipline in a `builds` app with concrete models** — rejected.
There is nothing for core to build. Concrete models would need a migration, a
table, and an admin surface for rows that only ever belong to a plugin.

**Let the writer write its own markers, and document the ordering** — rejected.
The ordering failure is silent on both sides: the writer sees a successful write
and the reader sees a valid marker. A rule that is only ever violated invisibly
belongs in the code that can refuse it.

## Consequences

- No migration. Both models are abstract, so `georivacore` is unchanged at 0015.
- `BucketType.PUBLICATIONS`, a `GEORIVA_BUCKETS` entry, `GEORIVA_PUBLICATIONS_BUCKET`,
  `StorageManager.publications` and a `setup_minio` config. Existing deployments
  get the bucket on the next `setup_minio` run, which the entrypoint already does
  at startup.
- `Bucket.list_keys` joins `list_files`: names only, no per-object `size` and
  `get_modified_time`. `list_files` costs three requests per object, and a
  published area is thousands of small ones.
- `PublicationSink._put` deletes before saving on backends that do not overwrite.
  Django storage backends disagree — S3 obeys `file_overwrite` (true on every
  GeoRiva bucket) and `FileSystemStorage` renames instead of replacing — and a
  publication's keys are derived and polled by name, so a rename is a silent
  failure rather than a duplicate.
- A plugin declaring a build-disciplined model owns its own tasks and their
  queues. ADR 0025 still applies: sweeps go on `georiva-default`, builds on
  `georiva-processing`, nothing new on `georiva-ingestion`, and `dispatch_build`
  uses `delay()` so the AST sweep in `ingestion/tests/test_queue_routing.py`
  stays satisfied.
- `delete_prefix` refuses to delete a completion marker unless asked with
  `include_markers=True`. Removing one turns a live reader's next poll into a
  missing-version error, so it is never the default — but dropping a superseded
  version whole does require it, because that version's own manifest is a
  marker and a directory that cannot lose it never goes away.

### Amendment, 2026-09-10 — the instance-wide root

The paragraph above beginning *"Core also gains the publications bucket"* says
keys are rooted at `{org}/{publication-slug}/` and that **that root is the
tenancy boundary**. That is still true of the org-rooted form, which is still the
default and still what every existing caller uses. It is no longer true of every
publication.

**What changed.** The reader this ADR was written for — met.no's
`rawdataforecaster` — gained a per-request area filter and reports the area that
answered. Before that, the process was the only place a tenant could be
distinguished, so a prefix per organisation implied a *process* per organisation.
It no longer does, and one process holding every organisation's areas needs one
prefix holding every organisation's data.

**What core does about it.** `PublicationSink.instance_wide(root)` roots a
publication at `{root}/`. It is a second constructor rather than a flag because
the two forms make different promises, and the call site should say which one it
is making.

**What is given up, precisely.** Cross-tenant resolution is no longer impossible
by construction. A reader given `{root}/` can see every organisation publishing
there, by design, so the boundary moves out of the storage layer to whatever
decides which tenant a request may be answered from — and that check is now
somebody's code and somebody's test, not a property of the prefix. Core cannot
give this back; pretending otherwise by adding a check here would only move the
same trust to a place with less information.

**What is kept, by construction.** Two things, both narrower than the sentence
they replace:

1. *An instance-wide root cannot be spelled as an organisation slug.* A root of
   `kenya` **is** organisation `kenya`'s prefix: a publication rooted there
   writes into a tenant's space today, and a tenant registered tomorrow inherits
   a prefix already full of another's data — silently in both directions, since
   neither side is looking. The test is `ORG_SLUG_RE` rather than the
   organisations that exist, because the collision that matters is with the one
   created *after* the root was chosen; the reserved-name list is deliberately
   not consulted, since it can shrink and a root that was safe must not stop
   being so. `_forti` is safe because `_` is outside the grammar.
2. *`delete_prefix` refuses an empty relpath on an instance-wide sink.* Under
   `{org}/{slug}/` "delete the root" means dropping one publication, which a
   caller may legitimately want. Under a shared root it means dropping every
   organisation's, which no retention pass wants and which nothing downstream
   would report — the readers would simply stop finding data.

   This second one is a backstop, not a boundary, and it is worth being blunt
   about the gap it leaves. On an instance-wide sink *any* prefix may be shared:
   a publisher's pointer directory or its config file is as instance-wide as the
   root, and deleting either costs every organisation. Core cannot guard those,
   because which names mean what under the root is the publisher's grammar and
   core does not have it — the same reason `children()` at such a root can no
   longer promise to be enumerating areas. Retention on a shared root is the
   publisher's to get right, and its tests are where that is established.

   The identity is fixed at construction for the same reason the checks exist at
   all: `organisation_slug`, `slug` and `root` are read-only, so a sink cannot
   be re-rooted into something no rule ever saw. And the grammar rule runs in
   both directions — an `organisation_slug` must *match* `ORG_SLUG_RE`, or
   `PublicationSink("_forti", "central")` would root a supposedly org-owned
   publication inside the shared prefix, where a shared reader would serve it as
   though somebody had published it there.

Everything else is unchanged: key derivation, the escape check, the marker
patterns and the marker-ordering rules are all written against `root` and behave
identically under both forms.
