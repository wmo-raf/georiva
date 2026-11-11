# Derived items follow their inputs out

## Status

accepted. Extends [ADR-0004](0004-staging-tier-and-abstract-stac-models.md) and
[ADR-0005](0005-generic-derivation-engine.md); closes the gap
[ADR-0032](0032-published-asset-objects-follow-their-rows-out-on-commit.md) left open (issue #495).

## Context

Deleting an input `Item` (published) or `StagingItem` cascades its outgoing `DerivationLink` rows away
(`source_published_item` / `source_staging_item` are `on_delete=CASCADE`). The derived items stayed published, their
lineage showed one input fewer, and nothing recomputed them or said they were stale. `invalidate_downstream` walks
lineage forward when an input *changes*, never when it is *gone*.

Two things happened to the orphan afterwards, both by accident:

- A many-input unit (a climatology over thirty dailies) had its `input_hash` change, so the periodic
  `sweep_stale_units` eventually recomputed it on what remained, with no event and no telling when.
- A single-input unit (a promotion) could not even be re-run: `outputs()` looks its source up, the source is gone,
  and the unit failed on every sweep, forever, while its old item went on being served.

`SET_NULL` on the source FKs is not available: `derivationlink_exactly_one_source` requires exactly one.

## Decision

**A derived item exists only while its unit is ready.** One generic rule, no per-recipe policy.

1. **Deleting an input re-runs what was derived from it, on commit.** A `pre_delete` receiver on `Item` and
   `StagingItem` (`processing/input_deletion.py`) reads the lineage *before* the cascade removes it: the
   `DerivationRun` rows whose produced item has a `DerivationLink` from the instance. They are buffered on the
   connection for the transaction, so a bulk delete of every input of one unit re-runs it once, and a rollback
   dispatches nothing. On commit each unit whose produced item still exists is re-dispatched with the new retry
   reason `input_deleted`. A derived item deleted in the same transaction as its input is not re-run.

2. **The engine decides what that re-run means, as it would for any re-run.** Readiness is now checked *before*
   `outputs()` (a recipe maps a unit onto its output from its inputs, which a deleted input no longer offers):
   - **ready on the remaining inputs** → it recomputes and overwrites the item in place (the climatology case);
   - **not ready, and the run produced an item before** → the engine **retracts** it: deletes the item, parks the run
     as `not_ready` with no produced item, and records a `DerivationAttemptFinished` with the new outcome `retracted`,
     naming the withdrawn item (the promotion case);
   - **not ready, never produced** → `not_ready`, as before.

3. **Retraction is a delete like any other, so it carries itself forward.** The retracted item's assets take their
   objects with them on commit (ADR 0032), and its own outgoing `DerivationLink` rows make it a deleted input to
   whatever was derived from it, which step 1 re-runs in turn.

4. **A retracted unit comes back like any parked unit.** It is `not_ready`, so `resurrect_not_ready_units` revives it
   when its inputs resolve again (ADR 0020). A re-ingested promotion source is a new `StagingItem`, hence a new unit,
   and promotes on arrival as usual.

The same rule applies on every path that re-runs a unit, not only an input delete: a stale-sweep re-run that finds its
unit not ready also retracts, which ends the forever-failing promotion.

## Consequences

- Deleting a source now deletes what was promoted from it. That is the point, but it is new: an operator deleting a
  StagingItem to "clean up" also unpublishes its Published counterpart.
- Retention (`prune_forecast_items`) deletes input Items too. A derived item built on pruned forecast items is
  recomputed on what remains, or retracted if nothing does. Products that must outlive their inputs need to say so in
  their recipe's readiness, not rely on the old orphaning.
- Each deleted input costs one indexed query to find what it fed, inside the delete. Bulk deletes pay it per row, as
  ADR 0032's receiver already does for assets.
- A recomputed (not retracted) intermediate does not re-dispatch its own dependents here. They follow through the
  existing `sweep_stale_units` hash check, as for any changed input.
- Refusing a delete while dependents live, and a per-recipe policy knob, were considered and not taken: refusing
  blocks retention and feed deletes on lineage the operator cannot see, and a knob adds surface without a recipe that
  needs a different answer yet. A recipe that tolerates a missing input already says so through `readiness`.
