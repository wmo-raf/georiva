# Not-ready runs are resurrected

## Status

accepted

## Context

A derivation unit whose required input is absent at run time parks its
`DerivationRun` as `NOT_READY` (per-unit readiness, ADR 0005). Until now that
was a dead end dressed up as a pause:

- `sweep_stale_units` only inspects *terminal* (completed/skipped) runs, and
  `reclaim_stale_running` only RUNNING ones — no pass ever revisited
  `NOT_READY`, even though the engine's log claimed "will retry via the 5-min
  sweep when inputs arrive".
- The completion-chaining trigger (`published_item_trigger`) does fire when the
  missing input is finally derived, but recipes deliberately ignore
  published-item triggers in `candidate_units` — the guard that stops a
  baseline/promotion output being double-fired or misread as a value input.

CHIRPS made this concrete: an anomaly backfill dispatched before the
climatology normals existed parked 636 units as `not_ready`, and nothing would
ever revive them. The only recovery — pressing "Run now" again — was itself
unavailable for the anomaly, because the product card hid the run button for
`trigger_mode="event"` products (`can_run` was `manual`/`scheduled` only),
even though `run_product_now` fully supports them.

## Decision

A `not_ready` run is now a **promise that the system will revive it** once its
inputs exist, via two paths sharing one readiness-gated primitive,
`processing.invocation.resurrect_not_ready_units`:

1. **Periodic sweep** — `sweep_derivations` gains a third pass that scans all
   `NOT_READY` runs, resolves each unit's inputs, and re-dispatches only those
   whose `recipe.readiness` now passes (retry reason `not_ready_sweep`). A
   unit whose inputs will never materialise stays parked as an honest
   `not_ready` row rather than churning the queue every sweep.

2. **Completion wake-up** — when a unit completes, the engine sends a new
   `unit_completed` Django signal (`processing/signals.py`). A receiver in
   `sources/derivation_invocation.py` matches the produced item's collection
   against pinned *published*-tier `DerivedProductInput` bindings (the same
   indexed join as `dispatch_for_input`, ADR 0010 §4) and revives only the
   matching enabled products' parked runs (retry reason `input_arrived`).
   The signal keeps ADR 0005's import direction intact: the engine never
   imports `DerivedProduct`; the feed layer subscribes. The receiver is
   best-effort — a wake-up failure never fails the producing task; the sweep
   is the safety net behind it.

   The wake-up deliberately bypasses `candidate_units`, so the published-
   trigger double-fire guard stays untouched: a run row is either parked or
   nothing happens.

3. **Backfill button for event products** — the product card's `can_run` is
   now true for every trigger mode, labelled **Backfill** for `event` products
   (and still **Run now** for `manual`/`scheduled`), readiness-gating
   unchanged. `run_product_now`'s wide selector already made this the
   documented backfill path; the card was the only thing hiding it.

## Consequences

- Operators no longer need to re-run a product by hand after its upstream
  catches up; the parked runs revive within one sweep interval at worst,
  near-instantly via the wake-up in the common case.
- Automatic revival only covers units that *have* a parked run row. A file
  staged while its product was disabled never got a run row, so its units are
  discoverable only by the Backfill button's wide enumeration — that gap is
  accepted and is the button's job.
- Two new `DerivationRun.RetryReason` values (`not_ready_sweep`,
  `input_arrived`) let the tracking UI show *why* a unit revived.
- First engine→feed-layer notification exists, but as a signal — ADR 0005's
  "engine never imports DerivedProduct" boundary is preserved.
