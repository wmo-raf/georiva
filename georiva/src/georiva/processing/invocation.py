"""
Invocation surface for the derivation engine.

Event-driven, scheduled/backfill, and the periodic sweep are all thin callers of
the one ``run(recipe, selector)`` primitive — they differ only in how wide a
selector (or how narrow a trigger) they build. This module holds the event-side
helpers:

- ``dispatch_for_trigger`` — an arriving input fans out to every registered
  recipe; each recipe's ``candidate_units(trigger)`` decides whether (and which)
  units it feeds, so irrelevant recipes contribute nothing.
- ``invalidate_downstream`` — walk ``DerivationLink`` forward from a changed
  input through its derived items (transitively, through internal
  intermediates) and re-dispatch each one.

See issue #125 and docs/adr/0005-generic-derivation-engine.md.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def staging_item_trigger(staging_item) -> dict:
    """The arriving-input trigger for a newly registered StagingItem. Carries the
    linked published-tier ``collection_id`` (the StagingCollection's core
    Collection FK) alongside the slug (ADR-0010 §3); ``collection_id`` is ``None``
    when the staging collection isn't linked yet, so dispatch matches nothing."""
    return {
        "staging_item_id": staging_item.pk,
        "collection_id": staging_item.collection.collection_id,
        "collection_slug": staging_item.collection.slug,
    }


def published_item_trigger(item) -> dict:
    """The arriving-input trigger for a Published item produced by a derivation
    (an intermediate that may itself feed a further derivation). Carries the
    collection's ``collection_id`` (its own pk) alongside the slug (ADR-0010 §3)."""
    return {
        "published_item_id": item.pk,
        "collection_id": item.collection_id,
        "collection_slug": item.collection.slug,
    }


def _dispatch_unit(recipe_type: str, unit: dict, *, dispatch: bool = True, reason="initial") -> None:
    """Re-run one known ProductionUnit via the same per-unit primitive ``run``
    fans out to — no enumeration/orchestration is duplicated. ``reason`` records
    why this re-dispatch fired (recorded on the DerivationRun by ``acquire``)."""
    if dispatch:
        from .tasks import run_unit_task
        run_unit_task.delay(recipe_type=recipe_type, unit=unit, reason=reason)
    else:
        from .engine import run_unit
        from .registry import recipe_registry
        recipe = recipe_registry.get(recipe_type)
        if recipe is not None:
            run_unit(recipe, unit, reason=reason)


def current_input_hash(recipe, unit: dict) -> str:
    """The input_hash a unit would have *now*, from its inputs' current
    checksums — compared against the recorded hash to detect staleness."""
    from .recipe import compute_input_hash

    return compute_input_hash(recipe.resolve_inputs(unit), recipe.version)


def sweep_stale_units(*, dispatch: bool = True) -> int:
    """
    The write-side mirror of ``sweep_unprocessed``: find already-computed units
    whose inputs have since changed (recorded ``input_hash`` ≠ current) and
    recompute them — without any event. Returns the number re-dispatched.
    """
    from .models import DerivationRun

    terminal = [DerivationRun.Status.COMPLETED, DerivationRun.Status.SKIPPED]
    stale = 0
    for run_rec in DerivationRun.objects.filter(status__in=terminal):
        recipe = _recipe_for(run_rec.recipe_type)
        if recipe is None:
            continue
        try:
            current = current_input_hash(recipe, run_rec.unit_key)
        except Exception as e:  # a recipe that can't resolve is skipped, not fatal
            logger.warning("Sweep: cannot hash %s: %s", run_rec, e)
            continue
        if current != run_rec.input_hash:
            _dispatch_unit(
                run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch,
                reason=DerivationRun.RetryReason.INPUT_STALE,
            )
            stale += 1
            # Recomputing this unit will change its output, so anything derived
            # from it is stale too — propagate forward in this same pass (the
            # intermediate hasn't recomputed yet, so a hash check wouldn't catch
            # it). Walks transitively through internal intermediates.
            if run_rec.produced_item_id:
                invalidate_downstream(run_rec.produced_item, dispatch=dispatch)
    if stale:
        logger.info("Sweep: re-dispatched %d stale unit(s)", stale)
    return stale


def resurrect_not_ready_units(*, dispatch: bool = True, origins=None, reason=None) -> int:
    """
    Re-dispatch parked ``NOT_READY`` runs whose required inputs now resolve
    (ADR-0020). A unit that went not-ready waits for an input that hadn't been
    derived yet (e.g. an anomaly slice waiting on its climatology normal); once
    that input exists, nothing event-shaped revives the run — its recipe never
    re-fires on published triggers by design. This pass is the revival.

    Readiness-gated: each candidate's inputs are resolved first and only units
    whose ``recipe.readiness`` now passes are dispatched — a unit whose inputs
    will never materialise stays parked as an honest ``not_ready`` row instead
    of churning the queue every sweep.

    ``origins`` narrows the scan to runs stamped with those origin keys (the
    completion wake-up scopes to the dependent products); ``None`` scans all
    parked runs (the periodic sweep). ``reason`` is recorded on the revived
    run; defaults to ``NOT_READY_SWEEP``. Returns the number re-dispatched.
    """
    from .models import DerivationRun

    if reason is None:
        reason = DerivationRun.RetryReason.NOT_READY_SWEEP
    qs = DerivationRun.objects.filter(status=DerivationRun.Status.NOT_READY)
    if origins is not None:
        origins = list(origins)
        if not origins:
            return 0
        qs = qs.filter(origin__in=origins)

    revived = 0
    for run_rec in qs:
        recipe = _recipe_for(run_rec.recipe_type)
        if recipe is None:
            continue
        try:
            resolved = recipe.resolve_inputs(run_rec.unit_key)
        except Exception as e:  # a unit that can't resolve is skipped, not fatal
            logger.warning("Resurrect: cannot resolve %s: %s", run_rec, e)
            continue
        if not recipe.readiness(run_rec.unit_key, resolved):
            continue
        _dispatch_unit(
            run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch, reason=reason,
        )
        revived += 1
    if revived:
        logger.info("Resurrect: re-dispatched %d now-ready unit(s)", revived)
    return revived


def reclaim_stale_running(*, dispatch: bool = True) -> int:
    """
    Re-dispatch units stuck in RUNNING past the lock timeout.

    A worker that dies mid-unit (crash, OOM, hard time-limit kill, or a dev
    auto-reload) leaves its ``DerivationRun`` in RUNNING with no live task to
    finish it. ``sweep_stale_units`` only inspects *terminal*
    (completed/skipped) runs, so without this pass such a unit is reclaimed only
    when that exact unit happens to be re-triggered by hand or a new input — it
    otherwise sits stuck indefinitely.

    Reclaiming just re-dispatches through the same per-unit primitive: when the
    new task runs, ``run_unit`` → ``DerivationRun.acquire`` atomically steals the
    stale lock (its claim query re-checks the timeout, so a still-live lock is
    never stolen) and recomputes. No ``origin`` is passed, so ``acquire`` keeps
    the run's existing product-origin stamp. Returns the number re-dispatched.
    """
    from django.utils import timezone

    from .models import DerivationRun

    cutoff = timezone.now() - DerivationRun.LOCK_TIMEOUT
    stale_runs = DerivationRun.objects.filter(
        status=DerivationRun.Status.RUNNING, locked_at__lt=cutoff,
    )
    reclaimed = 0
    for run_rec in stale_runs:
        if _recipe_for(run_rec.recipe_type) is None:
            logger.warning(
                "Sweep: stale RUNNING unit %s names unknown recipe '%s' — leaving as-is",
                run_rec, run_rec.recipe_type,
            )
            continue
        logger.warning(
            "Sweep: reclaiming stale RUNNING unit %s (locked_by=%s at %s, past %s) — re-dispatching",
            run_rec, run_rec.locked_by or "-", run_rec.locked_at, DerivationRun.LOCK_TIMEOUT,
        )
        _dispatch_unit(
            run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch,
            reason=DerivationRun.RetryReason.STALE_RUNNING_RECLAIM,
        )
        reclaimed += 1
    if reclaimed:
        logger.info("Sweep: reclaimed %d stale RUNNING unit(s)", reclaimed)
    return reclaimed


def _recipe_for(recipe_type: str):
    from .registry import recipe_registry

    return recipe_registry.get(recipe_type)


def invalidate_downstream(changed_item, *, dispatch: bool = True) -> int:
    """
    Walk ``DerivationLink`` forward from a changed input and recompute every
    item derived from it — transitively, through internal intermediates.

    ``changed_item`` is a StagingItem or a Published Item. Each derived item is
    re-run via its recorded ``DerivationRun`` (recipe_type + unit_key). Returns
    the number of downstream units re-dispatched.
    """
    from georiva.staging.models import DerivationLink, StagingItem

    from .models import DerivationRun

    count = 0
    seen: set[tuple[str, int]] = set()
    frontier = [changed_item]
    while frontier:
        node = frontier.pop()
        if isinstance(node, StagingItem):
            links = DerivationLink.objects.filter(source_staging_item=node)
        else:  # a Published item that is itself an input to further derivations
            links = DerivationLink.objects.filter(source_published_item=node)

        for link in links.select_related("derived_item__collection"):
            derived = link.derived_item
            key = ("item", derived.pk)
            if key in seen:
                continue
            seen.add(key)
            for run_rec in DerivationRun.objects.filter(produced_item=derived):
                _dispatch_unit(
                    run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch,
                    reason=DerivationRun.RetryReason.INPUT_STALE,
                )
                count += 1
            frontier.append(derived)  # continue forward through intermediates
    return count


def dispatch_for_trigger(trigger: dict, *, dispatch: bool = True) -> list:
    """
    Run every registered recipe against an arriving-input ``trigger``.

    Each recipe's ``candidate_units(trigger)`` maps the input back to the units
    it feeds (or ``[]`` if the recipe does not consume this input), so a single
    arriving input auto-triggers exactly the right units across all recipes.
    """
    from .engine import run
    from .registry import recipe_registry

    results = []
    for recipe_type in recipe_registry.all_types():
        recipe = recipe_registry.get(recipe_type)
        if recipe is None:
            continue
        results.extend(run(recipe, trigger, dispatch=dispatch))
    return results
