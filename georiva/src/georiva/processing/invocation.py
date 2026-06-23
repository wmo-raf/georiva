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
    """The arriving-input trigger for a newly registered StagingItem."""
    return {
        "staging_item_id": staging_item.pk,
        "collection_slug": staging_item.collection.slug,
    }


def published_item_trigger(item) -> dict:
    """The arriving-input trigger for a Published item produced by a derivation
    (an intermediate that may itself feed a further derivation)."""
    return {
        "published_item_id": item.pk,
        "collection_slug": item.collection.slug,
    }


def _dispatch_unit(recipe_type: str, unit: dict, *, dispatch: bool = True) -> None:
    """Re-run one known ProductionUnit via the same per-unit primitive ``run``
    fans out to — no enumeration/orchestration is duplicated."""
    if dispatch:
        from .tasks import run_unit_task
        run_unit_task.delay(recipe_type=recipe_type, unit=unit)
    else:
        from .engine import run_unit
        from .registry import recipe_registry
        recipe = recipe_registry.get(recipe_type)
        if recipe is not None:
            run_unit(recipe, unit)


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
            _dispatch_unit(run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch)
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
                _dispatch_unit(run_rec.recipe_type, run_rec.unit_key, dispatch=dispatch)
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
