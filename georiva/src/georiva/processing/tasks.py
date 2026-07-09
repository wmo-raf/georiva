"""
Celery tasks for the derivation engine.

Per-unit compute runs on the dedicated ``georiva-processing`` queue so a heavy
backfill cannot starve live ingestion. Recovery is via the backfill sweep
(a later slice), so retries are bounded.
"""
import logging

from georiva.config.celery import app
from georiva.processing.constants import (
    RUN_UNIT_HARD_TIME_LIMIT_SECONDS,
    RUN_UNIT_SOFT_TIME_LIMIT_SECONDS,
)

logger = logging.getLogger(__name__)


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    """Register the periodic backfill sweep (mirror of sweep_unprocessed)."""
    try:
        from django_celery_beat.models import IntervalSchedule, PeriodicTask

        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES,
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.processing.sweep_derivations",
            defaults={
                "task": "georiva.processing.tasks.sweep_derivations",
                "interval": schedule_5min,
                "enabled": True,
            },
        )
    except Exception as e:  # DB may be unavailable at import/finalize time
        logger.debug("Skipped derivation sweep schedule setup: %s", e)


@app.task(
    name="georiva.processing.tasks.run_unit_task",
    bind=True,
    max_retries=2,
    acks_late=True,
    queue="georiva-processing",
    # Bound a single unit so a hung/thrashing task can't run indefinitely: the
    # soft limit raises inside the task (run_unit catches it → mark_failed →
    # releases the lock immediately); the hard limit force-kills as a backstop.
    # DerivationRun.LOCK_TIMEOUT is aligned just above the hard limit.
    soft_time_limit=RUN_UNIT_SOFT_TIME_LIMIT_SECONDS,
    time_limit=RUN_UNIT_HARD_TIME_LIMIT_SECONDS,
)
def run_unit_task(self, recipe_type: str, unit: dict, origin: str = None,
                  unit_index: int = None, unit_total: int = None,
                  reason: str = "initial"):
    """Run a single ProductionUnit for a recipe (one DerivationRun).

    ``unit_index``/``unit_total`` are the batch ordinal stamped at dispatch
    (``engine.run``) so each task's logs read ``[unit i/N]`` even though units
    run in independent worker processes.

    ``reason`` records why this run fired (recorded on the DerivationRun). A
    Celery auto-retry (``self.request.retries > 0``) overrides it — from the
    operator's view the most recent trigger is the retry itself, not whatever
    originally dispatched the unit.
    """
    from georiva.processing.engine import run_unit
    from georiva.processing.models import DerivationRun
    from georiva.processing.registry import recipe_registry

    recipe = recipe_registry.get(recipe_type)
    if recipe is None:
        logger.error("Unknown recipe '%s' — dropping unit", recipe_type)
        return

    if self.request.retries:
        reason = DerivationRun.RetryReason.CELERY_RETRY

    worker_id = self.request.id or ""
    pos = f"{unit_index}/{unit_total}" if unit_index and unit_total else "?"
    logger.info(
        "[task %s] run_unit_task received recipe=%s origin=%s reason=%s (celery_id=%s)",
        pos, recipe_type, origin, reason, worker_id or "-",
    )
    result = run_unit(
        recipe, unit, worker_id=worker_id, origin=origin,
        unit_index=unit_index, unit_total=unit_total, reason=reason,
    )
    logger.info("[task %s] run_unit_task finished recipe=%s → %s",
                pos, recipe_type, result.status)

    # Completion chaining: a produced Published item is itself a derivation
    # input, so stream a downstream trigger to its consumers (internal
    # intermediates → their dependent products).
    if result.status == "completed" and result.item_id:
        from georiva.core.models import Item
        from georiva.processing.invocation import (
            dispatch_for_trigger,
            published_item_trigger,
        )

        item = Item.objects.filter(pk=result.item_id).select_related("collection").first()
        if item is not None:
            dispatch_for_trigger(published_item_trigger(item))


@app.task(
    name="georiva.processing.tasks.sweep_derivations",
    queue="georiva-default",
)
def sweep_derivations():
    """
    Periodic backfill sweep — the write-side mirror of ``sweep_unprocessed``.

    Two independent recovery passes, neither depending on an event:
      1. ``sweep_stale_units`` — recomputes units whose inputs changed since they
         were last derived (recorded ``input_hash`` ≠ current).
      2. ``reclaim_stale_running`` — re-dispatches units stuck in RUNNING past the
         lock timeout (a worker died mid-unit), which pass 1 never inspects.
    """
    from georiva.processing.invocation import (
        reclaim_stale_running,
        sweep_stale_units,
    )

    input_stale = sweep_stale_units()
    reclaimed = reclaim_stale_running()
    logger.info(
        "sweep_derivations: %d input-stale re-dispatched, %d stale-RUNNING reclaimed",
        input_stale, reclaimed,
    )
    return {"input_stale": input_stale, "reclaimed": reclaimed}


@app.task(
    name="georiva.processing.tasks.run_recipe_task",
    queue="georiva-default",
)
def run_recipe_task(recipe_type: str, selector: dict = None):
    """Enumerate a recipe's units for a selector and fan out per-unit tasks."""
    from georiva.processing.engine import run
    from georiva.processing.registry import recipe_registry

    recipe = recipe_registry.get(recipe_type)
    if recipe is None:
        logger.error("Unknown recipe '%s'", recipe_type)
        return

    run(recipe, selector or {}, dispatch=True)
