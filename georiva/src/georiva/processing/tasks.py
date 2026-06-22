"""
Celery tasks for the derivation engine.

Per-unit compute runs on the dedicated ``georiva-processing`` queue so a heavy
backfill cannot starve live ingestion. Recovery is via the backfill sweep
(a later slice), so retries are bounded.
"""
import logging

from georiva.config.celery import app

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.processing.tasks.run_unit_task",
    bind=True,
    max_retries=2,
    acks_late=True,
    queue="georiva-processing",
)
def run_unit_task(self, recipe_type: str, unit: dict):
    """Run a single ProductionUnit for a recipe (one DerivationRun)."""
    from georiva.processing.engine import run_unit
    from georiva.processing.registry import recipe_registry

    recipe = recipe_registry.get(recipe_type)
    if recipe is None:
        logger.error("Unknown recipe '%s' — dropping unit", recipe_type)
        return

    worker_id = self.request.id or ""
    run_unit(recipe, unit, worker_id=worker_id)


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
