"""
Product-driven derivation invocation (ADR-0008).

The application-layer flip of derivation from *recipe-driven* to
*DerivedProduct-driven*: an arriving input finds the enabled DerivedProducts
whose declared inputs match its collection/tier, builds a selector from each
product's config, and calls the engine's generic run(recipe, selector), stamping
the run with the product origin.

This is the ONLY place that joins the feed layer (DerivedProduct) to the engine,
so the engine itself never imports DerivedProduct (ADR-0005). The feed layer
depending on the engine is the allowed direction.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def product_origin(product) -> str:
    """The opaque DerivationRun.origin key for a product — the stable identity
    the tracking UI groups runs by (ADR-0008)."""
    return f"derived_product:{product.pk}"


def _trigger_tier(trigger: dict) -> str:
    """The tier of the arriving input: staging items vs published items."""
    return "staging" if trigger.get("staging_item_id") else "published"


def definition_for(product):
    """The product's DerivedProductDefinition, looked up on its feed by key."""
    for definition in product.data_feed.get_derived_products():
        if definition.key == product.definition_key:
            return definition
    return None


def _matches(definition, collection_slug: str, tier: str) -> bool:
    """True if the definition declares an input on this collection at this tier."""
    return any(
        ref.collection == collection_slug and ref.tier == tier
        for ref in definition.inputs
    )


def collection_routes_to_staging(data_feed, collection_slug: str) -> bool:
    """
    Auto-derived target tier (ADR-0008): a collection routes to staging iff some
    enabled DerivedProduct of this feed consumes it at the staging tier.
    Otherwise it publishes directly (no StagingItems) — "no derivation, no
    staging". Replaces the manual DataFeed.target_tier field, removing the
    publish-vs-products drift.
    """
    from georiva.sources.models import DerivedProduct

    for product in DerivedProduct.objects.filter(data_feed=data_feed, is_enabled=True):
        definition = definition_for(product)
        if definition is not None and _matches(definition, collection_slug, "staging"):
            return True
    return False


def dispatch_for_input(trigger: dict, *, dispatch: bool = True) -> list:
    """
    Route an arriving-input ``trigger`` to every enabled DerivedProduct that
    consumes it. For each match, build ``selector = {**config, **trigger}`` and
    run the product's recipe, stamping the run with the product origin.
    """
    from georiva.processing.engine import run
    from georiva.processing.registry import recipe_registry

    from georiva.sources.models import DerivedProduct

    collection_slug = trigger.get("collection_slug")
    tier = _trigger_tier(trigger)

    results = []
    for product in DerivedProduct.objects.filter(is_enabled=True):
        definition = definition_for(product)
        if definition is None or not _matches(definition, collection_slug, tier):
            continue
        recipe = recipe_registry.get(definition.recipe_type)
        if recipe is None:
            logger.error("Product %s names unknown recipe '%s'", product.pk, definition.recipe_type)
            continue
        selector = {**(product.config or {}), **trigger}
        results.extend(
            run(recipe, selector, origin=product_origin(product), dispatch=dispatch)
        )
    return results


def run_product_now(product, *, dispatch: bool = True) -> list:
    """
    Manually trigger a product (ADR-0008) with a *wide* selector built from its
    config and no event coordinate, so the recipe enumerates all of the
    product's units — the same path as a backfill. Reuses the engine's run() and
    the product origin stamping. The caller (the tracking view) gates this on
    product readiness; the engine's per-unit readiness still applies underneath.
    """
    from georiva.processing.engine import run
    from georiva.processing.registry import recipe_registry

    definition = definition_for(product)
    if definition is None:
        return []
    recipe = recipe_registry.get(definition.recipe_type)
    if recipe is None:
        logger.error("Product %s names unknown recipe '%s'", product.pk, definition.recipe_type)
        return []
    selector = dict(product.config or {})
    return run(recipe, selector, origin=product_origin(product), dispatch=dispatch)


def dispatch_due_scheduled_products(*, dispatch: bool = True) -> int:
    """
    The scheduled-product beat (ADR-0008): fire every enabled DerivedProduct
    whose declared trigger_mode is ``scheduled`` and whose interval has elapsed,
    via the same product-driven path as a manual/backfill run. Event-driven and
    manual products are never fired here. Returns the number dispatched.
    """
    from django.utils import timezone

    from georiva.sources.models import DerivedProduct

    dispatched = 0
    for product in DerivedProduct.objects.filter(is_enabled=True):
        definition = definition_for(product)
        if definition is None or definition.trigger_mode != "scheduled":
            continue
        if not product.is_due():
            continue
        run_product_now(product, dispatch=dispatch)
        product.last_run_at = timezone.now()
        product.save(update_fields=["last_run_at"])
        dispatched += 1
    return dispatched
