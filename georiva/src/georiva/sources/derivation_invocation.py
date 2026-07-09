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
    """The product's DerivedProductDefinition, looked up on its feed by key.

    ``DataFeed`` is a django-polymorphic model. Resolve the *real* subclass
    instance before calling ``get_derived_products()``: a caller that fetched the
    product via ``select_related("data_feed")`` gets the **base** ``DataFeed``
    back (select_related can't join the unknown child table), whose base
    ``get_derived_products()`` returns ``[]`` — which would mislabel every product
    as orphaned. ``get_real_instance()`` downcasts to e.g. ``CHIRPSDataFeed`` so
    the declarations are found regardless of how the product was queried.
    """
    feed = product.data_feed
    if hasattr(feed, "get_real_instance"):
        feed = feed.get_real_instance()
    for definition in feed.get_derived_products():
        if definition.key == product.definition_key:
            return definition
    return None


def _binding(product) -> dict:
    """The product's *pinned* collections, flattened into selector keys (ADR-0008/
    0010 §4). Read from the ``DerivedProductInput`` / ``DerivedProductOutput``
    binding rows — so every entry carries a resolved ``collection_id`` (an FK,
    rename-safe) alongside the declared ``collection`` key and ``tier`` a recipe
    reads. Injected into every selector so a recipe can read its source/baseline/
    output collections without reconstructing slugs — the only way a scheduled/
    manual product (no trigger to learn from) knows which collections it operates
    on. The engine still treats the selector as opaque; only recipes interpret it.
    """
    return {
        "inputs": [
            {"role": b.role, "collection": b.source_key, "tier": b.tier,
             "collection_id": b.collection_id}
            for b in product.input_bindings.all()
        ],
        "outputs": [
            {"role": b.role, "collection": b.output_key,
             "collection_id": b.collection_id}
            for b in product.output_bindings.all()
        ],
    }


def collection_routes_to_staging(data_feed, collection_slug: str) -> bool:
    """
    Auto-derived target tier (ADR-0008; ADR-0010 §4): a collection routes to
    staging iff some enabled DerivedProduct of this feed has a *pinned* staging
    input on it. Otherwise it publishes directly (no StagingItems) — "no
    derivation, no staging". Matched by the input binding's ``Collection`` FK
    (resolved from this feed's catalog + slug), not by re-matching declarations,
    so there are no ``get_derived_products()`` calls on this path.
    """
    from georiva.core.models import Collection
    from georiva.sources.models import DerivedProductInput

    if data_feed.catalog_id is None:
        return False
    collection = Collection.objects.filter(
        catalog_id=data_feed.catalog_id, slug=collection_slug
    ).first()
    if collection is None:
        return False
    return DerivedProductInput.objects.filter(
        product__data_feed=data_feed,
        product__is_enabled=True,
        tier="staging",
        collection=collection,
    ).exists()


def dispatch_for_input(trigger: dict, *, dispatch: bool = True) -> list:
    """
    Route an arriving-input ``trigger`` to every enabled DerivedProduct with a
    *pinned* input on the trigger's collection at its tier (ADR-0010 §4). Matched
    by ``collection_id`` + tier against ``DerivedProductInput`` rows in one
    indexed query — feed/catalog scoping falls out of the FK, so an item in one
    catalog can't trigger another catalog's products even with a shared slug, and
    an unbound product (no rows) simply never matches. For each match, build
    ``selector = {**config, **binding, **trigger}`` and run the product's recipe,
    stamping the run with the product origin. No ``get_derived_products()`` here:
    the recipe comes from the stored ``recipe_type`` and the binding from the
    pinned rows.
    """
    from georiva.processing.engine import run
    from georiva.processing.registry import recipe_registry

    from georiva.sources.models import DerivedProduct, DerivedProductInput

    collection_id = trigger.get("collection_id")
    tier = _trigger_tier(trigger)
    if collection_id is None:
        return []

    product_ids = (
        DerivedProductInput.objects.filter(
            collection_id=collection_id, tier=tier, product__is_enabled=True
        )
        .values_list("product_id", flat=True)
        .distinct()
    )

    results = []
    for product in DerivedProduct.objects.filter(pk__in=list(product_ids)):
        recipe = recipe_registry.get(product.recipe_type)
        if recipe is None:
            logger.error(
                "Product %s names unknown recipe '%s'", product.pk, product.recipe_type
            )
            continue
        selector = {**(product.config or {}), **_binding(product), **trigger}
        results.extend(
            run(recipe, selector, origin=product_origin(product), dispatch=dispatch)
        )
    return results


def run_product_now(product, *, dispatch: bool = True) -> list:
    """
    Manually trigger a product (ADR-0008) with a *wide* selector built from its
    config plus its pinned binding (inputs/outputs, with collection_id) and no
    event coordinate, so the recipe enumerates all of the product's units — the
    same path as a backfill. Reuses the engine's run() and the product origin
    stamping. The caller (the tracking view) gates this on product readiness; the
    engine's per-unit readiness still applies underneath.
    """
    from georiva.processing.engine import run
    from georiva.processing.registry import recipe_registry

    # A disabled product is inert on every path — the event and scheduled paths
    # filter on is_enabled upstream, and this manual/backfill overlay is the one
    # entry they don't pre-filter, so it must gate here too.
    if not product.is_enabled:
        return []

    # An orphan (definition the plugin no longer declares) is inert too. The
    # event path excludes it structurally — an orphan has no matching binding row
    # — but the manual/scheduled overlay isn't collection-triggered, so it guards
    # explicitly here (this is not the event dispatch path ADR-0010 §4 keeps
    # get_derived_products-free).
    if definition_for(product) is None:
        return []

    recipe = recipe_registry.get(product.recipe_type)
    if recipe is None:
        logger.error(
            "Product %s names unknown recipe '%s'", product.pk, product.recipe_type
        )
        return []
    origin = product_origin(product)
    logger.info(
        "[run-now] manual run for product %s (key=%s recipe=%s) → origin=%s",
        product.pk, product.definition_key, product.recipe_type, origin,
    )
    selector = {**(product.config or {}), **_binding(product)}
    from georiva.processing.models import DerivationRun

    return run(
        recipe, selector, origin=origin, dispatch=dispatch,
        reason=DerivationRun.RetryReason.MANUAL_RERUN,
    )


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
