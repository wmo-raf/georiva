"""
Single write-path for derived-product enable/disable (ADR-0008/0009).

Every surface — the wizard, the feed-detail panel, the tracking dashboard —
routes enable/disable through this module so the structural invariant *no
enabled product may have a disabled (or missing) dependency* holds everywhere.

- Enabling is gated on the transitive dependency closure: each upstream product
  must have a row that exists and is enabled, else ``ProductActionError`` names
  the offenders. Data availability is a *separate* runtime gate (``product_
  readiness``) — a whole chain may be enabled before any upstream data exists.
- Disabling cascades to the transitive dependent closure, atomically, with the
  closure recomputed here from the declaration (never trusted from a request).

The chain math lives in the pure ``core.product_chain`` module; this layer binds
it to the feed's ``DerivedProduct`` rows.
"""
from __future__ import annotations

from django.db import transaction
from django.utils.translation import gettext as _


class ProductActionError(Exception):
    """An enable/disable action would break the dependency invariant."""


def _chain(product):
    """Resolve a product's feed chain: (feed, declarations, rows_by_key).
    Declarations come from an *instance* ``get_derived_products()`` on the real
    feed subclass (it binds products to the feed's collections)."""
    feed = product.data_feed.get_real_instance()
    definitions = feed.get_derived_products()
    rows_by_key = {row.definition_key: row for row in feed.derived_products.all()}
    return feed, definitions, rows_by_key


def _label_of(key, definitions):
    for defn in definitions:
        if defn.key == key:
            return defn.label
    return key


def _definition_of(key, definitions):
    for defn in definitions:
        if defn.key == key:
            return defn
    return None


def product_label(product):
    """The product's display name for every surface — the operator override, else
    the declared label, else the definition key (an orphan)."""
    return product.display_label


def build_chain(feed):
    """
    Read-model for the feed-detail chain panel (and the wizard's manage view):
    the feed's declared products laid out in topological stage lanes, each bound
    to its ``DerivedProduct`` row plus run status, readiness, and the catalog
    Collections its outputs have materialised into.

    Truthful across plugin upgrades by merging the live declaration with DB
    state. Returns ``{"stages": [[card, ...], ...], "orphans": [card, ...]}``:

    - a declared definition **with** a row is a normal card;
    - a declared definition **without** a row (added by an upgrade) is a card
      with ``is_new=True`` and ``product=None``;
    - a row whose definition key is **no longer declared** (removed/renamed) is
      an ``orphaned=True`` card in the trailing orphan lane.

    Card keys: ``definition``, ``product``, ``enabled``, ``is_new``,
    ``orphaned``, ``display_label`` / ``display_description``, ``needs``
    (dependency chip labels), ``status`` / ``last_activity``, ``readiness`` /
    ``readiness_hint``, ``output_collections``, ``can_run``.
    """
    from georiva.core.models import Collection
    from georiva.core.product_chain import product_dependencies, topological_stages
    from georiva.sources.derivation_tracking import product_readiness, product_status

    real_feed = feed.get_real_instance()
    definitions = real_feed.get_derived_products()
    rows = {row.definition_key: row for row in real_feed.derived_products.all()}
    declared_keys = {d.key for d in definitions}
    # Chips and card names use each product's display label (override -> declared
    # -> key), so a rename shows on every dependent too.
    label_by_key = {
        d.key: rows[d.key].display_label if d.key in rows else d.label
        for d in definitions
    }
    deps = product_dependencies(definitions)

    def _declared_card(definition):
        product = rows.get(definition.key)
        needs = [label_by_key[k] for k in sorted(deps.get(definition.key, ()))]
        if product is None:
            # A new definition an upgrade added, not yet provisioned.
            return {
                "definition": definition, "product": None, "enabled": False,
                "is_new": True, "orphaned": False,
                "display_label": definition.label,
                "display_description": definition.description,
                "needs": needs, "status": None, "last_activity": None,
                "readiness": None, "readiness_hint": None,
                "output_collections": [], "can_run": False,
            }
        status = product_status(product)
        readiness = product_readiness(product)
        output_slugs = [ref.collection for ref in definition.outputs]
        return {
            "definition": definition, "product": product,
            "enabled": product.is_enabled, "is_new": False, "orphaned": False,
            "display_label": product.display_label,
            "display_description": product.display_description,
            "needs": needs,
            "status": status.status, "last_activity": status.last_completed_at,
            "readiness": readiness,
            "readiness_hint": _readiness_hint(definition, readiness),
            "output_collections": list(
                Collection.objects.filter(
                    catalog=real_feed.catalog, slug__in=output_slugs
                )
            ),
            "can_run": definition.trigger_mode in ("manual", "scheduled"),
        }

    stages = [
        [_declared_card(defn) for defn in stage]
        for stage in topological_stages(definitions)
    ]

    orphans = [
        {
            "definition": None, "product": row, "enabled": row.is_enabled,
            "is_new": False, "orphaned": True,
            "display_label": row.display_label, "display_description": "",
            "needs": [], "status": product_status(row).status,
            "last_activity": None, "readiness": None, "readiness_hint": None,
            "output_collections": [], "can_run": False,
        }
        for row in rows.values() if row.definition_key not in declared_keys
    ]
    return {"stages": stages, "orphans": orphans}


def _readiness_hint(definition, readiness):
    """A human nudge when a product is blocked. If the empty required input is a
    staging-tier one, the data was likely fetched while the product was disabled
    (so it went to sources, not staging) — point the operator at re-running the
    feed to backfill."""
    if readiness.ready or not readiness.blocked_by:
        return None
    blocked_ref = next(
        (r for r in definition.inputs if r.role == readiness.blocked_by), None
    )
    if blocked_ref is not None and blocked_ref.tier == "staging":
        return _(
            "No staged data yet — re-run the feed to backfill this product's input."
        )
    return None


def materialise_output_collections(feed, definition):
    """
    Get-or-create the catalog Collections a product declares as outputs, from
    each ``OutputRef``'s display metadata (name = title, slug fallback;
    description; visibility). Called when a product is enabled — including at
    provision time — so its outputs appear in the catalog with proper titles
    *before* any recipe run.

    Get-or-create **only**, never update: an operator's later rename, description
    or visibility edit survives every subsequent enable, upgrade, or run. The
    recipes' own lazy get_or_create then finds this pre-materialised row and
    becomes an inert fallback. Returns the Collections (created or pre-existing).
    """
    from georiva.core.models import Collection

    collections = []
    for ref in definition.outputs:
        collection, _created = Collection.objects.get_or_create(
            catalog=feed.catalog,
            slug=ref.collection,
            defaults={
                "name": ref.title or ref.collection,
                "description": ref.description,
                "visibility": ref.visibility,
            },
        )
        collections.append(collection)
    return collections


def is_orphaned(product) -> bool:
    """True if the plugin no longer declares this row's definition key (removed
    or renamed by an upgrade). Orphans are inert — every dispatch path already
    skips a product whose ``definition_for`` is None."""
    return product.definition is None


def enable_new_definition(feed, definition, config):
    """
    Inline-provision a declared-but-rowless product (added by a plugin upgrade)
    and enable it — the chain panel's "New — not enabled" action, without a
    wizard re-run. Validates config, writes the row (disabled), then enables it
    through the structural gate (which materialises its output collections). The
    config is validated *before* the row is written, and the enable is atomic, so
    a gate failure leaves no half-enabled row. Returns the enabled product.
    """
    from georiva.sources.models import DerivedProduct

    cleaned = definition.validate_config(config or {})
    with transaction.atomic():
        product, _created = DerivedProduct.objects.update_or_create(
            data_feed=feed,
            definition_key=definition.key,
            defaults={"recipe_type": definition.recipe_type, "config": cleaned},
            create_defaults={
                "recipe_type": definition.recipe_type,
                "config": cleaned,
                "is_enabled": False,
            },
        )
        enable_product(product)
    return product


def delete_orphan(product):
    """Delete an orphaned product's configuration row. Guarded: only a row whose
    definition is gone may be deleted this way. Removes *only* the row — the
    output Collections it once materialised and their published items are kept
    (a DerivedProduct owns no catalog data)."""
    if not is_orphaned(product):
        raise ProductActionError(
            _("'%s' is still declared by the plugin and can't be deleted here.")
            % product.display_label
        )
    product.delete()


def enable_product(product):
    """
    Enable ``product`` after checking every transitive dependency exists and is
    enabled. Raises ``ProductActionError`` (naming the missing dependencies by
    display label) otherwise. Atomic.
    """
    from georiva.core.product_chain import dependencies_closure

    feed, definitions, rows = _chain(product)
    needed = dependencies_closure(definitions, product.definition_key)
    missing = [
        _label_of(key, definitions)
        for key in sorted(needed)
        if rows.get(key) is None or not rows[key].is_enabled
    ]
    if missing:
        raise ProductActionError(
            _("%(product)s needs %(deps)s to be enabled first.") % {
                "product": _label_of(product.definition_key, definitions),
                "deps": ", ".join(missing),
            }
        )
    definition = _definition_of(product.definition_key, definitions)
    with transaction.atomic():
        # Output collections materialise at enable-time, so they appear in the
        # catalog with declared titles before any run.
        if definition is not None:
            materialise_output_collections(feed, definition)
        product.is_enabled = True
        product.save(update_fields=["is_enabled"])
    return product


def enabled_dependents(product):
    """The currently-enabled rows that transitively depend on ``product``, in
    declaration order — the set a disable would cascade to (the confirmation
    list). Excludes ``product`` itself."""
    from georiva.core.product_chain import dependents_closure

    _feed, definitions, rows = _chain(product)
    downstream = dependents_closure(definitions, product.definition_key)
    return [
        rows[defn.key]
        for defn in definitions
        if defn.key in downstream and defn.key in rows and rows[defn.key].is_enabled
    ]


def disable_product(product):
    """
    Disable ``product`` and every enabled product that transitively depends on
    it, in a single transaction. The dependent closure is recomputed here from
    the declaration — never trusted from a caller — so a stale or forged list
    can't leave an enabled product with a disabled dependency. Returns the rows
    that were disabled (the product first, then its dependents).
    """
    dependents = enabled_dependents(product)
    with transaction.atomic():
        disabled = []
        for row in [product, *dependents]:
            if row.is_enabled:
                row.is_enabled = False
                row.save(update_fields=["is_enabled"])
            disabled.append(row)
    return disabled
