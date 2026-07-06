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
    """The product's display label from its feed declaration, falling back to
    its definition key (e.g. an orphaned row whose definition is gone)."""
    _feed, definitions, _rows = _chain(product)
    return _label_of(product.definition_key, definitions)


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
