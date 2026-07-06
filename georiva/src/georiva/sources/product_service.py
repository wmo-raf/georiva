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
    """Resolve a product's feed chain: (declarations, rows_by_key). Declarations
    come from an *instance* ``get_derived_products()`` on the real feed
    subclass (it binds products to the feed's collections)."""
    feed = product.data_feed.get_real_instance()
    definitions = feed.get_derived_products()
    rows_by_key = {row.definition_key: row for row in feed.derived_products.all()}
    return definitions, rows_by_key


def _label_of(key, definitions):
    for defn in definitions:
        if defn.key == key:
            return defn.label
    return key


def product_label(product):
    """The product's display label from its feed declaration, falling back to
    its definition key (e.g. an orphaned row whose definition is gone)."""
    definitions, _ = _chain(product)
    return _label_of(product.definition_key, definitions)


def enable_product(product):
    """
    Enable ``product`` after checking every transitive dependency exists and is
    enabled. Raises ``ProductActionError`` (naming the missing dependencies by
    display label) otherwise. Atomic.
    """
    from georiva.core.product_chain import dependencies_closure

    definitions, rows = _chain(product)
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
    with transaction.atomic():
        product.is_enabled = True
        product.save(update_fields=["is_enabled"])
    return product


def enabled_dependents(product):
    """The currently-enabled rows that transitively depend on ``product``, in
    declaration order — the set a disable would cascade to (the confirmation
    list). Excludes ``product`` itself."""
    from georiva.core.product_chain import dependents_closure

    definitions, rows = _chain(product)
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
