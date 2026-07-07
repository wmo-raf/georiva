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
                "is_new": True, "orphaned": False, "unbound": False,
                "display_label": definition.label,
                "display_description": definition.description,
                "needs": needs, "status": None, "last_activity": None,
                "readiness": None, "readiness_hint": None,
                "output_collections": [], "can_run": False,
            }
        status = product_status(product)
        readiness = product_readiness(product)
        return {
            "definition": definition, "product": product,
            "enabled": product.is_enabled, "is_new": False, "orphaned": False,
            "unbound": _is_unbound(product, definition),
            "display_label": product.display_label,
            "display_description": product.display_description,
            "needs": needs,
            "status": status.status, "last_activity": status.last_completed_at,
            "readiness": readiness,
            "readiness_hint": _readiness_hint(definition, readiness),
            # Output collections are read from the pinned DerivedProductOutput
            # rows (ADR-0010 §2), so an operator slug rename can't drop them.
            "output_collections": [
                b.collection
                for b in product.output_bindings.select_related("collection")
            ],
            "can_run": definition.trigger_mode in ("manual", "scheduled"),
        }

    stages = [
        [_declared_card(defn) for defn in stage]
        for stage in topological_stages(definitions)
    ]

    orphans = [
        {
            "definition": None, "product": row, "enabled": row.is_enabled,
            "is_new": False, "orphaned": True, "unbound": False,
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


def _is_unbound(product, definition) -> bool:
    """True if an enabled, still-declared product has lost binding coverage — its
    declared inputs/outputs aren't all pinned to a Collection (ADR-0010 §6). The
    usual cause is a bound Collection deleted outside the feed lifecycle: the
    binding row cascades away, leaving the product inert on dispatch (slice 4
    matches by binding row) until it is re-bound.

    Gated on the product having *some* binding row: a row that has never been
    through enable-time pinning (only an artificial or pre-backfill state) has
    zero rows and is not flagged, while a real product — always materialised with
    output bindings at enable — is flagged the moment any row goes missing."""
    if not product.is_enabled:
        return False
    input_roles = set(product.input_bindings.values_list("role", flat=True))
    output_roles = set(product.output_bindings.values_list("role", flat=True))
    if not input_roles and not output_roles:
        return False
    missing_input = any(r.role not in input_roles for r in definition.inputs)
    missing_output = any(r.role not in output_roles for r in definition.outputs)
    return missing_input or missing_output


def is_unbound(product) -> bool:
    """Public predicate mirroring ``is_orphaned`` for the panel/view: an enabled,
    declared product whose binding rows no longer cover its declaration (ADR-0010
    §6). A row with no live definition (an orphan) is not unbound — orphan is the
    other, distinct drift state."""
    definition = product.definition
    if definition is None:
        return False
    return _is_unbound(product, definition)


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


def _feed_collection_keys(feed) -> set:
    """The raw collection keys this feed can resolve an input to: the keys the
    plugin declares (``get_collection_definitions``) unioned with the
    ``definition_key``s of the collections actually provisioned on this feed (its
    ``DataFeedCollectionLink``s). The latter is what a raw input pins through, so
    a provisioned-but-not-declared link still counts."""
    keys = {d.key for d in type(feed).get_collection_definitions()}
    keys |= set(feed.collection_links.values_list("definition_key", flat=True))
    keys.discard("")
    return keys


def _resolve_inputs_or_raise(feed, definition, definitions):
    """Every declared input of ``definition`` must resolve within the feed's
    collection-key namespace — a raw ``CollectionDefinition`` key or a sibling
    product's output key (ADR-0010 §2). Raises ``ProductActionError`` naming the
    first key that doesn't, so a mis-declared product fails loudly at enable time
    rather than sitting inert (this slice checks resolvability; the next pins the
    resolved ``Collection`` as rows)."""
    from georiva.core.product_chain import collection_namespace

    namespace = collection_namespace(definitions, _feed_collection_keys(feed))
    for ref in definition.inputs:
        if ref.collection not in namespace:
            raise ProductActionError(
                _("%(product)s input '%(role)s' names collection "
                  "'%(collection)s', which this feed neither provides nor "
                  "produces.") % {
                    "product": _label_of(definition.key, definitions),
                    "role": ref.role,
                    "collection": ref.collection,
                }
            )


def enable_product(product):
    """
    Enable ``product`` after checking (a) every declared input resolves within
    the feed namespace and (b) every transitive dependency exists and is
    enabled. Raises ``ProductActionError`` (naming the unresolved input key, or
    the missing dependencies by display label) otherwise. Atomic — a gate
    failure leaves the row unchanged.
    """
    from georiva.core.product_chain import dependencies_closure

    feed, definitions, rows = _chain(product)
    definition = _definition_of(product.definition_key, definitions)
    if definition is not None:
        _resolve_inputs_or_raise(feed, definition, definitions)
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
        # Output collections materialise at enable-time, so they appear in the
        # catalog with declared titles before any run.
        if definition is not None:
            materialise_and_pin(product, definition, feed)
        product.is_enabled = True
        product.save(update_fields=["is_enabled"])
    return product


def rebind_product(product):
    """Re-run enable-time resolution for an unbound product, restoring its binding
    rows (ADR-0010 §6) — the chain panel's "re-bind" action. Re-materialises a
    deleted output collection and re-pins every input/output that resolves. Raises
    ``ProductActionError`` if a *required* input still can't be resolved (its
    collection is gone and un-recreatable here), atomically — a failed re-bind
    leaves no partial rows. Only meaningful for an enabled, declared product."""
    feed, definitions, rows = _chain(product)
    definition = _definition_of(product.definition_key, definitions)
    if definition is None:
        raise ProductActionError(
            _("'%s' is no longer declared by the plugin and can't be re-bound.")
            % product.display_label
        )
    with transaction.atomic():
        materialise_and_pin(product, definition, feed)
        bound = set(product.input_bindings.values_list("role", flat=True))
        missing = [
            ref.role for ref in definition.inputs
            if ref.required and ref.role not in bound
        ]
        if missing:
            raise ProductActionError(
                _("%(product)s can't be re-bound: required input(s) %(roles)s "
                  "have no collection — provision or re-create it first.") % {
                    "product": _label_of(definition.key, definitions),
                    "roles": ", ".join(sorted(missing)),
                }
            )
    return product


def materialise_and_pin(product, definition, feed):
    """Materialise a product's output collections (get-or-create) and pin its
    input/output bindings to the resolved catalog Collections — the two coupled
    writes every enable path performs (ADR-0010 §2). Idempotent."""
    collections = materialise_output_collections(feed, definition)
    pin_bindings(product, definition, feed, collections)


def pin_bindings(product, definition, feed, output_collections):
    """Resolve ``definition``'s declared inputs/outputs to catalog ``Collection``s
    and pin them as ``DerivedProductInput`` / ``DerivedProductOutput`` rows,
    upserting on ``(product, role)`` (ADR-0010 §2). Idempotent — a re-enable or
    upgrade re-resolves and re-pins in place. ``output_collections`` are the rows
    ``materialise_output_collections`` just get-or-created, in declaration order.

    Output roles always resolve (we just materialised them). An input resolves to
    the raw collection its ``CollectionDefinition`` link points at, or the sibling
    product's materialised output collection; an input that can't resolve yet
    (its collection not provisioned) is left unpinned and re-pins on a later
    enable — surfaced as an *unbound* product in a later slice, never a hard
    failure here."""
    from georiva.sources.models import DerivedProductInput, DerivedProductOutput

    for ref, collection in zip(definition.outputs, output_collections):
        DerivedProductOutput.objects.update_or_create(
            product=product, role=ref.role,
            defaults={"output_key": ref.collection, "collection": collection},
        )

    for ref in definition.inputs:
        collection = _resolve_input_collection(feed, ref)
        if collection is None:
            continue
        DerivedProductInput.objects.update_or_create(
            product=product, role=ref.role,
            defaults={
                "tier": ref.tier,
                "required": ref.required,
                "source_key": ref.collection,
                "collection": collection,
            },
        )


def backfill_bindings() -> int:
    """Pin bindings for every enabled ``DerivedProduct`` that predates pinning —
    the one-time ADR-0010 §2 backfill, called from the data migration. Re-runs
    the enable-time resolution (materialise get-or-creates the output collections,
    then pin upserts), so it is idempotent and safe to run more than once. Orphans
    (declaration gone) and products whose inputs aren't provisioned are skipped
    without error. Returns the number of products pinned."""
    from georiva.sources.models import DerivedProduct

    pinned = 0
    for product in DerivedProduct.objects.filter(is_enabled=True):
        feed = product.data_feed.get_real_instance()
        definition = _definition_of(
            product.definition_key, feed.get_derived_products()
        )
        if definition is None:
            continue
        materialise_and_pin(product, definition, feed)
        pinned += 1
    return pinned


def _resolve_input_collection(feed, ref):
    """The catalog ``Collection`` an input key resolves to, or ``None`` if not yet
    provisioned. A raw key resolves through its ``DataFeedCollectionLink``
    (``definition_key`` → the raw collection); any other key resolves as a
    sibling product's materialised output collection (slug == output key)."""
    from georiva.core.models import Collection
    from georiva.sources.models import DataFeedCollectionLink

    link = DataFeedCollectionLink.objects.filter(
        data_feed=feed, definition_key=ref.collection
    ).select_related("collection").first()
    if link is not None:
        return link.collection
    return Collection.objects.filter(
        catalog=feed.catalog, slug=ref.collection
    ).first()


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
