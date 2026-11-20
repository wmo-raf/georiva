"""
The Recipe contract — what a derivation family *declares*.

A Recipe declares input selectors, how to enumerate units, a readiness
predicate, a pure transform, and an outputs mapping. It does NOT own the run
loop — the engine does (see engine.py). Recipes may override individual steps
(``resolve_inputs``, ``enumerate_units``, ``readiness``) via these methods.

A ProductionUnit is an **opaque** hashable coordinate whose semantics the
recipe owns; the engine treats it only as an identity/idempotency key.

See docs/adr/0005-generic-derivation-engine.md.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# Re-exported: what a recipe declares to read an input as it is held.
from georiva.core.values.units import SOURCE_UNIT  # noqa: F401

# A ProductionUnit is any JSON-serialisable mapping of coordinates. The engine
# never interprets it; it only serialises + hashes it for the lock/idempotency.
ProductionUnit = dict


def unit_to_canonical_json(unit: ProductionUnit) -> str:
    return json.dumps(unit, sort_keys=True, default=str, separators=(",", ":"))


def unit_hash(unit: ProductionUnit) -> str:
    return hashlib.sha256(unit_to_canonical_json(unit).encode()).hexdigest()


def compute_input_hash(resolved: dict[str, ResolvedInput], recipe_version: str) -> str:
    """input_hash = sha256(sorted(input checksums) + recipe_version)."""
    checksums = sorted(c for ri in resolved.values() for c in ri.checksums if c)
    payload = "|".join(checksums) + "|" + recipe_version
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass
class ResolvedInput:
    """The catalog items + their data assets resolved for one named selector.

    Its values are read with :meth:`field` / :meth:`series`, in the unit the
    recipe declares for the input (``BaseRecipe.input_units``, ADR 0046 §13), in
    the reading session the engine opens for the production unit.
    """

    name: str
    required: bool
    items: list = field(default_factory=list)  # StagingItem | core.Item
    assets: list = field(default_factory=list)  # their data/source assets
    #: The unit this input's values are read in; set by the engine.
    in_unit: str | None = None
    #: The production unit's reading session; set by the engine.
    session: Any = field(default=None, repr=False, compare=False)

    @property
    def present(self) -> bool:
        return len(self.items) > 0

    def field(self, asset):
        """One of this input's assets as a whole field (``values.Field``), in its declared unit."""
        return self._read(lambda s: s.field(asset, in_unit=self._unit()))

    def series(self, asset):
        """One of this input's multi-temporal files over time (an xarray
        DataArray), in its declared unit."""
        return self._read(lambda s: s.series(asset, in_unit=self._unit()))

    def _unit(self) -> str:
        if not self.in_unit:
            raise ValueError(
                f"Input '{self.name}' has no declared unit to be read in; "
                "its recipe declares one in input_units (ADR 0046 §13)."
            )
        return self.in_unit

    def _read(self, read):
        if self.session is not None:
            return read(self.session)
        from georiva.core import values

        with values.session() as s:
            return read(s)

    @property
    def checksums(self) -> list[str]:
        return [getattr(a, "checksum", "") for a in self.assets]


def resolve_declared_inputs(inputs, *, unit=None) -> dict[str, ResolvedInput]:
    """
    Resolve a product's declared inputs into ``ResolvedInput``s by querying the
    catalog — the StagingItem tier for ``tier="staging"`` inputs, the Published
    Item tier for ``tier="published"``.

    Each input is resolved by its **collection identity** when it carries a
    ``collection_id`` (the pinned binding rows do — ADR-0010 §5): the staging
    tier through the ``StagingCollection -> core Collection`` link
    (``collection__collection_id``), the published tier directly
    (``collection_id``). This scopes resolution to one catalog's collection, so
    two catalogs sharing a slug never cross-contaminate, and an operator slug
    rename can't misroute it. An input without a ``collection_id`` (a bare
    ``InputRef`` from the generic declaration contract) falls back to a slug
    match.

    An input whose collection has no rows resolves to an absent
    (``present=False``) ``ResolvedInput`` keyed by its role, which is how product
    readiness reports a blocked product. ``unit`` is accepted for forward
    compatibility and is unused here.
    """
    from georiva.core.models import Item
    from georiva.staging.models import StagingItem

    resolved: dict[str, ResolvedInput] = {}
    for ref in inputs:
        staging = ref.tier == "staging"
        model = StagingItem if staging else Item
        collection_id = getattr(ref, "collection_id", None)
        if collection_id is not None:
            # Resolve by identity (FK), not slug. Staging items reach the core
            # Collection through their StagingCollection's link.
            field = "collection__collection_id" if staging else "collection_id"
            qs = model.objects.filter(**{field: collection_id})
        else:
            qs = model.objects.filter(collection__slug=ref.collection)
        items = list(qs.prefetch_related("assets"))
        assets = [a for it in items for a in it.assets.all()]
        resolved[ref.role] = ResolvedInput(ref.role, required=ref.required, items=items, assets=assets)
    return resolved


def binding_input_collection_id(selector, tier):
    """The pinned ``collection_id`` of the first selector-binding input at
    ``tier`` (ADR-0010 §5), or ``None``. Recipes query the catalog by this FK
    instead of the declared slug, so resolution is catalog-scoped and rename-safe.
    """
    for ref in (selector or {}).get("inputs", []):
        if ref.get("tier") == tier:
            return ref.get("collection_id")
    return None


def binding_output_collection_id(selector, role):
    """The pinned ``collection_id`` of the selector-binding output with ``role``
    (ADR-0010 §5), or ``None`` — the materialised output collection a recipe
    writes into, resolved by FK rather than by rebuilding its slug."""
    for ref in (selector or {}).get("outputs", []):
        if ref.get("role") == role:
            return ref.get("collection_id")
    return None


@dataclass
class OutputItem:
    """The Published Item a unit maps to. The recipe owns this mapping."""

    collection: Any  # core.Collection (recipe resolves/creates)
    time: datetime
    reference_time: datetime | None = None
    bounds: list | None = None
    crs: str = "EPSG:4326"
    width: int | None = None
    height: int | None = None
    properties: dict = field(default_factory=dict)


@dataclass
class OutputAsset:
    """
    One asset the engine should write + register under the output Item.

    Exactly one production mode:
      - ``array`` + ``format="cog"`` (default) → engine materializes the served
                               COG via the shared AssetMaterializer, clipped
                               to the catalog boundary — the same as
                               ingestion writes
      - ``passthrough`` set    → engine copies an existing object (bucket, href)
                               into the assets bucket as-is

    ``format="png"`` is legacy: visual textures are derived on demand by
    Titiler (ADR 0021), so an explicit png OutputAsset is skipped by the engine.
    """

    variable: Any  # core.Variable
    roles: list = field(default_factory=lambda: ["data"])
    format: str = "cog"
    array: Any = None
    passthrough: tuple | None = None  # (bucket_type, source_href)
    bounds: list | None = None
    crs: str = "EPSG:4326"
    width: int | None = None
    height: int | None = None
    stats: dict | None = None
    checksum: str = ""


class BaseRecipe(ABC):
    """Base class for derivation recipes. Subclasses register on the engine."""

    type: str = ""
    version: str = "1"
    #: Whether this recipe creates the Variables of the collections it writes
    #: into. Only then is an output collection stamped with Provenance
    #: (``Collection.derived_by_recipe``, #711) and its Variables exempt from
    #: needing sources; a recipe that copies into a collection of file-read
    #: Variables (promotion) says False.
    creates_output_variables: bool = True
    #: The unit each input is read in, by role (ADR 0046 §13): a unit symbol, or
    #: :data:`SOURCE_UNIT` to read it as it is held, for arithmetic that holds in
    #: any unit and an output written down in the unit it read in. Never the
    #: served unit, which anybody may change. Declaring none — or an input
    #: without one — refuses the recipe at registration.
    input_units: dict[str, str] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    # ---- declarative surface -------------------------------------------------

    @abstractmethod
    def enumerate_units(self, selector: Any) -> Iterable[ProductionUnit]:
        """Which ProductionUnits does this selector cover?"""

    def declared_inputs(self, unit: ProductionUnit) -> list:
        """
        The ``InputRef``s this recipe consumes for one unit (ADR-0008). Default:
        none. A declaration-driven recipe returns its product definition's
        inputs here, so the default ``resolve_inputs`` — and the dependency
        graph and readiness — work without a bespoke override.
        """
        return []

    def resolve_inputs(self, unit: ProductionUnit) -> dict[str, ResolvedInput]:
        """
        Resolve the named input selectors for one unit. The default consumes the
        recipe's ``declared_inputs`` (no hardcoded slugs); recipes whose
        per-unit selection needs more than a collection/tier lookup (e.g.
        Promotion's per-``staging_item_id`` resolution) override this.
        """
        return resolve_declared_inputs(self.declared_inputs(unit), unit=unit)

    def readiness(self, unit: ProductionUnit, resolved: dict[str, ResolvedInput]) -> bool:
        """Default: every required input resolved to at least one item."""
        return all(ri.present for ri in resolved.values() if ri.required)

    @abstractmethod
    def outputs(self, unit: ProductionUnit) -> OutputItem:
        """Map a unit onto the Published Item it produces."""

    @abstractmethod
    def transform(self, unit: ProductionUnit, resolved: dict[str, ResolvedInput]) -> list[OutputAsset]:
        """Pure compute: resolved inputs → output assets for this unit."""

    def output_unit(self, unit: ProductionUnit, resolved: dict[str, ResolvedInput]) -> str:
        """The unit symbol this unit's output values are in: the source unit (and
        the initial served unit) of the output Variables the recipe creates
        (ADR 0046 §13). A difference is declared in a difference unit
        (``core.unit_utils.difference_unit``). A recipe with
        ``creates_output_variables`` must declare it."""
        raise NotImplementedError(f"{type(self).__name__} declares no output unit")

    def output_source_unit(self, unit: ProductionUnit, resolved: dict[str, ResolvedInput]):
        """The ``Unit`` row of :meth:`output_unit`, created if it is new."""
        from georiva.core.models import Unit

        symbol = self.output_unit(unit, resolved)
        found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": symbol})
        return found

    # ---- candidate generation (event-driven; overridable) -------------------

    def candidate_units(self, trigger: Any) -> Iterable[ProductionUnit]:
        """
        Units that an arriving input might make runnable. Default delegates to
        enumerate_units (suitable for scheduled/backfill); event-driven recipes
        override to map an input back to the units it feeds.
        """
        return self.enumerate_units(trigger)
