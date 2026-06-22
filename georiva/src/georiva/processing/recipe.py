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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

# A ProductionUnit is any JSON-serialisable mapping of coordinates. The engine
# never interprets it; it only serialises + hashes it for the lock/idempotency.
ProductionUnit = dict


def unit_to_canonical_json(unit: ProductionUnit) -> str:
    return json.dumps(unit, sort_keys=True, default=str, separators=(",", ":"))


def unit_hash(unit: ProductionUnit) -> str:
    return hashlib.sha256(unit_to_canonical_json(unit).encode()).hexdigest()


def compute_input_hash(resolved: "dict[str, ResolvedInput]", recipe_version: str) -> str:
    """input_hash = sha256(sorted(input checksums) + recipe_version)."""
    checksums = sorted(
        c for ri in resolved.values() for c in ri.checksums if c
    )
    payload = "|".join(checksums) + "|" + recipe_version
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass
class ResolvedInput:
    """The catalog items + their data assets resolved for one named selector."""
    name: str
    required: bool
    items: list = field(default_factory=list)   # StagingItem | core.Item
    assets: list = field(default_factory=list)   # their data/source assets

    @property
    def present(self) -> bool:
        return len(self.items) > 0

    @property
    def checksums(self) -> list[str]:
        return [getattr(a, "checksum", "") for a in self.assets]


@dataclass
class OutputItem:
    """The Published Item a unit maps to. The recipe owns this mapping."""
    collection: Any                       # core.Collection (recipe resolves/creates)
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
      - ``array`` set        → engine writes a COG via AssetWriter.write_cog
      - ``passthrough`` set  → engine copies an existing object (bucket, href)
                               into the assets bucket as-is (e.g. Promotion)
    """
    variable: Any                         # core.Variable
    roles: list = field(default_factory=lambda: ["data"])
    format: str = "cog"
    array: Any = None
    passthrough: tuple | None = None      # (bucket_type, source_href)
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

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    # ---- declarative surface -------------------------------------------------

    @abstractmethod
    def enumerate_units(self, selector: Any) -> Iterable[ProductionUnit]:
        """Which ProductionUnits does this selector cover?"""

    @abstractmethod
    def resolve_inputs(self, unit: ProductionUnit) -> "dict[str, ResolvedInput]":
        """Resolve the named input selectors for one unit."""

    def readiness(self, unit: ProductionUnit, resolved: "dict[str, ResolvedInput]") -> bool:
        """Default: every required input resolved to at least one item."""
        return all(ri.present for ri in resolved.values() if ri.required)

    @abstractmethod
    def outputs(self, unit: ProductionUnit) -> OutputItem:
        """Map a unit onto the Published Item it produces."""

    @abstractmethod
    def transform(
        self, unit: ProductionUnit, resolved: "dict[str, ResolvedInput]"
    ) -> list[OutputAsset]:
        """Pure compute: resolved inputs → output assets for this unit."""

    # ---- candidate generation (event-driven; overridable) -------------------

    def candidate_units(self, trigger: Any) -> Iterable[ProductionUnit]:
        """
        Units that an arriving input might make runnable. Default delegates to
        enumerate_units (suitable for scheduled/backfill); event-driven recipes
        override to map an input back to the units it feeds.
        """
        return self.enumerate_units(trigger)
