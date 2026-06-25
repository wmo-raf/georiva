"""
Generic derived-product contract (ADR-0008).

Plugins declare the derived products a feed offers by implementing
``DataFeed.get_derived_products()`` and returning a list of
``DerivedProductDefinition``. This is the single source of truth the wizard
(form + validation), invocation (selector building), tracking (run grouping),
and chain UI (the planned DAG) all read from.

The contract is **pure declaration** — dataclasses with string enums and no
database or engine imports. It lives in ``core`` so both the feed layer
(``sources``) and the engine (``processing``) can import it without a backwards
dependency (ADR-0005): the engine must not depend on the feed layer, and a
shared declaration in ``core`` keeps the contract reachable from both.

The DB-backed resolution of a declared ``InputRef`` into the catalog items it
points at lives in ``processing`` (``resolve_declared_inputs``), not here, so
``core`` stays free of ``staging``/``processing`` imports.
"""
from __future__ import annotations

from dataclasses import dataclass

TRIGGER_MODES = ("event", "scheduled", "manual")
CONFIG_FIELD_TYPES = ("str", "int", "float", "bool", "choice")
TIERS = ("staging", "published")


@dataclass(frozen=True)
class InputRef:
    """One declared input a product consumes: a collection at a tier."""
    role: str
    collection: str
    tier: str
    required: bool = True

    def __post_init__(self):
        if not self.role:
            raise ValueError("InputRef: 'role' is required and must be non-empty")
        if not self.collection:
            raise ValueError("InputRef: 'collection' is required and must be non-empty")
        if self.tier not in TIERS:
            raise ValueError(
                f"InputRef '{self.role}': tier must be one of {TIERS}, got '{self.tier}'"
            )


@dataclass(frozen=True)
class OutputRef:
    """One collection a product produces."""
    role: str
    collection: str

    def __post_init__(self):
        if not self.role:
            raise ValueError("OutputRef: 'role' is required and must be non-empty")
        if not self.collection:
            raise ValueError("OutputRef: 'collection' is required and must be non-empty")


@dataclass(frozen=True)
class ConfigField:
    """One operator-configurable option, driving the wizard form + validation."""
    key: str
    type: str
    default: object = None
    choices: tuple = None

    def __post_init__(self):
        if self.type not in CONFIG_FIELD_TYPES:
            raise ValueError(
                f"ConfigField '{self.key}': type must be one of "
                f"{CONFIG_FIELD_TYPES}, got '{self.type}'"
            )
        if self.type == "choice":
            if not self.choices:
                raise ValueError(f"ConfigField '{self.key}': choice type requires 'choices'")
            if self.default is not None and self.default not in self.choices:
                raise ValueError(
                    f"ConfigField '{self.key}': default '{self.default}' is not "
                    f"among choices {self.choices}"
                )


@dataclass(frozen=True)
class DerivedProductDefinition:
    """Blueprint for one derived product a feed can produce (mirrors
    CollectionDefinition)."""
    key: str
    recipe_type: str
    label: str
    description: str
    config_schema: tuple
    inputs: tuple
    outputs: tuple
    trigger_mode: str

    def __post_init__(self):
        for field in ("key", "recipe_type", "label"):
            if not getattr(self, field):
                raise ValueError(
                    f"DerivedProductDefinition: '{field}' is required and must be non-empty"
                )
        if self.trigger_mode not in TRIGGER_MODES:
            raise ValueError(
                f"DerivedProductDefinition '{self.key}': trigger_mode must be "
                f"one of {TRIGGER_MODES}, got '{self.trigger_mode}'"
            )

    def dependency_edges(self) -> list:
        """The input collections this product consumes, as
        ``(collection, tier, required)`` tuples — the product's incoming edges
        in the chain DAG. Pure: derived from the declaration with no DB access
        or recipe execution."""
        return [(ref.collection, ref.tier, ref.required) for ref in self.inputs]
