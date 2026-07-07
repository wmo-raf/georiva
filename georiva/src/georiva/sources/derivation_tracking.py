"""
Product run-tracking (ADR-0008).

The read-side counterpart of product-driven invocation: summarise a
DerivedProduct's DerivationRuns for the tracking view by joining on the opaque
`origin` key the invocation layer stamped. The engine never groups by product —
this application-layer aggregate does, keeping the ADR-0005 layering intact.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from georiva.sources.derivation_invocation import definition_for, product_origin


@dataclass
class ProductReadiness:
    """Whether a product can run now (ADR-0008) — a coarse gate computed from the
    declared inputs, in front of the engine's per-unit readiness."""
    ready: bool
    blocked_by: str | None = None     # role of the first empty required input
    reason: str | None = None         # human reason, e.g. "normals empty"


def product_readiness(product) -> ProductReadiness:
    """
    A product is ready iff every *required* declared input is pinned to a
    collection that exists and is non-empty — resolved from the product's binding
    rows by collection identity (ADR-0010 §5), no recipe execution. A required
    input with no binding row (unbound) blocks, as does one whose collection has
    no items. Optional inputs never block. When blocked, names the first offender.
    """
    from georiva.processing.recipe import resolve_declared_inputs

    definition = definition_for(product)
    if definition is None:
        return ProductReadiness(ready=False, reason="no product definition")

    bindings = {b.role: b for b in product.input_bindings.all()}
    resolved = resolve_declared_inputs(list(bindings.values()))
    for ref in definition.inputs:
        if not ref.required:
            continue
        if ref.role not in bindings or not resolved[ref.role].present:
            return ProductReadiness(
                ready=False, blocked_by=ref.role, reason=f"{ref.role} empty",
            )
    return ProductReadiness(ready=True)


@dataclass
class ProductStatus:
    """A product's aggregate run state for the tracking view."""
    status: str                                  # idle | running | failed | completed
    total: int = 0
    counts: dict = field(default_factory=dict)
    last_completed_at: datetime | None = None


def product_status(product) -> ProductStatus:
    """Aggregate a product's DerivationRuns (joined by origin) into one status."""
    from georiva.processing.models import DerivationRun

    runs = DerivationRun.objects.filter(origin=product_origin(product))
    counts = dict(Counter(runs.values_list("status", flat=True)))
    total = sum(counts.values())
    if total == 0:
        return ProductStatus(status="idle")

    completed = runs.filter(status=DerivationRun.Status.COMPLETED)
    last_completed_at = (
        completed.order_by("-completed_at")
        .values_list("completed_at", flat=True)
        .first()
    )

    if runs.filter(status=DerivationRun.Status.RUNNING).exists():
        status = "running"
    elif runs.filter(status=DerivationRun.Status.FAILED).exists():
        status = "failed"
    elif completed.exists():
        status = "completed"
    else:
        status = "idle"

    return ProductStatus(
        status=status, total=total, counts=counts, last_completed_at=last_completed_at,
    )
