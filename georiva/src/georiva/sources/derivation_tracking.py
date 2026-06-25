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

from georiva.sources.derivation_invocation import product_origin


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
