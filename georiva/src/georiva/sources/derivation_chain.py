"""
Chain diagram — the planned DAG (ADR-0008).

Builds the pipeline topology a feed declares: collections are nodes, products
are edges (one hyperedge per product, carrying its declared input/output
collections), overlaid with each product's run status and readiness. Built from
the declarations + the configured DerivedProducts — no recipe execution — so
configured-but-unrun (blocked) edges appear alongside the ones that have run.

The read-side topology view; the engine stays unaware (ADR-0005).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from georiva.sources.derivation_invocation import definition_for


@dataclass
class ChainNode:
    """A collection in the pipeline."""
    collection: str


@dataclass
class ChainEdge:
    """A product: a hyperedge from its input collections to its output ones."""
    product_id: int
    label: str
    recipe_type: str
    trigger_mode: str
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    status: str = "idle"
    ready: bool = True
    reason: str | None = None


@dataclass
class ChainGraph:
    nodes: list = field(default_factory=list)
    edges: list = field(default_factory=list)


def build_chain_graph(data_feed) -> ChainGraph:
    """Turn a feed's enabled products + their declarations into a planned DAG."""
    from georiva.sources.derivation_tracking import product_readiness, product_status
    from georiva.sources.models import DerivedProduct

    edges = []
    node_slugs = []
    for product in DerivedProduct.objects.filter(data_feed=data_feed, is_enabled=True):
        definition = definition_for(product)
        if definition is None:
            continue
        inputs = [ref.collection for ref in definition.inputs]
        outputs = [ref.collection for ref in definition.outputs]
        for slug in inputs + outputs:
            if slug not in node_slugs:
                node_slugs.append(slug)
        readiness = product_readiness(product)
        edges.append(ChainEdge(
            product_id=product.pk,
            label=definition.label,
            recipe_type=definition.recipe_type,
            trigger_mode=definition.trigger_mode,
            inputs=inputs,
            outputs=outputs,
            status=product_status(product).status,
            ready=readiness.ready,
            reason=readiness.reason,
        ))

    return ChainGraph(nodes=[ChainNode(slug) for slug in node_slugs], edges=edges)


def item_lineage(item) -> list:
    """The input items a produced item was derived from (ADR-0008) — the
    item-level provenance drill-down, read from DerivationLink. Returns the
    source StagingItems/Items recorded for ``item``."""
    from georiva.staging.models import DerivationLink

    links = (
        DerivationLink.objects
        .filter(derived_item=item)
        .select_related("source_staging_item", "source_published_item")
    )
    sources = []
    for link in links:
        src = link.source_staging_item or link.source_published_item
        if src is not None:
            sources.append(src)
    return sources
