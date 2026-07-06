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
    """A product: a hyperedge from its input collections to its output ones.

    ``state`` mirrors the management panel's vocabulary so the two agree:
    ``enabled`` (configured and on), ``disabled`` (configured but off), ``new``
    (a declared definition with no row yet), or ``orphaned`` (a row the plugin no
    longer declares). ``product_id`` is None for a ``new`` edge.
    """
    product_id: int | None
    label: str
    recipe_type: str
    trigger_mode: str
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    status: str = "idle"
    ready: bool = True
    reason: str | None = None
    state: str = "enabled"


@dataclass
class ChainGraph:
    nodes: list = field(default_factory=list)
    edges: list = field(default_factory=list)


def build_chain_graph(data_feed) -> ChainGraph:
    """Turn a feed's declarations + configured products into a planned DAG that
    agrees with the management panel: every declared product is an edge tagged
    with its state (enabled / disabled / new), plus a flagged orphan edge for any
    row the plugin no longer declares. Enabled edges keep the status/readiness
    overlay."""
    from georiva.sources.derivation_tracking import product_readiness, product_status
    from georiva.sources.models import DerivedProduct

    real_feed = data_feed.get_real_instance()
    definitions = real_feed.get_derived_products()
    rows = {row.definition_key: row for row in DerivedProduct.objects.filter(data_feed=data_feed)}
    declared_keys = {d.key for d in definitions}

    edges = []
    node_slugs = []

    def _add_nodes(slugs):
        for slug in slugs:
            if slug not in node_slugs:
                node_slugs.append(slug)

    for definition in definitions:
        inputs = [ref.collection for ref in definition.inputs]
        outputs = [ref.collection for ref in definition.outputs]
        _add_nodes(inputs + outputs)
        row = rows.get(definition.key)
        if row is None:
            # A declared definition an upgrade added, not yet provisioned.
            edges.append(ChainEdge(
                product_id=None, label=definition.label,
                recipe_type=definition.recipe_type,
                trigger_mode=definition.trigger_mode,
                inputs=inputs, outputs=outputs, state="new",
            ))
            continue
        readiness = product_readiness(row)
        edges.append(ChainEdge(
            product_id=row.pk, label=row.display_label,
            recipe_type=definition.recipe_type,
            trigger_mode=definition.trigger_mode,
            inputs=inputs, outputs=outputs,
            status=product_status(row).status,
            ready=readiness.ready, reason=readiness.reason,
            state="enabled" if row.is_enabled else "disabled",
        ))

    # Orphaned rows: no declaration, so no inputs/outputs to draw — a flagged,
    # collection-less edge listed after the topology.
    for row in rows.values():
        if row.definition_key not in declared_keys:
            edges.append(ChainEdge(
                product_id=row.pk, label=row.display_label,
                recipe_type=row.recipe_type, trigger_mode="",
                inputs=[], outputs=[],
                status=product_status(row).status, state="orphaned",
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
