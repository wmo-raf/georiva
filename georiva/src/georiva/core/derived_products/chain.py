"""
Pure product-dependency chain (ADR-0008/0009).

The product-level DAG over a feed's declared derived products: product P depends
on product Q iff a **required** input of P at **published** tier names a
collection among Q's outputs, unioned with P's explicit ``depends_on``. Tier
awareness is essential — a staging-tier input is fed by the loader, not by
another product, so a tier-blind rule would fabricate edges (e.g. anomaly ->
promotion in CHIRPS).

Everything here is pure over a ``Sequence[DerivedProductDefinition]``: no DB, no
recipe execution, so the module is importable from both the feed layer
(``sources``) and the engine (``processing``) without a backwards dependency
(ADR-0005). The DB-backed resolution of declared inputs into catalog items lives
in ``processing``; this module is graph math only.
"""
from __future__ import annotations


class ChainError(ValueError):
    """A derived-product chain declaration is invalid (bad structure)."""


class ChainCycleError(ChainError):
    """The declared products form a dependency cycle."""


def _raw_dependencies(defs) -> dict:
    """Dependency map *including* any self-edge — used for cycle detection so a
    product that (nonsensically) consumes its own output at required published
    tier surfaces as a self-loop rather than being silently dropped."""
    outputs_to_producers: dict[str, set[str]] = {}
    for defn in defs:
        for ref in defn.outputs:
            outputs_to_producers.setdefault(ref.collection, set()).add(defn.key)

    deps: dict[str, set[str]] = {}
    for defn in defs:
        result: set[str] = set()
        for ref in defn.inputs:
            if not ref.required or ref.tier != "published":
                continue
            result |= outputs_to_producers.get(ref.collection, set())
        result |= set(defn.depends_on)
        deps[defn.key] = result
    return deps


def product_dependencies(defs) -> dict:
    """
    Map each product key to the set of product keys it depends on.

    An edge P -> Q exists iff a *required*, *published*-tier input of P names a
    collection among Q's outputs, or Q is listed in P's ``depends_on``. A
    product never lists itself (a self-edge is dropped here; it is caught as a
    cycle by ``validate_chain``).
    """
    deps = _raw_dependencies(defs)
    for key, upstream in deps.items():
        upstream.discard(key)
    return deps


def product_dependents(defs) -> dict:
    """Map each product key to the set of keys that depend on it — the inverse
    of ``product_dependencies``, the cascade direction for disable."""
    deps = product_dependencies(defs)
    dependents: dict[str, set[str]] = {key: set() for key in deps}
    for key, upstream in deps.items():
        for target in upstream:
            dependents.setdefault(target, set()).add(key)
    return dependents


def _closure(adjacency: dict, key) -> set:
    """Transitive reachable set from ``key`` over ``adjacency``, excluding
    ``key`` itself."""
    seen: set[str] = set()
    stack = list(adjacency.get(key, set()))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(adjacency.get(node, set()))
    seen.discard(key)
    return seen


def dependencies_closure(defs, key) -> set:
    """Every product ``key`` transitively depends on — the set that must be
    enabled for ``key`` to be enabled (auto-tick / structural gate)."""
    return _closure(product_dependencies(defs), key)


def dependents_closure(defs, key) -> set:
    """Every product that transitively depends on ``key`` — the set disabling
    ``key`` cascades to (the confirmation set)."""
    return _closure(product_dependents(defs), key)


def output_keys(defs) -> set:
    """Every collection key produced by some product in ``defs``."""
    return {ref.collection for defn in defs for ref in defn.outputs}


def collection_namespace(defs, collection_keys) -> set:
    """The feed-local collection-key namespace an input may reference (ADR-0010
    §1): the feed's raw ``CollectionDefinition`` keys unioned with every product
    output key. A ``None`` ``collection_keys`` means "raw keys unknown here" —
    only the output keys are namespaced (used by the pure structural callers that
    don't have the collection definitions to hand)."""
    return set(collection_keys or ()) | output_keys(defs)


def validate_chain(defs, collection_keys=None) -> None:
    """
    Raise loudly on a malformed chain so a broken plugin fails at first
    render/provision, never silently mid-sweep:

    - duplicate product keys -> ``ChainError``
    - a ``depends_on`` naming a product that isn't declared -> ``ChainError``
    - an output collection declared by more than one product -> ``ChainError``
    - a dependency cycle, whether inferred from data flow or declared via
      ``depends_on`` (a self-loop is the degenerate 1-cycle) -> ``ChainCycleError``

    When ``collection_keys`` is supplied (the feed's raw ``CollectionDefinition``
    keys), every input is additionally required to resolve within the feed-local
    namespace — a raw collection key or a sibling product's output key (ADR-0010
    §1) — else ``ChainError``. Omitting ``collection_keys`` skips only this input
    check; an output key that *equals* a raw collection key is always allowed (a
    promotion serving the raw collection 1:1 reuses its key by design).
    """
    seen: set[str] = set()
    for defn in defs:
        if defn.key in seen:
            raise ChainError(f"duplicate product key '{defn.key}'")
        seen.add(defn.key)

    for defn in defs:
        for dep in defn.depends_on:
            if dep not in seen:
                raise ChainError(
                    f"product '{defn.key}' depends on unknown product '{dep}'"
                )

    producers: dict[str, str] = {}
    for defn in defs:
        for ref in defn.outputs:
            if ref.collection in producers:
                raise ChainError(
                    f"output collection '{ref.collection}' is declared by more "
                    f"than one product ('{producers[ref.collection]}' and "
                    f"'{defn.key}')"
                )
            producers[ref.collection] = defn.key

    graph = _raw_dependencies(defs)
    # Three-colour DFS: WHITE=unseen, GREY=on the current path, BLACK=done.
    # Re-encountering a GREY node closes a cycle.
    WHITE, GREY, BLACK = 0, 1, 2
    colour = {key: WHITE for key in graph}

    def visit(node):
        colour[node] = GREY
        for nxt in graph.get(node, ()):
            if colour[nxt] == GREY:
                raise ChainCycleError(
                    f"derived-product dependency cycle through '{nxt}'"
                )
            if colour[nxt] == WHITE:
                visit(nxt)
        colour[node] = BLACK

    for key in graph:
        if colour[key] == WHITE:
            visit(key)

    if collection_keys is not None:
        namespace = collection_namespace(defs, collection_keys)
        for defn in defs:
            for ref in defn.inputs:
                if ref.collection not in namespace:
                    raise ChainError(
                        f"product '{defn.key}' input '{ref.role}' names "
                        f"collection '{ref.collection}', which is neither a feed "
                        f"collection nor a product output"
                    )


def topological_stages(defs) -> list:
    """
    Group the products into topological stages (Kahn layering): every product in
    stage *n* depends only on products in stages < *n*. Order within a stage,
    and thus across the whole result, follows declaration order — so the wizard's
    stage lanes are stable. Calls ``validate_chain`` first, so a malformed chain
    raises rather than producing a partial layering.
    """
    validate_chain(defs)
    deps = product_dependencies(defs)
    by_key = {defn.key: defn for defn in defs}
    order = [defn.key for defn in defs]

    remaining = set(order)
    placed: set[str] = set()
    stages: list = []
    while remaining:
        ready = [k for k in order if k in remaining and deps[k] <= placed]
        stages.append([by_key[k] for k in ready])
        remaining -= set(ready)
        placed |= set(ready)
    return stages
