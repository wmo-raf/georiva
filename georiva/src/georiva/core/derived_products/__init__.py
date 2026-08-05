"""What a feed's derived products are, and how they depend on one another.

The contract core owns and the feed layer consumes (ADR 0008/0009):

``definitions``  the declaration a plugin returns from
                 ``DataFeed.get_derived_products()`` — the single source of
                 truth the wizard, invocation, tracking and chain UI all read
``chain``        the pure product-level DAG over those declarations. No DB, no
                 run: product P depends on Q iff a *required* input of P at
                 *published* tier names a collection among Q's outputs, unioned
                 with P's explicit ``depends_on``

Both are pure — no ORM, no recipe execution — which is what lets the feed layer
and the wizard read them without importing the engine.

Note the neighbours this is not: ``processing`` *runs* derivations, and
``sources`` tracks and invokes them. This package only says what they are and
what order they go in.

The re-exports below are the surface the old top-level ``core/derived_products.py``
offered, kept so the ~14 call sites in ``sources`` did not have to move with it.
The chain is imported by its own path, ``core.derived_products.chain``.
"""

from .definitions import (  # noqa: F401
    CONFIG_FIELD_TYPES,
    TIERS,
    TRIGGER_MODES,
    VISIBILITIES,
    ConfigField,
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
