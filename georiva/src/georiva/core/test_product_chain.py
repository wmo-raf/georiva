"""
Tests for the pure product-dependency chain (ADR-0008/0009, issue #166).

The chain is the product-level DAG: product P depends on product Q iff a
*required* input of P at *published* tier names a collection among Q's outputs
(tier-awareness is essential — staging inputs come from the loader, not another
product), unioned with each definition's explicit ``depends_on``. All functions
are pure over a sequence of DerivedProductDefinitions: no DB, no recipe
execution, so the module is importable from both the feed layer and the engine
(ADR-0005). The DB-backed resolution lives elsewhere; these tests assert the
graph math only.

The fixture is CHIRPS-shaped (one resolution): a promotion that serves raw
staging data at published tier, a climatology built from staging, and an anomaly
that consumes staging value + the *published* climatology baseline. The one true
edge is anomaly -> climatology; a tier-blind rule would fabricate
anomaly -> promotion, which these tests guard against.
"""
from django.test import SimpleTestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.product_chain import (
    ChainCycleError,
    ChainError,
    dependencies_closure,
    dependents_closure,
    product_dependencies,
    product_dependents,
    topological_stages,
    validate_chain,
)


def _product(key, *, inputs=(), outputs=(), recipe_type="recipe", depends_on=()):
    return DerivedProductDefinition(
        key=key,
        recipe_type=recipe_type,
        label=key.title(),
        description="",
        config_schema=(),
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        trigger_mode="scheduled",
        depends_on=tuple(depends_on),
    )


def _chirps_defs():
    """CHIRPS 'monthly' resolution: promotion, climatology, anomaly."""
    raw = "chirps-monthly"
    clim = "chirps-monthly-climatology"
    promotion = _product(
        "promotion",
        inputs=(InputRef(role="source", collection=raw, tier="staging"),),
        outputs=(OutputRef(role="served", collection=raw),),
    )
    climatology = _product(
        "climatology",
        inputs=(InputRef(role="value", collection=raw, tier="staging"),),
        outputs=(OutputRef(role="climatology", collection=clim),),
    )
    anomaly = _product(
        "anomaly",
        inputs=(
            InputRef(role="value", collection=raw, tier="staging"),
            InputRef(role="baseline", collection=clim, tier="published"),
        ),
        outputs=(OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),),
    )
    # Declaration order matters for stable topological output.
    return [promotion, climatology, anomaly]


class ProductDependenciesTests(SimpleTestCase):
    def test_anomaly_depends_on_climatology_and_not_promotion(self):
        # anomaly's required published baseline names climatology's output -> an
        # edge; its raw input is staging-tier, so no edge to promotion despite
        # promotion also outputting the raw collection at published tier.
        deps = product_dependencies(_chirps_defs())

        self.assertEqual(deps["anomaly"], {"climatology"})
        self.assertEqual(deps["climatology"], set())
        self.assertEqual(deps["promotion"], set())

    def test_explicit_depends_on_is_unioned_with_inferred_edges(self):
        # A non-data-flow dependency the tier rule can't see is declared
        # explicitly and joins the inferred set.
        defs = [
            _product("a", outputs=(OutputRef(role="o", collection="a-out"),)),
            _product("b", depends_on=("a",)),
        ]

        self.assertEqual(product_dependencies(defs)["b"], {"a"})

    def test_optional_or_staging_inputs_never_create_edges(self):
        # Only required + published inputs infer a dependency.
        producer = _product("p", outputs=(OutputRef(role="o", collection="shared"),))
        optional = _product("opt", inputs=(
            InputRef(role="x", collection="shared", tier="published", required=False),
        ))
        staging = _product("stg", inputs=(
            InputRef(role="x", collection="shared", tier="staging"),
        ))

        deps = product_dependencies([producer, optional, staging])
        self.assertEqual(deps["opt"], set())
        self.assertEqual(deps["stg"], set())


class ProductDependentsTests(SimpleTestCase):
    def test_dependents_is_the_inverse_of_dependencies(self):
        deps = product_dependents(_chirps_defs())

        # climatology is depended on by anomaly; nothing depends on anomaly.
        self.assertEqual(deps["climatology"], {"anomaly"})
        self.assertEqual(deps["anomaly"], set())
        self.assertEqual(deps["promotion"], set())


def _three_tier_chain():
    """a <- b <- c: c depends on b depends on a (published data-flow)."""
    a = _product("a", outputs=(OutputRef(role="o", collection="a-out"),))
    b = _product(
        "b",
        inputs=(InputRef(role="i", collection="a-out", tier="published"),),
        outputs=(OutputRef(role="o", collection="b-out"),),
    )
    c = _product(
        "c",
        inputs=(InputRef(role="i", collection="b-out", tier="published"),),
        outputs=(OutputRef(role="o", collection="c-out"),),
    )
    return [a, b, c]


class ClosureTests(SimpleTestCase):
    def test_dependencies_closure_is_transitive_upstream(self):
        # c needs b needs a -> enabling c requires both b and a (the auto-tick /
        # structural-gate set). Excludes the product itself.
        closure = dependencies_closure(_three_tier_chain(), "c")

        self.assertEqual(closure, {"a", "b"})

    def test_dependents_closure_is_transitive_downstream(self):
        # Disabling a cascades to b and c (the confirmation set).
        closure = dependents_closure(_three_tier_chain(), "a")

        self.assertEqual(closure, {"b", "c"})

    def test_closures_of_an_independent_product_are_empty(self):
        self.assertEqual(dependencies_closure(_three_tier_chain(), "a"), set())
        self.assertEqual(dependents_closure(_three_tier_chain(), "c"), set())


class ValidateChainTests(SimpleTestCase):
    def test_a_well_formed_chain_validates_silently(self):
        self.assertIsNone(validate_chain(_chirps_defs()))

    def test_duplicate_keys_are_rejected(self):
        with self.assertRaises(ChainError):
            validate_chain([_product("dup"), _product("dup")])

    def test_unknown_depends_on_target_is_rejected(self):
        with self.assertRaises(ChainError):
            validate_chain([_product("b", depends_on=("ghost",))])

    def test_explicit_dependency_cycle_is_rejected(self):
        a = _product("a", depends_on=("b",))
        b = _product("b", depends_on=("a",))
        with self.assertRaises(ChainCycleError):
            validate_chain([a, b])

    def test_data_flow_cycle_is_rejected(self):
        # a consumes b's published output and b consumes a's -> a real cycle the
        # tier-aware rule infers, caught before any run.
        a = _product(
            "a",
            inputs=(InputRef(role="i", collection="b-out", tier="published"),),
            outputs=(OutputRef(role="o", collection="a-out"),),
        )
        b = _product(
            "b",
            inputs=(InputRef(role="i", collection="a-out", tier="published"),),
            outputs=(OutputRef(role="o", collection="b-out"),),
        )
        with self.assertRaises(ChainCycleError):
            validate_chain([a, b])

    def test_self_loop_via_data_flow_is_rejected(self):
        # A product consuming its own published output at required tier.
        loop = _product(
            "loop",
            inputs=(InputRef(role="i", collection="loop-out", tier="published"),),
            outputs=(OutputRef(role="o", collection="loop-out"),),
        )
        with self.assertRaises(ChainCycleError):
            validate_chain([loop])

    def test_two_products_declaring_the_same_output_key_are_rejected(self):
        # An output collection may have exactly one producer — two products
        # claiming the same output key is ambiguous provenance (ADR-0010 §1).
        a = _product("a", outputs=(OutputRef(role="o", collection="shared-out"),))
        b = _product("b", outputs=(OutputRef(role="o", collection="shared-out"),))
        with self.assertRaises(ChainError) as ctx:
            validate_chain([a, b])
        self.assertIn("shared-out", str(ctx.exception))

    def test_input_key_outside_the_feed_namespace_is_rejected(self):
        # When collection_keys is supplied, every input must resolve to either a
        # raw collection-definition key or a sibling product's output key
        # (ADR-0010 §1). A key that is neither is a plugin bug.
        product = _product(
            "clim",
            inputs=(InputRef(role="value", collection="ghost-raw", tier="staging"),),
            outputs=(OutputRef(role="o", collection="clim-out"),),
        )
        with self.assertRaises(ChainError) as ctx:
            validate_chain([product], collection_keys={"chirps-monthly"})
        self.assertIn("ghost-raw", str(ctx.exception))

    def test_raw_input_resolving_only_via_a_collection_key_validates(self):
        # A staging input whose collection is a raw definition key (not any
        # product's output — e.g. a feed with no promotion) resolves through the
        # collection namespace and validates silently.
        product = _product(
            "clim",
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="o", collection="chirps-monthly-climatology"),),
        )
        self.assertIsNone(
            validate_chain([product], collection_keys={"chirps-monthly"})
        )

    def test_promotion_output_may_reuse_the_raw_collection_key(self):
        # A promotion serves the raw collection 1:1, so its output key *equals*
        # the raw collection-definition key by design (ADR-0010 §1, softened).
        # This must validate even with the raw collection namespace supplied —
        # the collision rule is output-vs-output only, never output-vs-raw-key.
        promotion = _product(
            "promotion",
            inputs=(InputRef(role="source", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="served", collection="chirps-monthly"),),
        )
        self.assertIsNone(
            validate_chain([promotion], collection_keys={"chirps-monthly"})
        )


class TopologicalStagesTests(SimpleTestCase):
    def test_dependencies_land_in_an_earlier_stage_than_dependents(self):
        stages = topological_stages(_chirps_defs())
        keys_by_stage = [[d.key for d in stage] for stage in stages]

        # climatology (and promotion, independent) come before anomaly; order
        # within a stage follows declaration order.
        self.assertEqual(keys_by_stage, [["promotion", "climatology"], ["anomaly"]])

    def test_stage_order_is_stable_by_declaration(self):
        stages = topological_stages(_three_tier_chain())
        self.assertEqual(
            [[d.key for d in stage] for stage in stages], [["a"], ["b"], ["c"]]
        )

    def test_a_cyclic_chain_raises_rather_than_looping(self):
        a = _product("a", depends_on=("b",))
        b = _product("b", depends_on=("a",))
        with self.assertRaises(ChainCycleError):
            topological_stages([a, b])
