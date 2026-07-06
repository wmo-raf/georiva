"""
Tests for the generic derived-product contract (ADR-0008, issue #143).

The contract is pure declaration: dataclasses plugins implement to describe the
derived products a feed offers. These tests assert validation and the
declaration-derived dependency graph — no DB, no recipe execution. The DB-backed
resolver (InputRef -> ResolvedInput) is tested in the processing app.

Mirrors the validation style of sources/collection_definitions.py's
CollectionVariable.
"""
from django.test import SimpleTestCase

from georiva.core.derived_products import (
    ConfigField,
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)


class InputRefTests(SimpleTestCase):
    def test_staging_and_published_tiers_are_accepted(self):
        for tier in ("staging", "published"):
            self.assertEqual(
                InputRef(role="value", collection="rainfall", tier=tier).tier, tier
            )

    def test_unknown_tier_is_rejected(self):
        with self.assertRaises(ValueError):
            InputRef(role="value", collection="rainfall", tier="archive")

    def test_required_defaults_to_true(self):
        self.assertTrue(
            InputRef(role="value", collection="rainfall", tier="staging").required
        )

    def test_empty_role_or_collection_is_rejected(self):
        with self.assertRaises(ValueError):
            InputRef(role="", collection="rainfall", tier="staging")
        with self.assertRaises(ValueError):
            InputRef(role="value", collection="", tier="staging")


class OutputRefTests(SimpleTestCase):
    def test_valid_output_exposes_its_fields(self):
        ref = OutputRef(role="anomaly", collection="rainfall-anomaly")
        self.assertEqual(ref.collection, "rainfall-anomaly")

    def test_empty_role_or_collection_is_rejected(self):
        with self.assertRaises(ValueError):
            OutputRef(role="", collection="rainfall-anomaly")
        with self.assertRaises(ValueError):
            OutputRef(role="anomaly", collection="")


def _definition(**overrides):
    """A minimal valid DerivedProductDefinition, overridable per-test."""
    kwargs = dict(
        key="anomaly",
        recipe_type="climatology",
        label="Rainfall anomaly",
        description="Anomaly vs a baseline climatology.",
        config_schema=(),
        inputs=(InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode="scheduled",
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


class DerivedProductDefinitionTests(SimpleTestCase):
    def test_valid_definition_exposes_its_fields(self):
        definition = _definition()

        self.assertEqual(definition.key, "anomaly")
        self.assertEqual(definition.recipe_type, "climatology")
        self.assertEqual(definition.label, "Rainfall anomaly")
        self.assertEqual(definition.trigger_mode, "scheduled")
        self.assertEqual(definition.inputs[0].collection, "rainfall")
        self.assertEqual(definition.outputs[0].collection, "rainfall-anomaly")

    def test_unknown_trigger_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            _definition(trigger_mode="hourly")

    def test_event_scheduled_manual_trigger_modes_are_accepted(self):
        for mode in ("event", "scheduled", "manual"):
            self.assertEqual(_definition(trigger_mode=mode).trigger_mode, mode)

    def test_empty_required_field_is_rejected(self):
        for field in ("key", "recipe_type", "label"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _definition(**{field: ""})

    def test_default_enabled_defaults_to_true(self):
        # An operator sees each product pre-ticked in the wizard unless the
        # plugin declares otherwise, so existing declarations keep provisioning
        # everything without change.
        self.assertTrue(_definition().default_enabled)

    def test_default_enabled_can_be_declared_false(self):
        self.assertFalse(_definition(default_enabled=False).default_enabled)

    def test_depends_on_defaults_to_empty(self):
        self.assertEqual(_definition().depends_on, ())

    def test_depends_on_accepts_declared_extras(self):
        # Non-data-flow dependencies the tier-aware rule can't infer are declared
        # explicitly; the chain module unions them with the inferred edges.
        self.assertEqual(
            _definition(depends_on=("climatology",)).depends_on, ("climatology",)
        )

    def test_empty_depends_on_entry_is_rejected(self):
        with self.assertRaises(ValueError):
            _definition(depends_on=("",))

    def test_self_referential_depends_on_is_rejected(self):
        with self.assertRaises(ValueError):
            _definition(key="anomaly", depends_on=("anomaly",))

    def test_dependency_edges_derived_from_declared_inputs(self):
        # The dependency graph is computable from the declaration alone — no DB,
        # no recipe execution — so the chain UI and readiness can be built ahead
        # of any run.
        definition = _definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="staging"),
            InputRef(role="normals", collection="rainfall-normals",
                     tier="published", required=False),
        ))

        self.assertEqual(
            definition.dependency_edges(),
            [
                ("rainfall", "staging", True),
                ("rainfall-normals", "published", False),
            ],
        )


class ValidateConfigTests(SimpleTestCase):
    def _definition_with_schema(self, *fields):
        return _definition(config_schema=tuple(fields))

    def test_fills_defaults_for_missing_keys(self):
        definition = self._definition_with_schema(
            ConfigField(key="min_years", type="int", default=30),
            ConfigField(key="quantity", type="choice",
                        choices=("anomaly", "value"), default="anomaly"),
        )

        cleaned = definition.validate_config({})

        self.assertEqual(cleaned, {"min_years": 30, "quantity": "anomaly"})

    def test_coerces_provided_values_to_the_declared_type(self):
        definition = self._definition_with_schema(
            ConfigField(key="min_years", type="int", default=30),
            ConfigField(key="threshold", type="float", default=0.0),
        )

        cleaned = definition.validate_config({"min_years": "25", "threshold": "1.5"})

        self.assertEqual(cleaned["min_years"], 25)
        self.assertEqual(cleaned["threshold"], 1.5)

    def test_value_outside_choices_is_rejected(self):
        definition = self._definition_with_schema(
            ConfigField(key="quantity", type="choice", choices=("anomaly", "value")),
        )
        with self.assertRaises(ValueError):
            definition.validate_config({"quantity": "trend"})

    def test_unknown_config_key_is_rejected(self):
        definition = self._definition_with_schema(
            ConfigField(key="min_years", type="int", default=30),
        )
        with self.assertRaises(ValueError):
            definition.validate_config({"min_years": 30, "bogus": 1})

    def test_value_of_wrong_type_is_rejected(self):
        definition = self._definition_with_schema(
            ConfigField(key="min_years", type="int", default=30),
        )
        with self.assertRaises(ValueError):
            definition.validate_config({"min_years": "not-a-number"})


class ConfigFieldTests(SimpleTestCase):
    def test_valid_field_types_are_accepted(self):
        for type_ in ("str", "int", "float", "bool"):
            self.assertEqual(ConfigField(key="opt", type=type_).type, type_)

    def test_unknown_field_type_is_rejected(self):
        with self.assertRaises(ValueError):
            ConfigField(key="opt", type="datetime")

    def test_choice_field_requires_choices(self):
        with self.assertRaises(ValueError):
            ConfigField(key="quantity", type="choice")

    def test_choice_default_must_be_among_choices(self):
        with self.assertRaises(ValueError):
            ConfigField(
                key="quantity", type="choice",
                choices=("anomaly", "value"), default="trend",
            )

    def test_choice_default_within_choices_is_accepted(self):
        field = ConfigField(
            key="quantity", type="choice",
            choices=("anomaly", "value"), default="anomaly",
        )
        self.assertEqual(field.default, "anomaly")
