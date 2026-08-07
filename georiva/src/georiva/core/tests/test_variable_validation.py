"""
Variable range validation (ADR 0022): `value_min < value_max` is enforced by
the model's clean(), the single validator every form path delegates to.
"""
from django.core.exceptions import ValidationError
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import make_organisation


class VariableRangeCleanTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models",
            file_format="grib2",
        )
        self.collection = Collection.objects.create(
            catalog=catalog, name="Surface", slug="surface"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")

    def _variable(self, value_min, value_max):
        return Variable(
            collection=self.collection, slug="tas", name="tas",
            unit=self.unit, value_min=value_min, value_max=value_max,
            sources=[("primary", {"source_name": "tas"})],
        )

    def _clean_errors(self, variable) -> dict:
        try:
            variable.clean()
        except ValidationError as e:
            return e.message_dict
        return {}

    def test_min_below_max_passes(self):
        self.assertNotIn("value_max", self._clean_errors(self._variable(0.0, 50.0)))

    def test_min_equal_to_max_is_rejected(self):
        errors = self._clean_errors(self._variable(10.0, 10.0))
        self.assertIn("value_max", errors)

    def test_min_above_max_is_rejected_with_a_clear_message(self):
        errors = self._clean_errors(self._variable(60.0, -60.0))
        self.assertIn("value_max", errors)
        self.assertIn("greater than", errors["value_max"][0])

    def test_missing_bound_is_not_a_range_error(self):
        # A form path may validate partial input; the range rule only fires
        # when both bounds are present.
        self.assertNotIn("value_max", self._clean_errors(self._variable(None, 50.0)))
        self.assertNotIn("value_max", self._clean_errors(self._variable(0.0, None)))


class ValidateValueRangeHelperTests(TestCase):
    """The static helper is the delegation point for non-ModelForm paths
    (upload wizard dicts, plain add form) — same rule, same message."""

    def test_valid_range_passes(self):
        Variable.validate_value_range(0.0, 1.0)

    def test_inverted_range_raises_on_value_max(self):
        with self.assertRaises(ValidationError) as ctx:
            Variable.validate_value_range(5.0, 5.0)
        self.assertIn("value_max", ctx.exception.message_dict)

    def test_missing_bounds_pass(self):
        Variable.validate_value_range(None, 1.0)
        Variable.validate_value_range(0.0, None)


class RangeAccessorCollapseTests(TestCase):
    """Exactly one range accessor remains on Variable (ADR 0022)."""

    def test_value_range_is_the_canonical_accessor(self):
        variable = Variable(value_min=-5.0, value_max=45.0)
        self.assertEqual(variable.value_range, (-5.0, 45.0))

    def test_the_alias_properties_are_gone(self):
        self.assertFalse(hasattr(Variable, "encoding_range"))
        self.assertFalse(hasattr(Variable, "palette_value_range"))
