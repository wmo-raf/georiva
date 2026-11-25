"""
A Variable declares its time statistic; a non-instant field has no lead-0 step
(#818, ADR 0051).
"""

from datetime import UTC, datetime, timedelta

from django.core.exceptions import ValidationError
from django.test import SimpleTestCase, TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.core.time_statistic import Declared
from georiva.organisations.testing import make_organisation

RUN = datetime(2026, 11, 25, 0, tzinfo=UTC)


def declared(statistic="instant", window=""):
    return Variable(time_statistic=statistic, statistic_window=window)


class TheLeadZeroRuleTests(SimpleTestCase):
    def test_a_new_variable_is_instant(self):
        self.assertEqual(Variable().time_statistic, Variable.TimeStatistic.INSTANT)
        self.assertEqual(Variable().statistic_window, "")

    def test_an_instant_variable_has_every_step_of_a_run(self):
        self.assertTrue(declared().has_step(RUN, RUN))
        self.assertTrue(declared().has_step(RUN, RUN + timedelta(hours=6)))

    def test_a_running_total_has_no_lead_zero_step(self):
        total = declared("sum", "run_start")

        self.assertFalse(total.has_step(RUN, RUN))
        self.assertTrue(total.has_step(RUN, RUN + timedelta(hours=6)))

    def test_a_statistic_over_the_preceding_interval_has_no_lead_zero_step_either(self):
        self.assertFalse(declared("mean", "preceding_interval").has_step(RUN, RUN))

    def test_the_rule_applies_only_to_a_step_of_a_run(self):
        # CHIRPS daily rainfall is a sum, and has no runs.
        self.assertTrue(declared("sum", "preceding_interval").has_step(None, RUN))

    def test_the_leads_a_variable_is_expected_at_leave_out_lead_zero_unless_it_is_instant(self):
        leads = frozenset(timedelta(hours=h) for h in (0, 3, 6))

        self.assertEqual(declared().expected_leads(leads), leads)
        self.assertEqual(
            declared("sum", "run_start").expected_leads(leads), frozenset(timedelta(hours=h) for h in (3, 6))
        )

    def test_only_a_sum_since_the_run_started_is_zero_at_the_run_start(self):
        self.assertTrue(declared("sum", "run_start").zero_at_run_start)
        self.assertFalse(declared("sum", "preceding_interval").zero_at_run_start)
        self.assertFalse(declared("mean", "run_start").zero_at_run_start)
        self.assertFalse(declared().zero_at_run_start)

    def test_its_declaration(self):
        self.assertEqual(declared("max", "preceding_interval").declared, Declared("max", "preceding_interval"))


class ADeclarationIsKeptConsistentTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )
        self.collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
        self.unit, _ = Unit.objects.get_or_create(symbol="mm", defaults={"name": "Millimetre"})

    def variable(self, statistic, window):
        return Variable(
            collection=self.collection,
            slug="tp",
            name="tp",
            source_unit=self.unit,
            unit=self.unit,
            value_min=0,
            value_max=100,
            sources=[("primary", {"source_name": "tp"})],
            time_statistic=statistic,
            statistic_window=window,
        )

    def test_a_statistic_needs_its_window(self):
        with self.assertRaises(ValidationError) as refused:
            self.variable("sum", "").clean()

        self.assertIn("statistic_window", refused.exception.message_dict)

    def test_a_statistic_with_its_window_is_clean(self):
        self.variable("sum", "run_start").clean()

    def test_an_instant_variable_keeps_no_window(self):
        variable = self.variable("instant", "run_start")
        variable.save()

        variable.refresh_from_db()
        self.assertEqual(variable.statistic_window, "")


class ASourceThatDisagreesTests(SimpleTestCase):
    def test_a_source_that_agrees_is_not_logged(self):
        variable = Variable(slug="tp", time_statistic="sum", statistic_window="run_start")

        with self.assertNoLogs("georiva.core.models.variable", "WARNING"):
            self.assertTrue(variable.agrees_with(Declared("sum", "run_start"), source="file x.grib2"))

    def test_a_source_that_says_nothing_is_not_logged(self):
        with self.assertNoLogs("georiva.core.models.variable", "WARNING"):
            self.assertTrue(Variable(slug="tp").agrees_with(None, source="file x.grib2"))

    def test_a_source_that_disagrees_is_logged_naming_the_variable_and_left_as_it_is(self):
        variable = Variable(slug="tp")

        with self.assertLogs("georiva.core.models.variable", "WARNING") as logged:
            self.assertFalse(variable.agrees_with(Declared("sum", "run_start"), source="file x.grib2"))

        self.assertIn("tp", logged.output[0])
        self.assertIn("sum (run start)", logged.output[0])
        self.assertIn("file x.grib2", logged.output[0])
        self.assertEqual(variable.time_statistic, "instant")
