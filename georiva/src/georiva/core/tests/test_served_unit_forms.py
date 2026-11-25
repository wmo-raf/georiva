"""Operators type and read ranges and stops in the served unit (ADR 0046 §3, #743).

Every number stored about values is in the Variable's ``source_unit``; the
forms convert at their boundary with the Variable's ``scale`` and ``offset``.
Driven through the Styling surface, the one place range and stops are tuned
(ADR 0022), on a Variable whose bytes are Kelvin and which is served in °C.
"""

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import ColorRamp, ColorRampStop, Unit, Variable, VariableStyle
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation
from georiva.organisations.tests.factories import PASSWORD, grant_everything, make_user


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


class ServedUnitStylingTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.org = make_organisation()
        tree = make_org_tree(cls.org)
        cls.collection = tree["collection"]
        cls.variable = tree["variable"]
        # The tree's asset is Kelvin: say so, past the lock (ADR 0046 §8).
        cls.variable.unit = unit("degC")
        cls.variable.correct_source_unit(unit("K"))
        cls.variable.value_min, cls.variable.value_max = 233.15, 323.15
        cls.variable.save()
        cls.ramp = ColorRamp.objects.create(name="Three", organisation=cls.org)
        for i, hex_value in enumerate(("#000000", "#808080", "#ffffff")):
            ColorRampStop.objects.create(ramp=cls.ramp, hex_value=hex_value, sort_order=i)

    def setUp(self):
        user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(user=user, organisation=self.org, role=OrganisationMembership.Role.ADMIN)
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)

    @property
    def form_url(self):
        return reverse("variable_styling", args=[self.collection.pk, self.variable.pk])

    def serve_in(self, symbol):
        self.variable.refresh_from_db()
        self.variable.unit = unit(symbol)
        self.variable.save()

    def stop_values_shown(self):
        response = self.client.get(self.form_url)
        self.assertEqual(response.status_code, 200)
        return [row["value"] for row in response.context["stop_rows"]]


class StopsAreTypedInTheServedUnitTests(ServedUnitStylingTestCase):
    def save_stops(self, values, colors):
        return self.client.post(
            self.form_url,
            {
                "action": "save-style",
                "style_slug": "",
                "name": "Official",
                "ramp": "",
                "mode": VariableStyle.Mode.CONTINUOUS,
                "steps": "",
                "stop_value": values,
                "stop_color": colors,
            },
        )

    def test_a_zero_celsius_stop_is_stored_as_kelvin_and_reads_back_as_zero(self):
        response = self.save_stops(["0", "30"], ["#0000ff", "#ff0000"])
        self.assertEqual(response.status_code, 302)

        style = VariableStyle.objects.get(variable=self.variable)
        self.assertEqual([stop["value"] for stop in style.stops], [273.15, 303.15])
        self.assertEqual(self.stop_values_shown(), [0.0, 30.0])

    def test_serving_in_fahrenheit_shows_the_same_stop_as_32_without_rewriting_it(self):
        self.save_stops(["0", "30"], ["#0000ff", "#ff0000"])
        self.serve_in("degF")

        self.assertEqual(self.stop_values_shown(), [32.0, 86.0])
        style = VariableStyle.objects.get(variable=self.variable)
        self.assertEqual([stop["value"] for stop in style.stops], [273.15, 303.15])


class TheRangeIsTypedInTheServedUnitTests(ServedUnitStylingTestCase):
    def test_the_range_reads_in_the_served_unit(self):
        response = self.client.get(self.form_url)
        self.assertEqual(response.context["range_form"].initial["value_min"], -40.0)
        self.assertEqual(response.context["range_form"].initial["value_max"], 50.0)

    def test_a_typed_range_is_stored_in_the_source_unit(self):
        response = self.client.post(
            self.form_url,
            {"action": "save-range", "value_min": "-10", "value_max": "45", "scale_type": "linear"},
        )
        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.value_range, (263.15, 318.15))

    def test_the_collection_listing_shows_the_served_range(self):
        response = self.client.get(reverse("collection_styling", args=[self.collection.pk]))
        self.assertContains(response, "-40")
        self.assertNotContains(response, "233.15")


class RampsAreAppliedInTheServedUnitTests(ServedUnitStylingTestCase):
    """``scale_type`` spaces stops as the operator sees them (ADR 0046 §3)."""

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        # 1–100 °C, log: the middle colour lands on 10 °C. Spaced in Kelvin it
        # would land near 46 °C instead.
        cls.variable.value_min, cls.variable.value_max = 274.15, 373.15
        cls.variable.scale_type = Variable.ScaleType.LOG
        cls.variable.save()

    def test_the_generated_stops_come_back_log_spaced_in_the_served_unit(self):
        response = self.client.get(
            reverse("variable_style_stops", args=[self.collection.pk, self.variable.pk]),
            {"ramp": str(self.ramp.pk), "mode": "continuous"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual([stop["value"] for stop in response.json()["stops"]], [1.0, 10.0, 100.0])

    def test_a_style_created_from_a_ramp_stores_the_served_spacing_converted(self):
        response = self.client.post(
            self.form_url,
            {
                "action": "start-from-ramp",
                "ramp": str(self.ramp.pk),
                "mode": VariableStyle.Mode.CONTINUOUS,
                "steps": "",
            },
        )
        self.assertEqual(response.status_code, 302)
        style = VariableStyle.objects.get(variable=self.variable)
        self.assertEqual([stop["value"] for stop in style.stops], [274.15, 283.15, 373.15])
