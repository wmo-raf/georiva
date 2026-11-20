"""Maps are drawn in the source unit; numbers that leave as numbers are served (ADR 0046 §14–§16, #742).

Rendering is a linear rescale over a range into a colormap built from absolute
stops, so raw pixels drawn with a raw range and raw stops are the same colours
whatever unit the Variable is served in. Titiler is therefore handed the raw
range and stops and converts no pixel; what it is also handed — ``scale``,
``offset`` and the served unit — is for the numbers it answers with. A client of
tile-config, and every surface that hands a browser a texture to unscale, is
given served numbers.
"""

from django.test import TestCase
from django.urls import reverse

from georiva.core.machine_plane import render_config_token, style_version_token
from georiva.core.machine_plane.palette_cache import build_variable_payload
from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Collection, Unit, Variable, VariableStyle
from georiva.organisations.testing import SHARED_TREE_SLUG, make_org_tree, make_organisation

#: Stored in Kelvin: a range of −40…50 °C and stops at −10, 0 and 30 °C.
RANGE_K = (233.15, 323.15)
STOPS_K = [
    {"value": 263.15, "color": "#0000ff"},
    {"value": 273.15, "color": "#ffffff"},
    {"value": 303.15, "color": "#ff0000"},
]
DRAWING = ("vmin", "vmax", "scale_type", "colormap")


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


class KelvinVariable:
    @classmethod
    def setUpTestData(cls):
        tree = make_org_tree(make_organisation("kenya"))
        Collection.objects.filter(pk=tree["collection"].pk).update(visibility=Collection.Visibility.PUBLIC)
        variable = tree["variable"]
        variable.source_unit, variable.unit = unit("K"), unit("K")
        variable.value_min, variable.value_max = RANGE_K
        variable.save()
        VariableStyle.objects.create(variable=variable, name="Temp", slug="temp", is_default=True, stops=STOPS_K)
        cls.variable_pk = variable.pk

    def setUp(self):
        self.variable = Variable.objects.get(pk=self.variable_pk)

    def serve_in(self, symbol):
        self.variable.unit = unit(symbol)
        self.variable.save()
        return Variable.objects.get(pk=self.variable_pk)


class RenderConfigTests(KelvinVariable, TestCase):
    def test_what_titiler_draws_with_is_the_same_whatever_the_served_unit(self):
        in_kelvin = build_variable_payload(self.variable)
        in_celsius = build_variable_payload(self.serve_in("degC"))

        self.assertEqual({k: in_kelvin[k] for k in DRAWING}, {k: in_celsius[k] for k in DRAWING})
        self.assertEqual((in_celsius["vmin"], in_celsius["vmax"]), RANGE_K)

    def test_it_carries_the_conversion_and_the_served_unit_for_the_numbers_titiler_answers_with(self):
        payload = build_variable_payload(self.serve_in("degC"))

        self.assertEqual((payload["scale"], payload["offset"], payload["unit"]), (1.0, -273.15, "degC"))

    def test_changing_the_served_unit_changes_the_version_tokens(self):
        before = render_config_token(self.variable), style_version_token(self.variable)
        celsius = self.serve_in("degC")

        self.assertNotEqual(render_config_token(celsius), before[0])
        self.assertNotEqual(style_version_token(celsius), before[1])


class TileConfigAnswerTests(KelvinVariable, TestCase):
    def answer(self):
        url = reverse("tile_config", args=["kenya", SHARED_TREE_SLUG, SHARED_TREE_SLUG, SHARED_TREE_SLUG])
        response = self.client.get(url, HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_a_client_is_given_the_served_range_stops_and_unit(self):
        self.serve_in("degC")

        served = self.answer()["served"]

        self.assertEqual(served["unit"], "degC")
        self.assertEqual((served["vmin"], served["vmax"]), (-40.0, 50.0))
        self.assertEqual([value for value, _ in served["stops"]], [-10.0, 0.0, 30.0])

    def test_titiler_is_given_the_raw_range_beside_it(self):
        self.serve_in("degC")

        answer = self.answer()

        self.assertEqual((answer["vmin"], answer["vmax"]), RANGE_K)


class RenderContractTests(KelvinVariable, TestCase):
    """What a browser unscales a texture with and draws a legend from: served numbers."""

    def test_the_texture_unscales_into_the_served_unit_and_the_legend_is_served(self):
        contract = render_contract(self.serve_in("degC"))

        self.assertEqual(contract.image_unscale, [-40.0, 50.0])
        self.assertEqual([value for value, _ in contract.palette], [-10.0, 0.0, 30.0])
        self.assertEqual(contract.legend_range, (-10.0, 30.0))
