"""A Variable's ``scale_type`` is its encoding curve (#787, spec #784).

``code = int(255 · f(t))`` with ``t = (v − vmin) / (vmax − vmin)`` clipped to
0–1: ``linear`` f(t) = t, ``sqrt`` f(t) = √t, ``log`` f(t) = log(v/vmin) /
log(vmax/vmin), and ``diverging`` encodes linearly. The decode is its inverse,
and builds the colour table. Tested at the render-payload seam (what Titiler
and the browser receive), the version token and the range form; Titiler's own
encoding of the same curves is tested over HTTP in ``titiler-app``.
"""

from django.test import TestCase

from georiva.core.machine_plane import render_config_token, style_version_token
from georiva.core.machine_plane.colour_table import decode, encode
from georiva.core.machine_plane.palette_cache import build_variable_payload
from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Unit, Variable, VariableStyle
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase
from georiva.organisations.testing import make_org_tree, make_organisation

CURVES = ("linear", "sqrt", "log")

BLUE, RED = "#0000ff", "#ff0000"
#: A narrow low band, 0…1, under a wide one, 1…100: drizzle under rain.
NARROW_LOW_BAND = [
    {"value": 0.0, "color": BLUE},
    {"value": 1.0, "color": BLUE},
    {"value": 1.0, "color": RED},
    {"value": 100.0, "color": RED},
]


class TheCurveTests(TestCase):
    """The curve, value by value: codes worked by hand."""

    def test_a_value_encodes_to_its_curves_code(self):
        # t = 0.09: linear 22.95, √0.09 = 0.3 → 76.5.
        self.assertEqual(encode(9.0, 0.0, 100.0, "linear"), 22)
        self.assertEqual(encode(9.0, 0.0, 100.0, "sqrt"), 76)
        # log(10/1) / log(100/1) = 0.5 → 127.5.
        self.assertEqual(encode(10.0, 1.0, 100.0, "log"), 127)

    def test_values_outside_the_range_take_the_end_codes(self):
        for curve in CURVES:
            with self.subTest(curve=curve):
                self.assertEqual(encode(0.5, 1.0, 100.0, curve), 0)
                self.assertEqual(encode(500.0, 1.0, 100.0, curve), 255)

    def test_a_code_decodes_to_the_value_it_stands_for(self):
        self.assertAlmostEqual(decode(76.5, 0.0, 100.0, "sqrt"), 9.0)
        self.assertAlmostEqual(decode(127.5, 1.0, 100.0, "log"), 10.0)
        self.assertAlmostEqual(decode(22.95, 0.0, 100.0, "linear"), 9.0)

    def test_every_code_round_trips_through_every_curve(self):
        for curve in CURVES:
            for code in range(256):
                with self.subTest(curve=curve, code=code):
                    # The middle of a code's step decodes into the step and encodes back to it.
                    self.assertEqual(encode(decode(code + 0.5, 1.0, 100.0, curve), 1.0, 100.0, curve), code)


class CurvedVariable(TestCase):
    @classmethod
    def setUpTestData(cls):
        variable = make_org_tree(make_organisation("kenya"))["variable"]
        variable.value_min, variable.value_max = 0.0, 100.0
        variable.save()
        VariableStyle.objects.create(
            variable=variable, name="Rain", slug="rain", is_default=True, stops=NARROW_LOW_BAND
        )
        cls.variable_pk = variable.pk

    def declared(self, scale_type, value_min=0.0):
        Variable.objects.filter(pk=self.variable_pk).update(scale_type=scale_type, value_min=value_min)
        return Variable.objects.get(pk=self.variable_pk)


class TheRenderPayloadNamesTheCurveTests(CurvedVariable):
    def test_titiler_is_told_the_curve_each_scale_type_encodes_with(self):
        for scale_type, curve in (("linear", "linear"), ("sqrt", "sqrt"), ("diverging", "linear")):
            with self.subTest(scale_type=scale_type):
                self.assertEqual(build_variable_payload(self.declared(scale_type))["curve"], curve)
        self.assertEqual(build_variable_payload(self.declared("log", value_min=1.0))["curve"], "log")

    def test_a_log_variable_whose_range_starts_at_zero_encodes_linearly(self):
        """The range form refuses one; one declared before it did still renders."""
        self.assertEqual(build_variable_payload(self.declared("log", value_min=0.0))["curve"], "linear")

    def test_a_sqrt_curve_gives_a_narrow_low_band_its_codes(self):
        """0…1 of 0…100 is codes 0–2 linearly, but codes 0–25 under √: 255·√0.01 = 25.5."""
        linear = render_contract(self.declared("linear")).colour_table
        sqrt = render_contract(self.declared("sqrt")).colour_table

        blue, red = [0, 0, 255, 255], [255, 0, 0, 255]
        self.assertEqual((linear[2], linear[3]), (blue, red))
        self.assertEqual((sqrt[25], sqrt[26]), (blue, red))

    def test_the_browser_is_told_how_to_decode_a_code(self):
        """The hover readout decodes through the curve over the stored range, then serves."""
        contract = render_contract(self.declared("log", value_min=1.0))

        self.assertEqual(contract.encoding, {"curve": "log", "range": [1.0, 100.0]})

    def test_changing_the_curve_changes_the_version_tokens(self):
        linear = self.declared("linear", value_min=1.0)
        before = render_config_token(linear), style_version_token(linear)

        for scale_type in ("sqrt", "log"):
            with self.subTest(scale_type=scale_type):
                curved = self.declared(scale_type, value_min=1.0)
                self.assertNotEqual(render_config_token(curved), before[0])
                self.assertNotEqual(style_version_token(curved), before[1])

    def test_changing_the_curve_leaves_the_styles_stops_alone(self):
        self.declared("sqrt")

        self.assertEqual(VariableStyle.objects.get(slug="rain").stops, NARROW_LOW_BAND)


class TheRangeFormRefusesAMeaninglessLogTests(StylingSurfaceTestCase):
    def save_range(self, value_min, scale_type="log"):
        return self.client.post(
            self.form_url,
            {"action": "save-range", "value_min": value_min, "value_max": "100", "scale_type": scale_type},
        )

    def test_log_over_a_range_starting_at_or_below_zero_is_refused(self):
        for value_min in ("0", "-5"):
            with self.subTest(value_min=value_min):
                response = self.save_range(value_min)

                self.assertEqual(response.status_code, 200)
                self.assertContains(response, "A logarithmic encoding needs a minimum above zero.")
                self.variable.refresh_from_db()
                self.assertNotEqual(self.variable.scale_type, "log")

    def test_log_over_a_range_above_zero_is_saved(self):
        response = self.save_range("0.1")

        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual((self.variable.scale_type, self.variable.value_min), ("log", 0.1))

    def test_the_minimum_that_counts_is_the_stored_one(self):
        """The curve encodes stored values: −10 °C is 263.15 K, above zero."""
        kelvin, _ = Unit.objects.get_or_create(symbol="K", defaults={"name": "kelvin"})
        celsius, _ = Unit.objects.get_or_create(symbol="degC", defaults={"name": "degree Celsius"})
        self.variable.unit = kelvin
        self.variable.correct_source_unit(kelvin)
        self.variable.save()
        self.variable.unit = celsius
        self.variable.save()

        response = self.save_range("-10")

        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.scale_type, "log")
