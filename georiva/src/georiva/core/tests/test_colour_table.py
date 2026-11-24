"""One colour table for every renderer (#785, spec #784).

A style's colours reach a map by two roads: Titiler colours a tile from the
256-entry table in its palette payload, and the browser colours the encoded
texture from a WeatherLayers palette over the same 256 codes. They must agree
for every pixel, so both are given one table, and these tests hold them to it
at the render-payload seam.
"""

import json
from datetime import UTC, datetime

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils.html import escape

from georiva.core.machine_plane.colour_table import LAYER_UNSCALE, encode
from georiva.core.machine_plane.palette_cache import build_variable_payload
from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Asset, Collection, Item, OutOfRange, Unit, Variable, VariableStyle
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.edr.ids import edr_collection_id
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation
from georiva.stac.serializers import build_style_renders

#: Stored in Kelvin, served in °C: −40…50 °C.
RANGE_K = (233.15, 323.15)

#: Every encoding curve (#787); the range starts above zero, so ``log`` applies.
CURVES = ("linear", "sqrt", "log")

CONTINUOUS = [
    {"value": 263.15, "color": "#0000ff"},
    {"value": 273.15, "color": "#ffffff"},
    {"value": 303.15, "color": "#ff0000"},
]
#: Three bands, each its colour twice at its edges: −40…0, 0…20, 20…50 °C.
STEPPED = [
    {"value": 233.15, "color": "#2166ac"},
    {"value": 273.15, "color": "#2166ac"},
    {"value": 273.15, "color": "#f7f7f7"},
    {"value": 293.15, "color": "#f7f7f7"},
    {"value": 293.15, "color": "#b2182b"},
    {"value": 323.15, "color": "#b2182b"},
]

#: Two bands inside the range, −10…10 and 10…30 °C, leaving room below and above.
INNER_STEPPED = [
    {"value": 263.15, "color": "#2166ac"},
    {"value": 283.15, "color": "#2166ac"},
    {"value": 283.15, "color": "#b2182b"},
    {"value": 303.15, "color": "#b2182b"},
]

#: Every under/over setting a style can take (#788).
OUT_OF_RANGE_SETTINGS = [(under, over) for under in OutOfRange.values for over in OutOfRange.values]


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


def probes():
    """Values across the range and past both ends, in Kelvin."""
    low, high = RANGE_K
    step = (high - low) / 97
    return [low - 5.0] + [low + i * step for i in range(98)] + [high + 5.0]


class ColourTableTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        variable = make_org_tree(make_organisation("kenya"))["variable"]
        variable.unit = unit("K")
        variable.correct_source_unit(unit("K"))
        variable.value_min, variable.value_max = RANGE_K
        variable.save()
        variable.unit = unit("degC")
        variable.save()
        cls.variable_pk = variable.pk

    def setUp(self):
        self.variable = Variable.objects.get(pk=self.variable_pk)

    def style(self, stops, under=OutOfRange.CLAMP, over=OutOfRange.CLAMP):
        VariableStyle.objects.create(
            variable=self.variable, name="Style", slug="style", is_default=True, stops=stops, under=under, over=over
        )
        return Variable.objects.get(pk=self.variable_pk)

    def titiler_colour(self, variable, value):
        """What Titiler paints ``value`` with: its code's colormap entry, or its
        grayscale fallback when the payload carries none."""
        payload = build_variable_payload(variable)
        code = encode(value, payload["vmin"], payload["vmax"], payload["curve"])
        colormap = payload.get("colormap")
        return colormap[code] if colormap else [code, code, code, 255]

    def browser_colour(self, variable, value):
        """What the browser paints ``value`` with: the texture holds the code
        Titiler encodes, the layer unscales it by identity, and the palette
        colours the code."""
        contract = render_contract(variable)
        self.assertEqual(contract.layer_unscale, LAYER_UNSCALE)
        palette = dict((code, colour) for code, colour in contract.layer_palette)
        low, high = variable.value_range
        return palette[encode(value, low, high, variable.scale_type)]

    def assert_renderers_agree(self, variable):
        for value in probes():
            with self.subTest(value=value):
                self.assertEqual(self.browser_colour(variable, value), self.titiler_colour(variable, value))

    def curved(self, variable, scale_type):
        """``variable`` declared with ``scale_type``, its encoding curve (#787)."""
        variable.scale_type = scale_type
        variable.save()
        return Variable.objects.get(pk=variable.pk)

    def test_titiler_and_the_browser_agree_on_a_continuous_style(self):
        variable = self.style(CONTINUOUS)
        for curve in CURVES:
            with self.subTest(curve=curve):
                self.assert_renderers_agree(self.curved(variable, curve))

    def test_titiler_and_the_browser_agree_on_a_stepped_style(self):
        variable = self.style(STEPPED)
        for curve in CURVES:
            with self.subTest(curve=curve):
                self.assert_renderers_agree(self.curved(variable, curve))

    def test_titiler_and_the_browser_agree_on_a_styleless_variable(self):
        for curve in CURVES:
            with self.subTest(curve=curve):
                self.assert_renderers_agree(self.curved(self.variable, curve))

    def test_a_curves_table_colours_each_code_by_the_value_it_stands_for(self):
        """Titiler encodes a value through the curve; its code's colour is the value's own."""
        variable = self.style(STEPPED)
        for curve in CURVES:
            variable = self.curved(variable, curve)
            for value, colour in ((263.15, [0x21, 0x66, 0xAC, 255]), (283.15, [0xF7, 0xF7, 0xF7, 255])):
                with self.subTest(curve=curve, value=value):
                    self.assertEqual(self.titiler_colour(variable, value), colour)

    def test_titiler_and_the_browser_agree_whatever_the_under_and_over(self):
        for curve in CURVES:
            for stops in (CONTINUOUS, STEPPED, INNER_STEPPED):
                for under, over in OUT_OF_RANGE_SETTINGS:
                    with self.subTest(curve=curve, stops=stops, under=under, over=over):
                        VariableStyle.objects.filter(variable=self.variable).delete()
                        self.assert_renderers_agree(self.curved(self.style(stops, under, over), curve))

    def test_transparent_under_and_over_leave_values_outside_the_stops_undrawn(self):
        variable = self.style(INNER_STEPPED, OutOfRange.TRANSPARENT, OutOfRange.TRANSPARENT)
        for curve in CURVES:
            variable = self.curved(variable, curve)
            values = (240.0, 262.0, 264.0, 302.0, 304.0, 320.0)
            with self.subTest(curve=curve):
                alpha = {value: self.titiler_colour(variable, value)[3] for value in values}
                self.assertEqual(alpha, {240.0: 0, 262.0: 0, 264.0: 255, 302.0: 255, 304.0: 0, 320.0: 0})

    def test_clamped_under_and_over_draw_the_end_colours(self):
        variable = self.style(INNER_STEPPED)
        self.assertEqual(self.titiler_colour(variable, 240.0), [0x21, 0x66, 0xAC, 255])
        self.assertEqual(self.titiler_colour(variable, 320.0), [0xB2, 0x18, 0x2B, 255])

    def test_under_and_over_are_independent(self):
        variable = self.style(INNER_STEPPED, under=OutOfRange.TRANSPARENT)
        self.assertEqual(self.titiler_colour(variable, 240.0)[3], 0)
        self.assertEqual(self.titiler_colour(variable, 320.0), [0xB2, 0x18, 0x2B, 255])

    def test_values_clipped_past_the_range_take_the_under_and_over(self):
        """A style whose stops span the whole range: what lies beyond it is
        clipped into the end codes, and a transparent under or over hides them."""
        variable = self.style(STEPPED, OutOfRange.TRANSPARENT, OutOfRange.TRANSPARENT)
        low, high = RANGE_K
        self.assertEqual(self.titiler_colour(variable, low - 5.0)[3], 0)
        self.assertEqual(self.titiler_colour(variable, high + 5.0)[3], 0)
        self.assertEqual(self.titiler_colour(variable, low + 1.0), [0x21, 0x66, 0xAC, 255])
        self.assertEqual(self.titiler_colour(variable, high - 1.0), [0xB2, 0x18, 0x2B, 255])

    def test_a_stepped_style_keeps_its_hard_edges(self):
        palette = dict(render_contract(self.style(STEPPED)).layer_palette)
        # 0 °C sits at code 113.33: every code below it is the cold band, every code above the mild one.
        self.assertEqual(palette[0], [0x21, 0x66, 0xAC, 255])
        self.assertEqual(palette[113], [0x21, 0x66, 0xAC, 255])
        self.assertEqual(palette[114], [0xF7, 0xF7, 0xF7, 255])
        # 20 °C sits at code 170: the mild band holds until it, the warm one after.
        self.assertEqual(palette[169], [0xF7, 0xF7, 0xF7, 255])
        self.assertEqual(palette[171], [0xB2, 0x18, 0x2B, 255])
        self.assertEqual(palette[255], [0xB2, 0x18, 0x2B, 255])

    def test_the_browser_decodes_a_code_into_the_served_value(self):
        """The hover readout reads a code; the contract's range turns it back
        into the value Titiler encoded, in the served unit."""
        contract = render_contract(self.style(CONTINUOUS))
        low, high = contract.image_unscale
        self.assertEqual((low, high), (-40.0, 50.0))
        self.assertAlmostEqual(low + 51 / 255 * (high - low), -22.0)


class EveryBrowserSurfaceCarriesTheTableTests(TestCase):
    """The surfaces that hand a browser a texture hand it the table Titiler colours with."""

    def setUp(self):
        dial_org(self.client)
        tree = make_org_tree(make_organisation())
        self.collection = tree["collection"]
        self.collection.visibility = Collection.Visibility.PUBLIC
        self.collection.save()
        variable = tree["variable"]
        variable.value_min, variable.value_max = 0.0, 300.0
        variable.save()
        make_style(variable, stops=[{"value": 0.0, "color": "#0000ff"}, {"value": 100.0, "color": "#ff0000"}])
        make_style(variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        self.variable = Variable.objects.get(pk=variable.pk)
        self.item = Item.objects.create(
            collection=self.collection, time=datetime(2026, 4, 1, tzinfo=UTC), bounds=[0.0, 0.0, 10.0, 10.0]
        )
        Asset.objects.create(item=self.item, variable=self.variable, href="x.tif", format=Asset.Format.COG)

    def titiler_table(self, style=None):
        colormap = build_variable_payload(self.variable, style)["colormap"]
        return [colormap[code] for code in range(256)]

    def test_the_edr_parameter_carries_the_default_styles_table(self):
        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.collection)]))
        xg = response.json()["parameter_names"][self.variable.slug]["x-georiva"]

        self.assertEqual(xg["colour_table"], self.titiler_table())
        # Legends and choropleths keep reading the stops, in value space.
        self.assertEqual(xg["palette"], [[0.0, [0, 0, 255]], [100.0, [255, 0, 0]]])

    def test_the_edr_parameter_says_how_a_code_decodes(self):
        """The hover readout decodes through the curve (#787)."""
        Variable.objects.filter(pk=self.variable.pk).update(scale_type=Variable.ScaleType.SQRT)

        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.collection)]))
        xg = response.json()["parameter_names"][self.variable.slug]["x-georiva"]

        self.assertEqual(xg["encoding"], {"curve": "sqrt", "range": [0.0, 300.0]})

    def test_the_item_preview_page_says_how_a_code_decodes(self):
        Variable.objects.filter(pk=self.variable.pk).update(scale_type=Variable.ScaleType.SQRT)
        self.client.force_login(get_user_model().objects.create_superuser("root", "r@x.org", "pw"))

        response = self.client.get(reverse("item_preview", args=[self.item.pk]))

        encoding = escape(json.dumps({"curve": "sqrt", "range": [0.0, 300.0]}))
        self.assertContains(response, f"data-asset-encoding='{encoding}'")

    def test_each_stac_render_carries_its_styles_table(self):
        renders = build_style_renders(self.variable, with_colormap=True)
        for style in self.variable.styles.all():
            with self.subTest(style=style.slug):
                colormap = renders[style.slug]["colormap"]
                self.assertEqual([colormap[code] for code in range(256)], self.titiler_table(style))

    def test_the_item_preview_page_carries_the_table(self):
        self.client.force_login(get_user_model().objects.create_superuser("root", "r@x.org", "pw"))
        response = self.client.get(reverse("item_preview", args=[self.item.pk]))
        self.assertContains(response, f"data-asset-colour-table='{json.dumps(self.titiler_table())}'")
