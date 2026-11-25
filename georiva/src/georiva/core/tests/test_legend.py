"""Legends that read like the map (#790, spec #784).

A legend is drawn from the render contract's ``legend``: colour stops and
ticks laid out along a bar from 0 to 1, with the style's under and over. A
continuous style is laid out along the Variable's encoding curve, so it
stretches where the map does; a stepped one as equal-width blocks, one per
band. Ticks read in the served unit. The browser only draws what it is given,
so these tests hold the layout at the render-payload seam.
"""

import itertools
import json
from datetime import UTC, datetime

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils.html import escape

from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Asset, Collection, Item, OutOfRange, Unit, Variable, VariableStyle
from georiva.edr.ids import edr_collection_id
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation

#: Stored in Kelvin, served in °C: −40…50 °C.
RANGE_K = (233.15, 323.15)

BLUE, WHITE, RED = [0, 0, 255, 255], [255, 255, 255, 255], [255, 0, 0, 255]

#: Three bands of very different widths: −40…−38, −38…0 and 0…50 °C.
STEPPED = [
    {"value": 233.15, "color": "#0000ff"},
    {"value": 235.15, "color": "#0000ff"},
    {"value": 235.15, "color": "#ffffff"},
    {"value": 273.15, "color": "#ffffff"},
    {"value": 273.15, "color": "#ff0000"},
    {"value": 323.15, "color": "#ff0000"},
]

#: −10, 0 and 30 °C.
CONTINUOUS = [
    {"value": 263.15, "color": "#0000ff"},
    {"value": 273.15, "color": "#ffffff"},
    {"value": 303.15, "color": "#ff0000"},
]

#: Where 0 °C sits along the CONTINUOUS legend, which runs −10…30 °C, by curve.
#: Worked by hand over the encoding range −40…50 °C (233.15…323.15 K):
#: linear  (0 − −10) / (30 − −10)                                    = 0.25
#: sqrt    (√(40/90) − √(30/90)) / (√(70/90) − √(30/90))
#:         = (0.666667 − 0.577350) / (0.881917 − 0.577350)            = 0.293258
#: log     (ln(273.15/233.15) − ln(263.15/233.15))
#:         / (ln(303.15/233.15) − ln(263.15/233.15))
#:         = (0.158339 − 0.121042) / (0.262546 − 0.121042)            = 0.263576
ZERO_AT = {"linear": 0.25, "sqrt": 0.293258, "log": 0.263576}


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


class LegendCase(TestCase):
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

    def legend(self, stops=None, scale_type="linear", under=OutOfRange.CLAMP, over=OutOfRange.CLAMP):
        Variable.objects.filter(pk=self.variable_pk).update(scale_type=scale_type)
        if stops is not None:
            VariableStyle.objects.create(
                variable_id=self.variable_pk,
                name="Style",
                slug="style",
                is_default=True,
                stops=stops,
                under=under,
                over=over,
            )
        return render_contract(Variable.objects.get(pk=self.variable_pk)).legend

    def assert_positions(self, actual, expected):
        self.assertEqual(len(actual), len(expected))
        for (position, value), (expected_position, expected_value) in zip(actual, expected, strict=True):
            self.assertAlmostEqual(position, expected_position, places=5)
            self.assertEqual(value, expected_value)


class LegendTests(LegendCase):
    def test_a_stepped_legend_gives_each_band_an_equal_block(self):
        legend = self.legend(STEPPED)

        third, two_thirds = 1 / 3, 2 / 3
        self.assert_positions(
            legend["gradient"],
            [(0.0, BLUE), (third, BLUE), (third, WHITE), (two_thirds, WHITE), (two_thirds, RED), (1.0, RED)],
        )

    def test_a_stepped_legend_ticks_each_level_in_the_served_unit(self):
        legend = self.legend(STEPPED)

        self.assert_positions(legend["ticks"], [(0.0, -40.0), (1 / 3, -38.0), (2 / 3, 0.0), (1.0, 50.0)])

    def test_a_stepped_legend_ignores_the_curve(self):
        for curve in ("sqrt", "log"):
            with self.subTest(curve=curve):
                VariableStyle.objects.filter(variable_id=self.variable_pk).delete()
                legend = self.legend(STEPPED, scale_type=curve)
                self.assert_positions(legend["ticks"], [(0.0, -40.0), (1 / 3, -38.0), (2 / 3, 0.0), (1.0, 50.0)])

    def test_a_continuous_legend_is_laid_out_along_the_encoding_curve(self):
        for curve, zero_at in ZERO_AT.items():
            with self.subTest(curve=curve):
                VariableStyle.objects.filter(variable_id=self.variable_pk).delete()
                legend = self.legend(CONTINUOUS, scale_type=curve)
                self.assert_positions(legend["gradient"], [(0.0, BLUE), (zero_at, WHITE), (1.0, RED)])
                self.assert_positions(legend["ticks"], [(0.0, -10.0), (zero_at, 0.0), (1.0, 30.0)])

    def test_a_diverging_variable_legends_linearly(self):
        legend = self.legend(CONTINUOUS, scale_type="diverging")
        self.assert_positions(legend["ticks"], [(0.0, -10.0), (0.25, 0.0), (1.0, 30.0)])

    def test_a_styleless_variable_legends_its_grayscale_over_the_range(self):
        legend = self.legend()

        self.assertEqual(legend["gradient"][0], [0.0, [0, 0, 0, 255]])
        self.assertEqual(legend["gradient"][-1], [1.0, [255, 255, 255, 255]])
        self.assertEqual(legend["ticks"][0], [0.0, -40.0])
        self.assertEqual(legend["ticks"][-1], [1.0, 50.0])
        self.assertEqual((legend["under"], legend["over"]), ("clamp", "clamp"))

    def test_the_legend_says_which_ends_are_clamped(self):
        for under, over in ((OutOfRange.CLAMP, OutOfRange.TRANSPARENT), (OutOfRange.TRANSPARENT, OutOfRange.CLAMP)):
            with self.subTest(under=under, over=over):
                VariableStyle.objects.filter(variable_id=self.variable_pk).delete()
                legend = self.legend(STEPPED, under=under, over=over)
                self.assertEqual((legend["under"], legend["over"]), (under, over))


def stepped(levels):
    """Stepped stops, one band between each pair of levels (in K), colours alternating."""
    stops = []
    for index, (low, high) in enumerate(itertools.pairwise(levels)):
        colour = "#0000ff" if index % 2 else "#ff0000"
        stops += [{"value": low, "color": colour}, {"value": high, "color": colour}]
    return stops


#: A precipitation-like style's eight levels (#812), offset onto the Kelvin range.
RAIN_LEVELS = [233.15 + level / 5 for level in (0.5, 2, 4, 10, 25, 50, 100, 250)]


class LegendLabelTests(LegendCase):
    """Which ticks are labelled (#812): every class boundary of a stepped legend
    up to 16 of them, thinned evenly beyond that with both ends kept; a
    continuous legend at most six, as before."""

    def levels(self, count):
        return [233.15 + index for index in range(count)]

    def test_a_stepped_legend_labels_every_boundary(self):
        legend = self.legend(stepped(RAIN_LEVELS))

        self.assertEqual(legend["labels"], list(range(8)))

    def test_sixteen_boundaries_are_all_labelled(self):
        legend = self.legend(stepped(self.levels(16)))

        self.assertEqual(legend["labels"], list(range(16)))

    def test_more_than_sixteen_boundaries_thin_evenly_keeping_both_ends(self):
        # 31 boundaries: every other one, 0 through 30 — sixteen labels.
        legend = self.legend(stepped(self.levels(31)))

        self.assertEqual(legend["labels"], list(range(0, 31, 2)))

    def test_a_thinned_legend_does_not_crowd_its_last_label(self):
        # 32 boundaries at every third: 30 sits right beside 31, so it gives way.
        legend = self.legend(stepped(self.levels(32)))

        self.assertEqual(legend["labels"], [*range(0, 30, 3), 31])

    def test_a_continuous_legend_still_labels_at_most_six_stops(self):
        # Eleven stops, every other one labelled, as before #812.
        stops = [{"value": 233.15 + index * 9, "color": f"#{index * 20:02x}0000"} for index in range(11)]

        legend = self.legend(stops)

        self.assertEqual(len(legend["ticks"]), 11)
        self.assertEqual(legend["labels"], [0, 2, 4, 6, 8, 10])

    def test_a_styleless_legend_labels_each_of_its_steps(self):
        self.assertEqual(self.legend()["labels"], [0, 1, 2, 3, 4])


class EveryLegendReadsTheContractsLegendTests(TestCase):
    """The item page reads EDR's ``x-georiva``, the admin item preview its data
    attributes: both carry the contract's legend as it is."""

    def setUp(self):
        dial_org(self.client)
        tree = make_org_tree(make_organisation())
        self.collection = tree["collection"]
        self.collection.visibility = Collection.Visibility.PUBLIC
        self.collection.save()
        variable = tree["variable"]
        variable.value_min, variable.value_max = 0.0, 300.0
        variable.save()
        VariableStyle.objects.create(
            variable=variable,
            name="Rain",
            slug="rain",
            is_default=True,
            stops=[
                {"value": 0.5, "color": "#0000ff"},
                {"value": 1.0, "color": "#0000ff"},
                {"value": 1.0, "color": "#ff0000"},
                {"value": 200.0, "color": "#ff0000"},
            ],
            under=OutOfRange.TRANSPARENT,
        )
        self.variable = Variable.objects.get(pk=variable.pk)
        self.item = Item.objects.create(
            collection=self.collection, time=datetime(2026, 4, 1, tzinfo=UTC), bounds=[0.0, 0.0, 10.0, 10.0]
        )
        Asset.objects.create(item=self.item, variable=self.variable, href="x.tif", format=Asset.Format.COG)

    #: Two equal blocks, 0.5–1 and 1–200, the drizzle below 0.5 undrawn.
    EXPECTED = {
        "gradient": [[0.0, BLUE], [0.5, BLUE], [0.5, RED], [1.0, RED]],
        "ticks": [[0.0, 0.5], [0.5, 1.0], [1.0, 200.0]],
        "labels": [0, 1, 2],
        "under": "transparent",
        "over": "clamp",
    }

    def test_the_edr_parameter_carries_the_legend(self):
        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.collection)]))
        xg = response.json()["parameter_names"][self.variable.slug]["x-georiva"]

        self.assertEqual(xg["legend"], self.EXPECTED)

    def test_the_item_preview_page_carries_the_legend(self):
        self.client.force_login(get_user_model().objects.create_superuser("root", "r@x.org", "pw"))

        response = self.client.get(reverse("item_preview", args=[self.item.pk]))

        self.assertContains(response, f"data-asset-legend='{escape(json.dumps(self.EXPECTED))}'")
