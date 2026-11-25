"""Backfilling ``VariableStyle.origin`` for the styles that predate it (#806).

One seam: the data migration's ``backfill_origins``, run against the live app
registry. A style is ``template`` when its stops are what its template produces
today, ``ramp`` when they are what its ramp produces over the Variable's current
range (forward or reversed), and ``custom`` otherwise. No stops change.
"""

import importlib

from django.apps import apps
from django.test import TestCase

from georiva.core.models import StyleTemplate, Variable, VariableStyle
from georiva.core.tests.test_styling_surface import make_ramp
from georiva.organisations.testing import make_org_tree, make_organisation

MIGRATION = importlib.import_module("georiva.core.migrations.0030_backfill_variablestyle_origin")
backfill_origins = MIGRATION.backfill_origins

# make_org_tree's variable spans 0–50 in "C"; the ramp runs black → white.
FORWARD = [{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}]
REVERSED = [{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}]
# The template's two bands, 0–10 and 10–20, as stepped stops.
BANDS = [
    {"value": 0.0, "color": "#0000ff"},
    {"value": 10.0, "color": "#0000ff"},
    {"value": 10.0, "color": "#ff0000"},
    {"value": 20.0, "color": "#ff0000"},
]


class BackfillOriginTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        org = make_organisation()
        cls.variable = make_org_tree(org)["variable"]
        cls.ramp = make_ramp("House ramp", organisation=org)
        cls.template = StyleTemplate.objects.create(
            organisation=org,
            name="Bands",
            slug="bands",
            levels=[0, 10, 20],
            colors=["#0000ff", "#ff0000"],
            unit=cls.variable.source_unit,
        )

    def backfilled(self, stops, **links):
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Style",
            slug=f"style-{VariableStyle.objects.count()}",
            stops=stops,
            origin="unset",
            **links,
        )
        backfill_origins(apps, None)
        style.refresh_from_db()
        self.assertEqual(style.stops, stops, "the backfill changed the stops")
        return style

    def test_stops_the_template_produces_are_a_template_style(self):
        style = self.backfilled(BANDS, template=self.template)

        self.assertEqual(style.origin, VariableStyle.Origin.TEMPLATE)

    def test_a_template_style_whose_stops_were_edited_is_custom(self):
        edited = [*BANDS[:3], {"value": 25.0, "color": "#ff0000"}]

        self.assertEqual(self.backfilled(edited, template=self.template).origin, VariableStyle.Origin.CUSTOM)

    def test_stops_the_ramp_produces_are_a_ramp_style(self):
        style = self.backfilled(FORWARD, ramp=self.ramp)

        self.assertEqual((style.origin, style.reverse), (VariableStyle.Origin.RAMP, False))

    def test_stops_the_ramp_produces_reversed_are_a_reversed_ramp_style(self):
        style = self.backfilled(REVERSED, ramp=self.ramp)

        self.assertEqual((style.origin, style.reverse), (VariableStyle.Origin.RAMP, True))

    def test_a_stepped_ramp_style_as_generated_is_a_ramp_style(self):
        style = VariableStyle(
            variable=self.variable, name="Classes", slug="classes", ramp=self.ramp, mode="stepped", steps=4
        )
        style.apply_ramp()
        style.origin = "unset"
        style.save()

        backfill_origins(apps, None)
        style.refresh_from_db()

        self.assertEqual(style.origin, VariableStyle.Origin.RAMP)

    def test_a_hand_tuned_ramp_style_is_custom(self):
        tuned = [{"value": 0.0, "color": "#000000"}, {"value": 42.0, "color": "#ffffff"}]

        self.assertEqual(self.backfilled(tuned, ramp=self.ramp).origin, VariableStyle.Origin.CUSTOM)

    def test_a_ramp_style_after_a_range_change_is_custom(self):
        Variable.objects.filter(pk=self.variable.pk).update(value_max=60)

        self.assertEqual(self.backfilled(FORWARD, ramp=self.ramp).origin, VariableStyle.Origin.CUSTOM)

    def test_a_style_with_neither_link_is_custom(self):
        self.assertEqual(self.backfilled(FORWARD).origin, VariableStyle.Origin.CUSTOM)

    def test_a_style_with_both_links_is_whichever_its_stops_match(self):
        self.assertEqual(
            self.backfilled(FORWARD, ramp=self.ramp, template=self.template).origin, VariableStyle.Origin.RAMP
        )
        self.assertEqual(
            self.backfilled(BANDS, ramp=self.ramp, template=self.template).origin, VariableStyle.Origin.TEMPLATE
        )
