"""Backfilling ``VariableStyle.origin`` for the styles that predate it (#806).

Two seams: the migration itself, run with the migration executor from 0029 to
0030 over rows made with the historical models — what a deploy does, and what
proves the generators it imports work on historical models; and its
``backfill_origins``, run against the live app registry for the rule's many
cases, which a migration round trip each would make slow.

A style is ``template`` when its stops are what its template produces today,
``ramp`` when they are what its ramp produces over the Variable's current range
(forward or reversed), and ``custom`` otherwise. No stops change.
"""

import importlib

from django.apps import apps
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
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


BEFORE = [("georivacore", "0029_variablestyle_origin_reverse")]
AFTER = [("georivacore", "0030_backfill_variablestyle_origin")]


class BackfillMigrationTests(TestCase):
    """The data migration, run forwards over rows the historical models made.

    A plain TestCase: between 0029 and 0030 no migration runs DDL, so going
    back and forth inside the test's transaction is safe, and its rollback
    restores the migration records with everything else.
    """

    def migrate(self, targets):
        executor = MigrationExecutor(connection)
        executor.loader.build_graph()
        executor.migrate(targets)
        return executor.loader.project_state(targets).apps

    def setUp(self):
        self.old_apps = self.migrate(BEFORE)

    def test_the_migration_records_each_styles_origin_from_its_stops(self):
        org = make_organisation()
        variable = make_org_tree(org)["variable"]
        old = self.old_apps.get_model
        ramp = old("georivacore", "ColorRamp").objects.create(name="House ramp", organisation_id=org.pk)
        for i, hex_value in enumerate(("#000000", "#ffffff")):
            old("georivacore", "ColorRampStop").objects.create(ramp=ramp, hex_value=hex_value, sort_order=i)
        template = old("georivacore", "StyleTemplate").objects.create(
            organisation_id=org.pk,
            name="Bands",
            slug="bands",
            levels=[0, 10, 20],
            colors=["#0000ff", "#ff0000"],
            unit_id=variable.source_unit_id,
        )
        OldStyle = old("georivacore", "VariableStyle")
        made = {
            "template": OldStyle.objects.create(
                variable_id=variable.pk, name="T", slug="t", stops=BANDS, template=template
            ),
            "forward": OldStyle.objects.create(variable_id=variable.pk, name="F", slug="f", stops=FORWARD, ramp=ramp),
            "reversed": OldStyle.objects.create(variable_id=variable.pk, name="R", slug="r", stops=REVERSED, ramp=ramp),
            "tuned": OldStyle.objects.create(
                variable_id=variable.pk,
                name="Tuned",
                slug="tuned",
                stops=[{"value": 0.0, "color": "#000000"}, {"value": 42.0, "color": "#ffffff"}],
                ramp=ramp,
                origin="ramp",
            ),
        }

        self.migrate(AFTER)

        recorded = {
            key: tuple(VariableStyle.objects.filter(pk=style.pk).values_list("origin", "reverse", "stops").get())
            for key, style in made.items()
        }
        self.assertEqual(recorded["template"], ("template", False, BANDS))
        self.assertEqual(recorded["forward"], ("ramp", False, FORWARD))
        self.assertEqual(recorded["reversed"], ("ramp", True, REVERSED))
        self.assertEqual(recorded["tuned"][:2], ("custom", False))
