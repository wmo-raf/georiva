"""The Styling surface (ADR 0022, issue #321): one place where styling is tuned.

Three seams:

* the model — stop ordering on ``VariableStyle.clean()`` and the atomic
  default-promotion helper, neither of which the earlier slices needed;
* the admin surface — the collection Styling page and the canonical
  per-variable form, driven through the admin test client: listing with
  swatches, ramp application, stop fine-tuning, default promotion, deletion
  guards, and org scoping of ramp choices;
* the map preview (#382) — which item it draws and what the panel says when
  there is nothing to draw. The map itself is JavaScript and is verified by
  hand, as ``item_preview.html``'s already is; what is tested here is the
  contract the page hands it.
"""
from datetime import datetime, timezone

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse

from georiva.core.machine_plane import titiler_encoded_preview_url
from georiva.core.models import (
    Asset,
    ColorRamp,
    ColorRampStop,
    Item,
    Variable,
    VariableStyle,
)
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation
from georiva.organisations.tests.factories import (
    PASSWORD,
    grant_everything,
    make_user,
)


def make_ramp(name="Test ramp", organisation=None, hexes=("#000000", "#ffffff")):
    ramp = ColorRamp.objects.create(name=name, organisation=organisation)
    for i, hex_value in enumerate(hexes):
        ColorRampStop.objects.create(ramp=ramp, hex_value=hex_value, sort_order=i)
    return ramp


class VariableStyleOrderingTests(TestCase):
    """Stop ordering is validated on the model — the single home of the rule."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation())

    def _style(self, stops):
        return VariableStyle(
            variable=self.tree["variable"], name="Default", slug="default",
            stops=stops,
        )

    def test_descending_stop_values_are_rejected(self):
        style = self._style([
            {"value": 10.0, "color": "#000000"},
            {"value": 5.0, "color": "#ffffff"},
        ])
        with self.assertRaises(ValidationError) as caught:
            style.full_clean()
        self.assertIn("stops", caught.exception.error_dict)

    def test_equal_neighbouring_values_are_allowed(self):
        # Stepped snapshots share class-boundary values by construction.
        style = self._style([
            {"value": 0.0, "color": "#000000"},
            {"value": 5.0, "color": "#000000"},
            {"value": 5.0, "color": "#ffffff"},
            {"value": 10.0, "color": "#ffffff"},
        ])
        style.full_clean()

    def test_ascending_stop_values_are_allowed(self):
        style = self._style([
            {"value": 0.0, "color": "#000000"},
            {"value": 50.0, "color": "#ffffff"},
        ])
        style.full_clean()


class PromoteToDefaultTests(TestCase):
    """The atomic demote-then-promote the partial unique constraint demands."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation())
        cls.official = VariableStyle.objects.create(
            variable=cls.tree["variable"], name="Official", slug="official",
            is_default=True,
        )
        cls.analyst = VariableStyle.objects.create(
            variable=cls.tree["variable"], name="Analyst", slug="analyst",
        )

    def test_promoting_flips_the_default_in_one_gesture(self):
        self.analyst.promote_to_default()
        self.official.refresh_from_db()
        self.analyst.refresh_from_db()
        self.assertFalse(self.official.is_default)
        self.assertTrue(self.analyst.is_default)

    def test_promoting_the_current_default_is_a_no_op(self):
        self.official.promote_to_default()
        self.official.refresh_from_db()
        self.assertTrue(self.official.is_default)
        self.assertEqual(
            VariableStyle.objects.filter(
                variable=self.tree["variable"], is_default=True
            ).count(),
            1,
        )


class StylingSurfaceTestCase(TestCase):
    """Shared fixture: one org tree, ramps in every tier, a signed-in member."""

    @classmethod
    def setUpTestData(cls):
        cls.org = make_organisation()
        cls.other_org = make_organisation("other-org")
        cls.tree = make_org_tree(cls.org)
        cls.collection = cls.tree["collection"]
        cls.variable = cls.tree["variable"]
        cls.global_ramp = ColorRamp.objects.get(
            organisation__isnull=True, name="viridis"
        )
        cls.org_ramp = make_ramp("House Rainfall", organisation=cls.org)
        cls.foreign_ramp = make_ramp("Foreign Rainfall", organisation=cls.other_org)

    def setUp(self):
        self.user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(
            user=self.user, organisation=self.org,
            role=OrganisationMembership.Role.ADMIN,
        )
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)

    @property
    def page_url(self):
        return reverse("collection_styling", args=[self.collection.pk])

    @property
    def form_url(self):
        return reverse(
            "variable_styling", args=[self.collection.pk, self.variable.pk]
        )

    @property
    def stops_url(self):
        return reverse(
            "variable_style_stops", args=[self.collection.pk, self.variable.pk]
        )


class StylingPageTests(StylingSurfaceTestCase):
    """The collection-level listing: every variable, range, default swatch."""

    def test_lists_every_variable_with_its_range(self):
        response = self.client.get(self.page_url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.variable.name)
        self.assertContains(response, "0")
        self.assertContains(response, "50")
        self.assertContains(response, self.form_url)

    def test_a_styleless_variable_shows_the_grayscale_default(self):
        response = self.client.get(self.page_url)
        self.assertContains(response, "Grayscale")

    def test_a_styled_variable_shows_its_default_swatch(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
            stops=[
                {"value": 0.0, "color": "#123456"},
                {"value": 50.0, "color": "#654321"},
            ],
        )
        response = self.client.get(self.page_url)
        self.assertContains(response, "#123456")

    def test_another_organisations_collection_is_not_found(self):
        foreign = make_org_tree(self.other_org)["collection"]
        response = self.client.get(
            reverse("collection_styling", args=[foreign.pk])
        )
        self.assertEqual(response.status_code, 404)

    def test_the_collection_items_page_links_here(self):
        response = self.client.get(
            reverse("collection_items_list", args=[self.collection.pk])
        )
        self.assertContains(response, self.page_url)


class VariableStylingFormTests(StylingSurfaceTestCase):
    """The canonical per-variable form."""

    def test_the_form_renders_with_range_and_ramp_choices(self):
        response = self.client.get(self.form_url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "viridis")
        self.assertContains(response, "House Rainfall")

    def test_ramp_choices_do_not_include_another_organisations(self):
        response = self.client.get(self.form_url)
        self.assertNotContains(response, "Foreign Rainfall")

    # -- the ramp picker (#383) --------------------------------------------

    def test_ramps_reach_the_page_grouped_by_type(self):
        """Grouping is the picker's one piece of teaching: an anomaly field
        wants a diverging ramp and a total wants a sequential one, and a flat
        list leaves that to memory."""
        diverging = make_ramp("House Anomaly", organisation=self.org)
        diverging.ramp_type = ColorRamp.RampType.DIVERGING
        diverging.save()

        response = self.client.get(self.form_url)
        groups = response.context["ramp_groups"]

        self.assertEqual(
            [group["label"] for group in groups], ["Sequential", "Diverging"]
        )
        names = {
            group["label"]: [ramp["name"] for ramp in group["ramps"]]
            for group in groups
        }
        self.assertIn("viridis", names["Sequential"])
        self.assertIn("RdBu", names["Diverging"])
        # Each ramp appears under its own type and nowhere else.
        self.assertIn("House Anomaly", names["Diverging"])
        self.assertNotIn("House Anomaly", names["Sequential"])

    def test_every_ramp_carries_its_own_gradient(self):
        # The markup needs no lookup table beside it, and the browser repaints
        # the toggle from the row it was handed.
        response = self.client.get(self.form_url)
        ramps = [
            ramp
            for group in response.context["ramp_groups"]
            for ramp in group["ramps"]
        ]
        self.assertTrue(ramps)
        self.assertTrue(all(ramp["gradient"].startswith("linear-gradient") for ramp in ramps))

    def test_another_organisations_ramp_is_in_no_group(self):
        response = self.client.get(self.form_url)
        names = [
            ramp["name"]
            for group in response.context["ramp_groups"]
            for ramp in group["ramps"]
        ]
        self.assertIn("House Rainfall", names)
        self.assertNotIn("Foreign Rainfall", names)

    def test_the_saved_ramp_comes_back_marked_selected(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp,
        )
        response = self.client.get(self.form_url)
        self.assertEqual(response.context["selected_ramp"]["name"], "House Rainfall")

    def test_a_styleless_variable_has_no_selected_ramp(self):
        response = self.client.get(self.form_url)
        self.assertIsNone(response.context["selected_ramp"])

    def test_the_ramp_still_saves_through_the_hidden_input(self):
        """The picker is presentation: the field is still a scoped
        ModelChoiceField named `ramp`, so nothing about saving moved."""
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            VariableStyle.objects.get(variable=self.variable).ramp, self.org_ramp
        )

    # -- range -------------------------------------------------------------

    def test_saving_the_range_updates_the_variable(self):
        response = self.client.post(self.form_url, {
            "action": "save-range",
            "value_min": "-10",
            "value_max": "45",
            "scale_type": Variable.ScaleType.LINEAR,
        })
        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.value_range, (-10.0, 45.0))

    def test_an_inverted_range_is_rejected_with_the_models_message(self):
        response = self.client.post(self.form_url, {
            "action": "save-range",
            "value_min": "45",
            "value_max": "10",
            "scale_type": Variable.ScaleType.LINEAR,
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response, "Maximum value must be greater than minimum value."
        )
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.value_range, (0.0, 50.0))

    def test_a_range_edit_alone_never_rewrites_stops(self):
        tuned = [
            {"value": 0.0, "color": "#111111"},
            {"value": 50.0, "color": "#222222"},
        ]
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp, stops=tuned,
        )
        response = self.client.post(self.form_url, {
            "action": "save-range",
            "value_min": "-20",
            "value_max": "60",
            "scale_type": Variable.ScaleType.LINEAR,
        })
        self.assertEqual(response.status_code, 302)
        style.refresh_from_db()
        self.assertEqual(style.stops, tuned)

    # -- style creation ----------------------------------------------------

    def test_creating_a_style_with_a_ramp_generates_stops_over_the_range(self):
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertEqual(response.status_code, 302)
        style = VariableStyle.objects.get(variable=self.variable)
        self.assertEqual(style.name, "Official")
        self.assertEqual(style.slug, "official")
        self.assertEqual(
            style.stops,
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )

    def test_the_first_style_becomes_the_default(self):
        self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertTrue(
            VariableStyle.objects.get(variable=self.variable).is_default
        )

    def test_a_second_style_does_not_steal_the_default(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
        )
        self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Analyst",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        analyst = VariableStyle.objects.get(variable=self.variable, slug="analyst")
        self.assertFalse(analyst.is_default)

    def test_stepped_mode_produces_n_discrete_classes(self):
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Classed",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.STEPPED,
            "steps": "7",
        })
        self.assertEqual(response.status_code, 302)
        style = VariableStyle.objects.get(variable=self.variable)
        # Two stops per class keep the edges hard through interpolation.
        self.assertEqual(len(style.stops), 14)
        self.assertEqual(style.steps, 7)

    def test_a_foreign_ramp_is_not_accepted(self):
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "",
            "name": "Official",
            "ramp": str(self.foreign_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            VariableStyle.objects.filter(variable=self.variable).exists()
        )

    # -- fine-tuning -------------------------------------------------------

    def test_submitted_stops_are_saved_verbatim(self):
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp,
            stops=[
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "official",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
            "stop_value": ["0", "0", "50"],
            "stop_color": ["#0000ff", "#ffffff", "#ff0000"],
        })
        self.assertEqual(response.status_code, 302)
        style.refresh_from_db()
        self.assertEqual(
            style.stops,
            [
                {"value": 0.0, "color": "#0000ff"},
                {"value": 0.0, "color": "#ffffff"},
                {"value": 50.0, "color": "#ff0000"},
            ],
        )

    def test_out_of_order_stops_are_rejected(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
        )
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "official",
            "name": "Official",
            "ramp": "",
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
            "stop_value": ["50", "0"],
            "stop_color": ["#000000", "#ffffff"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "ascending")

    def test_a_non_numeric_stop_value_is_rejected(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
        )
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "official",
            "name": "Official",
            "ramp": "",
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
            "stop_value": ["zero"],
            "stop_color": ["#000000"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Not a number")

    def test_a_non_hex_color_is_rejected(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
        )
        response = self.client.post(self.form_url, {
            "action": "save-style",
            "style_slug": "official",
            "name": "Official",
            "ramp": "",
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
            "stop_value": ["0"],
            "stop_color": ["red;}"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "hex color")

    # -- applying a ramp ---------------------------------------------------

    def test_applying_a_ramp_writes_nothing(self):
        """Applying is a form gesture now (#382), not a database write.

        The endpoint hands back stops the browser fills the form with; only
        Save persists them. A saved snapshot the operator has not saved over
        must therefore come back untouched.
        """
        tuned = [
            {"value": 0.0, "color": "#111111"},
            {"value": 25.0, "color": "#123123"},
            {"value": 50.0, "color": "#222222"},
        ]
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp, stops=tuned,
        )
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous"}
        )
        self.assertEqual(response.status_code, 200)
        style.refresh_from_db()
        self.assertEqual(style.stops, tuned)

    def test_the_generated_stops_are_the_ramp_over_the_saved_range(self):
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous"}
        )
        self.assertEqual(
            response.json()["stops"],
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )

    def test_stepped_generation_doubles_each_class_boundary(self):
        response = self.client.get(
            self.stops_url,
            {"ramp": str(self.org_ramp.pk), "mode": "stepped", "steps": "7"},
        )
        # Two stops per class keep the edges hard through interpolation.
        self.assertEqual(len(response.json()["stops"]), 14)

    def test_stepped_generation_without_classes_is_refused(self):
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "stepped"}
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("classes", response.json()["error"])

    def test_an_unknown_rendering_mode_is_refused(self):
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "swirly"}
        )
        self.assertEqual(response.status_code, 400)

    def test_generating_without_a_ramp_is_refused(self):
        response = self.client.get(self.stops_url, {"mode": "continuous"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("ramp", response.json()["error"])

    def test_a_non_numeric_ramp_is_refused_rather_than_erroring(self):
        response = self.client.get(
            self.stops_url, {"ramp": "viridis", "mode": "continuous"}
        )
        self.assertEqual(response.status_code, 400)

    def test_another_organisations_ramp_cannot_be_generated_from(self):
        response = self.client.get(
            self.stops_url,
            {"ramp": str(self.foreign_ramp.pk), "mode": "continuous"},
        )
        self.assertEqual(response.status_code, 400)

    def test_another_organisations_variable_is_not_found(self):
        foreign = make_org_tree(self.other_org)
        response = self.client.get(
            reverse(
                "variable_style_stops",
                args=[foreign["collection"].pk, foreign["variable"].pk],
            ),
            {"ramp": str(self.org_ramp.pk), "mode": "continuous"},
        )
        self.assertEqual(response.status_code, 404)


class StyleSetManagementTests(StylingSurfaceTestCase):
    """Named styles: promote-to-default and deletion guards."""

    def setUp(self):
        super().setUp()
        self.official = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
        )
        self.analyst = VariableStyle.objects.create(
            variable=self.variable, name="Analyst", slug="analyst",
        )

    def test_promoting_a_style_flips_the_default(self):
        response = self.client.post(self.form_url, {
            "action": "promote",
            "style_slug": "analyst",
        })
        self.assertEqual(response.status_code, 302)
        self.official.refresh_from_db()
        self.analyst.refresh_from_db()
        self.assertFalse(self.official.is_default)
        self.assertTrue(self.analyst.is_default)

    def test_a_non_default_style_can_be_deleted(self):
        response = self.client.post(self.form_url, {
            "action": "delete-style",
            "style_slug": "analyst",
        })
        self.assertEqual(response.status_code, 302)
        self.assertFalse(
            VariableStyle.objects.filter(pk=self.analyst.pk).exists()
        )

    def test_the_default_cannot_be_deleted_while_siblings_remain(self):
        response = self.client.post(self.form_url, {
            "action": "delete-style",
            "style_slug": "official",
        })
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            VariableStyle.objects.filter(pk=self.official.pk).exists()
        )

    def test_the_last_style_can_be_deleted_back_to_grayscale(self):
        self.analyst.delete()
        response = self.client.post(self.form_url, {
            "action": "delete-style",
            "style_slug": "official",
        })
        self.assertEqual(response.status_code, 302)
        self.assertFalse(
            VariableStyle.objects.filter(variable=self.variable).exists()
        )


class DemotedCollectionFormTests(StylingSurfaceTestCase):
    """The Wagtail collection form's inline variables panel after issue #323:
    range and styling are read-only there — a swatch, the range, and a link to
    the Styling surface. Exactly one surface tunes styling (ADR 0022)."""

    @property
    def edit_url(self):
        return reverse("collection:edit", args=[self.collection.pk])

    def test_the_inline_variables_panel_has_no_range_inputs(self):
        response = self.client.get(self.edit_url)
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "value_min")
        self.assertNotContains(response, "value_max")

    def test_the_inline_variables_panel_links_to_the_styling_form(self):
        response = self.client.get(self.edit_url)
        self.assertContains(response, self.form_url)

    def test_the_inline_variables_panel_shows_the_range_and_default_swatch(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
            stops=[
                {"value": 0.0, "color": "#123456"},
                {"value": 50.0, "color": "#654321"},
            ],
        )
        response = self.client.get(self.edit_url)
        self.assertContains(response, "#123456")
        self.assertContains(response, "Official")

    def test_a_variable_created_without_a_range_gets_the_grayscale_default(self):
        """The seeding fallback the demoted surfaces rely on: a variable
        provisioned with no declared range comes up 0–1 grayscale, to be tuned
        on the Styling page."""
        variable = Variable.objects.create(
            collection=self.collection, name="Fresh", slug="fresh",
            unit=self.variable.unit,
        )
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 1.0))


class PreviewItemChoiceTests(TestCase):
    """Which item the map preview draws (#382).

    The rule is newest run, earliest horizon — because a forecast feed's newest
    *valid* time is its furthest horizon, and styling against a ten-day-out
    field judges a guess rather than the weather.
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation())
        cls.variable = cls.tree["variable"]
        cls.collection = cls.tree["collection"]
        # The fixture's own asset carries no format, so it is not a COG and
        # never stands in for one.
        cls.tree["item"].delete()

    def _item(self, time, reference_time=None, fmt=Asset.Format.COG,
              variable=None, bounds=(0, 0, 10, 10)):
        item = Item.objects.create(
            collection=self.collection, time=time,
            reference_time=reference_time, bounds=list(bounds),
        )
        Asset.objects.create(
            item=item, variable=variable or self.variable,
            href="x.tif", format=fmt,
        )
        return item

    def test_a_forecast_feed_previews_the_newest_runs_earliest_horizon(self):
        old_run = datetime(2026, 3, 1, tzinfo=timezone.utc)
        new_run = datetime(2026, 3, 2, tzinfo=timezone.utc)
        self._item(datetime(2026, 3, 10, tzinfo=timezone.utc), old_run)
        analysis = self._item(new_run, new_run)
        # The furthest horizon of the newest run: newest valid time overall,
        # and the wrong answer.
        self._item(datetime(2026, 3, 12, tzinfo=timezone.utc), new_run)
        self.assertEqual(Item.objects.latest_for_variable(self.variable), analysis)

    def test_a_feed_without_reference_times_previews_the_newest_valid_time(self):
        self._item(datetime(2026, 3, 1, tzinfo=timezone.utc))
        newest = self._item(datetime(2026, 3, 3, tzinfo=timezone.utc))
        self.assertEqual(Item.objects.latest_for_variable(self.variable), newest)

    def test_an_item_carrying_no_cog_is_not_a_candidate(self):
        self._item(datetime(2026, 3, 3, tzinfo=timezone.utc), fmt=Asset.Format.PNG)
        self.assertIsNone(Item.objects.latest_for_variable(self.variable))

    def test_another_variables_item_is_not_a_candidate(self):
        other = Variable.objects.create(
            collection=self.collection, name="Other", slug="other",
            unit=self.variable.unit, value_min=0, value_max=1,
        )
        self._item(datetime(2026, 3, 3, tzinfo=timezone.utc), variable=other)
        self.assertIsNone(Item.objects.latest_for_variable(self.variable))

    def test_a_variable_with_no_items_previews_nothing(self):
        self.assertIsNone(Item.objects.latest_for_variable(self.variable))


class PreviewPanelTests(StylingSurfaceTestCase):
    """What the styling form hands the map, and what it says when it cannot."""

    def _cog_item(self, bounds=(0.0, 0.0, 10.0, 10.0)):
        item = Item.objects.create(
            collection=self.collection,
            time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            bounds=list(bounds) if bounds else None,
        )
        Asset.objects.create(
            item=item, variable=self.variable, href="x.tif",
            format=Asset.Format.COG,
        )
        return item

    def test_the_texture_url_and_extent_reach_the_page(self):
        item = self._cog_item()
        response = self.client.get(self.form_url)
        config = response.context["preview"]["config"]
        # From the database, because the texture's ``v`` token hashes the
        # range's repr — the fixture's ints and the stored floats spell it
        # differently.
        self.variable.refresh_from_db()
        self.assertEqual(config["textureUrl"], titiler_encoded_preview_url(item, self.variable))
        self.assertEqual(config["bounds"], [0.0, 0.0, 10.0, 10.0])
        # The unscale is the variable's saved range — what the texture was
        # encoded against, and what the clipping warning measures against.
        self.assertEqual(config["imageUnscale"], [0.0, 50.0])

    def test_the_panel_links_to_the_full_item_preview(self):
        item = self._cog_item()
        response = self.client.get(self.form_url)
        self.assertContains(response, reverse("item_preview", args=[item.pk]))

    def test_a_variable_with_no_ingested_data_says_so(self):
        response = self.client.get(self.form_url)
        self.assertIsNone(response.context["preview"]["config"])
        self.assertEqual(response.context["preview"]["unavailable"], "no-data")
        self.assertContains(response, "no data has been ingested")

    def test_an_item_without_an_extent_says_so_differently(self):
        self._cog_item(bounds=None)
        response = self.client.get(self.form_url)
        self.assertEqual(response.context["preview"]["unavailable"], "no-bounds")
        self.assertContains(response, "no spatial extent")
