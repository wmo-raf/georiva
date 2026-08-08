"""The Styling surface (ADR 0022, issue #321): one place where styling is tuned.

Two seams:

* the model — stop ordering on ``VariableStyle.clean()`` and the atomic
  default-promotion helper, neither of which the earlier slices needed;
* the admin surface — the collection Styling page and the canonical
  per-variable form, driven through the admin test client: listing with
  swatches, ramp application, stop fine-tuning, the re-apply warning path,
  default promotion, deletion guards, and org scoping of ramp choices.
"""
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import ColorRamp, ColorRampStop, Variable, VariableStyle
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

    # -- re-apply ramp -----------------------------------------------------

    def test_reapplying_over_tuned_stops_asks_before_discarding(self):
        tuned = [
            {"value": 0.0, "color": "#111111"},
            {"value": 25.0, "color": "#123123"},
            {"value": 50.0, "color": "#222222"},
        ]
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp, stops=tuned,
        )
        response = self.client.post(self.form_url, {
            "action": "apply-ramp",
            "style_slug": "official",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "discard")
        style.refresh_from_db()
        self.assertEqual(style.stops, tuned)

    def test_a_confirmed_reapply_regenerates_the_snapshot(self):
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp,
            stops=[
                {"value": 0.0, "color": "#111111"},
                {"value": 25.0, "color": "#123123"},
                {"value": 50.0, "color": "#222222"},
            ],
        )
        response = self.client.post(self.form_url, {
            "action": "apply-ramp",
            "style_slug": "official",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
            "confirm_discard": "1",
        })
        self.assertEqual(response.status_code, 302)
        style.refresh_from_db()
        self.assertEqual(
            style.stops,
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )

    def test_applying_onto_an_empty_snapshot_needs_no_confirmation(self):
        style = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True, ramp=self.org_ramp,
        )
        response = self.client.post(self.form_url, {
            "action": "apply-ramp",
            "style_slug": "official",
            "name": "Official",
            "ramp": str(self.org_ramp.pk),
            "mode": VariableStyle.Mode.CONTINUOUS,
            "steps": "",
        })
        self.assertEqual(response.status_code, 302)
        style.refresh_from_db()
        self.assertEqual(len(style.stops), 2)


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
