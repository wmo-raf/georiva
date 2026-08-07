"""The Color ramp catalog (ADR 0022): value-free aesthetics, tiered, seeded.

Three subjects, each at its agreed seam:

* the model — ordered colors, optional 0–1 positions, the gradient helper;
* the seed migration — the curated instance-wide catalog under
  matplotlib-compatible names, asserted against the migrated test database;
* the admin surface — the same tier rules the palette admin has (org-scoped
  listing, global tier read-only to operators, instance admin curates), plus
  the gradient preview in the listing.
"""
from importlib import import_module

from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from django.urls import NoReverseMatch, reverse

from georiva.core.models import ColorRamp, ColorRampStop
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from georiva.organisations.tests.factories import (
    PASSWORD,
    add_member,
    grant_everything,
    make_user,
)

KENYA_HOST = "kenya.georiva.test"

#: The seed catalog, imported from the data migration itself so the test and
#: the seed can never drift apart. (importlib because the module name starts
#: with a digit.)
SEED_RAMPS = import_module(
    "georiva.core.migrations.0011_seed_color_ramp_catalog"
).SEED_RAMPS


class ColorRampModelTests(TestCase):

    def _ramp(self, hexes, *, ramp_type=ColorRamp.RampType.SEQUENTIAL, positions=None):
        ramp = ColorRamp.objects.create(name="Test ramp", ramp_type=ramp_type)
        positions = positions or [None] * len(hexes)
        for i, (hex_value, position) in enumerate(zip(hexes, positions)):
            ColorRampStop.objects.create(
                ramp=ramp, hex_value=hex_value, position=position, sort_order=i
            )
        return ramp

    def test_colors_without_positions_spread_evenly(self):
        ramp = self._ramp(["#000000", "#808080", "#ffffff"])
        self.assertEqual(
            ramp.css_gradient(),
            "linear-gradient(to right, #000000 0%, #808080 50%, #ffffff 100%)",
        )

    def test_explicit_positions_are_honoured(self):
        ramp = self._ramp(["#000000", "#ff0000", "#ffffff"], positions=[0, 0.9, 1])
        self.assertEqual(
            ramp.css_gradient(),
            "linear-gradient(to right, #000000 0%, #ff0000 90%, #ffffff 100%)",
        )

    def test_out_of_order_positions_are_clamped_non_decreasing(self):
        # CSS silently clamps out-of-order gradient stops, so a preview built
        # from them would misrepresent the ramp; the gradient does the
        # clamping itself instead. Here the evenly-spread middle color (50%)
        # would fall behind the pinned 90% first stop.
        ramp = self._ramp(["#000000", "#ff0000", "#ffffff"], positions=[0.9, None, 1])
        self.assertEqual(
            ramp.css_gradient(),
            "linear-gradient(to right, #000000 90%, #ff0000 90%, #ffffff 100%)",
        )

    def test_a_qualitative_ramp_renders_hard_edged_blocks(self):
        ramp = self._ramp(
            ["#111111", "#222222"], ramp_type=ColorRamp.RampType.QUALITATIVE
        )
        self.assertEqual(
            ramp.css_gradient(),
            "linear-gradient(to right, #111111 0% 50%, #222222 50% 100%)",
        )

    def test_a_bare_hex_gets_its_hash_back_for_css(self):
        ramp = self._ramp(["000000", "ffffff"])
        self.assertIn("#000000 0%", ramp.css_gradient())

    def test_a_ramp_with_no_colors_has_no_gradient_and_no_preview(self):
        ramp = ColorRamp.objects.create(name="Empty")
        self.assertEqual(ramp.css_gradient(), "")
        self.assertEqual(ramp.preview(), "")

    def test_a_position_outside_zero_one_is_rejected(self):
        ramp = ColorRamp.objects.create(name="Test ramp")
        stop = ColorRampStop(ramp=ramp, hex_value="#ffffff", position=1.5)
        with self.assertRaises(ValidationError):
            stop.full_clean()

    def test_a_non_hex_color_is_rejected(self):
        ramp = ColorRamp.objects.create(name="Test ramp")
        stop = ColorRampStop(ramp=ramp, hex_value="red;}")
        with self.assertRaises(ValidationError):
            stop.full_clean()


class SeededCatalogTests(TestCase):
    """The data migration's work, visible in the migrated test database."""

    def test_the_curated_catalog_is_seeded_at_the_instance_wide_tier(self):
        for name, ramp_type, hexes in SEED_RAMPS:
            with self.subTest(name=name):
                ramp = ColorRamp.objects.get(organisation__isnull=True, name=name)
                self.assertEqual(ramp.ramp_type, ramp_type)
                self.assertEqual(
                    [stop.hex_value for stop in ramp.stops.all()], hexes
                )

    def test_the_catalog_is_a_curated_set_not_the_full_matplotlib_registry(self):
        count = ColorRamp.objects.filter(organisation__isnull=True).count()
        self.assertGreaterEqual(count, 15)
        self.assertLessEqual(count, 20)

    def test_the_headline_ramps_are_present_under_matplotlib_names(self):
        names = set(
            ColorRamp.objects.filter(organisation__isnull=True).values_list(
                "name", flat=True
            )
        )
        self.assertLessEqual({"viridis", "cividis", "RdBu", "Blues", "Greys"}, names)

    def test_seeded_ramps_carry_no_values_only_colors(self):
        # The catalog is value-free by construction: positions are empty, so
        # the ramp implies even spacing and nothing else.
        self.assertFalse(
            ColorRampStop.objects.filter(
                ramp__organisation__isnull=True, position__isnull=False
            ).exists()
        )


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ColorRampAdminTierTests(TestCase):
    """The seeded catalog plus one ramp each for two organisations."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.shipped = ColorRamp.objects.get(organisation__isnull=True, name="viridis")
        cls.kenya_ramp = ColorRamp.objects.create(
            name="Kenya Rainfall", organisation=cls.kenya
        )
        ColorRampStop.objects.create(
            ramp=cls.kenya_ramp, hex_value="#ffffff", sort_order=0
        )
        ColorRampStop.objects.create(
            ramp=cls.kenya_ramp, hex_value="#0000ff", sort_order=1
        )
        cls.uganda_ramp = ColorRamp.objects.create(
            name="Uganda Rainfall", organisation=cls.uganda
        )

    def setUp(self):
        self.user = grant_everything(make_user("amina"))
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = KENYA_HOST

    # -- listing -----------------------------------------------------------

    def test_the_listing_shows_the_instance_wide_tier_and_this_organisations(self):
        response = self.client.get(reverse("colorramp:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "viridis")
        self.assertContains(response, "Kenya Rainfall")

    def test_the_listing_does_not_show_another_organisations_ramp(self):
        response = self.client.get(reverse("colorramp:index"))
        self.assertNotContains(response, "Uganda Rainfall")

    def test_the_listing_shows_a_gradient_preview_per_ramp(self):
        response = self.client.get(reverse("colorramp:index"))
        self.assertContains(response, "linear-gradient(to right, #440154 0%")
        self.assertContains(
            response, "linear-gradient(to right, #ffffff 0%, #0000ff 100%)"
        )

    def test_the_listing_offers_no_edit_link_for_an_instance_wide_ramp(self):
        response = self.client.get(reverse("colorramp:index"))
        self.assertNotContains(
            response, reverse("colorramp:edit", args=[self.shipped.pk])
        )
        self.assertContains(
            response, reverse("colorramp:edit", args=[self.kenya_ramp.pk])
        )

    # -- writes ------------------------------------------------------------

    def test_a_member_cannot_edit_an_instance_wide_ramp(self):
        url = reverse("colorramp:edit", args=[self.shipped.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_member_cannot_delete_an_instance_wide_ramp(self):
        url = reverse("colorramp:delete", args=[self.shipped.pk])
        self.assertEqual(self.client.post(url).status_code, 404)
        self.assertTrue(ColorRamp.objects.filter(pk=self.shipped.pk).exists())

    def test_a_member_can_edit_their_own_organisations_ramp(self):
        url = reverse("colorramp:edit", args=[self.kenya_ramp.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_a_member_cannot_edit_another_organisations_ramp(self):
        url = reverse("colorramp:edit", args=[self.uganda_ramp.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_member_can_delete_their_own_organisations_ramp(self):
        doomed = ColorRamp.objects.create(name="Doomed", organisation=self.kenya)
        url = reverse("colorramp:delete", args=[doomed.pk])
        self.assertEqual(self.client.post(url).status_code, 302)
        self.assertFalse(ColorRamp.objects.filter(pk=doomed.pk).exists())

    def test_a_superuser_can_edit_an_instance_wide_ramp(self):
        grant_everything(make_user("root", superuser=True))
        self.client.login(username="root", password=PASSWORD)
        url = reverse("colorramp:edit", args=[self.shipped.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_there_is_no_copy_view_to_rewrite_an_instance_wide_ramp_through(self):
        """Wagtail's copy view saves over its source, so ramps do not offer one."""
        with self.assertRaises(NoReverseMatch):
            reverse("colorramp:copy", args=[self.shipped.pk])

    def test_a_superuser_can_create_an_instance_wide_ramp(self):
        grant_everything(make_user("root", superuser=True))
        self.client.login(username="root", password=PASSWORD)
        response = self.client.post(
            reverse("colorramp:add"),
            {
                "name": "Shipped Temperature",
                "ramp_type": ColorRamp.RampType.SEQUENTIAL,
                "organisation": "",
                "stops-TOTAL_FORMS": "0",
                "stops-INITIAL_FORMS": "0",
                "stops-MIN_NUM_FORMS": "0",
                "stops-MAX_NUM_FORMS": "1000",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertIsNone(ColorRamp.objects.get(name="Shipped Temperature").organisation)

    def test_a_member_is_not_offered_the_tier_choice(self):
        response = self.client.get(reverse("colorramp:add"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="organisation"')

    def test_a_ramp_created_here_belongs_to_this_organisation(self):
        response = self.client.post(
            reverse("colorramp:add"),
            {
                "name": "Kenya Temperature",
                "ramp_type": ColorRamp.RampType.SEQUENTIAL,
                "stops-TOTAL_FORMS": "2",
                "stops-INITIAL_FORMS": "0",
                "stops-MIN_NUM_FORMS": "0",
                "stops-MAX_NUM_FORMS": "1000",
                "stops-0-hex_value": "#ffffff",
                "stops-0-position": "",
                "stops-0-ORDER": "0",
                "stops-1-hex_value": "#ff0000",
                "stops-1-position": "",
                "stops-1-ORDER": "1",
            },
        )
        if response.status_code == 200:
            self.fail(f"form did not save: {response.context['form'].errors}")
        self.assertEqual(response.status_code, 302)
        created = ColorRamp.objects.get(name="Kenya Temperature")
        self.assertEqual(created.organisation, self.kenya)
        self.assertEqual(
            [stop.hex_value for stop in created.stops.all()],
            ["#ffffff", "#ff0000"],
        )
