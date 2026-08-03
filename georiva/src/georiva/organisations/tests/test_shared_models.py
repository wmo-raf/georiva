"""The models that are not simply one organisation's, and how each behaves.

Three verdicts are under test here (decision #269), and they differ:

* ``Topic`` — instance-global and instance-curated. Every organisation reads the
  same taxonomy; only the instance admin may change it.
* ``ColorPalette`` — global *with* org overrides. An organisation reads the
  instance-wide library and its own, and writes only its own.
* ``AdminBoundary`` — shared reference data whose chooser is deliberately
  unscoped, because a regional centre clips against several countries.

What is asserted is external behavior on the real admin URLs: which rows a
listing shows, which edits are refused, which options a form offers.
"""
from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection, ColorPalette, Unit, Variable
from georiva.core.models.catalog import Topic
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable
from georiva.organisations.access import is_shared_reference
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

KENYA_HOST = "kenya.georiva.test"


def grant_everything(user):
    """Every capability Wagtail knows, so only tenancy decides the outcome."""
    group = Group.objects.create(name=f"{user.username} everything")
    group.permissions.add(*Permission.objects.all())
    user.groups.add(group)
    return user


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class TopicIsInstanceAdminOnlyTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def _login(self, username, *, superuser=False):
        user = grant_everything(make_user(username, superuser=superuser))
        if not superuser:
            add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username=username, password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = KENYA_HOST
        return user

    def test_superuser_can_reach_the_topic_admin(self):
        self._login("root", superuser=True)
        self.assertEqual(self.client.get(reverse("topic:index")).status_code, 200)

    def test_org_admin_cannot_reach_the_topic_index(self):
        self._login("amina")
        self.assertEqual(self.client.get(reverse("topic:index")).status_code, 302)

    def test_org_admin_cannot_create_a_topic(self):
        self._login("amina")
        response = self.client.post(
            reverse("topic:add"), {"name": "Smuggled", "description": "", "sort_order": 0}
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Topic.objects.filter(name="Smuggled").exists())

    def test_the_topic_menu_item_is_hidden_from_org_admins(self):
        self._login("amina")
        response = self.client.get(reverse("wagtailadmin_home"))
        self.assertNotContains(response, reverse("topic:index"))

    def test_topics_are_still_offered_on_the_catalog_form(self):
        """The other half: members meet topics as options, and must keep doing so."""
        Topic.objects.create(name="Rainfall")
        self._login("amina")
        response = self.client.get(reverse("catalog:add"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Rainfall")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ColorPaletteTierTests(TestCase):
    """One instance-wide palette, one palette each for two organisations."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.shipped = ColorPalette.objects.create(name="Shipped Rainfall")
        cls.kenya_palette = ColorPalette.objects.create(
            name="Kenya Rainfall", organisation=cls.kenya
        )
        cls.uganda_palette = ColorPalette.objects.create(
            name="Uganda Rainfall", organisation=cls.uganda
        )

    def setUp(self):
        self.user = grant_everything(make_user("amina"))
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = KENYA_HOST

    # -- listing -----------------------------------------------------------

    def test_the_listing_shows_the_instance_wide_tier_and_this_organisations(self):
        response = self.client.get(reverse("colorpalette:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shipped Rainfall")
        self.assertContains(response, "Kenya Rainfall")

    def test_the_listing_does_not_show_another_organisations_palette(self):
        response = self.client.get(reverse("colorpalette:index"))
        self.assertNotContains(response, "Uganda Rainfall")

    def test_the_listing_offers_no_edit_link_for_an_instance_wide_palette(self):
        response = self.client.get(reverse("colorpalette:index"))
        self.assertNotContains(
            response, reverse("colorpalette:edit", args=[self.shipped.pk])
        )
        self.assertContains(
            response, reverse("colorpalette:edit", args=[self.kenya_palette.pk])
        )

    # -- writes ------------------------------------------------------------

    def test_a_member_cannot_edit_an_instance_wide_palette(self):
        url = reverse("colorpalette:edit", args=[self.shipped.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_member_cannot_delete_an_instance_wide_palette(self):
        url = reverse("colorpalette:delete", args=[self.shipped.pk])
        self.assertEqual(self.client.post(url).status_code, 404)
        self.assertTrue(ColorPalette.objects.filter(pk=self.shipped.pk).exists())

    def test_a_member_can_edit_their_own_organisations_palette(self):
        url = reverse("colorpalette:edit", args=[self.kenya_palette.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_a_member_cannot_edit_another_organisations_palette(self):
        url = reverse("colorpalette:edit", args=[self.uganda_palette.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_superuser_can_edit_an_instance_wide_palette(self):
        grant_everything(make_user("root", superuser=True))
        self.client.login(username="root", password=PASSWORD)
        url = reverse("colorpalette:edit", args=[self.shipped.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_there_is_no_copy_view_to_rewrite_an_instance_wide_palette_through(self):
        """Wagtail's copy view saves over its source, so palettes do not offer one.

        Left enabled it would be the one write a member could reach an
        instance-wide palette with — and it would rename it for every
        organisation at once.
        """
        from django.urls import NoReverseMatch

        with self.assertRaises(NoReverseMatch):
            reverse("colorpalette:copy", args=[self.shipped.pk])

    def test_a_superuser_can_create_an_instance_wide_palette(self):
        """The tier has to be authorable by somebody, and that somebody is the
        instance admin."""
        grant_everything(make_user("root", superuser=True))
        self.client.login(username="root", password=PASSWORD)
        response = self.client.post(
            reverse("colorpalette:add"),
            {
                "name": "Shipped Temperature",
                "palette_type": ColorPalette.PaletteType.SEQUENTIAL,
                "center_value": "",
                "organisation": "",
                "stops-TOTAL_FORMS": "0",
                "stops-INITIAL_FORMS": "0",
                "stops-MIN_NUM_FORMS": "0",
                "stops-MAX_NUM_FORMS": "1000",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertIsNone(ColorPalette.objects.get(name="Shipped Temperature").organisation)

    def test_a_member_is_not_offered_the_tier_choice(self):
        response = self.client.get(reverse("colorpalette:add"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="organisation"')

    def test_a_palette_created_here_belongs_to_this_organisation(self):
        response = self.client.post(
            reverse("colorpalette:add"),
            {
                "name": "Kenya Temperature",
                "palette_type": ColorPalette.PaletteType.SEQUENTIAL,
                "center_value": "",
                "stops-TOTAL_FORMS": "0",
                "stops-INITIAL_FORMS": "0",
                "stops-MIN_NUM_FORMS": "0",
                "stops-MAX_NUM_FORMS": "1000",
            },
        )
        self.assertEqual(response.status_code, 302, response.context.get("form").errors
                         if response.status_code == 200 else "")
        created = ColorPalette.objects.get(name="Kenya Temperature")
        self.assertEqual(created.organisation, self.kenya)

    # -- choosing ----------------------------------------------------------

    def test_the_collection_editors_inline_variables_offer_only_this_organisations_palettes(self):
        """The inline panel: a variable's palette is chosen inside the collection form."""
        catalog = Catalog.objects.create(
            organisation=self.kenya, name="Forecast", slug="forecast",
            file_format=Catalog.FileFormat.GEOTIFF,
        )
        collection = Collection.objects.create(catalog=catalog, name="Daily", slug="daily")
        unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
        Variable.objects.create(
            collection=collection, name="Temperature", slug="temperature",
            unit=unit, value_min=0, value_max=1,
        )

        response = self.client.get(reverse("collection:edit", args=[collection.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shipped Rainfall")
        self.assertContains(response, "Kenya Rainfall")
        # Both the rows that exist and the blank row the "add another" button
        # clones are rendered here; neither may name another institution's palette.
        self.assertNotContains(response, "Uganda Rainfall")

    def test_the_variable_form_offers_both_tiers_but_not_another_organisation(self):
        """Where a palette is actually picked: the variable editor's palette field."""
        catalog = Catalog.objects.create(
            organisation=self.kenya, name="Forecast", slug="forecast",
            file_format=Catalog.FileFormat.GEOTIFF,
        )
        collection = Collection.objects.create(catalog=catalog, name="Daily", slug="daily")
        unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
        Variable.objects.create(
            collection=collection, name="Temperature", slug="temperature",
            unit=unit, value_min=0, value_max=1,
        )
        config = ManualUploadConfig.objects.create(
            catalog=catalog, name="Daily upload",
            valid_time_format=ManualUploadConfig.ValidTimeFormat.YYYYMMDD,
        )
        mcv = ManualUploadConfigVariable.objects.create(
            config=config, collection=collection, variable_name="temperature"
        )

        response = self.client.get(
            reverse("manual_upload_variable_edit", args=[config.pk, mcv.pk])
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shipped Rainfall")
        self.assertContains(response, "Kenya Rainfall")
        self.assertNotContains(response, "Uganda Rainfall")


class SharedReferenceDataTests(TestCase):
    """The exemption, asserted rather than left to a comment."""

    def test_admin_boundaries_read_as_shared_reference_data(self):
        from adminboundarymanager.models import AdminBoundary

        self.assertTrue(is_shared_reference(AdminBoundary))
