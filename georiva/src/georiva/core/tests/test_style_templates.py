"""Style templates (#786): value-anchored, unit-declaring band sets, tiered like ramps.

Two seams:

* the model — what a template must hold to be one (``full_clean``);
* the admin catalogue — the tier rules the ramp catalog has (org-scoped listing,
  global tier read-only to operators, instance admin curates), plus the rule
  ramps don't have: a shipped template (one with an ``origin``) is read-only to
  everyone, and "Copy to my organisation" is how anyone adapts one.
"""

from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from django.urls import NoReverseMatch, reverse

from georiva.core.models import StyleTemplate, Unit
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, grant_everything, make_user

KENYA_HOST = "kenya.georiva.test"


def _unit(symbol="mm"):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


def _template(**overrides):
    fields = {
        "name": "Precipitation",
        "slug": "precipitation",
        "quantity": StyleTemplate.Quantity.PRECIPITATION,
        "levels": [0.5, 1, 2, 5],
        "colors": ["#ffffff", "#a0c8f0", "#0000ff"],
        "unit": _unit(),
    }
    fields.update(overrides)
    return StyleTemplate(**fields)


class StyleTemplateValidationTests(TestCase):
    def test_a_well_formed_template_is_valid(self):
        _template().full_clean()

    def test_out_of_range_behaviour_defaults_to_clamp(self):
        template = _template()
        self.assertEqual(template.under, "clamp")
        self.assertEqual(template.over, "clamp")

    def test_levels_must_be_strictly_ascending(self):
        for levels in ([0, 1, 1, 2], [0, 2, 1, 3]):
            with self.subTest(levels=levels), self.assertRaises(ValidationError) as caught:
                _template(levels=levels).full_clean()
            self.assertIn("levels", caught.exception.message_dict)

    def test_there_is_exactly_one_colour_per_band(self):
        for colors in (["#ffffff", "#0000ff"], ["#ffffff", "#a0c8f0", "#0000ff", "#000000"]):
            with self.subTest(colors=colors), self.assertRaises(ValidationError) as caught:
                _template(colors=colors).full_clean()
            self.assertIn("colors", caught.exception.message_dict)

    def test_a_template_needs_at_least_two_bands(self):
        for levels, colors in (([1], []), ([0, 1], ["#ffffff"])):
            with self.subTest(levels=levels), self.assertRaises(ValidationError) as caught:
                _template(levels=levels, colors=colors).full_clean()
            self.assertIn("levels", caught.exception.message_dict)

    def test_two_bands_are_enough(self):
        _template(levels=[0, 1, 2], colors=["#ffffff", "#0000ff"]).full_clean()

    def test_levels_must_be_numbers(self):
        with self.assertRaises(ValidationError) as caught:
            _template(levels=[0, "one", 2, 3]).full_clean()
        self.assertIn("levels", caught.exception.message_dict)

    def test_colours_must_be_hex(self):
        with self.assertRaises(ValidationError) as caught:
            _template(colors=["#ffffff", "red;}", "#0000ff"]).full_clean()
        self.assertIn("colors", caught.exception.message_dict)

    def test_transparent_is_an_out_of_range_choice(self):
        _template(under="transparent", over="transparent").full_clean()
        with self.assertRaises(ValidationError):
            _template(under="hide").full_clean()


def _saved(**overrides):
    template = _template(**overrides)
    template.full_clean()
    template.save()
    return template


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class StyleTemplateCatalogueTests(TestCase):
    """A shipped template, an instance-admin one, and one each for two organisations."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.shipped = _saved(
            name="Shipped precipitation",
            slug="shipped-precip",
            origin="earthkit-plots",
            source_name="precip-ek",
            under="transparent",
        )
        cls.instance = _saved(
            name="Instance temperature",
            slug="instance-temp",
            quantity=StyleTemplate.Quantity.TEMPERATURE,
            unit=_unit("degC"),
            levels=[-4, 0, 4],
            colors=["#0000ff", "#ff0000"],
        )
        cls.kenya_template = _saved(name="Kenya rainfall", slug="kenya-rain", organisation=cls.kenya)
        cls.uganda_template = _saved(name="Uganda rainfall", slug="uganda-rain", organisation=cls.uganda)

    def setUp(self):
        self.user = grant_everything(make_user("amina"))
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self._login("amina")

    def _login(self, username):
        self.client.login(username=username, password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = KENYA_HOST

    def _superuser(self):
        grant_everything(make_user("root", superuser=True))
        self._login("root")

    # -- listing -----------------------------------------------------------

    def test_the_listing_shows_the_instance_wide_tier_and_this_organisations(self):
        response = self.client.get(reverse("styletemplate:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shipped precipitation")
        self.assertContains(response, "Instance temperature")
        self.assertContains(response, "Kenya rainfall")

    def test_the_listing_does_not_show_another_organisations_template(self):
        response = self.client.get(reverse("styletemplate:index"))
        self.assertNotContains(response, "Uganda rainfall")

    def test_the_listing_groups_by_quantity(self):
        content = self.client.get(reverse("styletemplate:index")).content.decode()
        # Temperature is listed before Precipitation, in the picker's order.
        self.assertLess(content.index("Instance temperature"), content.index("Shipped precipitation"))

    def test_the_listing_previews_the_bands_levels_and_unit(self):
        response = self.client.get(reverse("styletemplate:index"))
        self.assertContains(response, "linear-gradient(to right, #0000ff 0% 50%, #ff0000 50% 100%)")
        self.assertContains(response, "-4, 0, 4 degC")
        self.assertContains(response, "0.5, 1, 2, 5 mm")

    def test_the_listing_offers_edit_links_only_where_an_edit_would_be_admitted(self):
        response = self.client.get(reverse("styletemplate:index"))
        self.assertContains(response, reverse("styletemplate:edit", args=[self.kenya_template.pk]))
        self.assertNotContains(response, reverse("styletemplate:edit", args=[self.instance.pk]))
        self.assertNotContains(response, reverse("styletemplate:edit", args=[self.shipped.pk]))

    def test_the_instance_admin_is_offered_no_edit_link_for_a_shipped_template(self):
        self._superuser()
        response = self.client.get(reverse("styletemplate:index"))
        self.assertContains(response, reverse("styletemplate:edit", args=[self.instance.pk]))
        self.assertNotContains(response, reverse("styletemplate:edit", args=[self.shipped.pk]))
        self.assertNotContains(response, reverse("styletemplate:delete", args=[self.shipped.pk]))

    # -- writes ------------------------------------------------------------

    def test_a_member_cannot_edit_or_delete_an_instance_wide_template(self):
        self.assertEqual(self.client.get(reverse("styletemplate:edit", args=[self.instance.pk])).status_code, 404)
        self.assertEqual(self.client.post(reverse("styletemplate:delete", args=[self.instance.pk])).status_code, 404)
        self.assertTrue(StyleTemplate.objects.filter(pk=self.instance.pk).exists())

    def test_a_member_cannot_edit_another_organisations_template(self):
        url = reverse("styletemplate:edit", args=[self.uganda_template.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_member_can_edit_their_own_organisations_template(self):
        url = reverse("styletemplate:edit", args=[self.kenya_template.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_the_instance_admin_can_edit_an_instance_wide_template(self):
        self._superuser()
        url = reverse("styletemplate:edit", args=[self.instance.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_a_shipped_template_is_read_only_even_to_the_instance_admin(self):
        self._superuser()
        edit = reverse("styletemplate:edit", args=[self.shipped.pk])
        self.assertEqual(self.client.get(edit).status_code, 404)
        self.assertEqual(self.client.post(edit, {"name": "Renamed"}).status_code, 404)
        delete = reverse("styletemplate:delete", args=[self.shipped.pk])
        self.assertEqual(self.client.post(delete).status_code, 404)
        self.shipped.refresh_from_db()
        self.assertEqual(self.shipped.name, "Shipped precipitation")

    def test_a_shipped_template_is_read_only_in_an_organisations_tier_too(self):
        # An origin is what makes a template read-only, whatever tier it sits in.
        shipped_here = _saved(name="Org shipped", slug="org-shipped", organisation=self.kenya, origin="earthkit-plots")
        self.assertEqual(self.client.get(reverse("styletemplate:edit", args=[shipped_here.pk])).status_code, 404)
        self.assertEqual(self.client.post(reverse("styletemplate:delete", args=[shipped_here.pk])).status_code, 404)

    def test_there_is_no_wagtail_copy_view_to_rewrite_a_template_through(self):
        with self.assertRaises(NoReverseMatch):
            reverse("styletemplate:copy", args=[self.shipped.pk])

    # -- authoring ---------------------------------------------------------

    def _form(self, **overrides):
        data = {
            "name": "Kenya temperature",
            "slug": "kenya-temp",
            "quantity": StyleTemplate.Quantity.TEMPERATURE,
            "unit": str(_unit("degC").pk),
            "levels": "-4, 0, 4.5, 8",
            "colors": "#0000ff, #ffffff, #ff0000",
            "under": "clamp",
            "over": "transparent",
            "scale_type": "linear",
        }
        data.update(overrides)
        return data

    def test_a_template_created_here_belongs_to_this_organisation(self):
        response = self.client.post(reverse("styletemplate:add"), self._form())
        if response.status_code == 200:
            self.fail(f"form did not save: {response.context['form'].errors}")
        created = StyleTemplate.objects.get(slug="kenya-temp")
        self.assertEqual(created.organisation, self.kenya)
        self.assertEqual(created.levels, [-4, 0, 4.5, 8])
        self.assertEqual(created.colors, ["#0000ff", "#ffffff", "#ff0000"])
        self.assertEqual(created.over, "transparent")

    def test_an_operator_cannot_author_a_shipped_template(self):
        self.client.post(reverse("styletemplate:add"), self._form(origin="earthkit-plots"))
        self.assertEqual(StyleTemplate.objects.get(slug="kenya-temp").origin, "")

    def test_a_template_whose_colours_do_not_match_its_bands_is_refused(self):
        response = self.client.post(reverse("styletemplate:add"), self._form(colors="#0000ff, #ffffff"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("colors", response.context["form"].errors)
        self.assertFalse(StyleTemplate.objects.filter(slug="kenya-temp").exists())

    def test_levels_that_are_not_numbers_are_refused(self):
        response = self.client.post(reverse("styletemplate:add"), self._form(levels="0, one, 2, 3"))
        self.assertEqual(response.status_code, 200)
        self.assertIn("levels", response.context["form"].errors)

    def test_the_edit_form_shows_levels_and_colours_as_lists(self):
        response = self.client.get(reverse("styletemplate:edit", args=[self.kenya_template.pk]))
        self.assertContains(response, "0.5, 1, 2, 5")
        self.assertContains(response, "#ffffff, #a0c8f0, #0000ff")

    def test_a_member_is_not_offered_the_tier_choice(self):
        response = self.client.get(reverse("styletemplate:add"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="organisation"')

    def test_the_instance_admin_can_create_an_instance_wide_template(self):
        self._superuser()
        response = self.client.post(reverse("styletemplate:add"), self._form(organisation=""))
        if response.status_code == 200:
            self.fail(f"form did not save: {response.context['form'].errors}")
        self.assertIsNone(StyleTemplate.objects.get(slug="kenya-temp").organisation)

    # -- copy to my organisation -------------------------------------------

    def _copy_url(self, template):
        return reverse("styletemplate:copy_to_organisation", args=[template.pk])

    def test_the_listing_offers_a_copy_of_every_visible_template(self):
        response = self.client.get(reverse("styletemplate:index"))
        for template in (self.shipped, self.instance, self.kenya_template):
            self.assertContains(response, self._copy_url(template))

    def test_copying_asks_for_confirmation_first(self):
        response = self.client.get(self._copy_url(self.shipped))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shipped precipitation")
        self.assertFalse(StyleTemplate.objects.filter(organisation=self.kenya, name="Shipped precipitation").exists())

    def test_a_shipped_template_copies_into_this_organisation_as_an_editable_template(self):
        response = self.client.post(self._copy_url(self.shipped))
        copy = StyleTemplate.objects.get(organisation=self.kenya, slug="shipped-precip")
        self.assertRedirects(response, reverse("styletemplate:edit", args=[copy.pk]), fetch_redirect_response=False)
        self.assertEqual(copy.origin, "")
        self.assertEqual(copy.name, "Shipped precipitation")
        self.assertEqual(copy.levels, [0.5, 1, 2, 5])
        self.assertEqual(copy.colors, ["#ffffff", "#a0c8f0", "#0000ff"])
        self.assertEqual(copy.unit, self.shipped.unit)
        self.assertEqual(copy.under, "transparent")
        self.assertEqual(copy.source_name, "precip-ek")
        self.assertEqual(self.client.get(reverse("styletemplate:edit", args=[copy.pk])).status_code, 200)
        self.shipped.refresh_from_db()
        self.assertIsNone(self.shipped.organisation)

    def test_copying_again_makes_a_second_copy_under_a_free_slug(self):
        self.client.post(self._copy_url(self.kenya_template))
        self.client.post(self._copy_url(self.kenya_template))
        slugs = set(StyleTemplate.objects.filter(organisation=self.kenya).values_list("slug", flat=True))
        self.assertEqual(slugs, {"kenya-rain", "kenya-rain-copy", "kenya-rain-copy-2"})

    def test_another_organisations_template_cannot_be_copied(self):
        self.assertEqual(self.client.post(self._copy_url(self.uganda_template)).status_code, 404)
        self.assertFalse(StyleTemplate.objects.filter(organisation=self.kenya, slug="uganda-rain").exists())

    def test_the_instance_admin_copies_into_the_hosts_organisation(self):
        self._superuser()
        self.client.post(self._copy_url(self.shipped))
        self.assertTrue(StyleTemplate.objects.filter(organisation=self.kenya, slug="shipped-precip").exists())


class StyleTemplatesMenuTests(TestCase):
    """A reference library beside the Color ramp catalog, under Settings."""

    def test_style_templates_sit_in_settings_just_after_color_ramps(self):
        from django.contrib.auth import get_user_model
        from django.test import RequestFactory
        from wagtail.admin.menu import settings_menu

        request = RequestFactory().get("/admin/")
        request.user = get_user_model().objects.create_superuser("admin", "a@a.com", "pw")
        items = {str(i.label): i for i in settings_menu.menu_items_for_request(request)}
        self.assertIn("Style Templates", items)
        self.assertLess(items["Color Ramps"].order, items["Style Templates"].order)
        self.assertLess(items["Style Templates"].order, items["Topics"].order)
