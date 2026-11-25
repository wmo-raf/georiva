"""The shipped Style template catalogue (#789): earthkit-plots styles, vendored as JSON.

Two seams:

* the loader — ``load_catalogue``, what the data migration calls: it upserts
  instance-wide templates by slug and creates the units they need;
* the catalogue an operator sees — every organisation lists the shipped
  templates, and none can edit them.
"""

import json
import tempfile
from pathlib import Path

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import StyleTemplate, Unit
from georiva.core.style_catalogue import EARTHKIT_PLOTS, MAGICS, load_catalogue
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, grant_everything, make_user


def _vendored():
    return json.loads(EARTHKIT_PLOTS.read_text())


def _catalogue_file(templates, units):
    """A catalogue of our own, in the vendored file's shape."""
    catalogue = {"source": {"name": "test-origin", "tag": "0"}, "units": units, "templates": templates}
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)  # noqa: SIM115 - closed below
    with handle:
        json.dump(catalogue, handle)
    return Path(handle.name)


def _entry(**overrides):
    entry = {
        "slug": "rain-rate",
        "name": "Rain rate",
        "quantity": "precipitation",
        "levels": [1, 2, 4],
        "colors": ["#a0c8f0", "#0000ff"],
        "unit": "mi/h",
        "under": "transparent",
        "over": "clamp",
        "scale_type": "sqrt",
        "source_name": "sh-rain-rate",
    }
    entry.update(overrides)
    return entry


MILES_PER_HOUR = {"symbol": "mi/h", "name": "Mile per hour", "description": "Test unit."}


class ShippedCatalogueTests(TestCase):
    """What the data migration left behind."""

    def test_every_vendored_template_is_shipped_at_the_instance_wide_tier(self):
        for entry in _vendored()["templates"]:
            with self.subTest(slug=entry["slug"]):
                template = StyleTemplate.objects.get(organisation=None, slug=entry["slug"])
                self.assertEqual(template.origin, "earthkit-plots")
                self.assertEqual(template.source_name, entry["source_name"])
                self.assertEqual(template.unit.symbol, entry["unit"])
                self.assertEqual(template.levels, entry["levels"])
                self.assertTrue(template.is_shipped)

    def test_every_shipped_template_is_a_valid_template(self):
        shipped = StyleTemplate.objects.filter(origin="earthkit-plots")
        self.assertEqual(shipped.count(), len(_vendored()["templates"]))
        for template in shipped:
            with self.subTest(slug=template.slug):
                template.full_clean()

    def test_every_shipped_templates_unit_is_a_valid_unit(self):
        for unit in Unit.objects.filter(pk__in=StyleTemplate.objects.values("unit")):
            with self.subTest(symbol=unit.symbol):
                unit.full_clean()

    def test_the_catalogue_covers_every_curated_quantity(self):
        quantities = set(StyleTemplate.objects.filter(origin="earthkit-plots").values_list("quantity", flat=True))
        expected = set(StyleTemplate.Quantity.values) - {"pressure", "other"}  # no shaded MSLP upstream
        self.assertEqual(expected - quantities, set())

    def test_units_georiva_did_not_seed_are_shipped_with_the_catalogue(self):
        for symbol in ("°F", "kt", "g/kg", "µg/m³", "s"):
            with self.subTest(symbol=symbol):
                self.assertTrue(Unit.objects.filter(symbol=symbol).exists())

    def test_precipitation_leaves_drizzle_undrawn(self):
        # earthkit extends this style above its levels only: below 0.5 mm nothing is drawn.
        template = StyleTemplate.objects.get(slug="precipitation-0-5-to-200mm")
        self.assertEqual((template.under, template.over), ("transparent", "clamp"))
        self.assertEqual(template.levels[0], 0.5)
        self.assertEqual(template.unit.symbol, "mm")
        self.assertEqual(template.scale_type, "sqrt")


class ShippedMagicsCatalogueTests(TestCase):
    """The Magics catalogue (#811), an additional source beside earthkit-plots."""

    @classmethod
    def setUpTestData(cls):
        cls.vendored = json.loads(MAGICS.read_text())

    def test_every_vendored_magics_template_is_shipped_with_its_style_name(self):
        for entry in self.vendored["templates"]:
            with self.subTest(slug=entry["slug"]):
                template = StyleTemplate.objects.get(organisation=None, slug=entry["slug"])
                self.assertEqual(template.origin, "magics")
                self.assertEqual(template.source_name, entry["source_name"])
                self.assertEqual(template.unit.symbol, entry["unit"])
                self.assertEqual((template.levels, template.colors), (entry["levels"], entry["colors"]))
                template.full_clean()

    def test_the_earthkit_plots_catalogue_is_left_as_it_was(self):
        earthkit = _vendored()["templates"]
        self.assertEqual(StyleTemplate.objects.filter(origin="earthkit-plots").count(), len(earthkit))
        self.assertFalse({t["slug"] for t in earthkit} & {t["slug"] for t in self.vendored["templates"]})

    def test_precipitation_is_magics_level_list_as_magics_draws_it(self):
        template = StyleTemplate.objects.get(organisation=None, slug="magics-sh-blured-f05t300lst-mm")
        self.assertEqual(template.name, "Precipitation, 0.5 to 250 mm, 7 bands")
        self.assertEqual(template.quantity, "precipitation")
        self.assertEqual(template.levels, [0.5, 2, 4, 10, 25, 50, 100, 250])
        self.assertEqual(len(template.colors), 7)
        self.assertEqual((template.under, template.over), ("transparent", "transparent"))
        self.assertEqual(template.unit.symbol, "mm")

    def test_every_magics_templates_unit_resolves_in_the_unit_registry(self):
        for symbol in {t["unit"] for t in self.vendored["templates"]}:
            with self.subTest(symbol=symbol):
                Unit.objects.get(symbol=symbol).full_clean()


class LoaderTests(TestCase):
    def test_reloading_the_vendored_catalogue_changes_nothing(self):
        before = dict(StyleTemplate.objects.values_list("slug", "pk"))
        units_before = Unit.objects.count()
        load_catalogue(EARTHKIT_PLOTS)
        self.assertEqual(dict(StyleTemplate.objects.values_list("slug", "pk")), before)
        self.assertEqual(Unit.objects.count(), units_before)

    def test_reloading_puts_a_shipped_template_back_as_the_catalogue_has_it(self):
        entry = _vendored()["templates"][0]
        StyleTemplate.objects.filter(slug=entry["slug"], organisation=None).update(
            name="Tampered", colors=["#000000"] * (len(entry["levels"]) - 1), origin=""
        )
        load_catalogue(EARTHKIT_PLOTS)
        template = StyleTemplate.objects.get(slug=entry["slug"], organisation=None)
        self.assertEqual(template.name, entry["name"])
        self.assertEqual(template.colors, entry["colors"])
        self.assertEqual(template.origin, "earthkit-plots")

    def test_a_template_is_created_with_its_origin_and_source_name(self):
        load_catalogue(_catalogue_file([_entry()], [MILES_PER_HOUR]))
        template = StyleTemplate.objects.get(slug="rain-rate", organisation=None)
        self.assertEqual(template.origin, "test-origin")
        self.assertEqual(template.source_name, "sh-rain-rate")
        self.assertEqual((template.under, template.over, template.scale_type), ("transparent", "clamp", "sqrt"))
        self.assertEqual(template.colors, ["#a0c8f0", "#0000ff"])

    def test_a_missing_unit_is_created_as_shared_reference_data(self):
        self.assertFalse(Unit.objects.filter(symbol="mi/h").exists())
        path = _catalogue_file([_entry()], [MILES_PER_HOUR])
        load_catalogue(path)
        load_catalogue(path)
        unit = Unit.objects.get(symbol="mi/h")
        self.assertEqual(unit.name, "Mile per hour")
        self.assertEqual(StyleTemplate.objects.get(slug="rain-rate").unit, unit)

    def test_an_existing_unit_is_used_as_it_is(self):
        Unit.objects.create(symbol="mi/h", name="Miles an hour", description="An operator's own.")
        load_catalogue(_catalogue_file([_entry()], [MILES_PER_HOUR]))
        self.assertEqual(Unit.objects.get(symbol="mi/h").name, "Miles an hour")

    def test_a_unit_whose_name_is_taken_by_another_symbol_is_still_created(self):
        Unit.objects.create(symbol="mile/hour", name="Mile per hour")
        load_catalogue(_catalogue_file([_entry()], [MILES_PER_HOUR]))
        self.assertEqual(StyleTemplate.objects.get(slug="rain-rate").unit.symbol, "mi/h")

    def test_an_organisations_template_with_the_same_slug_is_left_alone(self):
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        mm, _ = Unit.objects.get_or_create(symbol="mm", defaults={"name": "test mm"})
        own = StyleTemplate.objects.create(
            organisation=kenya, name="Kenya rain", slug="rain-rate", levels=[0, 1], colors=["#ffffff"], unit=mm
        )
        load_catalogue(_catalogue_file([_entry()], [MILES_PER_HOUR]))
        own.refresh_from_db()
        self.assertEqual((own.name, own.origin), ("Kenya rain", ""))
        self.assertTrue(StyleTemplate.objects.filter(slug="rain-rate", organisation=None).exists())


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ShippedTemplatesInEveryOrganisationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.first = StyleTemplate.objects.filter(origin="earthkit-plots").in_catalogue_order().first()

    def _member_of(self, organisation, username):
        user = grant_everything(make_user(username))
        add_member(user, organisation, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username=username, password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = f"{organisation.slug}.georiva.test"

    def test_every_organisation_lists_the_shipped_templates(self):
        for organisation, username in ((self.kenya, "amina"), (self.uganda, "okello")):
            with self.subTest(organisation=organisation.slug):
                self._member_of(organisation, username)
                response = self.client.get(reverse("styletemplate:index"))
                self.assertContains(response, self.first.name)

    def test_no_organisation_can_edit_or_delete_a_shipped_template(self):
        self._member_of(self.kenya, "amina")
        self.assertEqual(self.client.get(reverse("styletemplate:edit", args=[self.first.pk])).status_code, 404)
        self.assertEqual(self.client.post(reverse("styletemplate:delete", args=[self.first.pk])).status_code, 404)
