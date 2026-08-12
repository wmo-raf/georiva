"""The manifest admin at the HTTP seam: org-scoped, useful columns, safe form.

The manifest used to be a bare ``@register_snippet`` — every organisation's
rows in one listing, raw status/lock fields on the form. These tests pin the
replacement: an org-scoped SnippetViewSet that fails closed across
organisations (following the organisations test patterns) and keeps the
derived coverage caches out of the form.
"""

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import (
    PASSWORD,
    add_member,
    grant_everything,
    make_user,
)
from georiva.virtual_zarr.models import VirtualZarrManifest

URL_NS = "wagtailsnippets_georivavirtualzarr_virtualzarrmanifest"


def build_manifest(organisation, *, slug):
    """One organisation's manifest, slugged after it — the manifest's admin
    label is built from slugs, so a distinct slug is what makes a row
    recognisable (or leak-proof) in a rendered listing."""
    catalog = Catalog.objects.create(
        organisation=organisation,
        name=slug,
        slug=slug,
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    collection = Collection.objects.create(catalog=catalog, name=slug, slug=slug)
    unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
    variable = Variable.objects.create(
        collection=collection,
        name=slug,
        slug=slug,
        unit=unit,
        value_min=0,
        value_max=1,
    )
    return VirtualZarrManifest.objects.create(variable=variable)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ManifestAdminTests(TestCase):
    """One organisation's member, dialling their own host, chasing the other's pks."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_manifest = build_manifest(cls.kenya, slug="kenya-forecast")
        cls.uganda_manifest = build_manifest(cls.uganda, slug="uganda-forecast")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        grant_everything(self.user)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    # -- cross-org: fail closed --------------------------------------------

    def test_listing_shows_only_this_organisations_manifests(self):
        response = self.client.get(reverse(f"{URL_NS}:list"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "kenya-forecast")
        self.assertNotContains(response, "uganda-forecast")

    def test_editing_another_organisations_manifest_is_not_found(self):
        response = self.client.get(reverse(f"{URL_NS}:edit", args=[self.uganda_manifest.pk]))
        self.assertEqual(response.status_code, 404)

    def test_deleting_another_organisations_manifest_is_not_found(self):
        response = self.client.get(reverse(f"{URL_NS}:delete", args=[self.uganda_manifest.pk]))
        self.assertEqual(response.status_code, 404)

    def test_the_same_views_serve_this_organisations_manifest(self):
        for url_name in ("edit", "delete"):
            with self.subTest(view=url_name):
                response = self.client.get(reverse(f"{URL_NS}:{url_name}", args=[self.kenya_manifest.pk]))
                self.assertEqual(response.status_code, 200)

    # -- listing: columns and filters --------------------------------------

    def test_listing_shows_status_and_built_at_columns(self):
        response = self.client.get(reverse(f"{URL_NS}:list"))
        self.assertContains(response, "Status")
        self.assertContains(response, "Built at")

    def test_listing_offers_a_status_filter(self):
        self.kenya_manifest.mark_ready(
            repo_path=VirtualZarrManifest.make_repo_path(self.kenya_manifest.variable),
            item_count=1,
            time_start="2026-01-01T00:00:00Z",
            time_end="2026-01-02T00:00:00Z",
        )
        response = self.client.get(
            reverse(f"{URL_NS}:list"),
            {"status": VirtualZarrManifest.Status.PENDING},
        )
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "kenya-forecast")

        response = self.client.get(
            reverse(f"{URL_NS}:list"),
            {"status": VirtualZarrManifest.Status.READY},
        )
        self.assertContains(response, "kenya-forecast")

    # -- form: derived caches stay derived ---------------------------------

    def test_coverage_summary_fields_are_not_editable(self):
        response = self.client.get(reverse(f"{URL_NS}:edit", args=[self.kenya_manifest.pk]))
        form = response.context["form"]
        for field in ("time_start", "time_end", "item_count", "watermark", "snapshot_id"):
            self.assertNotIn(field, form.fields)

    def test_lock_bookkeeping_is_not_editable(self):
        response = self.client.get(reverse(f"{URL_NS}:edit", args=[self.kenya_manifest.pk]))
        form = response.context["form"]
        for field in ("locked_at", "locked_by"):
            self.assertNotIn(field, form.fields)
