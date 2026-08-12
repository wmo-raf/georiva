"""
The per-collection Virtual Zarr tab at the HTTP seam.

Org-scoped and fail-closed like every other admin surface (the manifest-admin
tests' pattern): one organisation's member, dialling their own host, chasing
the other's collection pk. Rendering assertions stay at the level a client
sees — status labels, coverage counts, the stuck-vs-building distinction.
"""

from datetime import datetime, timedelta, timezone as dt_timezone

from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import (
    PASSWORD,
    add_member,
    grant_everything,
    make_user,
)
from georiva.virtual_zarr.models import VirtualZarrManifest

UTC = dt_timezone.utc


def build_collection(organisation, *, slug):
    catalog = Catalog.objects.create(
        organisation=organisation,
        name=slug,
        slug=slug,
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    return Collection.objects.create(catalog=catalog, name=slug, slug=slug)


def add_variable(collection, *, slug):
    unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
    return Variable.objects.create(
        collection=collection,
        name=slug,
        slug=slug,
        unit=unit,
        value_min=0,
        value_max=1,
    )


def add_cog(collection, variable, *, hours):
    item = Item.objects.create(
        collection=collection,
        time=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=hours),
    )
    return Asset.objects.create(
        item=item,
        variable=variable,
        format=Asset.Format.COG,
        href=f"{collection.slug}/{variable.slug}/{hours}.tif",
    )


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class CollectionVirtualZarrTabTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.collection = build_collection(cls.kenya, slug="kenya-forecast")
        cls.uganda_collection = build_collection(cls.uganda, slug="uganda-forecast")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        grant_everything(self.user)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _get(self, collection=None):
        collection = collection or self.collection
        return self.client.get(reverse("collection_virtual_zarr", args=[collection.pk]))

    # -- tenancy: fail closed ----------------------------------------------

    def test_another_organisations_collection_is_not_found(self):
        response = self._get(self.uganda_collection)
        self.assertEqual(response.status_code, 404)

    def test_anonymous_is_redirected_to_login(self):
        self.client.logout()
        response = self._get()
        self.assertEqual(response.status_code, 302)

    # -- rendering ----------------------------------------------------------

    def test_lists_every_variable_with_status_and_coverage(self):
        precip = add_variable(self.collection, slug="precip")
        add_cog(self.collection, precip, hours=0)
        add_cog(self.collection, precip, hours=24)
        # Signal-created manifest, still PENDING; blank the repo path so the
        # test never touches object storage.
        VirtualZarrManifest.objects.filter(variable=precip).update(repo_path="")

        response = self._get()

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "precip")
        self.assertContains(response, "Pending build")
        # repo 0 of 2 catalog timestamps, both missing
        self.assertContains(response, 'data-coverage="0/2"')
        self.assertContains(response, "2 missing")

    def test_variable_without_manifest_row_shows_sensibly(self):
        add_variable(self.collection, slug="untracked")

        response = self._get()

        self.assertContains(response, "untracked")
        self.assertContains(response, "Not tracked")

    def test_stuck_build_displays_distinctly_from_active_build(self):
        now = timezone.now()
        building = add_variable(self.collection, slug="active-build")
        VirtualZarrManifest.objects.create(
            variable=building,
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=now - timedelta(minutes=5),
        )
        stuck = add_variable(self.collection, slug="stuck-build")
        VirtualZarrManifest.objects.create(
            variable=stuck,
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=now - VirtualZarrManifest.LOCK_TIMEOUT - timedelta(minutes=1),
        )

        response = self._get()

        self.assertContains(response, "Stuck")
        self.assertContains(response, "Building")

    def test_stale_manifest_renders_its_status(self):
        variable = add_variable(self.collection, slug="gone-stale")
        VirtualZarrManifest.objects.create(
            variable=variable,
            status=VirtualZarrManifest.Status.STALE,
        )

        response = self._get()

        self.assertContains(response, "Stale")

    def test_failed_manifest_shows_its_error(self):
        variable = add_variable(self.collection, slug="broken")
        manifest = VirtualZarrManifest.objects.create(variable=variable)
        manifest.mark_failed("cannot reach store")

        response = self._get()

        self.assertContains(response, "Failed")
        self.assertContains(response, "cannot reach store")

    # -- navigation ---------------------------------------------------------

    def test_items_page_links_to_the_virtual_zarr_tab_and_back(self):
        tab_url = reverse("collection_virtual_zarr", args=[self.collection.pk])
        items_url = reverse("collection_items_list", args=[self.collection.pk])

        response = self.client.get(items_url)
        self.assertContains(response, tab_url)

        response = self._get()
        self.assertContains(response, items_url)
