"""
The per-variable drill-down page at the HTTP seam (spec #341, #345).

Org-scoped and fail-closed like the collection tab: one organisation's
member, dialling their own host, chasing the other's variable pk.  Rendering
assertions stay at the level a client sees — timestamp lists, build-history
rows, GC status, graceful degradation when no repo exists yet.
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
from georiva.virtual_zarr.models import VirtualZarrBuildLog, VirtualZarrManifest

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
class VariableDrilldownTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.collection = build_collection(cls.kenya, slug="kenya-forecast")
        cls.uganda_collection = build_collection(cls.uganda, slug="uganda-forecast")
        cls.uganda_variable = add_variable(cls.uganda_collection, slug="temp")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        grant_everything(self.user)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _variable(self, slug="precip"):
        variable = add_variable(self.collection, slug=slug)
        # Any signal-created manifest gets a blank repo path so the page
        # never touches object storage in these tests.
        VirtualZarrManifest.objects.filter(variable=variable).update(repo_path="")
        return variable

    def _get(self, variable):
        return self.client.get(reverse("variable_virtual_zarr", args=[variable.pk]))

    def _manifest(self, variable, **kwargs):
        manifest, _ = VirtualZarrManifest.objects.get_or_create(variable=variable)
        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(repo_path="", **kwargs)
        manifest.refresh_from_db()
        return manifest

    # -- tenancy: fail closed ----------------------------------------------

    def test_another_organisations_variable_is_not_found(self):
        response = self._get(self.uganda_variable)
        self.assertEqual(response.status_code, 404)

    def test_anonymous_is_redirected_to_login(self):
        variable = self._variable()
        self.client.logout()
        response = self._get(variable)
        self.assertEqual(response.status_code, 302)

    # -- rendering ----------------------------------------------------------

    def test_missing_timestamps_and_skipped_items_are_listed(self):
        variable = self._variable()
        add_cog(self.collection, variable, hours=0)
        add_cog(self.collection, variable, hours=24)
        Item.objects.create(  # no COG for this variable: the skipped set
            collection=self.collection,
            time=datetime(2026, 1, 3, tzinfo=UTC),
        )
        VirtualZarrManifest.objects.filter(variable=variable).update(repo_path="")

        response = self._get(variable)

        self.assertEqual(response.status_code, 200)
        # Both catalog timestamps are missing from the (nonexistent) repo.
        self.assertContains(response, "2026-01-01 00:00")
        self.assertContains(response, "2026-01-02 00:00")
        # The item without a COG shows up as skipped, not as coverage.
        self.assertContains(response, "2026-01-03 00:00")

    def test_build_history_renders_with_failures_and_gc_status(self):
        variable = self._variable()
        manifest = self._manifest(variable)
        now = timezone.now()
        VirtualZarrBuildLog.objects.create(
            manifest=manifest,
            kind=VirtualZarrBuildLog.Kind.BUILD,
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
            started_at=now - timedelta(hours=2),
            finished_at=now - timedelta(hours=2),
            error="cannot reach store",
        )
        VirtualZarrBuildLog.objects.create(
            manifest=manifest,
            kind=VirtualZarrBuildLog.Kind.BUILD,
            outcome=VirtualZarrBuildLog.Outcome.SUCCESS,
            mode=VirtualZarrBuildLog.Mode.APPEND,
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(hours=1) + timedelta(seconds=30),
            items_written=3,
        )
        VirtualZarrBuildLog.objects.create(
            manifest=manifest,
            kind=VirtualZarrBuildLog.Kind.GC,
            outcome=VirtualZarrBuildLog.Outcome.SUCCESS,
            started_at=now - timedelta(minutes=30),
            finished_at=now - timedelta(minutes=30),
        )

        response = self._get(variable)

        self.assertContains(response, "cannot reach store")
        self.assertContains(response, "Append")
        # The GC section reports the latest run's outcome.
        self.assertContains(response, "vz-gc-status")

    def test_failed_manifest_shows_error_and_last_failure_time(self):
        variable = self._variable()
        manifest = self._manifest(variable)
        manifest.mark_failed("disk full")
        VirtualZarrBuildLog.objects.create(
            manifest=manifest,
            kind=VirtualZarrBuildLog.Kind.BUILD,
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
            started_at=timezone.now(),
            finished_at=timezone.now(),
            error="disk full",
        )

        response = self._get(variable)

        self.assertContains(response, "disk full")
        self.assertContains(response, "vz-last-failure")

    def test_variable_with_no_repo_yet_degrades_gracefully(self):
        variable = self._variable()
        self._manifest(variable)

        response = self._get(variable)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No snapshots yet")
        self.assertContains(response, "No arrays yet")

    def test_variable_without_manifest_row_renders_sensibly(self):
        variable = self._variable()
        VirtualZarrManifest.objects.filter(variable=variable).delete()

        response = self._get(variable)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Not tracked")

    def test_cached_repo_size_renders(self):
        variable = self._variable()
        self._manifest(variable, repo_size_bytes=2048, repo_object_count=9)

        response = self._get(variable)

        self.assertContains(response, "2.0")  # 2.0 KB
        self.assertContains(response, "9")

    # -- navigation ---------------------------------------------------------

    def test_tab_links_each_variable_to_its_drilldown(self):
        variable = self._variable()
        drilldown_url = reverse("variable_virtual_zarr", args=[variable.pk])

        response = self.client.get(reverse("collection_virtual_zarr", args=[self.collection.pk]))

        self.assertContains(response, drilldown_url)

    def test_drilldown_links_back_to_the_tab(self):
        variable = self._variable()
        tab_url = reverse("collection_virtual_zarr", args=[self.collection.pk])

        response = self._get(variable)

        self.assertContains(response, tab_url)
