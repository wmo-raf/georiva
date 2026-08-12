"""
The queue-rebuild action at the HTTP seam (issue #346).

The action never dispatches a build task itself: it flips the manifest to
PENDING and lets the 5-minute sweep pick it up through ``get_buildable`` —
the same locking/recovery path every other build goes through, so a
UI-triggered rebuild cannot race an in-flight build.  These tests pin the
status transitions, the POST-only/permission gates, and the cross-org
fail-closed behaviour, in the pattern of the collection-tab tests.
"""

from datetime import timedelta

from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

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


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class QueueRebuildActionTests(TestCase):
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

    def _variable(self, *, slug="precip", collection=None):
        return add_variable(collection or self.collection, slug=slug)

    def _manifest(self, variable, *, status, locked_at=None):
        return VirtualZarrManifest.objects.create(
            variable=variable,
            status=status,
            locked_at=locked_at,
        )

    def _post(self, variable, **extra):
        return self.client.post(
            reverse("variable_virtual_zarr_queue_rebuild", args=[variable.pk]),
            **extra,
        )

    # -- status transitions --------------------------------------------------

    def test_ready_manifest_is_queued_and_becomes_buildable(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.READY,
        )

        response = self._post(variable)

        self.assertEqual(response.status_code, 302)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)
        self.assertIn(manifest, VirtualZarrManifest.get_buildable())

    def test_failed_and_stale_and_no_data_manifests_are_queued(self):
        for status in (
            VirtualZarrManifest.Status.FAILED,
            VirtualZarrManifest.Status.STALE,
            VirtualZarrManifest.Status.NO_DATA,
        ):
            with self.subTest(status=status):
                variable = self._variable(slug=f"var-{status}")
                manifest = self._manifest(variable, status=status)

                self._post(variable)

                manifest.refresh_from_db()
                self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)

    def test_pending_manifest_stays_pending(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.PENDING,
        )

        response = self._post(variable, follow=True)

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)
        self.assertContains(response, "queued")

    def test_actively_building_manifest_is_not_disturbed(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=timezone.now() - timedelta(minutes=5),
        )
        manifest_locked_at = manifest.locked_at

        response = self._post(variable, follow=True)

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.BUILDING)
        self.assertEqual(manifest.locked_at, manifest_locked_at)
        self.assertContains(response, "already building")

    def test_stuck_building_manifest_is_queued_and_lock_cleared(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=(timezone.now() - VirtualZarrManifest.LOCK_TIMEOUT - timedelta(minutes=1)),
        )

        self._post(variable)

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)
        self.assertIsNone(manifest.locked_at)
        self.assertEqual(manifest.locked_by, "")

    def test_variable_without_manifest_row_gets_one_created_pending(self):
        variable = self._variable(slug="untracked")

        response = self._post(variable)

        self.assertEqual(response.status_code, 302)
        manifest = VirtualZarrManifest.objects.get(variable=variable)
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)

    # -- gates: method, permission, tenancy ----------------------------------

    def test_get_is_not_allowed(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.READY,
        )

        response = self.client.get(reverse("variable_virtual_zarr_queue_rebuild", args=[variable.pk]))

        self.assertEqual(response.status_code, 405)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)

    def test_unpermissioned_member_is_denied(self):
        variable = self._variable()
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.READY,
        )
        plain = make_user("wanjiru")
        add_member(plain, self.kenya)
        # Admin access only — no VirtualZarrManifest change permission.
        from django.contrib.auth.models import Permission

        plain.user_permissions.add(Permission.objects.get(codename="access_admin"))
        self.client.login(username="wanjiru", password=PASSWORD)

        response = self._post(variable)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], reverse("wagtailadmin_home"))
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)

    def test_anonymous_is_redirected_to_login(self):
        variable = self._variable()
        self.client.logout()

        response = self._post(variable)

        self.assertEqual(response.status_code, 302)
        self.assertIn("login", response.headers["Location"])

    def test_cross_org_request_fails_closed(self):
        variable = self._variable(slug="uganda-var", collection=self.uganda_collection)
        manifest = self._manifest(
            variable,
            status=VirtualZarrManifest.Status.READY,
        )

        response = self._post(variable)

        self.assertEqual(response.status_code, 404)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)

    # -- redirect ------------------------------------------------------------

    def test_redirects_back_to_the_page_the_button_lives_on(self):
        variable = self._variable()
        self._manifest(variable, status=VirtualZarrManifest.Status.READY)
        tab_url = reverse("collection_virtual_zarr", args=[self.collection.pk])

        response = self._post(variable, data={"next": tab_url})

        self.assertEqual(response.headers["Location"], tab_url)

    def test_offsite_next_is_ignored(self):
        variable = self._variable()
        self._manifest(variable, status=VirtualZarrManifest.Status.READY)

        response = self._post(variable, data={"next": "https://evil.example/phish"})

        self.assertEqual(
            response.headers["Location"],
            reverse("variable_virtual_zarr", args=[variable.pk]),
        )

    # -- the button on the monitoring surfaces -------------------------------

    def test_collection_tab_offers_the_action(self):
        variable = self._variable()
        self._manifest(variable, status=VirtualZarrManifest.Status.STALE)

        response = self.client.get(reverse("collection_virtual_zarr", args=[self.collection.pk]))

        self.assertContains(
            response,
            reverse("variable_virtual_zarr_queue_rebuild", args=[variable.pk]),
        )

    def test_drilldown_page_offers_the_action(self):
        variable = self._variable()
        VirtualZarrManifest.objects.create(
            variable=variable,
            status=VirtualZarrManifest.Status.FAILED,
            repo_path="",
        )

        response = self.client.get(reverse("variable_virtual_zarr", args=[variable.pk]))

        self.assertContains(
            response,
            reverse("variable_virtual_zarr_queue_rebuild", args=[variable.pk]),
        )

    def test_unpermissioned_member_sees_no_button(self):
        variable = self._variable()
        self._manifest(variable, status=VirtualZarrManifest.Status.STALE)
        plain = make_user("wanjiru")
        add_member(plain, self.kenya)
        from django.contrib.auth.models import Permission

        plain.user_permissions.add(Permission.objects.get(codename="access_admin"))
        self.client.login(username="wanjiru", password=PASSWORD)

        response = self.client.get(reverse("collection_virtual_zarr", args=[self.collection.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(
            response,
            reverse("variable_virtual_zarr_queue_rebuild", args=[variable.pk]),
        )
