"""
Model-level tests for VirtualZarrManifest after the Icechunk swap.

The record now tracks an Icechunk repository (org-first prefix on the zarr
bucket) instead of a kerchunk JSON key.  Django fields are derived caches;
the repo's commit metadata is the source of truth (design decision 5).
"""

from datetime import datetime, timezone

from django.test import TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.models import VirtualZarrManifest

UTC = timezone.utc


class VirtualZarrManifestTests(TestCase):
    def setUp(self):
        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="CHIRPS",
            slug="chirps",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            name="Monthly",
            slug="chirps-monthly",
        )
        self.unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precipitation",
            name="Precipitation",
            unit=self.unit,
            value_min=0,
            value_max=500,
        )

    def _manifest(self, **kwargs) -> VirtualZarrManifest:
        return VirtualZarrManifest.objects.create(variable=self.variable, **kwargs)

    # -- repo path derivation ------------------------------------------------

    def test_make_repo_path_is_org_first_prefix_with_trailing_slash(self):
        path = VirtualZarrManifest.make_repo_path(self.variable)
        self.assertEqual(
            path,
            f"{self.organisation.slug}/chirps/chirps-monthly/precipitation/",
        )

    def test_get_repo_path_derives_when_blank(self):
        manifest = self._manifest()
        self.assertEqual(
            manifest.get_repo_path(),
            VirtualZarrManifest.make_repo_path(self.variable),
        )

    def test_get_repo_path_prefers_stored_value(self):
        manifest = self._manifest(repo_path="acme/other/prefix/")
        self.assertEqual(manifest.get_repo_path(), "acme/other/prefix/")

    # -- READY gate ----------------------------------------------------------

    def test_open_dataset_refuses_non_ready(self):
        manifest = self._manifest()
        with self.assertRaises(ValueError):
            manifest.open_dataset()

    # -- mark_ready persists the derived cache -------------------------------

    def test_mark_ready_records_repo_state(self):
        manifest = self._manifest()
        watermark = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
        t0 = datetime(2026, 1, 1, tzinfo=UTC)
        t1 = datetime(2026, 6, 1, tzinfo=UTC)
        manifest.mark_ready(
            repo_path="kenya/chirps/chirps-monthly/precipitation/",
            item_count=6,
            time_start=t0,
            time_end=t1,
            snapshot_id="ABCDEF123456",
            watermark=watermark,
        )
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)
        self.assertEqual(manifest.repo_path, "kenya/chirps/chirps-monthly/precipitation/")
        self.assertEqual(manifest.item_count, 6)
        self.assertEqual(manifest.time_start, t0)
        self.assertEqual(manifest.time_end, t1)
        self.assertEqual(manifest.snapshot_id, "ABCDEF123456")
        self.assertEqual(manifest.watermark, watermark)
        self.assertEqual(manifest.locked_by, "")
        self.assertIsNone(manifest.locked_at)

    def test_mark_stale_only_transitions_ready_or_no_data(self):
        manifest = self._manifest()
        manifest.mark_stale()
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)

        for status in (
            VirtualZarrManifest.Status.READY,
            VirtualZarrManifest.Status.NO_DATA,
        ):
            VirtualZarrManifest.objects.filter(pk=manifest.pk).update(status=status)
            manifest.mark_stale()
            manifest.refresh_from_db()
            self.assertEqual(manifest.status, VirtualZarrManifest.Status.STALE)

    # -- NO_DATA parking -----------------------------------------------------

    def test_mark_no_data_clears_lock_and_error(self):
        manifest = self._manifest()
        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(
            status=VirtualZarrManifest.Status.BUILDING,
            locked_by="celery-x",
            error="boom",
        )
        manifest.mark_no_data()
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.NO_DATA)
        self.assertEqual(manifest.locked_by, "")
        self.assertIsNone(manifest.locked_at)
        self.assertEqual(manifest.error, "")

    def test_no_data_is_excluded_from_buildable(self):
        manifest = self._manifest()
        manifest.mark_no_data()
        self.assertNotIn(
            manifest.pk,
            VirtualZarrManifest.get_buildable().values_list("pk", flat=True),
        )
