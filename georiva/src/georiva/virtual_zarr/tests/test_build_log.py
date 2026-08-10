"""
Build-log model tests plus the task seams that need no MinIO: a failing
build leaves a durable failure row, and the daily GC task prunes expired
build-log rows.
"""

from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.models import VirtualZarrBuildLog, VirtualZarrManifest


class BuildLogTestCase(TestCase):
    def setUp(self):
        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="CHIRPS", slug="chirps", file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog, name="Monthly", slug="chirps-monthly",
        )
        self.unit, _ = Unit.objects.get_or_create(
            name="Millimetre", defaults={"symbol": "mm"}
        )
        self.variable = Variable.objects.create(
            collection=self.collection, slug="precipitation",
            name="Precipitation", unit=self.unit, value_min=0, value_max=500,
        )
        self.manifest = VirtualZarrManifest.objects.create(
            variable=self.variable,
            repo_path=VirtualZarrManifest.make_repo_path(self.variable),
        )

    def _log(self, *, age=timedelta(), **kwargs) -> VirtualZarrBuildLog:
        now = timezone.now() - age
        defaults = dict(
            manifest=self.manifest,
            kind=VirtualZarrBuildLog.Kind.BUILD,
            outcome=VirtualZarrBuildLog.Outcome.SUCCESS,
            started_at=now,
            finished_at=now,
        )
        defaults.update(kwargs)
        return VirtualZarrBuildLog.objects.create(**defaults)


class BuildLogModelTests(BuildLogTestCase):
    def test_ordering_is_latest_first(self):
        older = self._log(age=timedelta(hours=2))
        newer = self._log()
        self.assertEqual(
            list(VirtualZarrBuildLog.objects.all()), [newer, older]
        )

    def test_prune_expired_removes_only_rows_past_retention(self):
        expired = self._log(age=VirtualZarrBuildLog.RETENTION + timedelta(days=1))
        kept_old = self._log(age=VirtualZarrBuildLog.RETENTION - timedelta(days=1))
        kept_new = self._log()

        removed = VirtualZarrBuildLog.prune_expired()

        self.assertEqual(removed, 1)
        remaining = set(VirtualZarrBuildLog.objects.values_list("pk", flat=True))
        self.assertEqual(remaining, {kept_old.pk, kept_new.pk})
        self.assertNotIn(expired.pk, remaining)


class BuildTaskFailureLogTests(BuildLogTestCase):
    def test_failed_build_leaves_durable_failure_row(self):
        from georiva.virtual_zarr.tasks import build_virtual_zarr_manifest

        # No Items at all → the build fails before touching object storage.
        build_virtual_zarr_manifest.apply(args=[self.manifest.pk])

        self.manifest.refresh_from_db()
        self.assertEqual(
            self.manifest.status, VirtualZarrManifest.Status.FAILED
        )

        log = self.manifest.build_logs.get()
        self.assertEqual(log.kind, VirtualZarrBuildLog.Kind.BUILD)
        self.assertEqual(log.outcome, VirtualZarrBuildLog.Outcome.FAILURE)
        self.assertIn("No COG assets", log.error)
        self.assertEqual(log.mode, "")
        self.assertEqual(log.items_written, 0)
        self.assertLessEqual(log.started_at, log.finished_at)

    def test_each_failure_gets_its_own_row(self):
        from georiva.virtual_zarr.tasks import build_virtual_zarr_manifest

        build_virtual_zarr_manifest.apply(args=[self.manifest.pk])
        build_virtual_zarr_manifest.apply(args=[self.manifest.pk])

        self.assertEqual(self.manifest.build_logs.count(), 2)


class GcPruneTests(BuildLogTestCase):
    def test_gc_task_prunes_expired_build_log_rows(self):
        from georiva.virtual_zarr.tasks import gc_virtual_zarr_repos

        # Blank repo_path keeps the manifest out of the per-repo GC loop, so
        # this exercises pruning without needing MinIO.
        VirtualZarrManifest.objects.filter(pk=self.manifest.pk).update(
            repo_path=""
        )
        expired = self._log(
            age=VirtualZarrBuildLog.RETENTION + timedelta(days=1)
        )
        kept = self._log()

        gc_virtual_zarr_repos.apply()

        remaining = set(VirtualZarrBuildLog.objects.values_list("pk", flat=True))
        self.assertEqual(remaining, {kept.pk})
        self.assertNotIn(expired.pk, remaining)
