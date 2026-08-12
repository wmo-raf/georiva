"""
Per-variable detail seam tests (spec #341, drill-down #345).

The detail report extends the coverage report with everything the drill-down
page shows: build history from the durable log, last-failure and last-GC
status, and — read live from the repo — Icechunk snapshot history and the
store structure at the tip.  Two layers, mirroring test_coverage:

  * the service against DB fixtures with no repo behind them (history,
    GC status, graceful degradation when the repo does not exist yet);
  * the service against a real local Icechunk repo built through the
    production build path (skipped unless the dev MinIO is configured).
"""

import unittest
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone

from django.conf import settings
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.coverage import variable_detail
from georiva.virtual_zarr.models import VirtualZarrBuildLog, VirtualZarrManifest

UTC = dt_timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)

S3_READY = bool(
    getattr(settings, "AWS_S3_ENDPOINT_URL", None) and getattr(settings, "GEORIVA_STORAGE_BACKEND", "") == "s3"
)

BOUNDS = (30.0, -10.0, 40.0, 0.0)


def _t(hours: int) -> datetime:
    return T0 + timedelta(hours=hours)


# ---------------------------------------------------------------------------
# Service over DB fixtures (no repo behind the manifests)
# ---------------------------------------------------------------------------


class VariableDetailTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="Models",
            slug="models",
            file_format="geotiff",
        )
        cls.collection = Collection.objects.create(
            catalog=cls.catalog,
            name="Surface",
            slug="surface",
        )
        unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        cls.variable = Variable.objects.create(
            collection=cls.collection,
            name="precip",
            slug="precip",
            unit=unit,
            value_min=0,
            value_max=1,
        )

    def setUp(self):
        self.manifest, _ = VirtualZarrManifest.objects.get_or_create(
            variable=self.variable,
        )
        # Blank repo path: the repo cannot exist yet, so the service never
        # touches object storage in these tests.
        VirtualZarrManifest.objects.filter(pk=self.manifest.pk).update(repo_path="")

    def _log(self, *, age=timedelta(), duration=timedelta(), **kwargs):
        started = timezone.now() - age
        defaults = dict(
            manifest=self.manifest,
            kind=VirtualZarrBuildLog.Kind.BUILD,
            outcome=VirtualZarrBuildLog.Outcome.SUCCESS,
            started_at=started,
            finished_at=started + duration,
        )
        defaults.update(kwargs)
        return VirtualZarrBuildLog.objects.create(**defaults)

    def test_detail_wraps_the_coverage_report(self):
        item = Item.objects.create(collection=self.collection, time=_t(0))
        Asset.objects.create(
            item=item,
            variable=self.variable,
            format=Asset.Format.COG,
            href="models/surface/precip/x.tif",
        )
        VirtualZarrManifest.objects.filter(pk=self.manifest.pk).update(repo_path="")

        detail = variable_detail(self.variable)

        self.assertEqual(detail.coverage.variable, self.variable)
        self.assertEqual(detail.coverage.catalog_count, 1)
        self.assertEqual(detail.coverage.missing, (_t(0),))

    def test_build_history_lists_build_attempts_newest_first(self):
        older = self._log(age=timedelta(hours=2))
        failed = self._log(
            age=timedelta(hours=1),
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
            error="boom",
        )
        newest = self._log(mode=VirtualZarrBuildLog.Mode.APPEND)
        self._log(kind=VirtualZarrBuildLog.Kind.GC)  # not a build attempt

        detail = variable_detail(self.variable)

        self.assertEqual(list(detail.build_history), [newest, failed, older])

    def test_build_log_row_knows_its_duration(self):
        self._log(duration=timedelta(seconds=90))

        detail = variable_detail(self.variable)

        self.assertEqual(detail.build_history[0].duration, timedelta(seconds=90))

    def test_last_failure_is_the_latest_failed_build(self):
        self._log(
            age=timedelta(hours=3),
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
        )
        latest_failure = self._log(
            age=timedelta(hours=1),
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
            error="cannot reach store",
        )
        self._log()  # a later success does not erase the failure history

        detail = variable_detail(self.variable)

        self.assertEqual(detail.last_failure_at, latest_failure.finished_at)

    def test_no_failures_means_no_last_failure_timestamp(self):
        self._log()
        self.assertIsNone(variable_detail(self.variable).last_failure_at)

    def test_last_gc_reflects_the_latest_gc_run(self):
        self._log(
            age=timedelta(days=2),
            kind=VirtualZarrBuildLog.Kind.GC,
        )
        latest = self._log(
            age=timedelta(days=1),
            kind=VirtualZarrBuildLog.Kind.GC,
            outcome=VirtualZarrBuildLog.Outcome.FAILURE,
            error="gc exploded",
        )

        detail = variable_detail(self.variable)

        self.assertEqual(detail.last_gc, latest)
        self.assertEqual(detail.last_gc.error, "gc exploded")

    def test_never_gced_repo_has_no_last_gc(self):
        self.assertIsNone(variable_detail(self.variable).last_gc)

    def test_variable_without_manifest_degrades_to_empty_detail(self):
        VirtualZarrManifest.objects.filter(pk=self.manifest.pk).delete()

        detail = variable_detail(self.variable)

        self.assertIsNone(detail.coverage.manifest)
        self.assertEqual(detail.build_history, ())
        self.assertIsNone(detail.last_failure_at)
        self.assertIsNone(detail.last_gc)
        self.assertEqual(detail.snapshots, ())
        self.assertEqual(detail.store_arrays, ())
        self.assertEqual(detail.detail_read_error, "")

    def test_repo_that_does_not_exist_yet_degrades_gracefully(self):
        detail = variable_detail(self.variable)

        self.assertEqual(detail.snapshots, ())
        self.assertEqual(detail.store_arrays, ())
        self.assertEqual(detail.detail_read_error, "")

    def test_history_is_capped_at_the_limit(self):
        for hours in range(5):
            self._log(age=timedelta(hours=hours))

        detail = variable_detail(self.variable, history_limit=3)

        self.assertEqual(len(detail.build_history), 3)


# ---------------------------------------------------------------------------
# Service over a real Icechunk repo (existing integration-test pattern)
# ---------------------------------------------------------------------------


def _purge_prefix(bucket, prefix: str) -> None:
    for entry in bucket.list_files(prefix, recursive=True):
        try:
            bucket.storage.delete(entry["path"])
        except Exception:
            pass


@unittest.skipUnless(S3_READY, "requires the dev MinIO (s3 storage backend)")
class VariableDetailIcechunkTests(TestCase):
    """Snapshot history and store structure against a real built repo."""

    def setUp(self):
        import numpy as np

        from georiva.core.storage import storage

        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Itest",
            slug=f"itest-{uuid.uuid4().hex[:8]}",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            name="Daily",
            slug="daily",
        )
        unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precip",
            name="Precip",
            unit=unit,
            value_min=0,
            value_max=500,
        )
        self.rng = np.random.default_rng(4)
        self.addCleanup(_purge_prefix, storage.assets, self.catalog.storage_prefix)
        self.addCleanup(_purge_prefix, storage.zarr, self.catalog.storage_prefix)

    def _add_item(self, day: int):
        from georiva.core.storage import storage
        from georiva.ingestion.asset_writer import AssetWriter

        ts = datetime(2026, 3, day, tzinfo=UTC)
        item = Item.objects.create(
            collection=self.collection,
            time=ts,
            bounds=list(BOUNDS),
            crs="EPSG:4326",
            width=64,
            height=64,
        )
        key = f"{self.catalog.storage_prefix}/daily/precip/2026/03/{day:02d}/precip.tif"
        AssetWriter(storage.assets).write_cog(self.rng.random((64, 64), dtype="float32"), key, BOUNDS)
        Asset.objects.create(
            item=item,
            variable=self.variable,
            format=Asset.Format.COG,
            href=key,
        )
        return item

    def _build(self):
        from georiva.virtual_zarr.tasks import _run_build

        manifest, _ = VirtualZarrManifest.objects.get_or_create(
            variable=self.variable,
        )
        _run_build(manifest)
        manifest.refresh_from_db()
        return manifest

    def test_snapshot_history_carries_committed_coverage_metadata(self):
        self._add_item(1)
        self._add_item(2)
        manifest = self._build()

        detail = variable_detail(self.variable)

        self.assertEqual(detail.detail_read_error, "")
        self.assertGreaterEqual(len(detail.snapshots), 1)
        tip = detail.snapshots[0]
        self.assertEqual(tip.id, manifest.snapshot_id)
        self.assertIsNotNone(tip.written_at)
        self.assertEqual(tip.metadata.get("item_count"), 2)
        self.assertIn("watermark", tip.metadata)

    def test_each_build_commit_adds_a_snapshot(self):
        self._add_item(1)
        self._build()
        before = len(variable_detail(self.variable).snapshots)

        self._add_item(2)
        self._build()

        after = variable_detail(self.variable).snapshots
        self.assertEqual(len(after), before + 1)

    def test_store_structure_lists_arrays_at_the_tip(self):
        self._add_item(1)
        self._add_item(2)
        self._build()

        detail = variable_detail(self.variable)

        by_name = {a.name: a for a in detail.store_arrays}
        self.assertIn("precip", by_name)
        self.assertIn("time", by_name)
        precip = by_name["precip"]
        self.assertEqual(precip.shape, (2, 64, 64))
        self.assertEqual(precip.dtype, "float32")
        self.assertEqual(len(precip.chunks), 3)
