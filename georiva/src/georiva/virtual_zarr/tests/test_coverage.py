"""
Coverage-report seam tests (spec #341, tracer #344).

Three layers, mirroring the classifier/integration split this app already
uses:

  * pure timestamp-diff functions — direct unit tests, classifier style;
  * the coverage service against DB fixtures with no repo behind them
    (pending manifests, missing manifests, items without COG assets);
  * the coverage service against a real local Icechunk repo built through
    the production build path (skipped unless the dev MinIO is configured).
"""

import unittest
import uuid
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone

from django.conf import settings
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.coverage import (
    TimestampDiff,
    as_utc,
    collection_coverage,
    diff_timestamps,
    freshness_lag,
    is_lock_expired,
    variable_coverage,
)
from georiva.virtual_zarr.models import VirtualZarrManifest

UTC = dt_timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)

S3_READY = bool(
    getattr(settings, "AWS_S3_ENDPOINT_URL", None) and getattr(settings, "GEORIVA_STORAGE_BACKEND", "") == "s3"
)

BOUNDS = (30.0, -10.0, 40.0, 0.0)


def _t(hours: int) -> datetime:
    return T0 + timedelta(hours=hours)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


class AsUtcTests(SimpleTestCase):
    def test_naive_is_stamped_utc(self):
        naive = datetime(2026, 1, 1, 12)
        self.assertEqual(as_utc(naive), datetime(2026, 1, 1, 12, tzinfo=UTC))

    def test_aware_is_converted(self):
        eat = dt_timezone(timedelta(hours=3))
        self.assertEqual(
            as_utc(datetime(2026, 1, 1, 15, tzinfo=eat)),
            datetime(2026, 1, 1, 12, tzinfo=UTC),
        )


class DiffTimestampsTests(SimpleTestCase):
    def test_identical_sets_diff_empty(self):
        diff = diff_timestamps([_t(0), _t(1)], [_t(1), _t(0)])
        self.assertEqual(diff, TimestampDiff(missing=(), extra=()))

    def test_catalog_only_timestamps_are_missing_sorted(self):
        diff = diff_timestamps([_t(2), _t(0), _t(1)], [_t(1)])
        self.assertEqual(diff.missing, (_t(0), _t(2)))
        self.assertEqual(diff.extra, ())

    def test_repo_only_timestamps_are_extra(self):
        diff = diff_timestamps([_t(0)], [_t(0), _t(5)])
        self.assertEqual(diff.extra, (_t(5),))

    def test_naive_repo_times_match_aware_catalog_times(self):
        # The builder writes tz-naive UTC into the repo's time axis; the
        # catalog side is tz-aware. They must compare equal.
        diff = diff_timestamps([_t(0)], [datetime(2026, 1, 1)])
        self.assertEqual(diff, TimestampDiff(missing=(), extra=()))

    def test_empty_both_sides(self):
        self.assertEqual(diff_timestamps([], []), TimestampDiff((), ()))


class FreshnessLagTests(SimpleTestCase):
    def test_none_when_no_assets(self):
        self.assertIsNone(freshness_lag(None, _t(0)))

    def test_none_when_never_committed(self):
        self.assertIsNone(freshness_lag(_t(0), None))

    def test_positive_lag(self):
        self.assertEqual(freshness_lag(_t(3), _t(1)), timedelta(hours=2))

    def test_watermark_ahead_clamps_to_zero(self):
        self.assertEqual(freshness_lag(_t(1), _t(3)), timedelta(0))


class LockExpiryTests(SimpleTestCase):
    TIMEOUT = timedelta(minutes=30)

    def test_fresh_lock_is_not_expired(self):
        self.assertFalse(is_lock_expired(_t(0), _t(0) + timedelta(minutes=10), self.TIMEOUT))

    def test_old_lock_is_expired(self):
        self.assertTrue(is_lock_expired(_t(0), _t(0) + timedelta(minutes=31), self.TIMEOUT))

    def test_missing_lock_stamp_counts_as_expired(self):
        # A BUILDING row without locked_at was abandoned before the stamp —
        # the sweep's reset query never matches it, so it must read as stuck.
        self.assertTrue(is_lock_expired(None, _t(0), self.TIMEOUT))


# ---------------------------------------------------------------------------
# Service over DB fixtures (no repo behind the manifests)
# ---------------------------------------------------------------------------


class CoverageServiceTests(TestCase):
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
        cls.unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})

    def _variable(self, slug):
        return Variable.objects.create(
            collection=self.collection,
            name=slug,
            slug=slug,
            unit=self.unit,
            value_min=0,
            value_max=1,
        )

    def _item(self, hours):
        return Item.objects.create(collection=self.collection, time=_t(hours))

    def _cog(self, item, variable):
        return Asset.objects.create(
            item=item,
            variable=variable,
            format=Asset.Format.COG,
            href=f"models/surface/{variable.slug}/{item.time:%Y/%m/%d}/x.tif",
        )

    def test_variable_without_manifest_reports_no_manifest(self):
        variable = self._variable("precip")
        self._cog(self._item(0), variable)
        # The COG save signal auto-creates a manifest; a variable without one
        # is the legacy/deleted-row case the tab must still render sensibly.
        VirtualZarrManifest.objects.filter(variable=variable).delete()

        report = variable_coverage(variable)

        self.assertIsNone(report.manifest)
        self.assertEqual(report.status, "")
        self.assertEqual(report.display_status, "no_manifest")
        self.assertFalse(report.stuck)
        self.assertEqual(report.catalog_count, 1)
        self.assertEqual(report.repo_count, 0)
        self.assertEqual(report.missing, (_t(0),))
        self.assertIsNone(report.lag)

    def test_pending_manifest_reports_all_catalog_timestamps_missing(self):
        variable = self._variable("precip")
        for h in (0, 24):
            self._cog(self._item(h), variable)
        # The COG save signal auto-created the manifest, still PENDING.

        report = variable_coverage(variable)

        self.assertEqual(report.status, VirtualZarrManifest.Status.PENDING)
        self.assertEqual(report.display_status, VirtualZarrManifest.Status.PENDING)
        self.assertEqual(report.catalog_count, 2)
        self.assertEqual(report.repo_count, 0)
        self.assertEqual(report.missing, (_t(0), _t(24)))
        self.assertEqual(report.extra, ())

    def test_items_without_cog_are_reported_not_counted_as_coverage(self):
        variable = self._variable("precip")
        self._cog(self._item(0), variable)
        self._item(24)  # no COG asset for this variable

        report = variable_coverage(variable)

        self.assertEqual(report.catalog_count, 1)
        self.assertEqual(report.items_without_cog, (_t(24),))
        self.assertEqual(report.skipped_count, 1)

    def test_other_variables_assets_do_not_leak_into_the_report(self):
        precip = self._variable("precip")
        temp = self._variable("temp")
        item = self._item(0)
        self._cog(item, precip)
        self._cog(item, temp)
        self._cog(self._item(24), temp)

        report = variable_coverage(precip)

        self.assertEqual(report.catalog_count, 1)
        # The temp-only item has no COG for precip: a skip, not coverage.
        self.assertEqual(report.items_without_cog, (_t(24),))

    def test_forecast_items_sharing_a_valid_time_count_once(self):
        # Two reference times, one valid time: the repo's axis holds the
        # timestamp once, so the catalog side must count it once too.
        variable = self._variable("precip")
        for ref_hours in (0, 6):
            item = Item.objects.create(
                collection=self.collection,
                time=_t(24),
                reference_time=_t(ref_hours),
            )
            self._cog(item, variable)

        report = variable_coverage(variable)

        self.assertEqual(report.catalog_count, 1)
        self.assertEqual(report.missing, (_t(24),))

    def test_stuck_build_is_distinct_from_active_build(self):
        variable = self._variable("precip")
        manifest = VirtualZarrManifest.objects.create(
            variable=variable,
            status=VirtualZarrManifest.Status.BUILDING,
        )
        now = timezone.now()

        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(locked_at=now - timedelta(minutes=5))
        active = variable_coverage(variable)
        self.assertFalse(active.stuck)
        self.assertEqual(active.display_status, VirtualZarrManifest.Status.BUILDING)

        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(
            locked_at=now - VirtualZarrManifest.LOCK_TIMEOUT - timedelta(minutes=1)
        )
        stuck = variable_coverage(variable)
        self.assertTrue(stuck.stuck)
        self.assertEqual(stuck.display_status, "stuck")

    def test_cached_size_figures_come_from_the_manifest_row(self):
        variable = self._variable("precip")
        manifest = VirtualZarrManifest.objects.create(variable=variable)
        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(
            repo_size_bytes=1234,
            repo_object_count=7,
        )

        report = variable_coverage(variable)

        self.assertEqual(report.repo_size_bytes, 1234)
        self.assertEqual(report.repo_object_count, 7)

    def test_collection_report_covers_every_variable_ordered_by_slug(self):
        b = self._variable("b-var")
        a = self._variable("a-var")
        VirtualZarrManifest.objects.create(variable=b)

        reports = collection_coverage(self.collection)

        self.assertEqual([r.variable for r in reports], [a, b])
        self.assertIsNone(reports[0].manifest)
        self.assertIsNotNone(reports[1].manifest)

    def test_failed_manifest_carries_its_error(self):
        variable = self._variable("precip")
        manifest = VirtualZarrManifest.objects.create(variable=variable)
        manifest.mark_failed("boom")

        report = variable_coverage(variable)

        self.assertEqual(report.status, VirtualZarrManifest.Status.FAILED)
        self.assertEqual(report.error, "boom")


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
class CoverageServiceIcechunkTests(TestCase):
    """The report against a repo built through the production build path."""

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

    def _add_item(self, day: int, *, with_cog: bool = True):
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
        if with_cog:
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

    def test_freshly_built_repo_reports_full_coverage_and_zero_lag(self):
        self._add_item(1)
        self._add_item(2)
        manifest = self._build()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)

        report = variable_coverage(self.variable)

        self.assertEqual(report.catalog_count, 2)
        self.assertEqual(report.repo_count, 2)
        self.assertEqual(report.missing, ())
        self.assertEqual(report.extra, ())
        self.assertEqual(report.repo_read_error, "")
        self.assertEqual(report.lag, timedelta(0))
        self.assertEqual(
            report.repo_timestamps,
            (datetime(2026, 3, 1, tzinfo=UTC), datetime(2026, 3, 2, tzinfo=UTC)),
        )

    def test_new_item_after_build_shows_as_missing_with_positive_lag(self):
        self._add_item(1)
        self._build()
        self._add_item(2)

        report = variable_coverage(self.variable)

        self.assertEqual(report.missing, (datetime(2026, 3, 2, tzinfo=UTC),))
        self.assertIsNotNone(report.lag)
        self.assertGreater(report.lag, timedelta(0))

    def test_deleted_item_shows_as_extra_in_the_repo(self):
        self._add_item(1)
        gone = self._add_item(2)
        self._build()
        gone.delete()

        report = variable_coverage(self.variable)

        self.assertEqual(report.extra, (datetime(2026, 3, 2, tzinfo=UTC),))

    def test_item_without_cog_is_reported_skipped(self):
        self._add_item(1)
        self._add_item(2, with_cog=False)
        self._build()

        report = variable_coverage(self.variable)

        self.assertEqual(report.repo_count, 1)
        self.assertEqual(report.items_without_cog, (datetime(2026, 3, 2, tzinfo=UTC),))
