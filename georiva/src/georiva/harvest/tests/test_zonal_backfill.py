"""Boundary statistics follow a harvest, over an archive that was never copied (#639).

A harvest that indexes or re-syncs Remote assets enqueues their zonal
statistics on ``georiva-bookkeeping`` — for the assets it wrote, inside the
feed's window, and no others. Computing them reads the asset by range where it
lives (ADR 0043 §7) and writes the same rows a local COG's would.

The Upstream is faked as everywhere in the harvest tests, and its bytes are
small GeoTIFFs on local disk opened through the one seam that opens an asset.
"""

import json
from datetime import timedelta
from unittest import mock

import numpy
from adminboundarymanager.models import AdminBoundary
from celery.exceptions import Retry
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.cache import cache
from django.core.management import call_command
from django.db import OperationalError, connection
from django.test import TestCase, override_settings
from django.utils import timezone
from rasterio.transform import from_bounds

from georiva.analysis.zonal_stats import tasks as zonal_tasks
from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.analysis.zonal_stats.service import compute_stats_from_array
from georiva.core.models import Asset, Collection, Item
from georiva.sources.models import FetchRun

from .support import HarvestFeedMixin, RemoteCollectionMixin, day, rain_href, stamp
from .test_read_plane_timeseries import BOUNDS, NODATA, SIZE, UpstreamBytes, grid

#: Lies across the fixture grid (lon 33..43, lat -5..5).
BOUNDARY_BOX = (34.0, -2.0, 38.0, 2.0)


def load_boundary(level=1):
    return AdminBoundary.objects.create(
        name_0="Testland", gid_0="TST", level=level, geom=MultiPolygon(Polygon.from_bbox(BOUNDARY_BOX))
    )


class DispatchRecorder:
    """Records the zonal statistics a harvest enqueues, once its writes commit."""

    def record_dispatches(self):
        self.enterContext(mock.patch.object(zonal_tasks.compute_boundary_zonal_stats, "delay"))
        # A harvest's enqueue claims its asset in the cache, which outlives the test's transaction.
        self.addCleanup(self.forget_claims)
        return zonal_tasks.compute_boundary_zonal_stats.delay

    def forget_claims(self):
        """As if every claim period had passed."""
        keys = [zonal_tasks.harvest_claim_key(pk) for pk in Asset.objects.values_list("pk", flat=True)]
        if keys:
            cache.delete_many(keys)

    def harvest_and_dispatch(self, **kwargs):
        """Harvest, and return the asset pks whose statistics it enqueued."""
        delay = zonal_tasks.compute_boundary_zonal_stats.delay
        delay.reset_mock()
        with self.captureOnCommitCallbacks(execute=True):
            self.harvest(**kwargs)
        return sorted(call.args[0] for call in delay.call_args_list)

    def resync_upstream(self, n):
        """The Upstream changes its item on day `n`: a new href, and a later `updated`."""
        self.upstream.drop_item(day(n))
        self.upstream.add_item(day(n), {"rainfall": rain_href(n).replace(".tif", "-v2.tif")}, updated=day(20))

    def asset_pks_at(self, *days):
        return sorted(Asset.objects.filter(item__time__in=[day(n) for n in days]).values_list("pk", flat=True))


class AHarvestEnqueuesZonalStatisticsTests(DispatchRecorder, HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        load_boundary()
        self.record_dispatches()
        self.map_variable("rainfall")
        for n in range(1, 4):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})

    def test_each_remote_asset_it_created_is_enqueued_once(self):
        dispatched = self.harvest_and_dispatch()

        self.assertEqual(dispatched, self.asset_pks_at(1, 2, 3))

    def test_nothing_is_enqueued_until_the_harvest_commits(self):
        with self.captureOnCommitCallbacks(execute=False) as pending:
            self.harvest()

        self.assertFalse(zonal_tasks.compute_boundary_zonal_stats.delay.called)
        self.assertTrue(pending)

    def test_an_unchanged_asset_is_not_enqueued_again_by_a_repeat_harvest(self):
        self.harvest_and_dispatch()

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_a_resynced_asset_is_enqueued_again_and_only_it(self):
        self.harvest_and_dispatch()
        self.resync_upstream(2)

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(2))

    def test_a_collection_computing_no_boundary_levels_enqueues_nothing(self):
        Collection.objects.filter(pk=self.feed.ensure_collection().pk).update(boundary_stats_levels=[])

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_levels_with_no_boundaries_loaded_enqueue_nothing(self):
        Collection.objects.filter(pk=self.feed.ensure_collection().pk).update(boundary_stats_levels=[2])

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_a_harvest_that_stops_after_writing_still_enqueues_what_it_wrote(self):
        # A retry would find these assets current and write them no more,
        # so what this run committed is enqueued as it commits.
        with mock.patch("georiva.harvest.harvester._extend_collection", side_effect=RuntimeError("boom")):
            dispatched = self.harvest_and_dispatch()

        self.assertEqual(dispatched, self.asset_pks_at(1, 2, 3))

    def test_an_asset_not_written_by_the_harvest_is_not_enqueued(self):
        self.harvest_and_dispatch()
        # Materialized since: out of the Mirror's Jurisdiction, so never re-synced.
        materialized = Asset.objects.get(item__time=day(2))
        Asset.objects.filter(pk=materialized.pk).update(href="org/chirps/chirps-daily/rain/2026/03/02/rain.tif")
        self.upstream.drop_item(day(2))
        self.upstream.add_item(day(2), {"rainfall": rain_href(2)}, updated=day(20))

        self.assertEqual(self.harvest_and_dispatch(), [])


class TheBackfillIsBoundedByTheFeedsWindowTests(DispatchRecorder, HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        load_boundary()
        self.record_dispatches()
        self.feed.window_start, self.feed.window_end = day(2), day(4)
        self.feed.save()
        self.map_variable("rainfall")
        for n in range(1, 7):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})
        # An Upstream that answers with all it has: the window is still the bound.
        self.upstream.ignores_window = True

    def test_only_assets_inside_the_window_are_enqueued(self):
        dispatched = self.harvest_and_dispatch()

        self.assertEqual(dispatched, self.asset_pks_at(2, 3, 4))
        self.assertEqual(len(dispatched), 3)


class AHarvestRecoversLostStatisticsTests(DispatchRecorder, HarvestFeedMixin, TestCase):
    """A run lost between enqueue and rows is enqueued again by a later harvest,
    however old its item: a harvested archive is past the sweep's reach (#716).

    Every item here is in March 2026, months before the sweep's 7-day window.
    """

    def setUp(self):
        super().setUp()
        self.boundary = load_boundary()
        self.record_dispatches()
        self.map_variable("rainfall")
        for n in range(1, 4):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})
        self.harvest_and_dispatch()
        self.forget_claims()

    def give_stats(self, *days):
        for asset in Asset.objects.filter(item__time__in=[day(n) for n in days]).select_related("item"):
            BoundaryZonalStats.objects.create(
                time=asset.item.time, item=asset.item, variable=asset.variable, boundary=self.boundary, mean=1.0
            )

    def test_an_asset_with_no_statistics_is_enqueued_again_by_a_later_harvest(self):
        self.give_stats(1, 3)

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(2))

    def test_an_asset_with_its_statistics_is_not_enqueued_again(self):
        self.give_stats(1, 2, 3)

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_an_asset_whose_rows_are_older_than_its_resync_is_enqueued_again(self):
        # The re-sync's own run was lost: its rows are still the old bytes' (#717).
        self.give_stats(1, 2, 3)
        self.resync_upstream(1)
        self.harvest_and_dispatch()
        self.forget_claims()

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(1))

    def test_rows_recomputed_since_the_resync_are_not_stale(self):
        self.resync_upstream(1)
        self.harvest_and_dispatch()
        self.give_stats(1, 2, 3)  # computed after the re-sync
        self.forget_claims()

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_an_asset_missing_one_configured_level_is_enqueued_again(self):
        load_boundary(level=2)
        Collection.objects.filter(pk=self.feed.ensure_collection().pk).update(boundary_stats_levels=[1, 2])
        self.give_stats(1, 2, 3)

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(1, 2, 3))

    def test_a_level_with_no_boundaries_loaded_does_not_count_as_missing(self):
        Collection.objects.filter(pk=self.feed.ensure_collection().pk).update(boundary_stats_levels=[1, 2])
        self.give_stats(1, 2, 3)

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_an_asset_whose_runs_keep_failing_is_enqueued_at_most_once_per_claim_period(self):
        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(1, 2, 3))

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_what_a_harvest_just_enqueued_is_not_taken_for_lost_by_the_next(self):
        # The first harvest's runs may still be queued: its own enqueue is a claim.
        self.upstream.add_item(day(4), {"rainfall": rain_href(4)})
        self.give_stats(1, 2, 3)
        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(4))

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_a_resynced_asset_is_enqueued_though_it_is_claimed(self):
        self.harvest_and_dispatch()
        self.give_stats(1, 3)
        self.resync_upstream(1)

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(1))

    def test_nothing_outside_the_window_is_enqueued(self):
        self.feed.window_start, self.feed.window_end = day(2), day(3)
        self.feed.save()

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(2, 3))

    def test_an_asset_outside_the_jurisdiction_is_not_enqueued(self):
        Asset.objects.filter(item__time=day(2)).update(href="org/chirps/chirps-daily/rain/2026/03/02/rain.tif")

        self.assertEqual(self.harvest_and_dispatch(), self.asset_pks_at(1, 3))

    def test_a_harvest_that_could_not_list_its_window_recovers_nothing(self):
        self.upstream.unreachable = "down"

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_a_collection_computing_no_levels_recovers_nothing(self):
        Collection.objects.filter(pk=self.feed.ensure_collection().pk).update(boundary_stats_levels=[])

        self.assertEqual(self.harvest_and_dispatch(), [])

    def test_recovery_is_capped_per_harvest(self):
        with mock.patch.object(zonal_tasks, "MISSING_STATS_BATCH", 2):
            self.assertEqual(len(self.harvest_and_dispatch()), 2)
            self.assertEqual(len(self.harvest_and_dispatch()), 1)


class RecoveryAndTheSweepTests(DispatchRecorder, HarvestFeedMixin, TestCase):
    """The harvest's recovery is bookkeeping beside the sweep, not in its way (#716)."""

    def setUp(self):
        super().setUp()
        load_boundary()
        self.record_dispatches()
        self.map_variable("rainfall")
        self.yesterday = (timezone.now() - timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
        self.upstream.add_item(self.yesterday, {"rainfall": rain_href(1)})

    def test_the_sweep_still_recovers_a_recent_harvested_asset_the_harvest_has_claimed(self):
        self.harvest_and_dispatch()
        asset = Asset.objects.get()
        Asset.objects.filter(pk=asset.pk).update(created=timezone.now() - timedelta(hours=1))

        # The sweep's own claim, which may linger from an earlier run that had this pk.
        sweep_claim = f"zonal-stats-redispatch:{asset.pk}"
        cache.delete(sweep_claim)
        self.addCleanup(cache.delete, sweep_claim)
        delay = zonal_tasks.compute_boundary_zonal_stats.delay
        delay.reset_mock()
        zonal_tasks.sweep_missing_boundary_stats()

        delay.assert_called_once_with(asset.pk)

    def test_a_recovery_that_fails_neither_fails_the_harvest_nor_keeps_titiler_from_being_told(self):
        with (
            mock.patch("georiva.harvest.harvester.enqueue_lost_boundary_stats", side_effect=OperationalError("down")),
            mock.patch("georiva.harvest.harvester.publish_variable") as published,
        ):
            self.harvest_and_dispatch()

        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED)
        published.assert_called_once()


class ComputingARemoteAssetsStatisticsTests(RemoteCollectionMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.boundary = load_boundary()
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])
        self.upstream_bytes = UpstreamBytes(self)
        self.upstream_bytes.put(rain_href(1), grid(10.0))
        self.upstream_bytes.put(rain_href(2), grid(20.0))
        self.asset = Asset.objects.get(item__time=day(1))

    def _expected(self, data):
        data = data.astype("float32")
        data[data == NODATA] = numpy.nan
        return compute_stats_from_array(data, from_bounds(*BOUNDS, SIZE, SIZE), "EPSG:4326", [self.boundary])[0]

    def test_it_writes_the_statistics_of_the_upstream_bytes(self):
        zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        row = BoundaryZonalStats.objects.get(item=self.asset.item, variable=self.variable, boundary=self.boundary)
        expected = self._expected(grid(10.0))
        for field in ("mean", "min", "max", "sum", "std", "count"):
            with self.subTest(field=field):
                self.assertAlmostEqual(getattr(row, field), expected[field], places=4)
        self.assertGreater(row.count, 0)
        self.assertEqual(self.upstream_bytes.opened, [rain_href(1)])

    def test_its_rows_are_in_the_boundary_stats_source(self):
        call_command("create_martin_function", verbosity=0)
        params = {
            "org": self.organisation.slug,
            "catalog": "chirps",
            "collection": "chirps-daily",
            "variable": "rain",
            "admin_level": "1",
            "time": day(1).isoformat(),
        }
        self.assertEqual(self._tile(params), b"")

        zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        self.assertTrue(self._tile(params))

    @staticmethod
    def _tile(params):
        with connection.cursor() as cursor:
            cursor.execute("SELECT georiva_boundary_stats(0, 0, 0, %s::json)", [json.dumps(params)])
            return bytes(cursor.fetchone()[0])

    def test_a_resynced_asset_has_its_statistics_recomputed(self):
        zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)
        moved = rain_href(1).replace(".tif", "-v2.tif")
        self.upstream_bytes.put(moved, grid(50.0))
        self.upstream.drop_item(day(1))
        self.upstream.add_item(day(1), {"rainfall": moved}, updated=day(20))
        self.harvest()

        zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        row = BoundaryZonalStats.objects.get(item=self.asset.item, boundary=self.boundary)
        self.assertAlmostEqual(row.mean, self._expected(grid(50.0))["mean"], places=4)
        self.assertEqual(BoundaryZonalStats.objects.filter(item=self.asset.item).count(), 1)

    def test_an_upstream_read_failure_is_retried_and_changes_no_catalog_row(self):
        self.upstream_bytes.unreadable.add(rain_href(1))
        before = self._catalog_rows()

        with (
            mock.patch.object(zonal_tasks.compute_boundary_zonal_stats, "retry", side_effect=Retry()) as retry,
            self.assertRaises(Retry),
        ):
            zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        retry.assert_called_once()
        self.assertEqual(zonal_tasks.compute_boundary_zonal_stats.max_retries, 3)
        self.assertEqual(self._catalog_rows(), before)
        self.assertFalse(BoundaryZonalStats.objects.exists())

    def test_one_assets_failure_leaves_the_others_computable(self):
        self.upstream_bytes.unreadable.add(rain_href(1))
        with (
            mock.patch.object(zonal_tasks.compute_boundary_zonal_stats, "retry", side_effect=Retry()),
            self.assertRaises(Retry),
        ):
            zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        other = Asset.objects.get(item__time=day(2))
        zonal_tasks.compute_boundary_zonal_stats(other.pk)

        self.assertEqual(list(BoundaryZonalStats.objects.values_list("item_id", flat=True)), [other.item_id])

    def test_the_assets_own_row_is_not_rewritten(self):
        before = self._catalog_rows()

        zonal_tasks.compute_boundary_zonal_stats(self.asset.pk)

        self.assertEqual(self._catalog_rows(), before)

    def _catalog_rows(self):
        return (
            list(Asset.objects.order_by("pk").values()),
            list(Item.objects.order_by("pk").values()),
            list(Collection.objects.order_by("pk").values()),
        )


@override_settings(ALLOWED_HOSTS=["*"])
class RenderingDoesNotWaitOnTheBackfillTests(RemoteCollectionMixin, TestCase):
    def setUp(self):
        super().setUp()
        load_boundary()
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])
        UpstreamBytes(self).put(rain_href(1), grid(10.0))

    def test_tile_config_is_the_same_before_and_after_the_backfill(self):
        before = [self.tile_config(f"?time={stamp(1)}").json(), self.tile_config().json()]
        published = self.published()

        zonal_tasks.compute_boundary_zonal_stats(Asset.objects.get(item__time=day(1)).pk)

        self.assertTrue(BoundaryZonalStats.objects.exists())
        self.assertEqual([self.tile_config(f"?time={stamp(1)}").json(), self.tile_config().json()], before)
        self.assertEqual(self.published(), published)
