"""Boundary statistics follow a harvest, over an archive that was never copied (#639).

A harvest that indexes or re-syncs Remote assets enqueues their zonal
statistics on ``georiva-bookkeeping`` — for the assets it wrote, inside the
feed's window, and no others. Computing them reads the asset by range where it
lives (ADR 0043 §7) and writes the same rows a local COG's would.

The Upstream is faked as everywhere in the harvest tests, and its bytes are
small GeoTIFFs on local disk opened through the one seam that opens an asset.
"""

import json
from unittest import mock

import numpy
from adminboundarymanager.models import AdminBoundary
from celery.exceptions import Retry
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.db import connection
from django.test import TestCase, override_settings
from rasterio.transform import from_bounds

from georiva.analysis.zonal_stats import tasks as zonal_tasks
from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.analysis.zonal_stats.service import compute_stats_from_array
from georiva.core.models import Asset, Collection, Item

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
        return zonal_tasks.compute_boundary_zonal_stats.delay

    def harvest_and_dispatch(self, **kwargs):
        """Harvest, and return the asset pks whose statistics it enqueued."""
        delay = zonal_tasks.compute_boundary_zonal_stats.delay
        delay.reset_mock()
        with self.captureOnCommitCallbacks(execute=True):
            self.harvest(**kwargs)
        return sorted(call.args[0] for call in delay.call_args_list)

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
        self.upstream.drop_item(day(2))
        self.upstream.add_item(day(2), {"rainfall": rain_href(2).replace(".tif", "-v2.tif")}, updated=day(20))

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
