"""
A completed harvest is a Mirror of its feed's window: what is gone upstream
loses its rows here and what changed upstream is re-synced — but only within
the Mirror's Jurisdiction, the assets that bear its own feed's Provenance and
are still Remote (ADR 0042 §5–7).

As in `test_harvest`, the only fake a harvest meets is the Upstream catalog
client. The read-path tests at the end break a read instead, to show that a
failed read is not a harvest.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Asset, Collection, Item
from georiva.harvest.upstream import UpstreamAsset, UpstreamStatistics
from georiva.organisations.testing import dial_org
from georiva.sources.models import FetchRun

from .support import ASSET_HOST, HarvestFeedMixin, day, rain_href

LOCAL_KEY = "org/chirps/chirps-daily/rain/2026/03/01/rain.tif"


def rows():
    """Every item and asset row, whole: what "no row changed" is judged against."""
    return {
        "items": list(Item.objects.order_by("pk").values()),
        "assets": list(Asset.objects.order_by("pk").values()),
    }


class MirrorRemovesWhatIsGoneUpstreamTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        for n in (1, 2, 3):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})
        self.harvest()

    def _times(self):
        return sorted(Item.objects.values_list("time", flat=True))

    def test_an_item_gone_upstream_loses_its_asset_and_its_item(self):
        self.upstream.drop_item(day(2))

        result = self.harvest()

        self.assertEqual(self._times(), [day(1), day(3)])
        self.assertFalse(Asset.objects.filter(href=rain_href(2)).exists())
        self.assertEqual(Asset.objects.count(), 2)
        run = FetchRun.objects.order_by("pk").last()
        self.assertEqual((result.removed, run.harvest_removed, run.status), (1, 1, FetchRun.Status.COMPLETED))
        self.assertEqual(Collection.objects.get().item_count, 2)

    def test_an_upstream_that_honestly_answers_empty_empties_the_window(self):
        self.upstream.items.clear()

        result = self.harvest()

        self.assertFalse(Item.objects.exists())
        self.assertFalse(Asset.objects.exists())
        self.assertEqual(result.removed, 3)

    def test_an_item_the_harvest_did_not_create_stays_when_its_remote_asset_goes(self):
        Item.objects.filter(time=day(2)).update(harvested_by=None)
        self.upstream.drop_item(day(2))

        self.harvest()

        self.assertEqual(self._times(), [day(1), day(2), day(3)])
        self.assertFalse(Asset.objects.filter(item__time=day(2)).exists())

    def test_an_item_that_still_holds_another_asset_stays(self):
        other = self.map_variable("quality", slug="quality")
        item = Item.objects.get(time=day(2))
        local = Asset.objects.create(item=item, variable=other, format=Asset.Format.COG, href=LOCAL_KEY)
        self.upstream.drop_item(day(2))

        self.harvest()

        self.assertEqual(self._times(), [day(1), day(2), day(3)])
        self.assertEqual(list(Asset.objects.filter(item=item)), [local])

    def test_an_asset_whose_key_left_an_item_still_upstream_is_gone_too(self):
        self.upstream.drop_item(day(2))
        self.upstream.add_item(day(2), {"quality": f"{ASSET_HOST}/q/2.tif"})

        result = self.harvest()

        self.assertEqual(self._times(), [day(1), day(3)])
        self.assertEqual(result.removed, 1)

    def test_a_listing_that_fails_part_way_removes_nothing(self):
        self.upstream.drop_item(day(1))
        self.upstream.drop_item(day(2))
        self.upstream.fails_after = 1
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual(FetchRun.objects.order_by("pk").last().status, FetchRun.Status.FAILED)
        self.assertEqual(result.removed, 0)

    def test_items_outside_the_window_are_untouched_though_absent_upstream(self):
        self.feed.window_start, self.feed.window_end = day(2), day(2)
        self.feed.save()
        self.upstream.items.clear()

        result = self.harvest()

        self.assertEqual(self._times(), [day(1), day(3)])
        self.assertEqual(Asset.objects.count(), 2)
        self.assertEqual(result.removed, 1)

    def test_removing_and_harvesting_again_removes_nothing_more(self):
        self.upstream.drop_item(day(2))
        self.harvest()
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual((result.removed, result.updated, result.indexed), (0, 0, 0))


class MirrorResyncsWhatChangedUpstreamTests(HarvestFeedMixin, TestCase):
    UPDATED = datetime(2026, 3, 5, 12, tzinfo=UTC)

    def setUp(self):
        super().setUp()
        self.variable = self.map_variable("rainfall")
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(
                    href=rain_href(1),
                    checksum="aaaa",
                    file_size=100,
                    statistics=UpstreamStatistics(minimum=0.0, maximum=10.0, mean=2.0, stddev=1.0),
                )
            },
            updated=self.UPDATED,
        )
        self.harvest()
        self.asset = Asset.objects.get()

    def _republish(self, asset, *, updated):
        self.upstream.items.clear()
        self.upstream.add_item(day(1), {"rainfall": asset}, updated=updated)

    def test_an_item_whose_updated_changed_has_href_statistics_and_checksum_refreshed(self):
        new_href = f"{ASSET_HOST}/chirps/v3/2026.03.01.tif"
        self._republish(
            UpstreamAsset(
                href=new_href,
                checksum="bbbb",
                file_size=200,
                statistics=UpstreamStatistics(minimum=1.0, maximum=20.0, mean=4.0, stddev=2.0),
            ),
            updated=self.UPDATED + timedelta(days=1),
        )

        result = self.harvest()

        asset = Asset.objects.get()
        self.assertEqual(asset.pk, self.asset.pk)
        self.assertEqual((asset.href, asset.checksum, asset.file_size), (new_href, "bbbb", 200))
        self.assertEqual((asset.stats_min, asset.stats_max, asset.stats_mean, asset.stats_std), (1.0, 20.0, 4.0, 2.0))
        self.assertEqual(asset.harvested_by_id, self.feed.pk)
        self.assertGreater(asset.modified, self.asset.modified)
        run = FetchRun.objects.order_by("pk").last()
        self.assertEqual((result.updated, run.harvest_updated, result.indexed, result.removed), (1, 1, 0, 0))

    def test_statistics_the_upstream_no_longer_publishes_become_null(self):
        self._republish(UpstreamAsset(href=rain_href(1)), updated=self.UPDATED + timedelta(days=1))

        self.harvest()

        asset = Asset.objects.get()
        self.assertEqual((asset.stats_min, asset.stats_max, asset.stats_mean, asset.stats_std), (None,) * 4)
        self.assertEqual(asset.checksum, "")

    def test_a_resynced_item_is_not_rewritten_again_by_the_next_harvest(self):
        self._republish(UpstreamAsset(href=rain_href(1), checksum="bbbb"), updated=self.UPDATED + timedelta(days=1))
        self.harvest()
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual(result.updated, 0)

    def test_an_item_whose_updated_did_not_change_is_not_rewritten(self):
        """Even when its content did: `updated` is the only signal (ADR 0042, consequences)."""
        self._republish(UpstreamAsset(href=f"{ASSET_HOST}/elsewhere.tif", checksum="bbbb"), updated=self.UPDATED)
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual(result.updated, 0)

    def test_two_upstream_items_at_one_time_do_not_rewrite_the_asset_on_every_harvest(self):
        """The first listed speaks for the time; the other's `updated` is not a change."""
        self.upstream.add_item(
            day(1),
            {"rainfall": UpstreamAsset(href=f"{ASSET_HOST}/other.tif")},
            id="another-at-the-same-time",
            updated=self.UPDATED + timedelta(days=1),
        )
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual(result.updated, 0)

    def test_a_changed_href_that_fails_the_allowlist_is_rejected_and_the_row_kept(self):
        self._republish(UpstreamAsset(href="https://evil.example/x.tif"), updated=self.UPDATED + timedelta(days=1))
        before = rows()

        result = self.harvest()

        self.assertEqual(rows(), before)
        self.assertEqual((result.rejected, result.updated, result.removed), (1, 0, 0))


class MirrorJurisdictionTests(HarvestFeedMixin, TestCase):
    """Mine, and still Remote: both, always. Each test makes the Upstream ask
    for a removal and then for a rewrite of a row outside the Jurisdiction."""

    UPDATED = datetime(2026, 3, 5, 12, tzinfo=UTC)

    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)}, updated=self.UPDATED)
        self.harvest()

    def _assert_neither_removed_nor_rewritten(self):
        before = rows()

        # Changed upstream: a Mirror would rewrite it.
        self.upstream.items.clear()
        self.upstream.add_item(
            day(1),
            {"rainfall": UpstreamAsset(href=f"{ASSET_HOST}/v3/1.tif", checksum="bbbb")},
            updated=self.UPDATED + timedelta(days=1),
        )
        changed = self.harvest()
        self.assertEqual(rows(), before)
        self.assertEqual((changed.updated, changed.removed, changed.indexed), (0, 0, 0))

        # Gone upstream: a Mirror would remove it.
        self.upstream.items.clear()
        gone = self.harvest()
        self.assertEqual(rows(), before)
        self.assertEqual((gone.updated, gone.removed, gone.indexed), (0, 0, 0))

    def test_a_materialized_asset_is_never_removed_or_rewritten_though_it_bears_the_feeds_provenance(self):
        Asset.objects.update(href=LOCAL_KEY)

        self._assert_neither_removed_nor_rewritten()

        self.assertEqual(Asset.objects.get().harvested_by_id, self.feed.pk)

    def test_a_local_asset_with_no_provenance_is_never_removed_or_rewritten(self):
        Asset.objects.update(href=LOCAL_KEY, harvested_by=None)

        self._assert_neither_removed_nor_rewritten()

    def test_a_remote_asset_with_no_provenance_is_never_removed_or_rewritten(self):
        Asset.objects.update(harvested_by=None)

        self._assert_neither_removed_nor_rewritten()

    def test_a_remote_asset_of_another_feed_is_never_removed_or_rewritten(self):
        Asset.objects.update(harvested_by_id=self.feed.pk + 1000)

        self._assert_neither_removed_nor_rewritten()

    def test_an_item_of_the_feed_stays_while_an_asset_outside_the_jurisdiction_is_on_it(self):
        Asset.objects.update(href=LOCAL_KEY)
        self.upstream.items.clear()

        self.harvest()

        self.assertEqual(Item.objects.get().harvested_by_id, self.feed.pk)

    def test_an_item_of_another_feed_is_not_removed_when_left_empty(self):
        Item.objects.update(harvested_by_id=self.feed.pk + 1000)
        self.upstream.items.clear()

        result = self.harvest()

        self.assertEqual(result.removed, 1)
        self.assertTrue(Item.objects.exists())


class MirrorAndAnUnreachableUpstreamTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.upstream.add_item(day(2), {"rainfall": rain_href(2)})
        self.harvest()
        # What the Upstream would say next, were it reachable.
        self.upstream.drop_item(day(2))
        self.upstream.add_item(day(3), {"rainfall": rain_href(3)})

    def test_an_unreachable_upstream_fails_the_run_due_its_retry_and_changes_no_row(self):
        self.upstream.unreachable = "upstream catalog unreachable: connection refused"
        before = rows()

        self.harvest()

        self.assertEqual(rows(), before)
        run = FetchRun.objects.order_by("pk").last()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.stopped_early, "source unavailable — upstream catalog unreachable: connection refused")
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertEqual((run.harvest_indexed, run.harvest_updated, run.harvest_removed), (0, 0, 0))

    def test_the_retry_harvests_the_window_again_and_mirrors_it(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"
        self.harvest()
        failed = FetchRun.objects.order_by("pk").last()
        self.upstream.unreachable = None

        self.harvest(resumed_from=failed)

        retry = FetchRun.objects.order_by("pk").last()
        self.assertEqual((retry.resumed_from, retry.status), (failed, FetchRun.Status.COMPLETED))
        self.assertEqual((retry.harvest_indexed, retry.harvest_removed), (1, 1))
        self.assertEqual(sorted(Item.objects.values_list("time", flat=True)), [day(1), day(3)])
        self.assertEqual(self.upstream.windows_asked[-1], self.feed.window)


class TheReadPathNeverWritesTests(HarvestFeedMixin, TestCase):
    """Only a harvest changes harvested rows. A request that cannot read the
    Upstream fails, and leaves every row as it was (ADR 0042 §6, ADR 0043)."""

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.harvest()
        self.before = rows()

    def test_a_stac_request_changes_no_row(self):
        response = self.client.get(
            reverse("stac:item-detail", args=["chirps", "chirps-daily", "rain", "20260301T000000Z"])
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(rows(), self.before)

    def test_a_timeseries_request_that_cannot_read_upstream_changes_no_row(self):
        gone = OSError("HTTP response code: 404")
        with mock.patch("georiva.analysis.timeseries.service.TimeseriesService._open_dataset", side_effect=gone):
            response = self.client.get(
                reverse("timeseries:point"), {"variable": "chirps/chirps-daily/rain", "lat": 0, "lon": 36}
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(rows(), self.before)

    def test_a_raster_read_that_fails_upstream_changes_no_row(self):
        """What a tile or a zonal read does: open the asset at its href."""
        asset = Asset.objects.get()
        with mock.patch("rasterio.open", side_effect=OSError("HTTP response code: 404")):
            with self.assertRaises(OSError):
                asset.location.open_raster()

        self.assertEqual(rows(), self.before)
