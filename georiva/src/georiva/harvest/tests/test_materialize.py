"""
Materializing a window (ADR 0042 §8, #640): a Harvest feed keeps a hot window
local while the deep archive stays Remote.

The feed's own runs find the Remote assets of its Jurisdiction inside its
materialization window and send each to the ingestion queue, where its bytes
are fetched — past the same Asset host allowlist as a harvested href — and
ingested into our storage by the shared write step ingestion ends in. The
asset keeps its row, its item and its Provenance; only its href becomes a key
of ours, and that takes it out of the Mirror's Jurisdiction for good.

The Upstream is the fake, and our storage a writer that keeps what it is given.
"""

from unittest import mock

import numpy
from django.core.cache import cache
from django.db import connection
from django.test import TestCase
from django.urls import reverse
from django_redis import get_redis_connection
from wagtail import hooks

from georiva.core.models import Asset, Item
from georiva.core.storage.asset_location import AssetLocation
from georiva.harvest import tasks
from georiva.harvest.materialize import (
    LEFT,
    MATERIALIZE_CLAIM,
    MATERIALIZE_CLAIM_LONGEST,
    MATERIALIZE_LANE_LEASE,
    MATERIALIZED,
    NOT_DUE,
    REFUSED,
    UNREACHABLE,
    UNRESOLVED_MARK,
    claim_key,
    lane_key,
    materialize,
    materialize_in_lane,
    refusals,
)
from georiva.harvest.upstream import UpstreamAsset
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.organisations.testing import make_organisation

from .support import ASSET_HOST, HarvestFeedMixin, RemoteCollectionMixin, day, rain_href, stamp


class KeptBucket:
    """The assets bucket behind a `KeptWriter`."""

    def __init__(self, written, versions):
        self.written = written
        self.versions = versions

    def size(self, path):
        return 4096

    def delete(self, path):
        self.versions.pop(path, None)
        return self.written.pop(path, None) is not None

    def version_of(self, path):
        return self.versions.get(path)

    def delete_if_version(self, path, version):
        if version is None or self.versions.get(path) != version:
            return False
        return self.delete(path)


class KeptWriter:
    """Our assets bucket, as far as a materialization can tell: what it is given is kept, by key.

    It also notes how deep in transactions each write was made, so a test can
    tell a write made under the row's lock from one made before it."""

    def __init__(self):
        self.written: dict[str, numpy.ndarray] = {}
        #: The nodata value each object was written to declare, by key.
        self.nodata: dict = {}
        self.depths: list[int] = []
        #: Which write each key holds: a count of writes, as `Bucket.version_of` tells writes apart.
        self.versions: dict[str, int] = {}
        self.bucket = KeptBucket(self.written, self.versions)

    def write_cog(self, data, output_path, bounds, crs="EPSG:4326", nodata=None):
        self.depths.append(len(connection.savepoint_ids))
        self.written[output_path] = numpy.array(data)
        self.nodata[output_path] = nodata
        self.versions[output_path] = len(self.depths)
        return output_path


class MaterializeMixin(HarvestFeedMixin):
    """Days 1–4 harvested; days 3 and 4 inside the materialization window."""

    def setUp(self):
        super().setUp()
        self.writer = KeptWriter()
        self.variable = self.map_variable("rainfall")
        for n in range(1, 5):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})
        self.feed.materialize_start = day(3)
        self.feed.save()
        self.sent: list[int] = []
        #: The lane each sent asset travels in, by asset.
        self.lanes: dict[int, str | None] = {}
        # A harvest's send claims its asset and its feed's lane in the cache, which outlives
        # the test's transaction — and a later run's test database may hand out the same pks.
        self.forget_claims()
        self.addCleanup(self.forget_claims)
        self.enterContext(mock.patch.object(tasks.materialize_harvested_asset, "delay", side_effect=self.send))

    def send(self, asset_id, lane=None):
        self.sent.append(asset_id)
        self.lanes[asset_id] = lane

    def forget_claims(self):
        """As if every claim period, and every lane's lease, had passed."""
        cache.delete_pattern(claim_key("*"))
        self.forget_lanes()

    def forget_lanes(self):
        """As if every lane's lease had passed."""
        redis = get_redis_connection("default")
        for key in redis.scan_iter(lane_key("*")):
            redis.delete(key)

    def lane_ttl(self):
        return get_redis_connection("default").ttl(lane_key(self.feed.pk))

    def harvest(self, **kwargs):
        # What a harvest sends, it sends as its writes commit.
        with self.captureOnCommitCallbacks(execute=True):
            return super().harvest(**kwargs)

    def materialize(self, asset_id):
        return materialize(asset_id, upstream=self.upstream, writer=self.writer)

    def materialize_sent(self):
        """Run what was sent to the ingestion queue, as its worker would: each
        passing its feed's lane on to the next asset due, which is sent in turn."""
        sent, self.sent = self.sent, []
        return [self.run_in_lane(pk, self.lanes.get(pk)) for pk in sent]

    def run_in_lane(self, asset_id, lane):
        """The task: a worker runs it in autocommit, so what it sends is sent at once."""
        with self.captureOnCommitCallbacks(execute=True):
            return materialize_in_lane(asset_id, lane, upstream=self.upstream, writer=self.writer)

    def materialize_window(self) -> list[str]:
        """Run what was sent, and what that sends, until nothing more is: what came of each."""
        self.ran, outcomes = [], []
        while self.sent:
            self.ran += self.sent
            outcomes += self.materialize_sent()
        return outcomes

    def drain(self) -> list[int]:
        """`materialize_window`: which assets ran, in order."""
        self.materialize_window()
        return self.ran

    def asset_on(self, n) -> Asset:
        return Asset.objects.get(item__time=day(n), variable=self.variable)


class AHarvestSendsItsMaterializationWindowToIngestionTests(MaterializeMixin, TestCase):
    def test_the_remote_assets_inside_the_window_are_sent(self):
        self.harvest()

        self.assertEqual(self.drain(), [self.asset_on(4).pk, self.asset_on(3).pk])

    def test_a_window_with_an_end_sends_only_what_is_inside_it(self):
        self.feed.materialize_end = day(3)
        self.feed.save()

        self.harvest()

        self.assertEqual(self.sent, [self.asset_on(3).pk])

    def test_a_feed_with_no_materialization_window_sends_nothing(self):
        self.feed.materialize_start = None
        self.feed.save()

        self.harvest()

        self.assertEqual(self.sent, [])

    def test_an_asset_already_sent_is_not_sent_again_while_it_waits_in_the_queue(self):
        self.harvest()
        self.sent = []

        self.harvest()

        self.assertEqual(self.sent, [])

    def test_an_asset_already_local_is_not_sent_again(self):
        self.harvest()
        self.drain()
        self.feed.materialize_start = day(1)
        self.feed.save()

        self.harvest()

        self.assertEqual(self.drain(), [self.asset_on(2).pk, self.asset_on(1).pk])

    def test_the_task_runs_on_the_ingestion_queue_and_the_harvest_does_not(self):
        self.assertEqual(tasks.materialize_harvested_asset.queue, "georiva-ingestion")
        self.assertEqual(tasks.run_harvest.queue, "georiva-default")


class AFeedMaterializesOneAssetAtATimeTests(MaterializeMixin, TestCase):
    """A feed has one materialization on the ingestion queue at most: its lane.
    Each, once run, sends the feed's next asset due to the back of the queue,
    so a fresh file arriving meanwhile is ahead of it, and a deep window drains
    newest first without waiting for the next harvest (#727)."""

    def setUp(self):
        super().setUp()
        self.feed.materialize_start = day(1)
        self.feed.save()

    def test_a_harvest_sends_only_the_newest_asset_due(self):
        self.harvest()

        self.assertEqual(self.sent, [self.asset_on(4).pk])

    def test_each_asset_run_sends_the_next_newest_due(self):
        self.harvest()

        self.materialize_sent()

        self.assertEqual(self.sent, [self.asset_on(3).pk])

    def test_the_window_drains_newest_first_without_another_harvest(self):
        self.harvest()

        self.assertEqual(self.drain(), [self.asset_on(n).pk for n in (4, 3, 2, 1)])
        self.assertFalse(Asset.objects.filter(item__collection=self.feed.collection, href__startswith="https").exists())

    def test_a_harvest_sends_nothing_while_its_feeds_lane_is_taken(self):
        self.harvest()
        self.materialize_sent()
        self.sent = []

        self.harvest()

        self.assertEqual(self.sent, [])

    def test_an_asset_that_could_not_be_had_passes_the_lane_on(self):
        self.upstream.unfetchable[rain_href(4)] = "upstream answered 503"
        self.harvest()

        self.assertEqual(self.materialize_sent(), [UNREACHABLE])
        self.assertEqual(self.sent, [self.asset_on(3).pk])

    def test_a_drained_window_frees_the_lane_for_what_arrives_later(self):
        self.harvest()
        self.drain()
        self.upstream.add_item(day(5), {"rainfall": rain_href(5)})

        self.harvest()

        self.assertEqual(self.sent, [self.asset_on(5).pk])

    def test_a_lane_whose_lease_lapsed_is_taken_again_and_the_old_one_stands_down(self):
        self.harvest()
        stale = self.sent
        self.forget_lanes()
        self.sent = []
        self.harvest()
        # Day 4 is still claimed by the lost task: day 3 is next.
        self.assertEqual(self.sent, [self.asset_on(3).pk])
        self.sent = []

        # The lost task turns up after all: it does its work, and sends nothing on —
        self.assertEqual(self.run_in_lane(stale[0], self.lanes[stale[0]]), MATERIALIZED)
        self.assertEqual(self.sent, [])
        # — nor closes the lane opened since, nor takes it over.
        self.harvest()
        self.assertEqual(self.sent, [])
        self.run_in_lane(self.asset_on(3).pk, self.lanes[self.asset_on(3).pk])
        self.assertEqual(self.sent, [self.asset_on(2).pk])

    def test_a_lane_stays_open_while_its_task_waits_behind_a_deep_queue(self):
        """Its asset's claim lapses long before its lease does: a harvest in the
        meantime does not open a second lane, which would be one per harvest."""
        self.harvest()
        cache.delete_pattern(claim_key("*"))
        self.sent = []

        self.harvest()

        self.assertEqual(self.sent, [])
        self.assertGreater(self.lane_ttl(), 12 * 3600)

    def test_a_task_renews_its_lane_as_it_starts(self):
        self.harvest()
        get_redis_connection("default").expire(lane_key(self.feed.pk), 60)
        self.upstream.unfetchable[rain_href(4)] = "upstream answered 503"
        self.upstream.unfetchable[rain_href(3)] = "upstream answered 503"
        seen = []
        real = materialize

        def materializing(*args, **kwargs):
            seen.append(self.lane_ttl())
            return real(*args, **kwargs)

        with mock.patch("georiva.harvest.materialize.materialize", side_effect=materializing):
            self.materialize_sent()

        self.assertGreater(seen[0], MATERIALIZE_LANE_LEASE.total_seconds() - 60)

    def test_a_task_that_raises_still_passes_the_lane_on(self):
        self.harvest()

        with mock.patch("georiva.harvest.materialize.materialize", side_effect=RuntimeError("worker fell over")):
            with self.assertRaises(RuntimeError):
                self.materialize_sent()

        self.assertEqual(self.sent, [self.asset_on(3).pk])

    def test_a_harvest_that_could_not_send_closes_the_lane_it_opened(self):
        with mock.patch("georiva.harvest.materialize._send_next", side_effect=RuntimeError("database went away")):
            self.harvest()
        self.assertEqual(self.sent, [])

        self.harvest()

        self.assertEqual(self.sent, [self.asset_on(4).pk])

    def test_a_task_sent_without_a_lane_sends_nothing_on(self):
        """One sent before lanes existed, still on the queue as they are deployed."""
        self.harvest()

        self.assertEqual(self.run_in_lane(self.asset_on(4).pk, None), MATERIALIZED)
        self.assertEqual(self.sent, [self.asset_on(4).pk])

    def test_each_feed_has_a_lane_of_its_own(self):
        self.harvest()
        self.sent = []
        self.feed = self.make_feed(make_organisation("second-org"), materialize_start=day(1))
        self.map_variable()

        self.harvest()

        self.assertEqual(self.sent, [Asset.objects.get(harvested_by_id=self.feed.pk, item__time=day(4)).pk])


class AMaterializedAssetIsTheSameAssetMadeLocalTests(MaterializeMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.upstream.rasters[rain_href(3)] = numpy.full((8, 8), 12.5, dtype="float32")
        self.harvest()
        self.before = self.asset_on(3)
        self.outcomes = self.materialize_window()

    def test_its_bytes_are_fetched_and_ingested_locally(self):
        self.assertEqual(self.outcomes, [MATERIALIZED, MATERIALIZED])
        self.assertCountEqual(self.upstream.fetched, [rain_href(3), rain_href(4)])
        asset = self.asset_on(3)
        self.assertFalse(asset.location.is_remote)
        self.assertIn(asset.href, self.writer.written)
        self.assertTrue(asset.href.startswith(f"{self.feed.catalog.organisation.slug}/chirps/"))

    def test_it_is_the_same_row_on_the_same_item_and_keeps_its_provenance(self):
        asset = self.asset_on(3)

        self.assertEqual((asset.pk, asset.item_id), (self.before.pk, self.before.item_id))
        self.assertEqual(asset.harvested_by_id, self.feed.pk)
        self.assertEqual(asset.item.harvested_by_id, self.feed.pk)
        self.assertEqual(Asset.objects.count(), 4)
        self.assertEqual(Item.objects.count(), 4)

    def test_its_statistics_are_its_local_bytes(self):
        asset = self.asset_on(3)

        self.assertEqual((asset.stats_min, asset.stats_max, asset.stats_mean), (12.5, 12.5, 12.5))

    def test_the_assets_outside_the_window_stay_remote(self):
        self.assertTrue(self.asset_on(1).location.is_remote)
        self.assertTrue(self.asset_on(2).location.is_remote)

    def test_materializing_it_again_fetches_nothing(self):
        fetched = list(self.upstream.fetched)

        self.assertEqual(self.materialize(self.before.pk), LEFT)
        self.assertEqual(self.upstream.fetched, fetched)


class AMaterializedCopyHoldsWhatTheUpstreamFileHeldTests(MaterializeMixin, TestCase):
    """Every band, in the upstream's own dtype, with its own nodata value: a
    copy, not a re-encoding (#725). Nothing downstream tells a Remote asset
    from its local copy beyond the href."""

    def setUp(self):
        super().setUp()
        self.harvest()
        self.asset = self.asset_on(3)

    def copy_of(self, data, nodata=-9999.0):
        self.upstream.rasters[rain_href(3)] = data
        self.upstream.nodata[rain_href(3)] = nodata
        self.assertEqual(self.materialize(self.asset.pk), MATERIALIZED)
        self.asset.refresh_from_db()
        return self.writer.written[self.asset.href], self.writer.nodata[self.asset.href]

    def test_integer_values_stay_integers_with_their_own_nodata(self):
        data = numpy.arange(64, dtype="int16").reshape(8, 8)
        data[0, 0] = -9999

        written, nodata = self.copy_of(data)

        self.assertEqual(written.dtype, numpy.int16)
        numpy.testing.assert_array_equal(written, data)
        self.assertEqual(nodata, -9999)
        self.assertEqual(self.asset.extra_fields["nodata"], -9999)

    def test_a_float64_upstream_keeps_its_precision(self):
        data = numpy.full((8, 8), 1 + 1e-12, dtype="float64")

        written, _ = self.copy_of(data)

        self.assertEqual(written.dtype, numpy.float64)
        numpy.testing.assert_array_equal(written, data)

    def test_a_file_declaring_no_nodata_is_not_given_one(self):
        data = numpy.full((8, 8), 255, dtype="uint8")

        written, nodata = self.copy_of(data, nodata=None)

        numpy.testing.assert_array_equal(written, data)
        self.assertIsNone(nodata)
        self.assertEqual(self.asset.stats_max, 255)

    def test_every_band_is_copied(self):
        data = numpy.stack([numpy.full((8, 8), band, dtype="float32") for band in (1.0, 2.0, 3.0)])

        written, _ = self.copy_of(data)

        numpy.testing.assert_array_equal(written, data)
        self.assertEqual(self.asset.bands, 3)

    def test_statistics_are_the_first_bands_and_leave_out_nodata(self):
        """Band 1 is the band the Variable maps, and the one every reader draws."""
        first = numpy.arange(64, dtype="int16").reshape(8, 8) + 10
        first[0, :] = -9999
        data = numpy.stack([first, numpy.full((8, 8), 500, dtype="int16")])

        self.copy_of(data)

        self.assertEqual((self.asset.stats_min, self.asset.stats_max), (18, 73))


class AMaterializedAssetKeepsItsUpstreamAsHistoryTests(MaterializeMixin, TestCase):
    UPDATED = day(20)

    def setUp(self):
        super().setUp()
        self.upstream.items = [item for item in self.upstream.items if item.datetime != day(3)]
        self.upstream.add_item(
            day(3),
            {"rainfall": UpstreamAsset(href=rain_href(3), checksum="1220abcd", nodata=-1.0)},
            updated=self.UPDATED,
        )
        self.harvest()
        self.materialize(self.asset_on(3).pk)
        self.asset = self.asset_on(3)

    def test_the_upstream_checksum_does_not_describe_the_local_bytes(self):
        self.assertEqual(self.asset.checksum, "")

    def test_its_extra_fields_describe_the_local_bytes(self):
        # The file's own nodata, which is what the copy keeps — not what the Upstream's catalog said of it.
        self.assertEqual(self.asset.extra_fields["nodata"], -9999.0)
        self.assertNotIn("upstream_updated", self.asset.extra_fields)

    def test_where_it_came_from_is_kept_as_history(self):
        self.assertEqual(
            self.asset.extra_fields["upstream"],
            {
                "href": rain_href(3),
                "checksum": "1220abcd",
                "nodata": -1.0,
                "updated": self.UPDATED.isoformat(),
            },
        )


class AMaterializedCopyIsNotClippedTests(MaterializeMixin, TestCase):
    """A catalog's boundary clips what is ingested, not what is harvested: a
    Remote asset is served as its Upstream publishes it, so its local copy is
    too, and a Mixed collection shows one footprint across the boundary (#726).
    A `display` catalog crops both alike, on the map only (ADR 0038)."""

    def setUp(self):
        super().setUp()
        from adminboundarymanager.models import AdminBoundary
        from django.contrib.gis.geos import MultiPolygon, Polygon

        self.boundary = AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=0, geom=MultiPolygon(Polygon.from_bbox((35.0, -3.0, 40.0, 3.0)))
        )
        self.harvest()
        self.asset = self.asset_on(3)
        self.data = numpy.arange(64, dtype="float32").reshape(8, 8)
        self.upstream.rasters[rain_href(3)] = self.data

    def copy_in(self, clip_mode):
        from georiva.core.models import Catalog

        Catalog.objects.filter(pk=self.feed.catalog_id).update(clip_mode=clip_mode, boundary=self.boundary)
        self.assertEqual(self.materialize(self.asset.pk), MATERIALIZED)
        self.asset.refresh_from_db()
        return self.writer.written[self.asset.href]

    def test_a_clipping_catalog_copies_the_whole_upstream_grid(self):
        from georiva.core.models import Catalog

        for clip_mode in (Catalog.ClipMode.BBOX, Catalog.ClipMode.MASK, Catalog.ClipMode.DISPLAY):
            with self.subTest(clip_mode=clip_mode):
                Asset.objects.filter(pk=self.asset.pk).update(href=rain_href(3), harvested_by=self.feed)
                written = self.copy_in(clip_mode)

                numpy.testing.assert_array_equal(written, self.data)
                self.assertEqual((self.asset.width, self.asset.height), (8, 8))

    def test_its_item_keeps_the_upstreams_bbox_as_its_remote_neighbours_do(self):
        from georiva.core.models import Catalog

        self.copy_in(Catalog.ClipMode.MASK)

        self.assertEqual(self.asset.item.bounds, [33.0, -5.0, 42.0, 5.0])
        self.assertEqual(self.asset.item.bounds, self.asset_on(1).item.bounds)


class AMaterializedAssetHasLeftTheMirrorsJurisdictionTests(MaterializeMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.harvest()
        self.materialize_window()
        self.local = {n: (self.asset_on(n).pk, self.asset_on(n).href) for n in (3, 4)}

    def kept(self):
        return {n: (self.asset_on(n).pk, self.asset_on(n).href) for n in (3, 4)}

    def test_a_later_harvest_neither_removes_nor_rewrites_it_when_upstream_removed_its_item(self):
        self.upstream.drop_item(day(3))
        self.upstream.drop_item(day(4))

        result = self.harvest()

        self.assertEqual(self.kept(), self.local)
        self.assertEqual(result.removed, 0)
        self.assertEqual(Item.objects.filter(time__in=[day(3), day(4)]).count(), 2)

    def test_a_later_harvest_does_not_rewrite_it_when_upstream_changed_its_item(self):
        self.upstream.items = [item for item in self.upstream.items if item.datetime != day(3)]
        self.upstream.add_item(
            day(3),
            {"rainfall": UpstreamAsset(href=f"{ASSET_HOST}/v3/3.tif")},
            updated=day(10),
        )

        result = self.harvest()

        self.assertEqual(self.kept(), self.local)
        self.assertEqual((result.updated, result.indexed), (0, 0))

    def test_a_window_moved_off_it_leaves_it_local(self):
        self.feed.materialize_start = None
        self.feed.save()
        self.upstream.drop_item(day(4))

        self.harvest()

        self.assertEqual(self.kept(), self.local)


class MaterializedBytesPassTheAllowlistTests(MaterializeMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.harvest()
        self.asset = self.asset_on(3)

    def assert_left_remote(self, outcome, expected):
        self.assertEqual(outcome, expected)
        self.asset.refresh_from_db()
        self.assertTrue(self.asset.location.is_remote)
        self.assertEqual(self.writer.written, {})

    def test_an_href_on_a_host_no_longer_allowed_is_not_fetched(self):
        Asset.objects.filter(pk=self.asset.pk).update(href="https://elsewhere.example/3.tif")

        self.assert_left_remote(self.materialize(self.asset.pk), REFUSED)
        self.assertEqual(self.upstream.fetched, [])

    def test_a_refused_asset_is_not_sent_again_until_its_href_or_the_allowlist_changes(self):
        Asset.objects.filter(pk=self.asset.pk).update(href="https://elsewhere.example/3.tif")
        self.assertEqual(self.materialize(self.asset.pk), REFUSED)
        self.forget_claims()
        self.sent = []

        self.harvest()
        self.assertEqual(self.drain(), [self.asset_on(4).pk])

        self.feed.asset_hosts = ["elsewhere.example"]
        self.feed.save()
        self.forget_claims()
        self.harvest()
        self.assertEqual(self.sent, [self.asset.pk])

    def test_a_refused_asset_whose_href_is_resynced_is_sent_again(self):
        Asset.objects.filter(pk=self.asset.pk).update(href="https://elsewhere.example/3.tif")
        self.materialize(self.asset.pk)
        Asset.objects.filter(pk=self.asset.pk).update(href=rain_href(3))
        self.forget_claims()
        self.sent = []

        self.harvest()

        self.assertIn(self.asset.pk, self.drain())

    def test_a_host_now_resolving_to_a_private_address_is_not_fetched(self):
        self.upstream.addresses["stac.upstream.example"] = ["10.0.0.7"]

        self.assert_left_remote(self.materialize(self.asset.pk), REFUSED)
        self.assertEqual(self.upstream.fetched, [])

    def test_a_redirect_off_the_allowlist_is_not_followed(self):
        self.upstream.redirects[rain_href(3)] = "http://169.254.169.254/latest/meta-data"

        self.assert_left_remote(self.materialize(self.asset.pk), UNREACHABLE)

    def test_a_redirect_to_an_allowed_host_is_followed(self):
        self.upstream.redirects[rain_href(3)] = f"{ASSET_HOST}/mirror/3.tif"

        self.assertEqual(self.materialize(self.asset.pk), MATERIALIZED)

    def test_an_upstream_that_cannot_serve_the_bytes_leaves_it_remote(self):
        self.upstream.unfetchable[rain_href(3)] = "upstream answered 503"

        self.assert_left_remote(self.materialize(self.asset.pk), UNREACHABLE)

    def test_bytes_that_are_not_a_raster_leave_it_remote(self):
        self.upstream.garbage.add(rain_href(3))

        self.assert_left_remote(self.materialize(self.asset.pk), UNREACHABLE)


class ARefusalForWhereAHostResolvesNowIsTriedAgainTests(MaterializeMixin, TestCase):
    """A host resolving privately, or not at all, says nothing of the href: the
    asset is tried again on the claim period, never marked for good (#724)."""

    HOST = "stac.upstream.example"

    def setUp(self):
        super().setUp()
        # Day 3 alone is due: the one a harvest sends.
        self.feed.materialize_end = day(3)
        self.feed.save()
        self.harvest()
        self.asset = self.asset_on(3)
        self.sent = []

    def refused_for_where_it_resolves(self, addresses):
        self.upstream.addresses[self.HOST] = addresses
        self.assertEqual(self.materialize(self.asset.pk), REFUSED)
        self.forget_claims()

    def test_a_host_that_resolved_privately_is_materialized_once_it_resolves_publicly(self):
        self.refused_for_where_it_resolves(["10.0.0.7"])
        del self.upstream.addresses[self.HOST]

        self.harvest()
        self.assertIn(self.asset.pk, self.sent)
        self.materialize_window()

        self.asset.refresh_from_db()
        self.assertFalse(self.asset.location.is_remote)

    def test_a_host_that_did_not_resolve_is_sent_again(self):
        self.refused_for_where_it_resolves([])

        self.harvest()

        self.assertIn(self.asset.pk, self.sent)

    def test_each_refusal_in_a_row_doubles_the_claim_period(self):
        self.refused_for_where_it_resolves(["10.0.0.7"])
        self.refused_for_where_it_resolves(["10.0.0.7"])

        self.harvest()

        self.assertIn(self.asset.pk, self.sent)
        self.assertGreater(cache.ttl(claim_key(self.asset.pk)), 3 * MATERIALIZE_CLAIM.total_seconds())

    def test_the_claim_period_stops_growing(self):
        for _ in range(12):
            self.refused_for_where_it_resolves(["10.0.0.7"])

        self.harvest()

        self.assertLessEqual(cache.ttl(claim_key(self.asset.pk)), MATERIALIZE_CLAIM_LONGEST.total_seconds())

    def test_a_host_that_resolves_publicly_again_starts_the_count_over(self):
        self.refused_for_where_it_resolves(["10.0.0.7"])
        del self.upstream.addresses[self.HOST]
        self.upstream.unfetchable[rain_href(3)] = "upstream answered 503"
        self.assertEqual(self.materialize(self.asset.pk), UNREACHABLE)
        self.forget_claims()

        self.harvest()

        self.assertLessEqual(cache.ttl(claim_key(self.asset.pk)), MATERIALIZE_CLAIM.total_seconds())
        self.assertEqual(refusals(self.feed, self.feed.collection), [])


class AMarkAndAHarvestWriteTheRowWithoutLosingEachOthersFieldsTests(MaterializeMixin, TestCase):
    """A materialization and a harvest each write their own keys of `extra_fields`
    and leave the rest as the database holds it at the write (#724)."""

    def setUp(self):
        super().setUp()
        self.harvest()
        self.asset = self.asset_on(3)

    def test_a_mark_keeps_what_a_harvest_wrote_while_the_host_was_looked_up(self):
        def harvest_meanwhile(host):
            Asset.objects.filter(pk=self.asset.pk).update(extra_fields={"nodata": -9999.0})
            return ["10.0.0.7"]

        with mock.patch.object(self.upstream, "resolve_host", side_effect=harvest_meanwhile):
            self.assertEqual(self.materialize(self.asset.pk), REFUSED)

        self.asset.refresh_from_db()
        self.assertEqual(self.asset.extra_fields["nodata"], -9999.0)
        self.assertIn(UNRESOLVED_MARK, self.asset.extra_fields)

    def test_a_resync_keeps_a_mark_written_after_the_harvest_read_the_row(self):
        from georiva.harvest.harvester import _Jurisdiction

        resync = _Jurisdiction.resync

        def refused_meanwhile(jurisdiction, mine, asset, item):
            self.upstream.addresses["stac.upstream.example"] = ["10.0.0.7"]
            self.materialize(self.asset.pk)
            del self.upstream.addresses["stac.upstream.example"]
            return resync(jurisdiction, mine, asset, item)

        self.upstream.items = [item for item in self.upstream.items if item.datetime != day(3)]
        self.upstream.add_item(day(3), {"rainfall": rain_href(3)}, updated=day(10))
        with mock.patch.object(_Jurisdiction, "resync", refused_meanwhile):
            self.harvest()

        self.asset.refresh_from_db()
        self.assertIn(UNRESOLVED_MARK, self.asset.extra_fields)
        self.assertIn("upstream_updated", self.asset.extra_fields)

    def test_a_refusal_for_an_href_since_rewritten_is_not_counted(self):
        self.upstream.addresses["stac.upstream.example"] = ["10.0.0.7"]
        self.materialize(self.asset.pk)

        Asset.objects.filter(pk=self.asset.pk).update(href=f"{ASSET_HOST}/v3/3.tif")

        self.assertEqual(refusals(self.feed, self.feed.collection), [])


class AnOperatorSeesWhatWasRefusedTests(MaterializeMixin, TestCase):
    """What in the window was refused, and why, counted by reason (#724)."""

    def setUp(self):
        super().setUp()
        self.harvest()

    def test_nothing_refused_says_nothing(self):
        self.materialize_window()

        self.assertEqual(refusals(self.feed, self.feed.collection), [])

    def test_refusals_are_counted_by_reason_and_kind(self):
        Asset.objects.filter(pk=self.asset_on(3).pk).update(href="https://elsewhere.example/3.tif")
        self.materialize(self.asset_on(3).pk)
        self.upstream.addresses["stac.upstream.example"] = ["10.0.0.7"]
        self.materialize(self.asset_on(4).pk)

        self.assertCountEqual(
            [(r.reason, r.count, r.retried) for r in refusals(self.feed, self.feed.collection)],
            [
                ("host elsewhere.example is not in the asset host allowlist", 1, False),
                ("host stac.upstream.example resolves to a private, loopback or link-local address", 1, True),
            ],
        )

    def test_a_refusal_the_allowlist_has_since_lifted_is_not_counted(self):
        Asset.objects.filter(pk=self.asset_on(3).pk).update(href="https://elsewhere.example/3.tif")
        self.materialize(self.asset_on(3).pk)

        self.feed.asset_hosts = ["stac.upstream.example", "elsewhere.example"]
        self.feed.save()

        self.assertEqual(refusals(self.feed, self.feed.collection), [])

    def test_the_feed_dashboard_says_what_was_refused_and_why(self):
        from django.contrib.auth import get_user_model

        from georiva.organisations.testing import dial_org

        Asset.objects.filter(pk=self.asset_on(3).pk).update(href="https://elsewhere.example/3.tif")
        self.materialize(self.asset_on(3).pk)
        self.client.force_login(get_user_model().objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client)

        response = self.client.get(reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))

        self.assertContains(response, "Not kept locally")
        self.assertContains(response, "host elsewhere.example is not in the asset host allowlist")


class OnlyWhatIsStillDueIsMaterializedTests(MaterializeMixin, TestCase):
    """Between being sent and being run, an asset may have moved on."""

    def setUp(self):
        super().setUp()
        self.harvest()
        self.asset = self.asset_on(3)

    def test_an_asset_gone_meanwhile_is_not_written_back(self):
        Asset.objects.filter(pk=self.asset.pk).delete()

        self.assertEqual(self.materialize(self.asset.pk), LEFT)
        self.assertFalse(Asset.objects.filter(item__time=day(3)).exists())

    def test_an_asset_outside_the_window_now_is_not_fetched(self):
        self.feed.materialize_start = day(4)
        self.feed.save()

        self.assertEqual(self.materialize(self.asset.pk), NOT_DUE)
        self.assertEqual(self.upstream.fetched, [])

    def test_an_asset_with_no_provenance_is_nobodys_to_materialize(self):
        Asset.objects.filter(pk=self.asset.pk).update(harvested_by=None)

        self.assertEqual(self.materialize(self.asset.pk), LEFT)
        self.assertEqual(self.upstream.fetched, [])

    def test_an_asset_that_leaves_the_jurisdiction_while_its_bytes_are_fetched_is_not_written(self):
        def gone_meanwhile(href, dest, *, addresses):
            Asset.objects.filter(pk=self.asset.pk).delete()
            return fetch(href, dest, addresses=addresses)

        fetch = self.upstream.fetch_asset
        with mock.patch.object(self.upstream, "fetch_asset", side_effect=gone_meanwhile):
            outcome = self.materialize(self.asset.pk)

        self.assertEqual(outcome, LEFT)
        self.assertEqual(self.writer.written, {}, "the object written for a row that went is left behind")
        self.assertFalse(Asset.objects.filter(item__time=day(3)).exists())

    def test_the_object_is_written_before_the_row_is_locked(self):
        """A Mirror judging the row waits on the lock, never on an upload."""
        depth = len(connection.savepoint_ids)

        self.assertEqual(self.materialize(self.asset.pk), MATERIALIZED)

        self.assertEqual(self.writer.depths, [depth])


class ALocalAssetIsBookkeptAsAnyIngestedAssetTests(MaterializeMixin, TestCase):
    def test_the_after_save_hooks_run_for_it(self):
        """Zonal statistics and virtual-Zarr manifests hang off these hooks (#715)."""
        self.harvest()
        seen = []

        with hooks.register_temporarily(GEORIVA_AFTER_SAVE_ASSET, lambda asset: seen.append(asset.pk)):
            self.materialize_window()

        self.assertCountEqual(seen, [self.asset_on(3).pk, self.asset_on(4).pk])

    def test_the_collection_is_extended_to_hold_it(self):
        self.harvest()
        collection = self.feed.collection

        self.materialize_window()

        collection.refresh_from_db()
        self.assertEqual((collection.time_start, collection.time_end), (day(1), day(4)))


class AStoreFeedMaterializesNothingTests(MaterializeMixin, TestCase):
    def test_a_store_feeds_window_sends_nothing(self):
        from georiva.harvest.models import UpstreamShape

        self.feed.upstream_shape = UpstreamShape.STORE
        self.assertFalse(self.feed.materializes)


class AMixedCollectionReadsAcrossTheBoundaryTests(RemoteCollectionMixin, TestCase):
    """Days 1 and 2 harvested; day 2 then Materialized. Nothing downstream
    tells the two apart beyond the href."""

    def setUp(self):
        super().setUp()
        self.writer = KeptWriter()
        self.feed.materialize_start = day(2)
        self.feed.save()
        self.local = Asset.objects.get(item__time=day(2))
        materialize(self.local.pk, upstream=self.upstream, writer=self.writer)
        self.local.refresh_from_db()

    def test_titiler_reads_the_local_copy_and_the_remote_archive_alike(self):
        self.assertEqual(self.published(), {"20260301T000000": rain_href(1)})
        self.assertNotIn("remote_href", self.tile_config(f"?time={stamp(2)}").json())
        self.assertEqual(self.tile_config(f"?time={stamp(1)}").json()["remote_href"], rain_href(1))

    def test_stac_serves_the_local_copys_url_beside_the_upstream_href(self):
        def href_on(stamp):
            response = self.client.get(reverse("stac:item-detail", args=["chirps", "chirps-daily", "rain", stamp]))
            self.assertEqual(response.status_code, 200)
            return response.json()["assets"]["rain_cog"]["href"]

        self.assertEqual(href_on("20260301T000000Z"), rain_href(1))
        # Our own URL for our own key, however the storage backend spells its base.
        self.assertFalse(AssetLocation(self.local.href).is_remote)
        local = href_on("20260302T000000Z")
        self.assertTrue(local.endswith(self.local.href), local)
        self.assertNotIn("upstream.example", local)
