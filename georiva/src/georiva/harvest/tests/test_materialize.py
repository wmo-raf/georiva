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
from wagtail import hooks

from georiva.core.models import Asset, Item
from georiva.core.storage.asset_location import AssetLocation
from georiva.harvest import tasks
from georiva.harvest.materialize import (
    LEFT,
    MATERIALIZE_CLAIM,
    MATERIALIZE_CLAIM_LONGEST,
    MATERIALIZED,
    NOT_DUE,
    REFUSED,
    UNREACHABLE,
    claim_key,
    materialize,
    refusals,
)
from georiva.harvest.upstream import UpstreamAsset
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET

from .support import ASSET_HOST, HarvestFeedMixin, RemoteCollectionMixin, day, rain_href, stamp


class KeptBucket:
    """The assets bucket behind a `KeptWriter`."""

    def __init__(self, written):
        self.written = written

    def size(self, path):
        return 4096

    def delete(self, path):
        return self.written.pop(path, None) is not None


class KeptWriter:
    """Our assets bucket, as far as a materialization can tell: what it is given is kept, by key.

    It also notes how deep in transactions each write was made, so a test can
    tell a write made under the row's lock from one made before it."""

    def __init__(self):
        self.written: dict[str, numpy.ndarray] = {}
        self.depths: list[int] = []
        self.bucket = KeptBucket(self.written)

    def write_cog(self, data, output_path, bounds, crs="EPSG:4326", nodata=None):
        self.depths.append(len(connection.savepoint_ids))
        self.written[output_path] = numpy.array(data)
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
        # A harvest's send claims its asset in the cache, which outlives the test's
        # transaction — and a later run's test database may hand out the same pks.
        self.forget_claims()
        self.addCleanup(self.forget_claims)
        self.enterContext(
            mock.patch.object(tasks.materialize_harvested_asset, "delay", side_effect=lambda pk: self.sent.append(pk))
        )

    def forget_claims(self):
        """As if every claim period had passed."""
        cache.delete_pattern(claim_key("*"))

    def harvest(self, **kwargs):
        # What a harvest sends, it sends as its writes commit.
        with self.captureOnCommitCallbacks(execute=True):
            return super().harvest(**kwargs)

    def materialize(self, asset_id):
        return materialize(asset_id, upstream=self.upstream, writer=self.writer)

    def materialize_sent(self):
        """Run what the harvest sent to the ingestion queue, as its worker would."""
        sent, self.sent = self.sent, []
        return [self.materialize(pk) for pk in sent]

    def asset_on(self, n) -> Asset:
        return Asset.objects.get(item__time=day(n), variable=self.variable)


class AHarvestSendsItsMaterializationWindowToIngestionTests(MaterializeMixin, TestCase):
    def test_the_remote_assets_inside_the_window_are_sent(self):
        self.harvest()

        self.assertCountEqual(self.sent, [self.asset_on(3).pk, self.asset_on(4).pk])

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
        self.materialize_sent()
        self.feed.materialize_start = day(1)
        self.feed.save()

        self.harvest()

        self.assertCountEqual(self.sent, [self.asset_on(1).pk, self.asset_on(2).pk])

    def test_the_task_runs_on_the_ingestion_queue_and_the_harvest_does_not(self):
        self.assertEqual(tasks.materialize_harvested_asset.queue, "georiva-ingestion")
        self.assertEqual(tasks.run_harvest.queue, "georiva-default")


class AMaterializedAssetIsTheSameAssetMadeLocalTests(MaterializeMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.upstream.rasters[rain_href(3)] = numpy.full((8, 8), 12.5, dtype="float32")
        self.harvest()
        self.before = self.asset_on(3)
        self.outcomes = self.materialize_sent()

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
        self.assertIsNone(self.asset.extra_fields["nodata"])
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


class AMaterializedAssetHasLeftTheMirrorsJurisdictionTests(MaterializeMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.harvest()
        self.materialize_sent()
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
        self.assertEqual(self.sent, [self.asset_on(4).pk])

        self.feed.asset_hosts = ["elsewhere.example"]
        self.feed.save()
        self.forget_claims()
        self.sent = []
        self.harvest()
        self.assertCountEqual(self.sent, [self.asset.pk, self.asset_on(4).pk])

    def test_a_refused_asset_whose_href_is_resynced_is_sent_again(self):
        Asset.objects.filter(pk=self.asset.pk).update(href="https://elsewhere.example/3.tif")
        self.materialize(self.asset.pk)
        Asset.objects.filter(pk=self.asset.pk).update(href=rain_href(3))
        self.forget_claims()
        self.sent = []

        self.harvest()

        self.assertIn(self.asset.pk, self.sent)

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
        self.materialize_sent()

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


class AnOperatorSeesWhatWasRefusedTests(MaterializeMixin, TestCase):
    """What in the window was refused, and why, counted by reason (#724)."""

    def setUp(self):
        super().setUp()
        self.harvest()

    def test_nothing_refused_says_nothing(self):
        self.materialize_sent()

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
            self.materialize_sent()

        self.assertCountEqual(seen, [self.asset_on(3).pk, self.asset_on(4).pk])

    def test_the_collection_is_extended_to_hold_it(self):
        self.harvest()
        collection = self.feed.collection

        self.materialize_sent()

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
