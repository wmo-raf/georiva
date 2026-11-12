"""
A harvest indexes an Upstream into ordinary Published rows whose assets point
upstream (ADR 0042).

Every test here drives a real Harvest feed through a real run; the only fake is
the Upstream catalog client, and nothing reaches a network.
"""

from datetime import timedelta

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Asset, Collection, Item
from georiva.harvest.harvester import harvest
from georiva.harvest.testing import FakeUpstream
from georiva.harvest.upstream import UpstreamAsset, UpstreamStatistics
from georiva.organisations.testing import dial_org
from georiva.sources.models import FetchedFile, FetchRun

from .support import ASSET_HOST, HarvestFeedMixin, day, rain_href


class HarvestIndexesMappedAssetsTests(HarvestFeedMixin, TestCase):
    def test_a_mapped_asset_becomes_an_item_and_a_remote_asset_in_the_feeds_catalog(self):
        variable = self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)}, bbox=[33.0, -5.0, 42.0, 5.0])

        self.harvest()

        asset = Asset.objects.get()
        self.assertEqual(asset.href, rain_href(1))
        self.assertTrue(asset.location.is_remote)
        self.assertEqual((asset.variable, asset.format, asset.roles), (variable, Asset.Format.COG, ["data"]))
        item = asset.item
        self.assertEqual((item.time, item.reference_time, item.bounds), (day(1), None, [33.0, -5.0, 42.0, 5.0]))
        self.assertEqual(item.collection.catalog, self.feed.catalog)

    def test_the_collection_is_created_in_the_feeds_catalog_and_described_by_the_upstream(self):
        self.harvest()

        collection = Collection.objects.get()
        self.assertEqual(collection.catalog, self.feed.catalog)
        self.assertEqual((collection.slug, collection.name), ("chirps-daily", "CHIRPS daily rainfall"))
        self.assertEqual(self.feed.collection, collection)

    def test_an_unmapped_asset_key_creates_nothing(self):
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"quality": f"{ASSET_HOST}/q/1.tif"})

        result = self.harvest()

        self.assertFalse(Item.objects.exists())
        self.assertFalse(Asset.objects.exists())
        self.assertEqual((result.indexed, result.rejected), (0, 0))

    def test_adding_the_key_to_the_mapping_and_harvesting_again_indexes_it(self):
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1), "quality": f"{ASSET_HOST}/q/1.tif"})
        self.harvest()
        self.assertEqual(Asset.objects.count(), 1)

        quality = self.map_variable("quality", slug="quality")
        result = self.harvest()

        self.assertEqual(result.indexed, 1)
        self.assertEqual(Asset.objects.get(variable=quality).href, f"{ASSET_HOST}/q/1.tif")
        self.assertEqual(Item.objects.count(), 1, "both assets sit on the one item at that time")

    def test_a_mapping_to_another_collections_variable_maps_nothing(self):
        variable = self.map_variable("rainfall")
        elsewhere = Collection.objects.create(catalog=self.feed.catalog, name="Elsewhere", slug="elsewhere")
        variable.collection = elsewhere
        variable.save()
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        self.harvest()

        self.assertFalse(Asset.objects.exists())

    def test_the_collections_extent_and_item_count_follow_what_was_indexed(self):
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)}, bbox=[33.0, -5.0, 42.0, 5.0])
        self.upstream.add_item(day(3), {"rainfall": rain_href(3)}, bbox=[30.0, -6.0, 40.0, 4.0])

        self.harvest()
        self.harvest()

        collection = Collection.objects.get()
        self.assertEqual((collection.item_count, collection.time_start, collection.time_end), (2, day(1), day(3)))
        self.assertEqual(collection.bounds, [30.0, -6.0, 42.0, 5.0])

    def test_an_asset_already_there_for_that_time_and_variable_is_left_alone(self):
        """A mixed collection: the local asset is not the harvest's to rewrite."""
        variable = self.map_variable("rainfall")
        local_item = Item.objects.create(collection=self.feed.collection, time=day(1))
        local = Asset.objects.create(
            item=local_item, variable=variable, format=Asset.Format.COG, href="org/chirps/chirps-daily/rain/a.tif"
        )
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        result = self.harvest()

        local.refresh_from_db()
        self.assertEqual((local.href, local.harvested_by_id), ("org/chirps/chirps-daily/rain/a.tif", None))
        self.assertEqual((Asset.objects.count(), result.indexed), (1, 0))


class HarvestWindowTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.feed.window_start, self.feed.window_end = day(2), day(4)
        self.feed.save()
        self.map_variable("rainfall")
        for n in range(1, 6):
            self.upstream.add_item(day(n), {"rainfall": rain_href(n)})

    def _times(self):
        return sorted(Item.objects.values_list("time", flat=True))

    def test_the_upstream_is_asked_for_the_feeds_window(self):
        self.harvest()

        self.assertEqual(self.upstream.windows_asked, [(day(2), day(4))])

    def test_only_items_inside_the_window_are_indexed(self):
        self.harvest()

        self.assertEqual(self._times(), [day(2), day(3), day(4)])

    def test_an_upstream_that_answers_beyond_the_window_is_still_held_to_it(self):
        self.upstream.ignores_window = True

        self.harvest()

        self.assertEqual(self._times(), [day(2), day(3), day(4)])

    def test_an_open_ended_window_takes_everything_from_its_start(self):
        self.feed.window_end = None
        self.feed.save()

        self.harvest()

        self.assertEqual(self._times(), [day(2), day(3), day(4), day(5)])


class HarvestProvenanceTests(HarvestFeedMixin, TestCase):
    def test_every_harvested_row_names_the_feed_that_created_it(self):
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        self.harvest()

        for row in (Collection.objects.get(), Item.objects.get(), Asset.objects.get()):
            with self.subTest(row=type(row).__name__):
                self.assertEqual(row.harvested_by_id, self.feed.pk)

    def test_provenance_outlives_the_feed(self):
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.harvest()
        feed_pk = self.feed.pk

        # The feed row alone: DataFeed.delete() would take its catalog with it.
        type(self.feed).objects.filter(pk=feed_pk).delete()

        for row in (Collection.objects.get(), Item.objects.get(), Asset.objects.get()):
            with self.subTest(row=type(row).__name__):
                self.assertEqual(row.harvested_by_id, feed_pk)

    def test_a_row_the_harvest_did_not_create_gains_no_provenance(self):
        self.map_variable("rainfall")
        existing = Item.objects.create(collection=self.feed.collection, time=day(1))
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        self.harvest()

        existing.refresh_from_db()
        self.assertIsNone(existing.harvested_by_id)
        self.assertEqual(Asset.objects.get(item=existing).harvested_by_id, self.feed.pk)


class AssetHostAllowlistTests(HarvestFeedMixin, TestCase):
    """Upstream decides what we index, never where our servers connect (ADR 0042 §9)."""

    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

    def _harvest_href(self, href):
        self.upstream.add_item(day(2), {"rainfall": href})
        return self.harvest()

    def assert_rejected(self, href):
        result = self._harvest_href(href)

        run = FetchRun.objects.get()
        self.assertEqual((run.status, run.harvest_rejected), (FetchRun.Status.COMPLETED, 1))
        self.assertEqual(result.rejected, 1)
        # The harvest still completed: the sound asset beside it was indexed.
        self.assertEqual(list(Asset.objects.values_list("href", flat=True)), [rain_href(1)])
        self.assertEqual(Item.objects.count(), 1, "an item with nothing admitted is not indexed")
        self.assertEqual(run.harvest_indexed, 1)

    def test_a_plain_http_href_is_rejected(self):
        self.assert_rejected("http://stac.upstream.example/chirps/2.tif")

    def test_a_plain_http_href_is_indexed_when_the_feed_allows_http(self):
        self.feed.allow_http = True
        self.feed.save()

        result = self._harvest_href("http://stac.upstream.example/chirps/2.tif")

        self.assertEqual((result.rejected, Asset.objects.count()), (0, 2))

    def test_a_scheme_that_is_not_http_at_all_is_rejected_even_then(self):
        self.feed.allow_http = True
        self.feed.save()

        for href in ("s3://stac.upstream.example/chirps/2.tif", "file:///etc/passwd", "/vsicurl/x.tif"):
            with self.subTest(href=href):
                FetchRun.objects.all().delete()
                self.upstream.items = self.upstream.items[:1]
                result = self._harvest_href(href)
                self.assertEqual((result.rejected, Asset.objects.count()), (1, 1))

    def test_a_host_outside_the_allowlist_is_rejected(self):
        self.assert_rejected("https://bucket.elsewhere.example/chirps/2.tif")

    def test_a_host_the_operator_added_to_the_allowlist_is_indexed(self):
        self.feed.asset_hosts = ["bucket.elsewhere.example"]
        self.feed.save()

        result = self._harvest_href("https://bucket.elsewhere.example/chirps/2.tif")

        self.assertEqual((result.rejected, Asset.objects.count()), (0, 2))

    def test_an_allowed_host_that_resolves_to_a_private_address_is_rejected(self):
        self.feed.asset_hosts = ["internal.elsewhere.example"]
        self.feed.save()
        self.upstream.addresses["internal.elsewhere.example"] = ["10.0.0.7"]

        self.assert_rejected("https://internal.elsewhere.example/chirps/2.tif")

    def test_a_host_with_any_private_address_among_several_is_rejected(self):
        self.feed.asset_hosts = ["mixed.elsewhere.example"]
        self.feed.save()
        self.upstream.addresses["mixed.elsewhere.example"] = ["93.184.216.34", "192.168.1.9"]

        self.assert_rejected("https://mixed.elsewhere.example/chirps/2.tif")

    def test_a_host_that_does_not_resolve_is_rejected(self):
        self.feed.asset_hosts = ["gone.elsewhere.example"]
        self.feed.save()
        self.upstream.addresses["gone.elsewhere.example"] = []

        self.assert_rejected("https://gone.elsewhere.example/chirps/2.tif")

    def test_loopback_link_local_and_private_address_literals_are_rejected_though_allowed_by_name(self):
        hosts = {
            "127.0.0.1": "loopback",
            "169.254.169.254": "link-local (cloud metadata)",
            "10.1.2.3": "private",
            "[::1]": "IPv6 loopback",
            "[fe80::1]": "IPv6 link-local",
            "[::ffff:10.0.0.1]": "a private IPv4 address written as IPv6",
        }
        self.feed.asset_hosts = [host.strip("[]") for host in hosts]
        self.feed.save()

        for host, what in hosts.items():
            with self.subTest(what):
                FetchRun.objects.all().delete()
                self.upstream.items = self.upstream.items[:1]
                result = self._harvest_href(f"https://{host}/chirps/2.tif")
                self.assertEqual((result.rejected, Asset.objects.count()), (1, 1))

    def test_an_href_a_forgiving_parser_would_read_differently_is_rejected(self):
        hrefs = {
            "leading whitespace": f" {ASSET_HOST}/chirps/2.tif",
            "an embedded newline": f"{ASSET_HOST}/chirps/\n2.tif",
            "credentials before the host": "https://user:secret@stac.upstream.example/chirps/2.tif",
            "a backslash hiding another host": "https://internal.example\\@stac.upstream.example/chirps/2.tif",
            "no href at all": "",
            "a bare path": "chirps/2.tif",
        }
        for what, href in hrefs.items():
            with self.subTest(what):
                FetchRun.objects.all().delete()
                self.upstream.items = self.upstream.items[:1]
                result = self._harvest_href(href)
                self.assertEqual((result.rejected, Asset.objects.count()), (1, 1))

    def test_an_ipv6_address_that_only_wraps_a_private_ipv4_one_is_rejected(self):
        self.feed.asset_hosts = ["nat64.elsewhere.example", "sixtofour.elsewhere.example"]
        self.feed.save()
        self.upstream.addresses["nat64.elsewhere.example"] = ["64:ff9b::a00:1"]
        self.upstream.addresses["sixtofour.elsewhere.example"] = ["2002:c0a8:101::1"]

        for host in self.feed.asset_hosts:
            with self.subTest(host):
                FetchRun.objects.all().delete()
                self.upstream.items = self.upstream.items[:1]
                result = self._harvest_href(f"https://{host}/chirps/2.tif")
                self.assertEqual((result.rejected, Asset.objects.count()), (1, 1))

    def test_an_href_too_long_to_store_whole_is_rejected(self):
        self.assert_rejected(f"{ASSET_HOST}/{'x' * 500}.tif")

    def test_a_host_is_resolved_once_however_many_assets_it_serves(self):
        self.upstream.add_item(day(2), {"rainfall": rain_href(2)})
        self.upstream.add_item(day(3), {"rainfall": rain_href(3)})

        self.harvest()

        self.assertEqual(self.upstream.hosts_resolved, ["stac.upstream.example"])


class HarvestStatisticsTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")

    def test_statistics_are_lifted_from_upstream_metadata(self):
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(
                    href=rain_href(1),
                    media_type="image/tiff; application=geotiff",
                    file_size=123456,
                    checksum="1220abc",
                    nodata=-9999.0,
                    statistics=UpstreamStatistics(minimum=0.0, maximum=87.5, mean=3.2, stddev=6.1),
                )
            },
        )

        self.harvest()

        asset = Asset.objects.get()
        self.assertEqual(
            (asset.stats_min, asset.stats_max, asset.stats_mean, asset.stats_std),
            (0.0, 87.5, 3.2, 6.1),
        )
        self.assertEqual((asset.file_size, asset.checksum, asset.nodata), (123456, "1220abc", -9999.0))
        self.assertEqual(asset.media_type, "image/tiff; application=geotiff")

    def test_statistics_are_null_when_the_upstream_publishes_none(self):
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        self.harvest()

        asset = Asset.objects.get()
        self.assertEqual(
            (asset.stats_min, asset.stats_max, asset.stats_mean, asset.stats_std),
            (None, None, None, None),
        )


class HarvestRunTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.upstream.add_item(day(2), {"rainfall": rain_href(2)})

    def test_a_harvest_is_a_completed_fetch_run_with_its_own_counts_and_no_files(self):
        self.harvest()

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.COMPLETED)
        self.assertEqual((run.data_feed_id, run.collection), (self.feed.pk, self.feed.collection))
        harvest_counts = (run.harvest_indexed, run.harvest_updated, run.harvest_removed, run.harvest_rejected)
        self.assertEqual(harvest_counts, (2, 0, 0, 0))
        self.assertEqual((run.files_requested, run.files_fetched, run.files_skipped, run.files_failed), (0, 0, 0, 0))
        self.assertFalse(FetchedFile.objects.exists())
        self.assertIsNone(run.retry_due_at)

    def test_the_feed_and_its_link_are_stamped_like_any_other_feeds(self):
        self.harvest()

        self.feed.refresh_from_db()
        self.assertEqual((self.feed.last_run_status, self.feed.total_runs), ("success", 1))
        self.assertIsNotNone(self.feed.collection_links.get().last_run_at)

    def test_harvesting_twice_changes_nothing_the_second_time(self):
        self.harvest()
        before = {
            "items": list(Item.objects.order_by("time").values_list("pk", "modified")),
            "assets": list(Asset.objects.order_by("pk").values_list("pk", "href", "modified")),
        }

        result = self.harvest()

        after = {
            "items": list(Item.objects.order_by("time").values_list("pk", "modified")),
            "assets": list(Asset.objects.order_by("pk").values_list("pk", "href", "modified")),
        }
        self.assertEqual(before, after)
        self.assertEqual(Collection.objects.count(), 1)
        second = FetchRun.objects.order_by("pk").last()
        self.assertEqual((second.status, second.harvest_indexed, result.indexed), (FetchRun.Status.COMPLETED, 0, 0))
        self.assertEqual(FetchRun.objects.count(), 2)

    def test_an_unreachable_upstream_fails_the_run_and_writes_no_row(self):
        self.upstream.unreachable = "upstream catalog unreachable: connection refused"

        result = self.harvest()

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.stopped_early, "source unavailable — upstream catalog unreachable: connection refused")
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertIsNotNone(result.source_unavailable)
        self.assertFalse(Item.objects.exists())
        self.assertFalse(Asset.objects.exists())
        self.assertFalse(FetchedFile.objects.exists())

    def test_an_upstream_without_the_feeds_collection_fails_the_run(self):
        result = harvest(self.feed, upstream=FakeUpstream("something-else"))

        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.FAILED)
        self.assertIn("upstream has no collection 'chirps-daily'", result.stop_reason)

    def test_a_collection_the_upstream_cannot_describe_is_harvested_from_its_items(self):
        # Digital Earth Africa answers its collections with an error page, and its items soundly.
        self.upstream.collections_unreachable = "upstream catalog did not answer JSON for …/collections"

        result = self.harvest()

        self.assertEqual((FetchRun.objects.get().status, result.indexed), (FetchRun.Status.COMPLETED, 2))
        # Nothing was read to describe it with, so it keeps the name it was created under.
        self.assertEqual(self.feed.collection.name, "chirps-daily")

    def test_a_collection_neither_described_nor_listed_is_a_failure_and_never_an_empty_window(self):
        self.harvest()
        # Nothing now says the collection is still there: not its description, not one item.
        self.upstream.collections_unreachable = "upstream catalog did not answer JSON for …/collections"
        self.upstream.items = []

        result = self.harvest()

        run = FetchRun.objects.order_by("pk").last()
        self.assertEqual((run.status, result.removed), (FetchRun.Status.FAILED, 0))
        self.assertIn("source unavailable", run.stopped_early)
        self.assertIsNotNone(run.retry_due_at)
        self.assertEqual(Asset.objects.count(), 2)

    def test_a_described_collection_with_an_empty_window_is_still_an_empty_window(self):
        self.harvest()
        self.upstream.items = []

        result = self.harvest()

        self.assertEqual((result.removed, Asset.objects.count()), (2, 0))

    def test_a_retry_harvests_the_same_window_again(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"
        self.harvest()
        failed = FetchRun.objects.get()
        self.upstream.unreachable = None

        self.harvest(resumed_from=failed)

        retry = FetchRun.objects.exclude(pk=failed.pk).get()
        self.assertEqual((retry.resumed_from, retry.status), (failed, FetchRun.Status.COMPLETED))
        self.assertEqual(retry.harvest_indexed, 2)


class HarvestedRowsInTheSTACAPITests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.harvest()

    def test_a_harvested_item_is_served_with_its_upstream_href(self):
        response = self.client.get(
            reverse("stac:item-detail", args=["chirps", "chirps-daily", "rain", "20260301T000000Z"])
        )

        self.assertEqual(response.status_code, 200)
        assets = response.json()["assets"]
        self.assertEqual(assets["rain_cog"]["href"], rain_href(1))

    def test_the_harvested_collection_is_listed(self):
        response = self.client.get(reverse("stac:collection-list", args=["chirps"]))

        self.assertEqual(response.status_code, 200)
        self.assertIn("chirps-daily/rain", {c["id"] for c in response.json()["collections"]})

    def test_harvested_rows_belong_to_the_feeds_organisation_and_no_other(self):
        organisation = self.feed.catalog.organisation
        asset = Asset.objects.get()
        self.assertEqual(asset.item.collection.catalog.organisation, organisation)

        # Another organisation's host serves none of it.
        dial_org(self.client, "other")
        response = self.client.get(
            reverse("stac:item-detail", args=["chirps", "chirps-daily", "rain", "20260301T000000Z"])
        )

        self.assertEqual(response.status_code, 404)
