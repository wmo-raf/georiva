"""
The Upstream survey: what the Harvest Feed Wizard learns about an Upstream
before a feed exists (ADR 0042 §2, §9).

It reads through the Upstream catalog client and writes nothing: these are
`SimpleTestCase`s, which a database query would fail. The only fake is the
Upstream, and nothing reaches a network.
"""

from datetime import UTC, datetime

from django.test import SimpleTestCase

from georiva.harvest.survey import Shape, survey_catalog, survey_collection
from georiva.harvest.testing import FakeUpstream
from georiva.harvest.upstream import (
    AssetProbe,
    UpstreamAsset,
    UpstreamCollection,
    UpstreamStatistics,
    UpstreamUnreachable,
)

CATALOG = "https://stac.upstream.example/api"
OWN = "https://stac.upstream.example/chirps"
COG = "image/tiff; application=geotiff; profile=cloud-optimized"
BUCKET = "deafrica-input-datasets"
BUCKET_HOST = "deafrica-input-datasets.s3.af-south-1.amazonaws.com"
BROKEN = "upstream catalog did not answer JSON for …/collections"


def day(n: int) -> datetime:
    return datetime(2026, 3, n, tzinfo=UTC)


class SurveyMixin:
    def setUp(self):
        super().setUp()
        self.upstream = FakeUpstream("chirps", title="CHIRPS monthly rainfall")
        self.upstream.regions[BUCKET] = "af-south-1"

    def survey(self):
        return survey_collection(self.upstream, CATALOG, "chirps")

    def add_days(self, href_of, days=(1, 2, 3), key="rainfall", **asset):
        for n in days:
            self.upstream.add_item(day(n), {key: UpstreamAsset(href=href_of(n), media_type=COG, **asset)})


class CatalogSurveyTests(SurveyMixin, SimpleTestCase):
    def test_the_upstreams_collections_are_offered_with_their_titles(self):
        self.upstream.collections.append(UpstreamCollection(id="era5", title="ERA5"))

        found = survey_catalog(self.upstream)

        self.assertTrue(found.has_api)
        offered = [(c.id, c.title) for c in found.collections]
        self.assertEqual(offered, [("chirps", "CHIRPS monthly rainfall"), ("era5", "ERA5")])
        self.assertTrue(found.described)

    def test_a_catalog_whose_collections_do_not_answer_offers_its_roots_children(self):
        # Digital Earth Africa: an error page for /collections, and a root that links each one.
        self.upstream.collections_unreachable = BROKEN

        found = survey_catalog(self.upstream)

        self.assertEqual([c.id for c in found.collections], ["chirps"])
        self.assertFalse(found.described, "ids only: there are no titles or extents to show")

    def test_a_catalog_with_no_api_offers_nothing_and_says_so(self):
        self.upstream.is_api = False

        found = survey_catalog(self.upstream)

        self.assertFalse(found.has_api)
        self.assertEqual(found.collections, [])

    def test_an_address_that_is_one_collections_own_document_offers_that_collection(self):
        # stac.dynamical.org/noaa-gfs-forecast/collection.json: plain files, and the address is the collection.
        self.upstream.is_api = False
        self.upstream.is_collection_document = True

        found = survey_catalog(self.upstream)

        self.assertFalse(found.has_api)
        self.assertEqual([c.id for c in found.collections], ["chirps"])

    def test_a_catalog_that_cannot_be_read_at_all_is_unreachable(self):
        self.upstream.unreachable = "upstream catalog answered 401 for https://stac.upstream.example/api"

        with self.assertRaisesMessage(UpstreamUnreachable, "401"):
            survey_catalog(self.upstream)


class ShapeTests(SurveyMixin, SimpleTestCase):
    def test_one_cog_per_time_step_can_be_harvested(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif")

        found = self.survey()

        self.assertEqual(found.shape, Shape.ONE_COG_PER_TIME)
        self.assertTrue(found.harvestable)
        self.assertEqual(found.reason, "")

    def test_many_items_at_one_time_are_tiled_scenes_and_cannot(self):
        self.upstream.add_item(day(1), {"red": UpstreamAsset(href=f"{OWN}/a.tif", media_type=COG)}, id="tile-a")
        self.upstream.add_item(day(1), {"red": UpstreamAsset(href=f"{OWN}/b.tif", media_type=COG)}, id="tile-b")

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.TILED, False))
        self.assertIn("several", found.reason)
        self.assertEqual(self.upstream.probed, [], "a shape that cannot be harvested costs no raster read")

    def test_items_that_each_cover_a_different_place_are_scenes_whatever_their_times(self):
        # A scene collection gives every scene its own instant, so no two items share a time.
        for n in (1, 2, 3):
            scene = {"red": UpstreamAsset(href=f"{OWN}/{n}.tif", media_type=COG)}
            self.upstream.add_item(day(n), scene, bbox=[30.0 + n, -5.0, 31.0 + n, -4.0])

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.TILED, False))

    def test_extents_that_differ_by_a_rounding_are_the_same_place(self):
        for n in (1, 2):
            self.upstream.add_item(
                day(n),
                {"rainfall": UpstreamAsset(href=f"{OWN}/{n}.tif", media_type=COG)},
                bbox=[-20.0, -40.000001192092896 * (1 + n * 1e-9), 55.0, 40.0],
            )

        self.assertTrue(self.survey().harvestable)

    def test_a_zarr_or_netcdf_store_on_the_items_cannot(self):
        for media_type, href in (
            ("application/vnd+zarr", "abfs://era5/2020/12/precip.zarr"),
            ("application/x-netcdf", f"{OWN}/2020-12.nc"),
            ("", f"{OWN}/2020-12.zarr"),
        ):
            with self.subTest(media_type=media_type, href=href):
                self.upstream.items = []
                self.upstream.add_item(day(1), {"precip": UpstreamAsset(href=href, media_type=media_type)})

                found = self.survey()

                self.assertEqual((found.shape, found.harvestable), (Shape.STORE, False))

    def test_a_store_on_the_collection_itself_cannot(self):
        self.upstream.collections = [
            UpstreamCollection(
                id="chirps",
                assets={"zarr": UpstreamAsset(href=f"{OWN}/chirps.zarr", media_type="application/vnd+zarr")},
            )
        ]

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.STORE, False))

    def test_files_that_cannot_be_read_without_an_account_cannot(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif")
        self.upstream.unreadable = "HTTP response code: 403"

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.NEEDS_ACCOUNT, False))
        self.assertIn("403", found.reason)

    def test_files_at_an_address_only_an_account_opens_cannot(self):
        self.add_days(lambda n: f"abfs://container/chirps/{n}.tif")

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.NEEDS_ACCOUNT, False))
        self.assertEqual(self.upstream.probed, [])

    def test_a_catalog_with_no_api_cannot(self):
        self.upstream.is_api = False

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.NO_API, False))

    def test_a_store_in_a_catalog_with_no_api_is_called_a_store(self):
        # Both are true of it; "one data store" is the one that tells an operator what they have.
        self.upstream.is_api = False
        self.upstream.collections = [
            UpstreamCollection(
                id="chirps",
                assets={
                    "icechunk": UpstreamAsset(
                        href="https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast/v0.2.7.icechunk",
                        media_type="application/x-icechunk",
                    )
                },
            )
        ]

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.STORE, False))
        self.assertIn("data store", found.reason)

    def test_a_collection_with_no_items_cannot(self):
        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.EMPTY, False))

    def test_items_with_no_raster_asset_cannot(self):
        self.upstream.add_item(day(1), {"thumbnail": UpstreamAsset(href=f"{OWN}/1.png", media_type="image/png")})

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.NO_RASTERS, False))

    def test_items_with_no_time_cannot(self):
        self.upstream.add_item(None, {"rainfall": UpstreamAsset(href=f"{OWN}/1.tif", media_type=COG)}, id="a")

        found = self.survey()

        self.assertEqual((found.shape, found.harvestable), (Shape.UNTIMED, False))

    def test_every_shape_that_cannot_be_harvested_says_why_in_plain_words(self):
        self.upstream.is_api = False
        reason = self.survey().reason

        self.assertTrue(reason)
        for jargon in ("STAC", "S3", "COG", "API"):
            self.assertNotIn(jargon, reason)

    def test_a_collection_the_upstream_cannot_describe_is_surveyed_from_its_items(self):
        self.upstream.collections_unreachable = BROKEN
        self.add_days(lambda n: f"{OWN}/{n}.tif")

        self.assertTrue(self.survey().harvestable)

    def test_only_a_sample_of_a_long_listing_is_read(self):
        for n in range(1, 29):
            self.upstream.add_item(day(n), {"rainfall": UpstreamAsset(href=f"{OWN}/{n}.tif", media_type=COG)})

        found = self.survey()

        self.assertLess(found.items_sampled, 28)
        self.assertEqual(len(self.upstream.probed), 1, "one Sample item per asset key")


class AssetTests(SurveyMixin, SimpleTestCase):
    def test_raster_asset_keys_are_reported_with_what_the_upstream_says_of_them(self):
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(
                    href=f"{OWN}/1.tif", media_type=COG, title="Monthly rainfall", unit="mm", nodata=-9999.0
                ),
                "count": UpstreamAsset(href=f"{OWN}/1-count.tiff"),
                "thumbnail": UpstreamAsset(href=f"{OWN}/1.png", media_type="image/png"),
            },
        )

        found = self.survey()

        self.assertEqual([a.key for a in found.assets], ["rainfall", "count"])
        rainfall = found.assets[0]
        self.assertEqual((rainfall.title, rainfall.unit, rainfall.nodata), ("Monthly rainfall", "mm", -9999.0))

    def test_a_key_only_some_items_carry_is_still_found(self):
        self.upstream.add_item(day(1), {"rainfall": UpstreamAsset(href=f"{OWN}/1.tif", media_type=COG)})
        self.upstream.add_item(day(2), {"anomaly": UpstreamAsset(href=f"{OWN}/2.tif", media_type=COG)})

        self.assertEqual(sorted(a.key for a in self.survey().assets), ["anomaly", "rainfall"])

    def test_the_value_range_is_lifted_from_upstream_statistics_without_reading_pixels(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif", statistics=UpstreamStatistics(minimum=0.0, maximum=412.5))

        asset = self.survey().assets[0]

        self.assertEqual((asset.value_min, asset.value_max, asset.range_source), (0.0, 412.5, "upstream"))
        self.assertEqual(self.upstream.probed, [(f"{OWN}/1.tif", False)], "opened, and no pixel asked for")

    def test_with_no_upstream_statistics_the_range_is_estimated_and_says_so(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif")
        self.upstream.probes[f"{OWN}/1.tif"] = AssetProbe(
            readable=True, tiled=True, overviews=True, crs="EPSG:4326", value_min=0.0, value_max=287.0
        )

        asset = self.survey().assets[0]

        self.assertEqual((asset.value_min, asset.value_max, asset.range_source), (0.0, 287.0, "estimated"))
        self.assertEqual(self.upstream.probed, [(f"{OWN}/1.tif", True)])
        self.assertEqual(asset.crs, "EPSG:4326")

    def test_half_a_range_upstream_is_no_range(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif", statistics=UpstreamStatistics(minimum=0.0))

        self.assertEqual(self.survey().assets[0].range_source, "estimated")

    def test_a_geotiff_with_no_overviews_is_flagged(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif")
        self.upstream.probes[f"{OWN}/1.tif"] = AssetProbe(readable=True, tiled=False, overviews=False)

        found = self.survey()

        asset = found.assets[0]
        self.assertTrue(found.harvestable, "it can be indexed; the operator is told what it will cost")
        self.assertFalse(asset.cloud_optimized)
        self.assertEqual(asset.range_source, "")
        self.assertIn("slow", asset.warning)

    def test_one_unreadable_asset_does_not_condemn_the_others(self):
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(href=f"{OWN}/1.tif", media_type=COG),
                "anomaly": UpstreamAsset(href=f"{OWN}/1-anom.tif", media_type=COG),
            },
        )
        self.upstream.probes[f"{OWN}/1-anom.tif"] = AssetProbe(readable=False, problem="HTTP response code: 404")

        found = self.survey()

        self.assertTrue(found.harvestable)
        readable = {a.key: a.readable for a in found.assets}
        self.assertEqual(readable, {"rainfall": True, "anomaly": False})


class FileLocationTests(SurveyMixin, SimpleTestCase):
    def test_files_on_the_catalogs_own_host_need_no_confirmation(self):
        self.add_days(lambda n: f"{OWN}/{n}.tif")

        found = self.survey()

        self.assertEqual([(loc.host, loc.own) for loc in found.locations], [("stac.upstream.example", True)])
        self.assertEqual(found.proposed_locations, [])

    def test_another_https_host_is_proposed(self):
        self.add_days(lambda n: f"https://data.elsewhere.example/chirps/{n}.tif")

        found = self.survey()

        (location,) = found.proposed_locations
        self.assertEqual((location.host, location.bucket, location.readable), ("data.elsewhere.example", "", True))

    def test_an_s3_bucket_is_proposed_as_the_public_host_its_region_gives_it(self):
        self.add_days(lambda n: f"s3://{BUCKET}/rainfall_chirps_monthly/{n}.tif")

        found = self.survey()

        (location,) = found.proposed_locations
        self.assertEqual((location.bucket, location.host, location.readable), (BUCKET, BUCKET_HOST, True))
        # What was opened is the address a harvest would store, not the one the catalog spelled.
        self.assertEqual(self.upstream.probed, [(f"https://{BUCKET_HOST}/rainfall_chirps_monthly/1.tif", True)])
        self.assertTrue(found.harvestable)

    def test_a_bucket_that_refuses_anonymous_reads_needs_an_account(self):
        self.add_days(lambda n: f"s3://{BUCKET}/rainfall_chirps_monthly/{n}.tif")
        self.upstream.unreadable = "HTTP response code: 403"

        found = self.survey()

        self.assertEqual(found.shape, Shape.NEEDS_ACCOUNT)
        self.assertEqual(found.proposed_locations, [])
        (location,) = found.locations
        self.assertFalse(location.readable)
        self.assertIn("403", location.problem)

    def test_a_bucket_whose_region_cannot_be_found_is_not_proposed(self):
        self.add_days(lambda n: f"s3://somewhere-unknown/{n}.tif")

        found = self.survey()

        self.assertEqual(found.proposed_locations, [])
        self.assertEqual(self.upstream.probed, [])
        self.assertEqual(found.shape, Shape.NEEDS_ACCOUNT)

    def test_a_host_that_resolves_to_a_private_address_is_never_proposed_or_opened(self):
        self.upstream.addresses["data.internal.example"] = ["10.0.0.7"]
        self.upstream.addresses[BUCKET_HOST] = ["169.254.169.254"]
        for href in ("https://data.internal.example/1.tif", f"s3://{BUCKET}/1.tif", "https://127.0.0.1/1.tif"):
            with self.subTest(href=href):
                self.upstream.items, self.upstream.probed = [], []
                self.upstream.add_item(day(1), {"rainfall": UpstreamAsset(href=href, media_type=COG)})

                found = self.survey()

                self.assertEqual(found.proposed_locations, [])
                self.assertEqual(self.upstream.probed, [])
                self.assertFalse(found.harvestable)

    def test_a_plain_http_host_is_not_proposed(self):
        self.add_days(lambda n: f"http://data.elsewhere.example/{n}.tif")

        found = self.survey()

        self.assertEqual(found.proposed_locations, [])
        self.assertEqual(self.upstream.probed, [])

    def test_a_bucket_name_that_could_steer_the_address_is_never_asked_about(self):
        for bucket in ("evil.example.com/x", "a..b", "UPPER", "x@169.254.169.254", "dotted.bucket.name"):
            with self.subTest(bucket=bucket):
                self.upstream.items, self.upstream.regions_asked = [], []
                self.upstream.add_item(day(1), {"rainfall": UpstreamAsset(href=f"s3://{bucket}/1.tif", media_type=COG)})

                found = self.survey()

                self.assertEqual(found.proposed_locations, [])
                self.assertEqual(self.upstream.regions_asked, [])

    def test_a_region_that_is_not_one_is_not_built_into_a_host(self):
        self.upstream.regions[BUCKET] = "af-south-1.evil.example/"
        self.add_days(lambda n: f"s3://{BUCKET}/{n}.tif")

        found = self.survey()

        self.assertEqual(found.proposed_locations, [])
        self.assertEqual(self.upstream.probed, [])

    def test_each_place_is_reported_once_however_many_assets_live_there(self):
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(href=f"s3://{BUCKET}/1.tif", media_type=COG),
                "anomaly": UpstreamAsset(href=f"s3://{BUCKET}/1-anom.tif", media_type=COG),
                "count": UpstreamAsset(href="https://data.elsewhere.example/1-count.tif", media_type=COG),
            },
        )

        found = self.survey()

        self.assertEqual({loc.host for loc in found.proposed_locations}, {BUCKET_HOST, "data.elsewhere.example"})
        self.assertEqual(len(found.locations), 2)
        self.assertEqual(self.upstream.regions_asked, [BUCKET])

    def test_what_a_confirmed_survey_pins_on_a_feed(self):
        self.upstream.add_item(
            day(1),
            {
                "rainfall": UpstreamAsset(href=f"s3://{BUCKET}/1.tif", media_type=COG),
                "count": UpstreamAsset(href="https://data.elsewhere.example/1-count.tif", media_type=COG),
                "own": UpstreamAsset(href=f"{OWN}/1.tif", media_type=COG),
            },
        )

        found = self.survey()

        self.assertEqual(found.pins(found.proposed_locations), ({BUCKET: BUCKET_HOST}, ["data.elsewhere.example"]))
        self.assertEqual(found.pins([]), ({}, []))
