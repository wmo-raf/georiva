"""
The Upstream survey: what the Harvest Feed Wizard learns about an Upstream
before a feed exists (ADR 0042 §2, §9).

It reads through the Upstream catalog client and writes nothing: these are
`SimpleTestCase`s, which a database query would fail. The only fake is the
Upstream, and nothing reaches a network.
"""

from datetime import UTC, datetime

from django.test import SimpleTestCase

from georiva.harvest.survey import Shape, reason_for, survey_catalog, survey_collection
from georiva.harvest.testing import FakeUpstream
from georiva.harvest.upstream import (
    AssetProbe,
    CubeDimension,
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

    def test_a_catalog_with_no_api_offers_the_collections_its_root_links_to(self):
        # stac.dynamical.org/catalog.json: plain files, and 43 collections a link away each.
        self.upstream.is_api = False
        self.upstream.collections.append(UpstreamCollection(id="noaa-gfs-forecast", title="NOAA GFS forecast"))

        found = survey_catalog(self.upstream)

        self.assertFalse(found.has_api)
        offered = [(c.id, c.title) for c in found.collections]
        self.assertEqual(offered, [("chirps", "CHIRPS monthly rainfall"), ("noaa-gfs-forecast", "NOAA GFS forecast")])
        self.assertTrue(found.described)

    def test_a_catalog_with_no_api_that_will_not_describe_its_collections_offers_them_by_id(self):
        self.upstream.is_api = False
        self.upstream.collections_unreachable = BROKEN

        found = survey_catalog(self.upstream)

        self.assertEqual([c.id for c in found.collections], ["chirps"])
        self.assertFalse(found.described)

    def test_a_catalog_with_no_api_and_no_collections_offers_nothing_and_says_so(self):
        self.upstream.is_api = False
        self.upstream.collections = []

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

    def test_a_collection_of_plain_item_files_in_a_catalog_with_no_api_still_cannot(self):
        # The items are there to be had; without an API they cannot be asked for by time.
        self.upstream.is_api = False
        self.add_days(lambda n: f"{OWN}/{n}.tif")

        found = self.survey()

        self.assertEqual((found.shape, found.accepted), (Shape.NO_API, False))
        self.assertIn("cannot be searched by time", found.reason)
        self.assertEqual(self.upstream.windows_asked, [], "its items are never listed")
        self.assertEqual((self.upstream.probed, self.upstream.stores_opened), ([], []))

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
        self.assertEqual(self.upstream.stores_opened, [], "a store that describes no cube is not opened")

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


STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
STORE_S3 = "s3://dynamical-noaa-gfs/noaa-gfs-forecast-virtual/v0.1.0.icechunk/"
STORE_HOST = "dynamical-noaa-gfs.s3.us-west-2.amazonaws.com"
GRIB_BUCKET = "s3://noaa-gfs-bdp-pds/"
FORECAST = {"init_time": None, "lead_time": 209, "latitude": 721, "longitude": 1440}
RUN_STEP_GRID = ("init_time", "lead_time", "latitude", "longitude")
#: What the store carries beside its fields: coordinates, and what it says of each run.
BESIDE = {
    "init_time": (("init_time",), (1024,)),
    "valid_time": (("init_time", "lead_time"), (1024, 209)),
    "expected_forecast_length": (("init_time",), (1024,)),
    "spatial_ref": ((), ()),
}
#: The words that are the store's makers', never an operator's.
STORE_JARGON = ("Zarr", "zarr", "Icechunk", "icechunk", "chunk", "codec")


class StoreSurveyTests(SurveyMixin, SimpleTestCase):
    """An Upstream store: one collection, one cube, no items (ADR 0044 §1, §5, §12)."""

    def setUp(self):
        super().setUp()
        self.upstream.is_api = False

    def map_shaped(self, **kwargs):
        """GFS as dynamical.org's virtual store publishes it: one whole field a chunk."""
        arrays = {
            "temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440)),
            "wind_u_10m": (RUN_STEP_GRID, (1, 1, 721, 1440)),
            **BESIDE,
        }
        self.upstream.become_store(STORE, FORECAST, kwargs.pop("arrays", arrays), **kwargs)

    def series_shaped(self):
        """The same model rewritten for a place over time: 105 steps over a 30° square."""
        arrays = {"temperature_2m": (RUN_STEP_GRID, (1, 105, 121, 121)), **BESIDE}
        self.upstream.become_store(STORE, FORECAST, arrays)

    def assert_plain(self, reason, *, but=()):
        for jargon in (*STORE_JARGON, "STAC", "S3", "COG", "API"):
            self.assertNotIn(jargon, reason if not but else reason.replace(but, ""))

    # Accepted.

    def test_a_map_shaped_store_whose_codecs_decode_is_accepted(self):
        self.map_shaped()

        found = self.survey()

        self.assertEqual(found.shape, Shape.MAP_STORE)
        self.assertTrue(found.accepted)
        self.assertEqual(found.reason, "")
        self.assertEqual(self.upstream.stores_opened, [STORE])
        self.assertEqual((found.store.href, found.store.fields), (STORE, 2))

    def test_an_accepted_store_is_not_yet_something_a_feed_can_be_set_up_from(self):
        self.map_shaped()

        self.assertFalse(self.survey().harvestable)

    def test_a_store_naming_the_grib_codec_core_carries_is_accepted(self):
        # The check is against what is installed here, and gribberish is in core's lock.
        self.map_shaped(codecs={name: ("scale_offset", "gribberish") for name in ("temperature_2m", "wind_u_10m")})

        found = self.survey()

        self.assertEqual(found.shape, Shape.MAP_STORE)
        self.assertEqual(found.store.codecs, ["scale_offset", "gribberish", "bytes", "zstd"])

    def test_an_analysis_store_with_one_time_is_map_shaped_too(self):
        self.upstream.become_store(
            STORE,
            {"time": None, "latitude": 721, "longitude": 1440},
            {"precipitation": (("time", "latitude", "longitude"), (1, 721, 1440))},
        )

        self.assertEqual(self.survey().shape, Shape.MAP_STORE)

    def test_a_store_behind_an_api_is_surveyed_as_a_store_too(self):
        self.upstream.is_api = True
        self.map_shaped()

        self.assertEqual(self.survey().shape, Shape.MAP_STORE)

    def test_the_https_address_is_the_one_opened_where_the_store_is_published_twice(self):
        self.map_shaped(s3_href=STORE_S3)

        found = self.survey()

        self.assertEqual(self.upstream.stores_opened, [STORE])
        self.assertEqual(found.store.href, STORE)
        self.assertEqual(self.upstream.regions_asked, [], "the s3 twin is never asked about")

    def test_where_the_store_lives_is_reported_as_a_place_that_was_read(self):
        self.map_shaped()

        (location,) = self.survey().locations

        self.assertEqual((location.host, location.readable, location.problem), (STORE_HOST, True, ""))

    def test_the_places_a_virtual_store_says_its_data_lives_in_are_carried(self):
        self.map_shaped(chunk_locations=(GRIB_BUCKET,))

        self.assertEqual(self.survey().store.chunk_locations, [GRIB_BUCKET])

    def test_surveying_a_store_lists_no_items_and_opens_no_file(self):
        self.map_shaped()

        self.survey()

        self.assertEqual((self.upstream.windows_asked, self.upstream.probed), ([], []))

    # Refused: its shape.

    def test_a_series_shaped_store_is_refused_with_its_reason(self):
        self.series_shaped()

        found = self.survey()

        self.assertEqual((found.shape, found.accepted, found.harvestable), (Shape.SERIES_STORE, False, False))
        self.assertIn("charting one place over time", found.reason)
        self.assertIn("not for drawing maps", found.reason)
        self.assertIn("cannot use it yet", found.reason)
        self.assert_plain(found.reason)

    def test_a_series_shaped_store_is_never_opened(self):
        self.series_shaped()

        self.survey()

        self.assertEqual(self.upstream.stores_opened, [], "its published layout already said no")

    def test_many_times_in_one_piece_is_series_shaped_whatever_the_ground_it_covers(self):
        self.upstream.become_store(
            STORE,
            {"time": None, "latitude": 2000, "longitude": 7200},
            {"precipitation": (("time", "latitude", "longitude"), (365, 60, 60))},
        )

        self.assertEqual(self.survey().shape, Shape.SERIES_STORE)

    def test_one_series_shaped_field_makes_the_store_series_shaped(self):
        self.map_shaped(
            arrays={
                "temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440)),
                "precipitation_surface": (RUN_STEP_GRID, (1, 105, 121, 121)),
            }
        )

        self.assertEqual(self.survey().shape, Shape.SERIES_STORE)

    def test_a_step_dimension_is_known_by_its_name_where_the_collection_does_not_call_it_temporal(self):
        dimensions = {**FORECAST, "lead_time": CubeDimension("lead_time", kind="other", size=209)}
        self.upstream.become_store(STORE, dimensions, {"temperature_2m": (RUN_STEP_GRID, (1, 105, 121, 121))})

        self.assertEqual(self.survey().shape, Shape.SERIES_STORE)

    def test_arrays_that_are_not_fields_do_not_decide_the_shape(self):
        # `valid_time` holds every step of every run in one piece, and is no field.
        self.map_shaped()

        self.assertEqual(self.survey().shape, Shape.MAP_STORE)

    # Refused: what it is packed with.

    def test_a_store_naming_an_unknown_codec_is_refused_and_the_codec_is_named(self):
        self.map_shaped(codecs={"wind_u_10m": ("bytes", "acme_wavelet")})

        found = self.survey()

        self.assertEqual((found.shape, found.accepted), (Shape.UNDECODABLE_STORE, False))
        self.assertIn("“acme_wavelet”", found.reason)
        self.assertNotIn("bytes", found.reason, "only what cannot be decoded is named")
        self.assert_plain(found.reason, but="acme_wavelet")

    def test_every_codec_that_cannot_be_decoded_is_named_once(self):
        self.map_shaped(codecs={"temperature_2m": ("acme_wavelet",), "wind_u_10m": ("acme_wavelet", "other_codec")})

        reason = self.survey().reason

        self.assertEqual(reason.count("acme_wavelet"), 1)
        self.assertIn("“other_codec”", reason)

    def test_an_array_that_is_no_field_still_has_to_decode(self):
        self.map_shaped(codecs={"valid_time": ("acme_wavelet",)})

        self.assertEqual(self.survey().shape, Shape.UNDECODABLE_STORE)

    # Refused: it cannot be read, or cannot be told.

    def test_a_store_that_does_not_open_is_refused_as_any_upstream_that_needs_an_account(self):
        self.map_shaped()
        self.upstream.store_unreadable = "it did not open for a reader with no account"

        found = self.survey()

        self.assertEqual((found.shape, found.accepted), (Shape.NEEDS_ACCOUNT, False))
        self.assertIn("may need an account", found.reason)
        self.assertIn("it did not open", found.reason)
        (location,) = found.locations
        self.assertFalse(location.readable)
        self.assertEqual(found.proposed_locations, [])

    def test_a_store_at_a_private_address_is_refused_and_never_opened(self):
        self.upstream.addresses[STORE_HOST] = ["10.0.0.7"]
        self.map_shaped()

        found = self.survey()

        self.assertEqual(found.shape, Shape.NEEDS_ACCOUNT)
        self.assertEqual(self.upstream.stores_opened, [])

    def test_a_store_published_over_plain_http_is_never_opened(self):
        self.upstream.become_store(
            STORE.replace("https://", "http://"), FORECAST, {"temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440))}
        )

        self.assertEqual(self.survey().shape, Shape.NEEDS_ACCOUNT)
        self.assertEqual(self.upstream.stores_opened, [])

    def test_a_store_published_only_by_s3_is_opened_at_the_public_host_of_its_bucket(self):
        self.upstream.regions["dynamical-noaa-gfs"] = "us-west-2"
        self.upstream.become_store(STORE_S3, FORECAST, {"temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440))})
        self.upstream.stores[f"{STORE}/"] = self.upstream.stores.pop(STORE_S3)

        found = self.survey()

        self.assertEqual(self.upstream.stores_opened, [f"{STORE}/"])
        self.assertEqual(found.shape, Shape.MAP_STORE)

    def test_a_store_whose_layout_the_collection_does_not_give_is_unreadable_and_says_what_is_missing(self):
        cases = {
            "how “temperature_2m” is divided up": (FORECAST, {"temperature_2m": (RUN_STEP_GRID, None)}),
            "how “wind_u_10m” is divided up": (
                FORECAST,
                {"temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440)), "wind_u_10m": (RUN_STEP_GRID, (1, 721, 1440))},
            ),
            "the size of its grid along “latitude”": (
                {**FORECAST, "latitude": None},
                {"temperature_2m": (RUN_STEP_GRID, (1, 1, 721, 1440))},
            ),
            "lists no fields on its grid": (FORECAST, dict(BESIDE)),
            "a grid with a width and a height": (
                {"init_time": None, "lead_time": 209, "station": 500},
                {"temperature_2m": (("init_time", "lead_time", "station"), (1, 1, 500))},
            ),
        }
        for missing, (dimensions, arrays) in cases.items():
            with self.subTest(missing=missing):
                self.upstream.stores_opened = []
                self.upstream.become_store(STORE, dimensions, arrays)

                found = self.survey()

                self.assertEqual((found.shape, found.accepted), (Shape.UNREADABLE_STORE, False))
                self.assertIn(missing, found.reason)
                self.assert_plain(found.reason)
                self.assertEqual(self.upstream.stores_opened, [])

    def test_fields_cut_into_tiles_within_one_step_are_neither_shape(self):
        self.upstream.become_store(STORE, FORECAST, {"temperature_2m": (RUN_STEP_GRID, (1, 1, 256, 256))})

        found = self.survey()

        self.assertEqual(found.shape, Shape.UNREADABLE_STORE)
        self.assertIn("small tiles", found.reason)

    def test_a_store_that_describes_no_cube_is_refused_as_before(self):
        self.map_shaped(described=False)

        found = self.survey()

        self.assertEqual((found.shape, found.accepted), (Shape.STORE, False))
        self.assertEqual(self.upstream.stores_opened, [])

    def test_no_reason_given_for_a_store_uses_its_makers_words(self):
        for shape in (Shape.STORE, Shape.SERIES_STORE, Shape.UNDECODABLE_STORE, Shape.UNREADABLE_STORE):
            with self.subTest(shape=shape):
                self.assertTrue(reason_for(shape))
                self.assert_plain(reason_for(shape))


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
