"""
Opening one file to see what it is: the read behind the Upstream survey's
Sample item (ADR 0042 §2).

The files here are written to a temporary directory and opened from it, the
way a remote one is opened by range. Nothing reaches a network.
"""

import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from django.test import SimpleTestCase
from rasterio.enums import Resampling
from rasterio.transform import from_origin

from georiva.harvest import upstream
from georiva.harvest.upstream import StacApiClient, parse_asset, parse_collection, parse_root, probe_raster


class ProbeRasterTests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

    def write(self, name, *, cog, values=None, nodata=-9999.0, size=512):
        data = np.linspace(5.0, 250.0, size * size, dtype="float32").reshape(size, size) if values is None else values
        path = self.folder / name
        layout = {"tiled": True, "blockxsize": 256, "blockysize": 256} if cog else {}
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(-20, 40, 0.05, 0.05),
            nodata=nodata,
            **layout,
        ) as dst:
            dst.write(data, 1)
            if cog:
                dst.build_overviews([2, 4], Resampling.nearest)
        return str(path)

    def test_a_cog_is_readable_tiled_with_overviews_and_a_range(self):
        probe = probe_raster(self.write("cog.tif", cog=True), estimate_range=True)

        self.assertTrue(probe.readable)
        self.assertEqual((probe.tiled, probe.overviews, probe.crs), (True, True, "EPSG:4326"))
        self.assertAlmostEqual(probe.value_min, 5.0, delta=5)
        self.assertAlmostEqual(probe.value_max, 250.0, delta=5)

    def test_no_pixel_is_read_unless_a_range_is_asked_for(self):
        probe = probe_raster(self.write("cog.tif", cog=True), estimate_range=False)

        self.assertTrue(probe.readable)
        self.assertEqual((probe.value_min, probe.value_max), (None, None))

    def test_nodata_and_values_that_are_not_numbers_stay_out_of_the_range(self):
        values = np.full((512, 512), 40.0, dtype="float32")
        values[:100] = -9999.0
        values[100:200] = np.nan
        values[300:] = 90.0

        probe = probe_raster(self.write("holes.tif", cog=True, values=values), estimate_range=True)

        self.assertEqual((probe.value_min, probe.value_max), (40.0, 90.0))

    def test_a_file_that_is_all_nodata_has_no_range(self):
        values = np.full((512, 512), -9999.0, dtype="float32")

        probe = probe_raster(self.write("empty.tif", cog=True, values=values), estimate_range=True)

        self.assertTrue(probe.readable)
        self.assertEqual((probe.value_min, probe.value_max), (None, None))

    def test_a_plain_geotiff_is_readable_and_flagged(self):
        probe = probe_raster(self.write("plain.tif", cog=False), estimate_range=True)

        self.assertTrue(probe.readable)
        self.assertEqual((probe.tiled, probe.overviews), (False, False))

    def test_a_large_file_with_no_overviews_is_not_read_whole_for_its_range(self):
        path = self.write("plain.tif", cog=False)
        self.enterContext(mock.patch.object(upstream, "PROBE_MAX_POINTS", 1000))

        probe = probe_raster(path, estimate_range=True)

        self.assertTrue(probe.readable)
        self.assertEqual((probe.value_min, probe.value_max), (None, None))

    def test_a_file_that_cannot_be_opened_is_an_answer_not_an_error(self):
        missing = probe_raster(str(self.folder / "missing.tif"), estimate_range=True)
        (self.folder / "junk.tif").write_text("not a raster")
        junk = probe_raster(str(self.folder / "junk.tif"), estimate_range=True)

        for probe in (missing, junk):
            self.assertFalse(probe.readable)
            self.assertTrue(probe.problem)


class ParseForTheSurveyTests(SimpleTestCase):
    def test_an_asset_is_read_with_its_title_and_unit(self):
        asset = parse_asset(
            {
                "href": "https://d.example/a.tif",
                "title": "Monthly rainfall",
                "raster:bands": [{"unit": "mm", "nodata": -9999}],
            }
        )

        self.assertEqual((asset.title, asset.unit, asset.nodata), ("Monthly rainfall", "mm", -9999.0))

    def test_a_collection_is_read_with_the_assets_it_carries_itself(self):
        collection = parse_collection(
            {"id": "era5", "assets": {"zarr": {"href": "https://d.example/era5.zarr", "type": "application/vnd+zarr"}}}
        )

        self.assertEqual(collection.assets["zarr"].media_type, "application/vnd+zarr")

    def test_a_root_that_conforms_or_links_to_its_search_is_an_api(self):
        self.assertTrue(parse_root({"conformsTo": ["https://api.stacspec.org/v1.0.0/core"]}).is_api)
        self.assertTrue(parse_root({"links": [{"rel": "search", "href": "/search"}]}).is_api)
        self.assertTrue(parse_root({"links": [{"rel": "data", "href": "/collections"}]}).is_api)

    def test_a_root_of_plain_files_is_not(self):
        root = parse_root({"id": "c", "links": [{"rel": "child", "href": "./2020/catalog.json"}]})

        self.assertFalse(root.is_api)
        self.assertEqual([c.id for c in root.children], ["2020"])
        self.assertIsNone(root.own)

    def test_a_catalog_of_plain_files_names_its_collections_by_folder_or_file(self):
        root = parse_root(
            {
                "type": "Catalog",
                "links": [
                    {"rel": "child", "href": "https://s.example/noaa-gfs-forecast/collection.json", "title": "GFS"},
                    {"rel": "child", "href": "./era5.json"},
                    {"rel": "root", "href": "https://s.example/catalog.json"},
                ],
            },
            base_url="https://s.example/catalog.json",
        )

        self.assertEqual([(c.id, c.title) for c in root.children], [("noaa-gfs-forecast", "GFS"), ("era5", "")])
        self.assertEqual(root.child_hrefs["era5"], "https://s.example/era5.json")

    def test_an_address_that_is_a_collections_own_document_is_that_collection(self):
        root = parse_root(
            {
                "type": "Collection",
                "id": "noaa-gfs-forecast",
                "assets": {"icechunk": {"href": "https://b.example/gfs.icechunk", "type": "application/x-icechunk"}},
                "links": [{"rel": "root", "href": "https://s.example/catalog.json"}],
            }
        )

        self.assertFalse(root.is_api)
        self.assertEqual(root.own.id, "noaa-gfs-forecast")
        self.assertEqual(root.own.assets["icechunk"].media_type, "application/x-icechunk")

    def test_a_roots_links_to_its_collections_name_them(self):
        root = parse_root(
            {
                "conformsTo": ["x"],
                "links": [
                    {"rel": "child", "href": "/stac/collections/rainfall_chirps_monthly", "title": "CHIRPS monthly"},
                    {"rel": "child", "href": "https://s.example/stac/collections/s2%20l2a/"},
                    {"rel": "child", "href": "/stac/catalogs/arrivals", "title": "Arrivals"},
                    {"rel": "self", "href": "/stac/collections/not-a-child"},
                ],
            },
            base_url="https://s.example/stac",
        )

        self.assertEqual(
            [(c.id, c.title) for c in root.children], [("rainfall_chirps_monthly", "CHIRPS monthly"), ("s2 l2a", "")]
        )


class _Pages:
    """Answers each URL from a table, and remembers what it was asked."""

    def __init__(self, pages):
        self.pages = pages
        self.asked = []

    def get(self, url, **kwargs):
        self.asked.append(url)
        return _Page(self.pages.get(url))


class _Page:
    headers = {}

    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200 if payload is not None else 404

    def json(self):
        return self.payload


class DescribeCollectionTests(SimpleTestCase):
    STATIC = "https://s.example/catalog.json"

    def describe(self, url, pages, collection_id):
        self.session = _Pages(pages)
        return StacApiClient(url, session=self.session).describe_collection(collection_id)

    def test_an_api_is_asked_for_the_collection_by_its_id(self):
        api = "https://s.example/api"
        found = self.describe(
            api,
            {api: {"conformsTo": ["x"]}, f"{api}/collections/s2%20l2a": {"id": "s2 l2a", "title": "Sentinel-2"}},
            "s2 l2a",
        )

        self.assertEqual((found.id, found.title), ("s2 l2a", "Sentinel-2"))

    def test_a_catalog_of_plain_files_is_followed_to_the_collections_own_document(self):
        child = "https://s.example/noaa-gfs-forecast/collection.json"
        found = self.describe(
            self.STATIC,
            {
                self.STATIC: {"links": [{"rel": "child", "href": child}]},
                child: {"id": "noaa-gfs-forecast", "assets": {"icechunk": {"href": "https://b.example/gfs.icechunk"}}},
            },
            "noaa-gfs-forecast",
        )

        self.assertIn("icechunk", found.assets)

    def test_a_child_on_another_host_is_never_followed(self):
        found = self.describe(
            self.STATIC,
            {self.STATIC: {"links": [{"rel": "child", "href": "http://169.254.169.254/gfs/collection.json"}]}},
            "gfs",
        )

        self.assertIsNone(found)
        self.assertEqual(self.session.asked, [self.STATIC])

    def test_an_address_that_is_the_collection_describes_it_without_another_request(self):
        address = "https://s.example/noaa-gfs-forecast/collection.json"
        pages = {address: {"type": "Collection", "id": "noaa-gfs-forecast"}}
        found = self.describe(address, pages, "noaa-gfs-forecast")

        self.assertEqual(found.id, "noaa-gfs-forecast")
        self.assertEqual(self.session.asked, [address])

    def test_a_collection_the_upstream_will_not_describe_has_no_description(self):
        api = "https://s.example/api"

        self.assertIsNone(self.describe(api, {api: {"conformsTo": ["x"]}}, "chirps"))
        self.assertIsNone(self.describe(self.STATIC, {self.STATIC: {"links": []}}, "chirps"))


class _Answer:
    def __init__(self, headers=None, status_code=400):
        self.headers = headers or {}
        self.status_code = status_code


class _S3:
    def __init__(self, answer):
        self.answer = answer
        self.asked = []

    def get(self, url, **kwargs):
        self.asked.append((url, kwargs))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class BucketRegionTests(SimpleTestCase):
    def region(self, bucket, answer):
        self.s3 = _S3(answer)
        return StacApiClient("https://stac.upstream.example/api", session=self.s3).bucket_region(bucket)

    def test_s3_names_the_region_even_on_an_answer_that_refuses(self):
        region = self.region("deafrica-input-datasets", _Answer({"x-amz-bucket-region": "af-south-1"}))

        self.assertEqual(region, "af-south-1")
        url, kwargs = self.s3.asked[0]
        self.assertEqual(url, "https://deafrica-input-datasets.s3.amazonaws.com/")
        self.assertIs(kwargs["allow_redirects"], False)

    def test_a_bucket_s3_says_nothing_of_has_no_region(self):
        self.assertIsNone(self.region("no-such-bucket", _Answer(status_code=404)))

    def test_s3_not_answering_is_no_region_and_not_an_error(self):
        import requests

        self.assertIsNone(self.region("deafrica-input-datasets", requests.ConnectionError("refused")))

    def test_only_a_bare_bucket_name_is_ever_put_into_an_address(self):
        for bucket in ("evil.example.com", "x@169.254.169.254", "a/b", "UPPER", "", "-lead", "a" * 64):
            with self.subTest(bucket=bucket):
                self.assertIsNone(self.region(bucket, _Answer({"x-amz-bucket-region": "us-east-1"})))
                self.assertEqual(self.s3.asked, [])
