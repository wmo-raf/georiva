"""Reading an asset's values, wherever its bytes are (ADR 0046 §10, #739).

One read module answers *what values* an asset holds, beside the location seam
that answers *where* its bytes are. A local COG is read by range from our
assets bucket; a Remote COG by range from its href. The same bytes read either
way give the same answer, and every answer is north-up float32 with NaN for
nodata.
"""

import math
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from affine import Affine
from django.test import SimpleTestCase, TestCase
from rasterio.transform import from_origin

from georiva.core import values
from georiva.core.models import Asset, Item
from georiva.core.storage import Bucket, BucketType
from georiva.core.storage.asset_location import AssetLocation
from georiva.core.tests.test_asset_deletion import AssetsBucketTestCase
from georiva.harvest.testing import FakeUpstream
from georiva.organisations.testing import make_org_tree, make_organisation

KEY = "kenya/c/c/v/2026/03/01/rain.tif"
NODATA = -9999.0

#: A 40×40 grid of one-degree cells over lon -20..20, lat -20..20: each cell holds
#: ``row * 40 + col`` (row 0 north), with a nodata hole in the north-west corner.
SIZE = 40
GRID = np.arange(SIZE * SIZE, dtype="float32").reshape(SIZE, SIZE)
GRID[0:5, 0:5] = NODATA
EXPECTED = np.where(GRID == NODATA, np.nan, GRID).astype("float32")
NORTH_UP = from_origin(-20, 20, 1, 1)

#: Covers the centres of the cells in rows 20..21 and columns 30..31.
AREA = {
    "type": "Polygon",
    "coordinates": [[[10.0, -2.0], [12.0, -2.0], [12.0, 0.0], [10.0, 0.0], [10.0, -2.0]]],
}


def cog_bytes(*, south_up: bool = False) -> bytes:
    """A tiled float32 GeoTIFF of :data:`GRID`; stored south-up (row 0 south) if asked."""
    data, transform = GRID, NORTH_UP
    if south_up:
        data = np.flipud(GRID)
        transform = Affine(1, 0, -20, 0, 1, -20)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "grid.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=SIZE,
            height=SIZE,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
            nodata=NODATA,
            tiled=True,
            blockxsize=16,
            blockysize=16,
        ) as dst:
            dst.write(data, 1)
        return path.read_bytes()


def large_tiff_bytes() -> bytes:
    """A 1 MB tiled GeoTIFF over the same box: big enough that one window is a fraction of it."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "big.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=512,
            height=512,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(-20, 20, 40 / 512, 40 / 512),
            tiled=True,
            blockxsize=64,
            blockysize=64,
        ) as dst:
            dst.write(np.ones((512, 512), dtype="float32"), 1)
        return path.read_bytes()


class LocalAndRemote:
    """The same bytes stored as a local COG in our assets bucket and served as a Remote COG by an Upstream."""

    def store_local(self, test: AssetsBucketTestCase, body: bytes) -> AssetLocation:
        path = test.bucket.storage.path(KEY)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        Path(path).write_bytes(body)
        patcher = mock.patch("georiva.core.storage.asset_location.storage", mock.Mock(assets=test.bucket))
        patcher.start()
        test.addCleanup(patcher.stop)
        return AssetLocation(KEY)

    def serve_remote(self, test, body: bytes) -> AssetLocation:
        self.upstream = FakeUpstream()
        serving = self.upstream.serving()
        self.served = serving.__enter__()
        test.addCleanup(serving.__exit__, None, None, None)
        return AssetLocation(self.served.put("chirps/rain.tif", body))


class ReadingSessionTests(AssetsBucketTestCase):
    def setUp(self):
        super().setUp()
        self.both = LocalAndRemote()
        body = cog_bytes()
        self.local = self.both.store_local(self, body)
        self.remote = self.both.serve_remote(self, body)
        self.assertTrue(self.remote.is_remote)

    def each(self):
        return {"local": self.local, "remote": self.remote}

    def test_a_whole_field_is_north_up_float32_with_nan_for_nodata(self):
        for kind, asset in self.each().items():
            with self.subTest(kind), values.session() as s:
                field = s.field(asset)
                self.assertEqual(field.data.dtype, np.float32)
                np.testing.assert_array_equal(field.data, EXPECTED)
                self.assertEqual(field.transform, NORTH_UP)
                self.assertEqual(tuple(field.bounds), (-20.0, -20.0, 20.0, 20.0))
                self.assertEqual((field.width, field.height), (SIZE, SIZE))
                self.assertIn("4326", field.crs)

    def test_a_window_holds_the_cells_under_its_bounding_box(self):
        for kind, asset in self.each().items():
            with self.subTest(kind), values.session() as s:
                field = s.window(asset, (10.0, -2.0, 12.0, 0.0))
                np.testing.assert_array_equal(field.data, EXPECTED[20:22, 30:32])
                self.assertEqual(tuple(field.bounds), (10.0, -2.0, 12.0, 0.0))
                self.assertEqual(field.transform, from_origin(10, 0, 1, 1))

    def test_a_point_is_the_value_of_the_cell_containing_it(self):
        for kind, asset in self.each().items():
            with self.subTest(kind), values.session() as s:
                self.assertEqual(s.point(asset, 10.5, -0.5), 20 * 40 + 30)
                self.assertTrue(math.isnan(s.point(asset, -19.5, 19.5)))  # nodata
                self.assertTrue(math.isnan(s.point(asset, 50.0, 0.0)))  # outside

    def test_a_series_reads_each_asset_in_order(self):
        with values.session() as s:
            self.assertEqual(s.point_series([self.local, self.remote], 10.5, -0.5), [830.0, 830.0])
            means = s.area_series([self.local, self.remote], AREA, "mean")
        self.assertEqual(means, [float(EXPECTED[20:22, 30:32].mean())] * 2)

    def test_the_same_bytes_read_the_same_locally_and_remotely(self):
        with values.session() as s:
            np.testing.assert_array_equal(s.field(self.local).data, s.field(self.remote).data)
            np.testing.assert_array_equal(
                s.window(self.local, (-15.5, 3.2, 7.1, 9.9)).data,
                s.window(self.remote, (-15.5, 3.2, 7.1, 9.9)).data,
            )
            self.assertEqual(s.area(self.local, AREA, "max"), s.area(self.remote, AREA, "max"))

    def test_a_local_cog_is_read_by_range_not_whole(self):
        with mock.patch.object(Bucket, "read_bytes", side_effect=AssertionError("read whole")), values.session() as s:
            s.field(self.local)
            s.window(self.local, (10.0, -2.0, 12.0, 0.0))

    def test_a_remote_window_reads_only_some_of_the_file(self):
        big = self.both.served.put("chirps/big.tif", large_tiff_bytes())

        values.window(AssetLocation(big), (10.0, -2.0, 12.0, 0.0))

        ranges = self.both.served.ranges(big)
        self.assertTrue(ranges)
        self.assertLess(sum(last - first + 1 for first, last in ranges), len(self.both.upstream.files[big]))

    def test_one_call_helpers_answer_as_a_session_does(self):
        np.testing.assert_array_equal(values.field(self.remote).data, EXPECTED)
        np.testing.assert_array_equal(values.window(self.local, (10.0, -2.0, 12.0, 0.0)).data, EXPECTED[20:22, 30:32])
        self.assertEqual(values.point(self.local, 10.5, -0.5), 830.0)
        self.assertEqual(values.area(self.remote, AREA, "min"), float(EXPECTED[20:22, 30:32].min()))

    def test_a_session_reads_nothing_once_it_has_ended(self):
        with values.session() as s:
            pass
        with self.assertRaises(values.SessionClosed):
            s.field(self.local)


class SouthUpSourceTests(AssetsBucketTestCase):
    """A source stored row 0 south is read row 0 north, as every reader assumes."""

    def setUp(self):
        super().setUp()
        self.asset = LocalAndRemote().store_local(self, cog_bytes(south_up=True))

    def test_a_field_comes_back_north_up(self):
        field = values.field(self.asset)

        np.testing.assert_array_equal(field.data, EXPECTED)
        self.assertEqual(field.transform, NORTH_UP)
        self.assertEqual(tuple(field.bounds), (-20.0, -20.0, 20.0, 20.0))

    def test_a_window_comes_back_north_up(self):
        field = values.window(self.asset, (10.0, -2.0, 12.0, 0.0))

        np.testing.assert_array_equal(field.data, EXPECTED[20:22, 30:32])
        self.assertEqual(field.transform, from_origin(10, 0, 1, 1))

    def test_a_point_is_the_same_cell_either_way_up(self):
        self.assertEqual(values.point(self.asset, 10.5, -0.5), 830.0)


class WhatCanBeReadTests(AssetsBucketTestCase):
    def test_an_asset_row_is_read_where_its_location_says(self):
        location = LocalAndRemote().store_local(self, cog_bytes())
        tree = make_org_tree(make_organisation("kenya"))
        item = Item.objects.create(collection=tree["collection"], time=datetime(2026, 3, 2, tzinfo=UTC))
        asset = Asset.objects.create(item=item, variable=tree["variable"], href=location.href, format=Asset.Format.COG)

        np.testing.assert_array_equal(values.field(asset).data, EXPECTED)

    def test_an_object_in_another_bucket_is_read_by_its_key(self):
        staging = Bucket(BucketType.STAGING, "georiva-assets")  # the same temporary directory
        path = self.bucket.storage.path(KEY)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        Path(path).write_bytes(cog_bytes(south_up=True))

        with mock.patch("georiva.core.values.session.storage.bucket", return_value=staging) as bucket:
            field = values.field(values.Stored(BucketType.STAGING, KEY))

        bucket.assert_called_with(BucketType.STAGING)
        np.testing.assert_array_equal(field.data, EXPECTED)


class WhichAssetsHaveValuesTests(TestCase):
    """``values.readable`` is the one answer to which assets the read module gives values for (#759)."""

    def setUp(self):
        self.tree = make_org_tree(make_organisation("kenya"))
        self.days = iter(range(1, 29))

    def asset(self, href, fmt):
        when = datetime(2026, 3, next(self.days), tzinfo=UTC)
        item = Item.objects.create(collection=self.tree["collection"], time=when)
        return Asset.objects.create(item=item, variable=self.tree["variable"], href=href, format=fmt)

    def test_cogs_local_and_remote_and_store_assets_have_values(self):
        readable = {
            self.asset("kenya/c/c/v/2026/03/01/rain.tif", Asset.Format.COG),
            self.asset("https://data.upstream.example/chirps/rain.tif", Asset.Format.COG),
            self.asset("s3://upstream-bucket/aifs.icechunk", Asset.Format.ZARR),
        }
        for fmt in (Asset.Format.PNG, Asset.Format.WEBP, Asset.Format.JPEG):
            self.asset(f"kenya/c/c/v/2026/03/01/rain.{fmt}", fmt)
        self.asset("kenya/c/c/v/2026/03/01/rain.zarr", Asset.Format.ZARR)  # a local Zarr is no Store asset
        self.asset("kenya/c/c/v/2026/03/01/rain.nc", Asset.Format.NETCDF)
        self.asset("https://data.upstream.example/chirps/rain.grib2", Asset.Format.GRIB2)

        self.assertEqual(set(values.readable(Asset.objects.all())), readable)

    def test_it_narrows_the_queryset_it_is_given(self):
        cog = self.asset("kenya/c/c/v/2026/03/01/rain.tif", Asset.Format.COG)
        self.asset("kenya/c/c/v/2026/03/01/other.tif", Asset.Format.COG)

        self.assertEqual(list(values.readable(Asset.objects.filter(pk=cog.pk))), [cog])


class StoreAssetTests(SimpleTestCase):
    def test_a_store_asset_is_not_read_as_a_cog(self):
        store = AssetLocation("https://data.upstream.example/gfs.icechunk", Asset.Format.ZARR)

        with self.assertRaises(ValueError):
            values.field(store)
