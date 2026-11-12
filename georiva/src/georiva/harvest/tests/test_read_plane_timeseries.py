"""Point and area timeseries answer over Remote assets (ADR 0043, #638).

The upstream bytes are served locally: each remote href is given a small
GeoTIFF on disk, and the one seam that opens an asset
(``AssetLocation.open_raster``) opens that instead. Everything from the request
down to the range read is real.
"""

import shutil
import tempfile
from pathlib import Path
from unittest import mock

import numpy
import rasterio
import xarray
from django.test import TestCase, override_settings
from django.urls import reverse
from rasterio.transform import from_bounds

from georiva.core.models import Asset, Item
from georiva.core.storage.asset_location import AssetLocation
from georiva.virtual_zarr.models import VirtualZarrManifest

from .support import RemoteCollectionMixin, day, nothing_written, rain_href

#: A 10×10 grid of one-degree cells over lon 33..43, lat -5..5.
BOUNDS = (33.0, -5.0, 43.0, 5.0)
SIZE = 10
NODATA = -9999.0

#: Inside the cell in column 3: its value is the day's base + 3.
POINT = {"lat": 0.5, "lon": 36.5}

#: Covers the centres of columns 1 and 2, rows either side of the equator.
AREA = {
    "type": "Polygon",
    "coordinates": [[[34.0, -1.0], [36.0, -1.0], [36.0, 1.0], [34.0, 1.0], [34.0, -1.0]]],
}


def grid(base: float) -> numpy.ndarray:
    """Every cell holds ``base`` plus its column index."""
    return (base + numpy.tile(numpy.arange(SIZE, dtype="float32"), (SIZE, 1))).astype("float32")


class UpstreamBytes:
    """The Upstream's objects, on local disk, and a record of which were opened."""

    def __init__(self, test):
        self.root = Path(tempfile.mkdtemp(prefix="georiva-upstream-"))
        test.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.files: dict[str, Path] = {}
        self.opened: list[str] = []
        self.unreadable: set[str] = set()
        upstream = self

        def open_raster(location):
            if not location.is_remote:
                raise AssertionError(f"a local asset was opened through the remote read: {location.href}")
            upstream.opened.append(location.href)
            if location.href in upstream.unreadable:
                raise rasterio.errors.RasterioIOError("HTTP response code: 503")
            return rasterio.open(upstream.files[location.href])

        patcher = mock.patch.object(AssetLocation, "open_raster", open_raster)
        patcher.start()
        test.addCleanup(patcher.stop)

    def put(self, href: str, data: numpy.ndarray) -> None:
        path = self.root / f"{len(self.files)}.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=SIZE,
            height=SIZE,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            nodata=NODATA,
            transform=from_bounds(*BOUNDS, SIZE, SIZE),
        ) as dst:
            dst.write(data, 1)
        self.files[href] = path


class RemoteTimeseriesMixin(RemoteCollectionMixin):
    def setUp(self):
        super().setUp()
        self.upstream_bytes = UpstreamBytes(self)
        self.upstream_bytes.put(rain_href(1), grid(10.0))
        self.upstream_bytes.put(rain_href(2), grid(20.0))

    def point(self, **params):
        return self.client.get(
            reverse("timeseries:point"),
            {"variable": "chirps/chirps-daily/rain", **POINT, **params},
        )

    def area(self, **body):
        return self.client.post(
            reverse("timeseries:area"),
            {"variable": "chirps/chirps-daily/rain", "geometry": AREA, **body},
            content_type="application/json",
        )

    def values(self, response):
        self.assertEqual(response.status_code, 200, response.content)
        return [(row["time"][:10], row["value"]) for row in response.json()["data"]]


class PointTimeseriesOverRemoteAssetsTests(RemoteTimeseriesMixin, TestCase):
    def test_it_answers_with_the_value_under_the_point_at_each_remote_timestep(self):
        self.assertEqual(self.values(self.point()), [("2026-03-01", 13.0), ("2026-03-02", 23.0)])

    def test_a_time_range_reads_only_the_assets_inside_it(self):
        response = self.point(time_start="2026-03-02T00:00:00Z", time_end="2026-03-05T00:00:00Z")

        self.assertEqual(self.values(response), [("2026-03-02", 23.0)])
        self.assertEqual(self.upstream_bytes.opened, [rain_href(2)])

    def test_a_point_on_a_nodata_cell_has_no_value_at_that_timestep(self):
        holed = grid(10.0)
        holed[:, 3] = NODATA
        self.upstream_bytes.put(rain_href(1), holed)

        self.assertEqual(self.values(self.point()), [("2026-03-02", 23.0)])

    def test_a_range_holding_no_timestep_answers_empty(self):
        response = self.point(time_start="2027-01-01T00:00:00Z", time_end="2027-02-01T00:00:00Z")

        self.assertEqual(self.values(response), [])

    def test_a_point_outside_the_grid_has_no_values(self):
        self.assertEqual(self.values(self.point(lat=40.0, lon=10.0)), [])

    def test_answering_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.point()

    def test_an_upstream_that_cannot_be_read_fails_the_request_and_touches_no_row(self):
        self.upstream_bytes.unreadable.add(rain_href(2))
        before = list(Asset.objects.order_by("pk").values_list("pk", "href", "modified"))

        with nothing_written(self):
            response = self.point()

        self.assertEqual(response.status_code, 500)
        self.assertEqual(list(Asset.objects.order_by("pk").values_list("pk", "href", "modified")), before)

    @override_settings(GEORIVA_TIMESERIES_MAX_REMOTE_ASSETS=1)
    def test_a_range_over_more_remote_assets_than_one_request_may_read_is_refused(self):
        response = self.point()

        self.assertEqual(response.status_code, 400)
        self.assertIn("time range", response.json()["detail"])
        self.assertEqual(self.upstream_bytes.opened, [])


class AreaTimeseriesOverRemoteAssetsTests(RemoteTimeseriesMixin, TestCase):
    def test_it_aggregates_the_cells_inside_the_polygon_at_each_remote_timestep(self):
        self.assertEqual(self.values(self.area()), [("2026-03-01", 11.5), ("2026-03-02", 21.5)])

    def test_it_honours_the_aggregation_asked_for(self):
        self.assertEqual(
            self.values(self.area(aggregation="max")),
            [("2026-03-01", 12.0), ("2026-03-02", 22.0)],
        )

    def test_a_polygon_outside_the_grid_has_no_values(self):
        elsewhere = {
            "type": "Polygon",
            "coordinates": [[[0.0, 40.0], [2.0, 40.0], [2.0, 42.0], [0.0, 42.0], [0.0, 40.0]]],
        }

        self.assertEqual(self.values(self.area(geometry=elsewhere)), [])

    def test_answering_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.area()

    @override_settings(GEORIVA_TIMESERIES_MAX_REMOTE_ASSETS=1)
    def test_a_range_over_more_remote_assets_than_one_request_may_read_is_refused(self):
        self.assertEqual(self.area().status_code, 400)


class TimeseriesOverAMixedCollectionTests(RemoteTimeseriesMixin, TestCase):
    """Day 3 is local and read from the Variable's virtual store, which covers
    local assets only; days 1 and 2 are Remote. One answer, in time order."""

    def setUp(self):
        super().setUp()
        self.add_local_day(3)
        VirtualZarrManifest.objects.filter(variable=self.variable).update(status=VirtualZarrManifest.Status.READY)

        lons = numpy.arange(BOUNDS[0] + 0.5, BOUNDS[2])
        lats = numpy.arange(BOUNDS[3] - 0.5, BOUNDS[1], -1.0)
        store = xarray.Dataset(
            {"rain": (("time", "lat", "lon"), grid(30.0)[numpy.newaxis])},
            coords={"time": [numpy.datetime64("2026-03-03T00:00:00")], "lat": lats, "lon": lons},
        )

        def open_dataset(manifest, **kwargs):
            if manifest.status != VirtualZarrManifest.Status.READY:
                raise ValueError("the virtual store is not ready")
            return store

        patcher = mock.patch.object(VirtualZarrManifest, "open_dataset", open_dataset)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_point_series_joins_the_local_window_and_the_remote_archive(self):
        self.assertEqual(
            self.values(self.point()),
            [("2026-03-01", 13.0), ("2026-03-02", 23.0), ("2026-03-03", 33.0)],
        )

    def test_an_area_series_joins_the_local_window_and_the_remote_archive(self):
        self.assertEqual(
            self.values(self.area()),
            [("2026-03-01", 11.5), ("2026-03-02", 21.5), ("2026-03-03", 31.5)],
        )

    def test_a_range_inside_the_local_window_reads_nothing_upstream(self):
        response = self.point(time_start="2026-03-03T00:00:00Z", time_end="2026-03-04T00:00:00Z")

        self.assertEqual(self.values(response), [("2026-03-03", 33.0)])
        self.assertEqual(self.upstream_bytes.opened, [])

    def test_a_range_inside_the_remote_archive_does_not_need_the_virtual_store(self):
        Item.objects.filter(time=day(3)).delete()
        VirtualZarrManifest.objects.filter(variable=self.variable).update(status=VirtualZarrManifest.Status.STALE)

        self.assertEqual(self.values(self.point()), [("2026-03-01", 13.0), ("2026-03-02", 23.0)])
