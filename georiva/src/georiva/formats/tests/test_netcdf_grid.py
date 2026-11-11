"""The NetCDF handler places only grids it can place: regular lat-lon.

A curvilinear grid (2D lat/lon) or a 1D axis whose spacing varies (e.g. WRF on
Mercator) cannot be described by bounds plus one resolution, so the handler
refuses it with a reason instead of stretching it into a box (#543).
"""

import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr
from django.test import SimpleTestCase

from georiva.formats.base import UnsupportedGridError
from georiva.formats.netcdf import NetCDFFormatPlugin
from georiva.formats.tests.ingest import ingest_file

TIME = datetime(2026, 1, 1)


class NetCDFGridTests(SimpleTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.plugin = NetCDFFormatPlugin()

    def _write(self, ds: xr.Dataset) -> Path:
        path = Path(self._tmp.name) / "grid.nc"
        ds.to_netcdf(path, engine="netcdf4")
        return path

    def _rectilinear(self, lats, lons) -> Path:
        data = np.zeros((1, len(lats), len(lons)), dtype=np.float32)
        ds = xr.Dataset(
            {"t2m": (("time", "lat", "lon"), data)},
            coords={"time": [np.datetime64(TIME)], "lat": lats, "lon": lons},
        )
        return self._write(ds)

    def _metadata(self, path: Path) -> dict:
        return self.plugin.get_metadata_for_variable(path, "t2m", timestamp=TIME)

    # ── accepted ──────────────────────────────────────────────────────────

    def test_regular_grid_is_placed(self):
        path = self._rectilinear(np.arange(-10, 10.01, 0.5), np.arange(20, 40.01, 0.5))

        meta = self._metadata(path)

        self.assertEqual(meta["bounds"], (19.75, -10.25, 40.25, 10.25))

    def test_float32_jitter_in_coordinates_still_passes(self):
        lats = np.linspace(-35.0, 15.0, 557).astype(np.float32)
        lons = np.linspace(-20.0, 55.0, 834).astype(np.float32)
        rng = np.random.default_rng(0)
        lats = lats + rng.normal(0, 1e-5, lats.shape).astype(np.float32)
        lons = lons + rng.normal(0, 1e-5, lons.shape).astype(np.float32)

        meta = self._metadata(self._rectilinear(lats, lons))

        self.assertEqual((meta["width"], meta["height"]), (834, 557))

    def test_descending_latitudes_still_pass(self):
        path = self._rectilinear(np.arange(10, -10.01, -0.5), np.arange(20, 40.01, 0.5))

        meta = self._metadata(path)

        self.assertEqual(meta["bounds"], (19.75, -10.25, 40.25, 10.25))

    # ── refused ───────────────────────────────────────────────────────────

    def test_curvilinear_lat_lon_is_refused(self):
        y, x = np.arange(20), np.arange(30)
        xx, yy = np.meshgrid(x, y)
        # A Lambert-ish grid: rows bend, so lat and lon both depend on x and y.
        lat = -5.0 + 0.1 * yy + 0.002 * (xx - 15) ** 2
        lon = 30.0 + 0.1 * xx + 0.01 * yy
        data = np.zeros((1, 20, 30), dtype=np.float32)
        ds = xr.Dataset(
            {"t2m": (("time", "y", "x"), data)},
            coords={
                "time": [np.datetime64(TIME)],
                "lat": (("y", "x"), lat),
                "lon": (("y", "x"), lon),
            },
        )

        with self.assertRaisesRegex(UnsupportedGridError, "curvilinear"):
            self._metadata(self._write(ds))

    def test_mercator_latitudes_are_refused(self):
        # Rows evenly spaced in Mercator y are unevenly spaced in latitude.
        cell_m, r = 9000.0, 6371000.0
        y0 = r * np.log(np.tan(np.pi / 4 + np.radians(-35.0) / 2))
        ys = y0 + cell_m * np.arange(200)
        lats = np.degrees(2 * np.arctan(np.exp(ys / r)) - np.pi / 2)
        lons = np.arange(20, 40, 0.08)

        with self.assertRaisesRegex(UnsupportedGridError, "lat.*not evenly spaced"):
            self._metadata(self._rectilinear(lats, lons))

    def test_unevenly_spaced_longitudes_are_refused(self):
        lons = np.concatenate([np.arange(20, 30, 0.5), np.arange(30, 40.01, 1.0)])

        with self.assertRaisesRegex(UnsupportedGridError, "lon.*not evenly spaced"):
            self._metadata(self._rectilinear(np.arange(-10, 10.01, 0.5), lons))

    def test_missing_coordinate_values_are_refused(self):
        lats = np.arange(-10, 10.01, 0.5)
        lats[-1] = np.nan

        with self.assertRaisesRegex(UnsupportedGridError, "lat.*missing"):
            self._metadata(self._rectilinear(lats, np.arange(20, 40.01, 0.5)))

    def test_wrf_coordinates_are_found_by_their_units_and_refused(self):
        # Raw WRF names its 2D coordinates XLAT/XLONG on south_north/west_east.
        xx, yy = np.meshgrid(np.arange(30), np.arange(20))
        data = np.zeros((1, 20, 30), dtype=np.float32)
        ds = xr.Dataset(
            {"t2m": (("time", "south_north", "west_east"), data)},
            coords={
                "time": [np.datetime64(TIME)],
                "XLAT": (("south_north", "west_east"), -5.0 + 0.1 * yy, {"units": "degree_north"}),
                "XLONG": (("south_north", "west_east"), 30.0 + 0.1 * xx, {"units": "degree_east"}),
            },
        )

        with self.assertRaisesRegex(UnsupportedGridError, "curvilinear"):
            self._metadata(self._write(ds))

    def test_a_grid_without_latitude_and_longitude_is_refused(self):
        data = np.zeros((1, 20, 30), dtype=np.float32)
        ds = xr.Dataset({"t2m": (("time", "rows", "cols"), data)}, coords={"time": [np.datetime64(TIME)]})

        with self.assertRaisesRegex(UnsupportedGridError, "no latitude/longitude"):
            self._metadata(self._write(ds))

    def test_metre_axes_without_a_declared_crs_are_refused(self):
        data = np.zeros((1, 20, 30), dtype=np.float32)
        ds = xr.Dataset(
            {"t2m": (("time", "y", "x"), data)},
            coords={
                "time": [np.datetime64(TIME)],
                "y": 1_000_000.0 + 9000.0 * np.arange(20),
                "x": 3_000_000.0 + 9000.0 * np.arange(30),
            },
        )

        with self.assertRaisesRegex(UnsupportedGridError, "not in degrees"):
            self._metadata(self._write(ds))

    def test_projected_axes_are_preferred_over_2d_auxiliary_lat_lon(self):
        xx, yy = np.meshgrid(np.arange(30), np.arange(20))
        data = np.zeros((1, 20, 30), dtype=np.float32)
        ds = xr.Dataset(
            {
                "t2m": (("time", "y", "x"), data),
                "spatial_ref": ((), 0, {"crs_wkt": "EPSG:3857"}),
            },
            coords={
                "time": [np.datetime64(TIME)],
                "y": 1000.0 * np.arange(20),
                "x": 1000.0 * np.arange(30),
                "lat": (("y", "x"), 0.01 * yy),
                "lon": (("y", "x"), 0.01 * xx),
            },
        )

        meta = self._metadata(self._write(ds))

        self.assertEqual(meta["bounds"], (-500.0, -500.0, 29500.0, 19500.0))
        self.assertEqual(meta["crs"], "EPSG:3857")

    # ── 0..360 longitudes ─────────────────────────────────────────────────

    def test_global_0_to_360_grid_is_recentred_on_greenwich(self):
        lons = np.arange(0, 360, 1.0)
        data = np.broadcast_to(lons, (1, 5, 360)).astype(np.float32)
        ds = xr.Dataset(
            {"t2m": (("time", "lat", "lon"), data)},
            coords={"time": [np.datetime64(TIME)], "lat": np.arange(-2, 3, 1.0), "lon": lons},
        )

        extracted = self.plugin.extract_variable(self._write(ds), "t2m", timestamp=TIME)

        # Columns run west to east from -179 (stored as 181) to 180.
        self.assertEqual(extracted.bounds[0], -179.5)
        self.assertEqual(extracted.data[0, 0], 181.0)
        self.assertEqual(extracted.data[0, -1], 180.0)

    def test_regional_grid_east_of_180_is_shifted_west(self):
        path = self._rectilinear(np.arange(-10, 10.01, 0.5), np.arange(190, 250.01, 0.5))

        meta = self._metadata(path)

        self.assertEqual(meta["bounds"], (-170.25, -10.25, -109.75, 10.25))

    def test_regional_grid_crossing_the_antimeridian_is_refused(self):
        path = self._rectilinear(np.arange(-10, 10.01, 0.5), np.arange(170, 200.01, 0.5))

        with self.assertRaisesRegex(UnsupportedGridError, "antimeridian"):
            self._metadata(path)

    def test_refusal_reaches_the_ingestion_failure_record(self):
        lats = np.concatenate([np.arange(-10, 0, 0.5), np.arange(0, 10.01, 1.0)])
        path = self._rectilinear(lats, np.arange(20, 40.01, 0.5))
        result = ingest_file(path, self.plugin, "t2m")

        self.assertFalse(result.success)
        self.assertTrue(
            any("not evenly spaced" in e for e in result.errors),
            result.errors,
        )
