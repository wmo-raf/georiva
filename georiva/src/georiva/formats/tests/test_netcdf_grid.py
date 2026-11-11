"""The NetCDF handler places only grids it can place: regular lat-lon.

A curvilinear grid (2D lat/lon) or a 1D axis whose spacing varies (e.g. WRF on
Mercator) cannot be described by bounds plus one resolution, so the handler
refuses it with a reason instead of stretching it into a box (#543).
"""

import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import xarray as xr
from django.test import SimpleTestCase

from georiva.formats.base import UnsupportedGridError
from georiva.formats.netcdf import NetCDFFormatPlugin
from georiva.ingestion.service import IngestionService

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

    def test_global_0_to_360_longitudes_still_pass(self):
        path = self._rectilinear(np.arange(-90, 90.01, 1.0), np.arange(0, 360, 1.0))

        meta = self._metadata(path)

        self.assertEqual(meta["width"], 360)

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

    def test_refusal_reaches_the_ingestion_failure_record(self):
        lats = np.concatenate([np.arange(-10, 0, 0.5), np.arange(0, 10.01, 1.0)])
        path = self._rectilinear(lats, np.arange(20, 40.01, 0.5))
        result = _ingest(path, self.plugin)

        self.assertFalse(result.success)
        self.assertTrue(
            any("not evenly spaced" in e for e in result.errors),
            result.errors,
        )


def _ingest(path: Path, plugin: NetCDFFormatPlugin):
    """Run one file through IngestionService with the real plugin reading *path*.

    The real IngestionHandler and VariableExtractor run; only what surrounds
    them (catalog and collection lookup, storage) is stubbed. The point is that
    the plugin's refusal travels out of process_timestamp into the result the
    failure record is built from.
    """
    primary = SimpleNamespace(block_type="primary", value={"source_name": "t2m"})
    variable = SimpleNamespace(slug="t2m", is_active=True, sources=[primary])
    collection = MagicMock(slug="t2m-collection")
    collection.variables.all.return_value = [variable]
    catalog = MagicMock(clip_mode="none")

    sfm = MagicMock()
    sfm.download_to_temp.return_value.__enter__.return_value = path
    with (
        patch("georiva.ingestion.service.format_registry") as registry,
        patch("georiva.ingestion.service.storage"),
        patch("georiva.ingestion.service.resolve_org_catalog", return_value=(catalog, None)),
        patch("georiva.ingestion.service.FileIngestion"),
        patch.object(IngestionService, "_resolve_collections", return_value=[collection]),
        patch.object(IngestionService, "_get_first_variable_name", return_value="t2m"),
    ):
        registry.get.return_value = plugin
        service = IngestionService()
        service._source_file_manager = sfm
        return service.process_file("org/cat/t2m-collection/2026/01/01/grid.nc")
