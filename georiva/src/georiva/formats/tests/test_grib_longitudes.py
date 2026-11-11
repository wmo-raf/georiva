"""The GRIB handler lays 0..360 longitudes out west to east (#616).

Wrapping only the coordinate values left the columns in their stored order: a
global grid starting at Greenwich was placed 180 degrees off, and a regional
grid straddling 180 was stretched across the globe. Each cell of these fixtures
holds its own longitude, so a test can read where a column really landed.
"""

import tempfile
from pathlib import Path

import eccodes
import numpy as np
from django.test import SimpleTestCase

from georiva.formats.base import UnsupportedGridError
from georiva.formats.grib import GRIBFormatPlugin
from georiva.formats.tests.ingest import ingest_file


class GRIBLongitudeTests(SimpleTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.plugin = GRIBFormatPlugin()

    def _grib(self, first_lon: float, last_lon: float, step: float = 1.0) -> Path:
        """A regular lat-lon GRIB2 over 2N..2S whose values are each cell's longitude."""
        span = (last_lon - first_lon) % 360 or (360 if last_lon != first_lon else 0)
        ni = int(round(span / step)) + 1
        lons = first_lon + step * np.arange(ni)
        lons = np.where(lons >= 360, lons - 360, lons)
        lons = np.where(lons > 180, lons - 360, lons) if first_lon > last_lon else lons
        values = np.broadcast_to(lons, (5, ni)).astype(np.float64)
        handle = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
        try:
            for key, value in {
                "Ni": ni,
                "Nj": 5,
                "latitudeOfFirstGridPointInDegrees": 2.0,
                "latitudeOfLastGridPointInDegrees": -2.0,
                "jDirectionIncrementInDegrees": 1.0,
                "longitudeOfFirstGridPointInDegrees": first_lon,
                "longitudeOfLastGridPointInDegrees": last_lon,
                "iDirectionIncrementInDegrees": step,
                "shortName": "2t",
            }.items():
                eccodes.codes_set(handle, key, value)
            eccodes.codes_set_values(handle, values.ravel())
            path = Path(self._tmp.name) / f"{first_lon:g}_{last_lon:g}.grib2"
            with open(path, "wb") as f:
                eccodes.codes_write(handle, f)
        finally:
            eccodes.codes_release(handle)
        return path

    def _extract(self, path: Path):
        try:
            return self.plugin.extract_variable(path, "2t")
        finally:
            self.plugin.clear_cache()

    def test_global_0_to_360_grid_is_recentred_on_greenwich(self):
        extracted = self._extract(self._grib(0, 359))

        self.assertEqual(extracted.bounds, (-179.5, -2.5, 180.5, 2.5))
        # Columns run west to east from -179 (stored as 181) to 180.
        self.assertEqual(extracted.data[0, 0], 181.0)
        self.assertEqual(extracted.data[0, -1], 180.0)

    def test_regional_grid_east_of_180_keeps_its_columns_and_shifts_west(self):
        extracted = self._extract(self._grib(190, 250))

        self.assertEqual(extracted.bounds, (-170.5, -2.5, -109.5, 2.5))
        self.assertEqual(extracted.data[0, 0], 190.0)

    def test_grid_within_minus_180_to_180_is_unchanged(self):
        extracted = self._extract(self._grib(20, 40))

        self.assertEqual(extracted.bounds, (19.5, -2.5, 40.5, 2.5))
        self.assertEqual(extracted.data[0, 0], 20.0)

    def test_windowed_read_of_a_global_grid_cuts_from_the_recentred_columns(self):
        try:
            with self.plugin.open_variable(self._grib(0, 359), "2t", window=(0, 0, 10, 5)) as info:
                first_column = float(info.compute()[0, 0])
                west = info.bounds[0]
        finally:
            self.plugin.clear_cache()

        self.assertEqual((west, first_column), (-179.5, 181.0))

    def test_regional_grid_starting_at_180_shifts_west_whole(self):
        extracted = self._extract(self._grib(180, 250))

        self.assertEqual(extracted.bounds, (-180.5, -2.5, -109.5, 2.5))
        self.assertEqual(extracted.data[0, 0], 180.0)
        self.assertEqual(extracted.data[0, -1], 250.0)

    def test_global_grid_repeating_its_seam_column_drops_the_repeat(self):
        # 0..360 inclusive: the 360 column is the 0 column again.
        extracted = self._extract(self._grib(0, 360))

        self.assertEqual(extracted.width, 360)
        self.assertEqual(extracted.bounds, (-179.5, -2.5, 180.5, 2.5))
        self.assertEqual(extracted.data[0, 0], 181.0)

    def test_grid_crossing_greenwich_stored_from_340_is_unchanged(self):
        extracted = self._extract(self._grib(340, 55))

        self.assertEqual(extracted.bounds, (-20.5, -2.5, 55.5, 2.5))
        self.assertEqual(extracted.data[0, 0], -20.0)

    def test_regional_grid_straddling_180_is_refused(self):
        with self.assertRaisesRegex(UnsupportedGridError, "antimeridian"):
            self._extract(self._grib(170, 200))

    def test_refusal_reaches_the_ingestion_failure_record(self):
        result = ingest_file(self._grib(170, 200), self.plugin, "2t")

        self.assertFalse(result.success)
        self.assertTrue(any("antimeridian" in e for e in result.errors), result.errors)
