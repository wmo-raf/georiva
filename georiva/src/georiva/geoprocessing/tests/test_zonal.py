import unittest

import numpy as np
from rasterio.transform import from_bounds

from georiva.geoprocessing.zonal import zonal_stats_from_array


def _square(x0, y0, x1, y1):
    return {
        "type": "Polygon",
        "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]],
    }


class ZonalStatsFromArrayTests(unittest.TestCase):
    def setUp(self):
        # 4x4 grid over bbox (0,0,4,4), pixel size 1, values 0..15.
        self.data = np.arange(16, dtype="float32").reshape(4, 4)
        self.transform = from_bounds(0, 0, 4, 4, 4, 4)
        self.crs = "EPSG:4326"

    def test_full_extent_aggregates_all_pixels(self):
        rows = zonal_stats_from_array(
            self.data, self.transform, self.crs, [("all", _square(0, 0, 4, 4))]
        )
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r["key"], "all")
        self.assertEqual(r["count"], 16)
        self.assertAlmostEqual(r["min"], 0.0)
        self.assertAlmostEqual(r["max"], 15.0)
        self.assertAlmostEqual(r["sum"], 120.0)

    def test_partial_geometry_counts_fewer_pixels(self):
        # Bottom-left quadrant only.
        rows = zonal_stats_from_array(
            self.data, self.transform, self.crs, [("q", _square(0, 0, 2, 2))]
        )
        self.assertLess(rows[0]["count"], 16)
        self.assertGreater(rows[0]["count"], 0)

    def test_none_geometry_returns_empty_stats(self):
        rows = zonal_stats_from_array(
            self.data, self.transform, self.crs, [("none", None)]
        )
        self.assertIsNone(rows[0]["mean"])
        self.assertIsNone(rows[0]["count"])
        self.assertEqual(rows[0]["key"], "none")

    def test_non_intersecting_geometry_returns_empty_stats(self):
        rows = zonal_stats_from_array(
            self.data, self.transform, self.crs, [("far", _square(100, 100, 101, 101))]
        )
        self.assertIsNone(rows[0]["mean"])

    def test_empty_geometries_returns_empty_list(self):
        self.assertEqual(
            zonal_stats_from_array(self.data, self.transform, self.crs, []), []
        )

    def test_nan_pixels_excluded(self):
        data = self.data.copy()
        data[0, 0] = np.nan
        rows = zonal_stats_from_array(
            data, self.transform, self.crs, [("all", _square(0, 0, 4, 4))]
        )
        self.assertEqual(rows[0]["count"], 15)


if __name__ == "__main__":
    unittest.main()
