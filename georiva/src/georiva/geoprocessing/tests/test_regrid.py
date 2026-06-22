import unittest

import numpy as np
from rasterio.transform import from_bounds

from georiva.geoprocessing.regrid import regrid_array


class RegridArrayTests(unittest.TestCase):
    def test_upsample_doubles_shape_and_preserves_range(self):
        data = np.array([[0.0, 10.0], [20.0, 30.0]], dtype="float32")
        src_t = from_bounds(0, 0, 2, 2, 2, 2)
        dst_t = from_bounds(0, 0, 2, 2, 4, 4)

        out = regrid_array(
            data, src_t, "EPSG:4326", dst_t, "EPSG:4326", (4, 4),
            resampling="nearest",
        )
        self.assertEqual(out.shape, (4, 4))
        self.assertAlmostEqual(float(np.nanmin(out)), 0.0)
        self.assertAlmostEqual(float(np.nanmax(out)), 30.0)

    def test_identity_grid_returns_same_values(self):
        data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
        t = from_bounds(0, 0, 2, 2, 2, 2)
        out = regrid_array(data, t, "EPSG:4326", t, "EPSG:4326", (2, 2),
                           resampling="nearest")
        np.testing.assert_array_almost_equal(out, data)

    def test_unknown_resampling_raises(self):
        data = np.zeros((2, 2), dtype="float32")
        t = from_bounds(0, 0, 2, 2, 2, 2)
        with self.assertRaises(ValueError):
            regrid_array(data, t, "EPSG:4326", t, "EPSG:4326", (2, 2),
                         resampling="bogus")


if __name__ == "__main__":
    unittest.main()
