import unittest

import numpy as np

from georiva.geoprocessing.algebra import raster_combine, safe_divide


class RasterCombineTests(unittest.TestCase):
    def setUp(self):
        self.a = np.array([[1.0, 2.0], [3.0, 4.0]])
        self.b = np.array([[5.0, 6.0], [7.0, 8.0]])

    def test_sum(self):
        np.testing.assert_array_equal(
            raster_combine(self.a, self.b, op="sum"),
            np.array([[6.0, 8.0], [10.0, 12.0]]),
        )

    def test_mean(self):
        np.testing.assert_array_equal(
            raster_combine(self.a, self.b, op="mean"),
            np.array([[3.0, 4.0], [5.0, 6.0]]),
        )

    def test_weighted_mean(self):
        out = raster_combine(self.a, self.b, op="mean", weights=[3.0, 1.0])
        # (3*1 + 1*5)/4 = 2.0 ; (3*2 + 1*6)/4 = 3.0
        np.testing.assert_array_almost_equal(out, np.array([[2.0, 3.0], [4.0, 5.0]]))

    def test_min_max_product(self):
        np.testing.assert_array_equal(raster_combine(self.a, self.b, op="min"), self.a)
        np.testing.assert_array_equal(raster_combine(self.a, self.b, op="max"), self.b)
        np.testing.assert_array_equal(
            raster_combine(self.a, self.b, op="product"),
            np.array([[5.0, 12.0], [21.0, 32.0]]),
        )

    def test_nan_is_skipped_when_other_present(self):
        a = np.array([[np.nan, 2.0]])
        b = np.array([[5.0, 6.0]])
        np.testing.assert_array_equal(
            raster_combine(a, b, op="sum"), np.array([[5.0, 8.0]])
        )

    def test_requires_two_arrays(self):
        with self.assertRaises(ValueError):
            raster_combine(self.a, op="sum")

    def test_unknown_op_raises(self):
        with self.assertRaises(ValueError):
            raster_combine(self.a, self.b, op="bogus")


class SafeDivideTests(unittest.TestCase):
    def test_divide_by_zero_is_nan_not_inf(self):
        out = safe_divide(np.array([1.0, 2.0]), np.array([0.0, 2.0]))
        self.assertTrue(np.isnan(out[0]))
        self.assertAlmostEqual(out[1], 1.0)


if __name__ == "__main__":
    unittest.main()
