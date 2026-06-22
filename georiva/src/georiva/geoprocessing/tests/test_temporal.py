import unittest

import numpy as np
import pandas as pd
import xarray as xr

from georiva.geoprocessing.temporal import anomaly, temporal_aggregate


def _series(values, start="2020-01-01", freq="MS"):
    time = pd.date_range(start, periods=len(values), freq=freq)
    return xr.DataArray(values, coords={"time": time}, dims=["time"])


class TemporalAggregateTests(unittest.TestCase):
    def test_collapse_whole_series_mean(self):
        da = _series([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(float(temporal_aggregate(da, how="mean")), 2.5)

    def test_collapse_whole_series_sum(self):
        da = _series([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(float(temporal_aggregate(da, how="sum")), 10.0)

    def test_resample_monthly_to_yearly_mean(self):
        da = _series([float(i) for i in range(24)])  # 24 months
        yearly = temporal_aggregate(da, freq="YS", how="mean")
        self.assertEqual(yearly.sizes["time"], 2)
        self.assertAlmostEqual(float(yearly.isel(time=0)), 5.5)   # mean 0..11
        self.assertAlmostEqual(float(yearly.isel(time=1)), 17.5)  # mean 12..23

    def test_unknown_how_raises(self):
        with self.assertRaises(ValueError):
            temporal_aggregate(_series([1.0]), how="bogus")


class AnomalyTests(unittest.TestCase):
    def test_absolute_anomaly_against_baseline_series(self):
        value = _series([10.0, 12.0])
        baseline = _series([2.0, 4.0, 6.0])  # mean 4.0
        out = anomaly(value, baseline)
        np.testing.assert_array_almost_equal(out.values, np.array([6.0, 8.0]))

    def test_relative_anomaly(self):
        value = _series([6.0])
        baseline = _series([4.0])  # mean 4.0
        out = anomaly(value, baseline, relative=True)
        # (6-4)/4 = 0.5
        np.testing.assert_array_almost_equal(out.values, np.array([0.5]))


if __name__ == "__main__":
    unittest.main()
