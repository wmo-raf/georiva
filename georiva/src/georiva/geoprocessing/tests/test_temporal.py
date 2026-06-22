import unittest

import numpy as np
import pandas as pd
import xarray as xr

from georiva.geoprocessing.temporal import (
    anomaly,
    climatology,
    select_season,
    temporal_aggregate,
)


def _series(values, start="2020-01-01", freq="MS"):
    time = pd.date_range(start, periods=len(values), freq=freq)
    return xr.DataArray(values, coords={"time": time}, dims=["time"])


def _spatial_series(monthly_scalars, ny=2, nx=3, start="2020-01-01", freq="MS"):
    """(time, y, x) cube; every pixel at time t equals monthly_scalars[t]."""
    time = pd.date_range(start, periods=len(monthly_scalars), freq=freq)
    data = np.broadcast_to(
        np.asarray(monthly_scalars, dtype=float)[:, None, None], (len(time), ny, nx)
    )
    return xr.DataArray(
        data, coords={"time": time}, dims=["time", "y", "x"]
    )


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

    def test_seasonal_anomaly_against_baseline_window(self):
        # JJA value period averages 13; JJA baseline period averages 10.
        value_window = _spatial_series([13.0] * 12)              # one year
        baseline_window = _spatial_series([10.0] * 24)           # two-year baseline
        value = climatology(value_window, season="JJA")
        baseline = climatology(baseline_window, season="JJA")

        absolute = anomaly(value, baseline)
        np.testing.assert_array_almost_equal(absolute.values, np.full((2, 3), 3.0))

        relative = anomaly(value, baseline, relative=True)
        np.testing.assert_array_almost_equal(relative.values, np.full((2, 3), 0.3))


class SelectSeasonTests(unittest.TestCase):
    def test_djf_keeps_only_dec_jan_feb(self):
        # Two full years of monthly data starting Jan 2020.
        da = _series([float(i) for i in range(24)], start="2020-01-01", freq="MS")
        djf = select_season(da, "DJF")
        months = sorted(set(int(m) for m in djf["time"].dt.month.values))
        self.assertEqual(months, [1, 2, 12])
        # 2 Januaries + 2 Februaries + 2 Decembers = 6 timesteps.
        self.assertEqual(djf.sizes["time"], 6)

    def test_annual_and_none_keep_all_timesteps(self):
        da = _series([float(i) for i in range(12)])
        self.assertEqual(select_season(da, "annual").sizes["time"], 12)
        self.assertEqual(select_season(da, None).sizes["time"], 12)

    def test_unknown_season_raises(self):
        with self.assertRaises(ValueError):
            select_season(_series([1.0, 2.0, 3.0]), "WET")

    def test_selects_by_month_on_360_day_calendar(self):
        # CMIP6-style 360-day calendar: months come from the file's time axis,
        # not a Gregorian assumption.
        time = xr.date_range(
            "2020-01-01", periods=24, freq="MS", calendar="360_day", use_cftime=True
        )
        da = xr.DataArray(
            np.arange(24, dtype=float), coords={"time": time}, dims=["time"]
        )
        djf = select_season(da, "DJF")
        months = sorted(set(int(m) for m in djf["time"].dt.month.values))
        self.assertEqual(months, [1, 2, 12])
        self.assertEqual(djf.sizes["time"], 6)


class ClimatologyTests(unittest.TestCase):
    def test_seasonal_mean_reduces_time_to_a_raster(self):
        # value at month m == m, for one year.
        cube = _spatial_series([float(m) for m in range(1, 13)])
        clim = climatology(cube, season="JJA")  # months 6,7,8 -> mean 7.0
        self.assertEqual(set(clim.dims), {"y", "x"})
        self.assertNotIn("time", clim.dims)
        np.testing.assert_array_almost_equal(clim.values, np.full((2, 3), 7.0))

    def test_no_season_collapses_whole_series_like_aggregate(self):
        da = _series([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(float(climatology(da)), 2.5)


if __name__ == "__main__":
    unittest.main()
