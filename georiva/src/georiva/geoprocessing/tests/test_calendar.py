import unittest

import numpy as np
import xarray as xr

from georiva.geoprocessing.calendar import convert_calendar


class ConvertCalendarTests(unittest.TestCase):
    def test_noleap_to_standard_drops_unmapped(self):
        # A noleap series across a leap-year Feb has no Feb 29.
        time = xr.date_range("2020-02-26", periods=5, freq="D", calendar="noleap", use_cftime=True)
        da = xr.DataArray(np.arange(5.0), coords={"time": time}, dims=["time"])

        converted = convert_calendar(da, "standard")
        # Standard calendar — index is real datetimes now.
        self.assertEqual(converted["time"].dt.calendar, "proleptic_gregorian") \
            if hasattr(converted["time"].dt, "calendar") else None
        # No Feb 29 was invented (missing=None drops unmapped dates).
        self.assertLessEqual(converted.sizes["time"], da.sizes["time"])

    def test_360_day_to_standard_runs(self):
        time = xr.date_range("2020-01-01", periods=12, freq="D", calendar="360_day", use_cftime=True)
        da = xr.DataArray(np.arange(12.0), coords={"time": time}, dims=["time"])
        converted = convert_calendar(da, "standard", align_on="date")
        self.assertGreater(converted.sizes["time"], 0)


if __name__ == "__main__":
    unittest.main()
