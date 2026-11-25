"""
A file says what its fields are over time: GRIB's stepType, NetCDF's
cell_methods. The format plugins pass it on, for a Variable created from the
field and for a check of one that exists (#818, ADR 0051).
"""

import tempfile
from datetime import datetime
from pathlib import Path

import eccodes
import numpy as np
import xarray as xr
from django.test import SimpleTestCase

from georiva.core.time_statistic import Declared
from georiva.formats.grib import GRIBFormatPlugin
from georiva.formats.netcdf import NetCDFFormatPlugin

TIME = datetime(2026, 1, 1)


class Files(SimpleTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.folder = Path(self._tmp.name)


class AGribFieldsDeclarationTests(Files):
    plugin = GRIBFormatPlugin()

    def grib(self, fields: dict[str, dict]) -> Path:
        """A GRIB2 file of one small field per short name, each with its own keys."""
        path = self.folder / "fields.grib2"
        with open(path, "wb") as out:
            for short_name, keys in fields.items():
                handle = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
                try:
                    for key, value in {
                        "Ni": 3,
                        "Nj": 3,
                        "latitudeOfFirstGridPointInDegrees": 1.0,
                        "latitudeOfLastGridPointInDegrees": -1.0,
                        "longitudeOfFirstGridPointInDegrees": 0.0,
                        "longitudeOfLastGridPointInDegrees": 2.0,
                        "iDirectionIncrementInDegrees": 1.0,
                        "jDirectionIncrementInDegrees": 1.0,
                        **keys,
                        "shortName": short_name,
                    }.items():
                        eccodes.codes_set(handle, key, value)
                    eccodes.codes_set_values(handle, np.ones(9))
                    eccodes.codes_write(handle, out)
                finally:
                    eccodes.codes_release(handle)
        self.addCleanup(self.plugin.clear_cache)
        return path

    def test_each_field_listed_says_what_it_declares(self):
        path = self.grib({"2t": {}, "tp": {"stepType": "accum", "startStep": 0, "endStep": 6}})

        listed = {v["short_name"]: v for v in self.plugin.list_variables(path)}

        self.assertEqual(listed["2t"]["time_statistic"], "instant")
        self.assertEqual((listed["tp"]["time_statistic"], listed["tp"]["statistic_window"]), ("sum", "run_start"))

    def test_an_opened_field_says_what_it_declares(self):
        path = self.grib({"tp": {"stepType": "accum", "startStep": 0, "endStep": 6}})

        with self.plugin.open_variable(path, "tp") as opened:
            self.assertEqual(opened.metadata["declared"], Declared("sum", "run_start"))


class ANetCDFVariablesDeclarationTests(Files):
    plugin = NetCDFFormatPlugin()

    def netcdf(self, **attrs) -> Path:
        ds = xr.Dataset(
            {"precip": (("time", "lat", "lon"), np.zeros((1, 3, 3), dtype=np.float32), attrs)},
            coords={"time": [np.datetime64(TIME)], "lat": [1.0, 0.0, -1.0], "lon": [0.0, 1.0, 2.0]},
        )
        path = self.folder / "precip.nc"
        ds.to_netcdf(path, engine="netcdf4")
        return path

    def test_a_daily_sum_is_listed_as_one(self):
        listed = self.plugin.list_variables(self.netcdf(cell_methods="time: sum"))[0]

        self.assertEqual((listed["time_statistic"], listed["statistic_window"]), ("sum", "preceding_interval"))

    def test_a_variable_that_says_nothing_is_listed_without_a_declaration(self):
        listed = self.plugin.list_variables(self.netcdf())[0]

        self.assertNotIn("time_statistic", listed)

    def test_an_opened_variable_says_what_it_declares(self):
        with self.plugin.open_variable(self.netcdf(cell_methods="time: maximum"), "precip", timestamp=TIME) as opened:
            self.assertEqual(opened.metadata["declared"], Declared("max", "preceding_interval"))
