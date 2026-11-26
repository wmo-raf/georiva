"""
What a source says of a field's time statistic (#818, ADR 0051).

Every source already says whether a field is a value at its time or a
statistic over a window: a store array's `step_type`, GRIB's `stepType`,
NetCDF's `cell_methods`. These read it, and say nothing where it says nothing.
"""

from django.test import SimpleTestCase

from georiva.core.time_statistic import (
    INSTANT,
    Declared,
    from_cell_methods,
    from_grib_step_type,
    from_store_array,
)

RUN_TOTAL = Declared("sum", "run_start")


class AStoreArraysDeclarationTests(SimpleTestCase):
    def test_an_instant_array_is_instant(self):
        self.assertEqual(from_store_array("temperature_2m", {"step_type": "instant"}), INSTANT)

    def test_an_aifs_run_total_is_a_sum_since_the_run_started(self):
        declared = from_store_array("total_precipitation_run_total_surface", {"step_type": "accum"})

        self.assertEqual(declared, RUN_TOTAL)

    def test_a_gfs_accumulation_from_initialization_is_a_sum_since_the_run_started(self):
        attrs = {"step_type": "accum", "comment": "Accumulated from the forecast initialization time to this step."}

        self.assertEqual(from_store_array("precipitation_surface", attrs), RUN_TOTAL)

    def test_a_gfs_bucket_over_the_preceding_hours_is_over_the_preceding_interval(self):
        attrs = {"step_type": "accum", "comment": "Accumulated over the preceding 1-6 hours of the forecast."}

        self.assertEqual(from_store_array("total_precipitation_surface", attrs), Declared("sum", "preceding_interval"))

    def test_an_average_maximum_and_minimum_are_over_the_preceding_interval(self):
        for step_type, statistic in (("avg", "mean"), ("max", "max"), ("min", "min")):
            with self.subTest(step_type):
                self.assertEqual(
                    from_store_array("x", {"step_type": step_type}), Declared(statistic, "preceding_interval")
                )

    def test_an_array_that_says_nothing_has_no_declaration(self):
        self.assertIsNone(from_store_array("temperature_2m", {}))
        self.assertIsNone(from_store_array("temperature_2m", {"step_type": "something-else"}))


class AGribStepTypeTests(SimpleTestCase):
    def test_instant_is_instant(self):
        self.assertEqual(from_grib_step_type("instant"), INSTANT)

    def test_an_accumulation_is_a_sum_since_the_run_started(self):
        # ECMWF's tp and ssrd: GRIB's stepType alone does not say where the window
        # starts, and a forecast's accumulation runs from its start (ADR 0051).
        self.assertEqual(from_grib_step_type("accum"), RUN_TOTAL)

    def test_an_average_maximum_and_minimum_are_over_the_preceding_interval(self):
        for step_type, statistic in (("avg", "mean"), ("max", "max"), ("min", "min")):
            with self.subTest(step_type):
                self.assertEqual(from_grib_step_type(step_type), Declared(statistic, "preceding_interval"))

    def test_a_step_type_not_known_has_no_declaration(self):
        self.assertIsNone(from_grib_step_type(""))
        self.assertIsNone(from_grib_step_type("diff"))


class ANetCDFCellMethodsTests(SimpleTestCase):
    def test_a_daily_rainfall_sum_is_over_the_preceding_interval(self):
        self.assertEqual(from_cell_methods("time: sum"), Declared("sum", "preceding_interval"))

    def test_the_time_method_is_found_among_others(self):
        self.assertEqual(from_cell_methods("area: mean time: maximum"), Declared("max", "preceding_interval"))
        self.assertEqual(from_cell_methods("time: minimum within days"), Declared("min", "preceding_interval"))

    def test_a_point_in_time_is_instant(self):
        self.assertEqual(from_cell_methods("time: point"), INSTANT)

    def test_no_time_method_is_no_declaration(self):
        self.assertIsNone(from_cell_methods(""))
        self.assertIsNone(from_cell_methods("area: mean"))
        self.assertIsNone(from_cell_methods(None))
