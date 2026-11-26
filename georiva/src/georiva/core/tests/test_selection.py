"""A Selection: one value of each extra axis a store array is read at (wmo-raf/georiva#830)."""

import numpy as np
from django.test import SimpleTestCase

from georiva.core.selection import (
    SelectionInvalid,
    described,
    field_key,
    index_of,
    selection_of,
    split_field_key,
)


class SelectionOfTests(SimpleTestCase):
    def test_a_selection_is_its_axes_in_order_of_name(self):
        self.assertEqual(
            selection_of({"statistic": "mean", "ensemble_member": 0}), (("ensemble_member", 0), ("statistic", "mean"))
        )

    def test_nothing_selected_is_an_empty_selection(self):
        self.assertEqual(selection_of(None), ())
        self.assertEqual(selection_of({}), ())

    def test_a_value_that_is_neither_text_nor_a_number_is_refused(self):
        for wrong in ({"statistic": True}, {"statistic": None}, {"statistic": [1]}, {"": 1}, {1: 1}, ["mean"]):
            with self.subTest(wrong=wrong), self.assertRaises(SelectionInvalid):
                selection_of(wrong)


class FieldKeyTests(SimpleTestCase):
    def test_an_array_read_whole_is_known_by_its_path(self):
        self.assertEqual(field_key("temperature_2m", ()), "temperature_2m")
        self.assertEqual(split_field_key("pressure_level/temperature"), ("pressure_level/temperature", ()))

    def test_two_selections_of_one_array_are_two_fields(self):
        mean = field_key("temperature_2m", selection_of({"statistic": "mean"}))
        p90 = field_key("temperature_2m", selection_of({"statistic": "p90"}))
        self.assertNotEqual(mean, p90)

    def test_a_key_gives_back_its_array_and_selection(self):
        selection = selection_of({"ensemble_member": 0, "statistic": "0"})
        self.assertEqual(split_field_key(field_key("t2m", selection)), ("t2m", selection))


class IndexOfTests(SimpleTestCase):
    def test_a_number_is_found_among_numbers(self):
        self.assertEqual(index_of(np.array([10.0, 500.0, 850.0]), 850), 2)
        self.assertEqual(index_of(np.arange(51), 0), 0)

    def test_text_is_found_among_text(self):
        text = ["mean", "p10", "p90"]
        for values in (np.array(text), np.array([word.encode() for word in text]), np.array(text, dtype=object)):
            with self.subTest(dtype=values.dtype):
                self.assertEqual(index_of(values, "p90"), 2)

    def test_a_value_of_the_other_kind_or_not_there_is_not_found(self):
        self.assertIsNone(index_of(np.arange(51), "0"))
        self.assertIsNone(index_of(np.array(["mean"]), 0))
        self.assertIsNone(index_of(np.arange(51), 51))


class DescribedTests(SimpleTestCase):
    def test_a_selection_is_said_in_words(self):
        self.assertEqual(described(selection_of({"statistic": "mean"})), "statistic mean")
        self.assertEqual(
            described(selection_of({"ensemble_member": 0, "pressure_level": 850}), units={"pressure_level": "hPa"}),
            "ensemble member 0, pressure level 850 hPa",
        )
