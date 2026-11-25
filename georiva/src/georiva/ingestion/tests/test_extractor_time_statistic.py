"""
A file that declares a field otherwise than its Variable is logged, naming the
Variable, and the Variable is left as it is (#818, ADR 0051).
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
from django.test import SimpleTestCase

from georiva.core.models import Variable
from georiva.core.time_statistic import Declared
from georiva.formats.base import ExtractedVariable
from georiva.ingestion.extractor import VariableExtractor

WHEN = datetime(2026, 1, 1, 6, tzinfo=UTC)


def plugin_declaring(declared):
    plugin = MagicMock()
    plugin.extract_variable.return_value = ExtractedVariable(
        data=np.ones((2, 2)),
        bounds=(0, 0, 1, 1),
        crs="EPSG:4326",
        width=2,
        height=2,
        resolution=(0.5, 0.5),
        timestamp=WHEN,
        variable_name="tp",
        metadata={"declared": declared},
    )
    return plugin


def tp(**declared):
    return Variable(slug="tp", sources=[("primary", {"source_name": "tp"})], **declared)


class AFileThatDisagreesTests(SimpleTestCase):
    def test_it_is_logged_once_per_file_naming_the_variable(self):
        extractor = VariableExtractor(plugin_declaring(Declared("sum", "run_start")))
        variable = tp()

        with self.assertLogs("georiva.core.models.variable", "WARNING") as logged:
            extractor.extract(variable, Path("ifs.grib2"), WHEN)
            extractor.extract(variable, Path("ifs.grib2"), WHEN)

        self.assertEqual(len(logged.output), 1)
        self.assertIn("tp", logged.output[0])
        self.assertIn("ifs.grib2", logged.output[0])
        self.assertEqual(variable.time_statistic, "instant")

    def test_a_file_that_agrees_or_says_nothing_is_not_logged(self):
        for declared in (Declared("sum", "run_start"), None):
            with self.subTest(declared=declared), self.assertNoLogs("georiva.core.models.variable", "WARNING"):
                VariableExtractor(plugin_declaring(declared)).extract(
                    tp(time_statistic="sum", statistic_window="run_start"), Path("ifs.grib2"), WHEN
                )
