"""
Ingesting a run writes no lead-0 asset of a non-instant Variable (#818, ADR 0051).

ECMWF's `tp` and `ssrd` are running totals since the run started: at lead 0
the window is empty, every value is 0, and that step does not exist.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.models import Asset, Variable
from georiva.core.storage import BucketType
from georiva.ingestion.models import FileIngestion
from georiva.ingestion.service import IngestionService
from georiva.organisations.testing import make_organisation

from .test_file_events import KEY, _tree
from .test_item_gone_mid_ingest import IngestionFixture


class ARunsLeadZeroTests(IngestionFixture):
    def setUp(self):
        super().setUp()
        # `precip` is a running total; `temp` and `wind` are instant.
        Variable.objects.filter(collection=self.collection, slug="precip").update(
            time_statistic="sum", statistic_window="run_start"
        )
        self.handler.ctx.reference_time = self.ts

    def written(self):
        written = Asset.objects.filter(variable__collection=self.collection)
        return sorted(written.values_list("variable__slug", flat=True))

    def test_a_running_total_has_no_asset_at_the_run_start(self):
        item, _assets, _clip, failed = self._process()

        self.assertIsNotNone(item)
        self.assertEqual(failed, [])
        self.assertEqual(self.written(), ["temp", "wind"])
        self.assertEqual(self.extracted, ["temp", "wind"], "a step that does not exist is not read either")

    def test_a_running_total_has_an_asset_at_every_later_step(self):
        self.ts = self.ts + timedelta(hours=6)

        self._process()

        self.assertEqual(self.written(), ["precip", "temp", "wind"])

    def test_a_running_total_alone_at_the_run_start_leaves_no_item(self):
        Variable.objects.filter(collection=self.collection).exclude(slug="precip").update(is_active=False)

        item, assets, _clip, failed = self._process()

        self.assertIsNone(item)
        self.assertEqual((assets, failed), ([], []))

    def test_a_sum_of_no_run_is_written(self):
        # CHIRPS daily rainfall: a sum, of no run.
        self.handler.ctx.reference_time = None

        self._process()

        self.assertEqual(self.written(), ["precip", "temp", "wind"])


class AFileOfARunsStartTests(TestCase):
    """A file that holds only a running total's lead-0 step has nothing to
    write, and that is not a failure."""

    def setUp(self):
        self.catalog, self.collection = _tree(make_organisation("kenya"))
        Variable.objects.filter(collection=self.collection).update(time_statistic="sum", statistic_window="run_start")
        self.run = datetime(2026, 9, 2, tzinfo=UTC)

    def process(self, timestamps):
        FileIngestion.register(bucket=BucketType.SOURCES, file_path=KEY)
        plugin = MagicMock()
        plugin.get_timestamps.return_value = timestamps
        handler = MagicMock()
        handler.process_timestamp.side_effect = lambda **kw: (MagicMock(pk=1), [MagicMock(pk=1)], None, [])
        with (
            patch("georiva.ingestion.service.storage"),
            patch("georiva.ingestion.service.format_registry") as registry,
            patch("georiva.ingestion.service.IngestionHandler", return_value=handler),
            patch.object(IngestionService, "_get_first_variable_name", return_value="rain"),
        ):
            registry.get.return_value = plugin
            service = IngestionService()
            service._source_file_manager = MagicMock()
            result = service.process_file(KEY, origin_bucket=BucketType.SOURCES, reference_time=self.run)
        return result, handler

    def test_it_succeeds_writing_nothing(self):
        result, handler = self.process([self.run])

        self.assertTrue(result.success)
        self.assertEqual((result.items_created, result.errors), ([], []))
        handler.process_timestamp.assert_not_called()

    def test_its_later_steps_are_written(self):
        result, handler = self.process([self.run, self.run + timedelta(hours=6)])

        self.assertTrue(result.success)
        self.assertEqual(handler.process_timestamp.call_count, 1)
