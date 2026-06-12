from unittest.mock import MagicMock, patch, call

from django.test import TestCase

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.models import DataArrival, FileIngestion


def _make_event(bucket_name: str, key: str) -> dict:
    return {"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}


def _run(ev, catalog_slug="test-catalog", collection_slug="test-collection"):
    with (
        patch("georiva.ingestion.consumer.validate_path") as mock_vp,
        patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
        patch("georiva.ingestion.consumer.process_incoming_file") as mock_task,
    ):
        mock_vp.return_value = {
            "catalog": catalog_slug,
            "collection": collection_slug,
            "reference_time": None,
        }
        mock_task.delay = MagicMock()
        _handle_event(ev)


class ConsumerDirectFileIngestionTests(TestCase):
    def setUp(self):
        Catalog.objects.create(name="Test", slug="test-catalog", file_format="grib2")
        self.file_path = "test-catalog/test-collection/somefile.grib2"
        self.ev = _make_event("georiva-sources", self.file_path)

    def test_direct_drop_creates_file_ingestion_without_data_arrival(self):
        _run(self.ev)

        log = FileIngestion.objects.get(file_path=self.file_path)
        self.assertIsNone(log.data_arrival)

    def test_direct_drop_does_not_create_data_arrival(self):
        _run(self.ev)
        self.assertEqual(DataArrival.objects.count(), 0)

    def test_time_extraction_failure_marks_file_ingestion_failed(self):
        with (
            patch("georiva.ingestion.consumer.validate_path") as mock_vp,
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
            patch("georiva.ingestion.consumer.process_incoming_file") as mock_task,
            patch("georiva.ingestion.consumer._required_time_error",
                  return_value="Could not extract valid time"),
        ):
            mock_vp.return_value = {
                "catalog": "test-catalog",
                "collection": "test-collection",
                "reference_time": None,
            }
            mock_task.delay = MagicMock()
            _handle_event(self.ev)

        log = FileIngestion.objects.get(file_path=self.file_path)
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertIn("valid time", log.error)
        self.assertEqual(DataArrival.objects.count(), 0)


class SweepDirectFileIngestionTests(TestCase):
    def setUp(self):
        Catalog.objects.create(name="Sweep", slug="sweep-cat", file_format="grib2")

    def test_sweep_registers_file_without_data_arrival(self):
        from georiva.core.storage import BucketType as BT
        from georiva.ingestion.tasks import sweep_unprocessed

        file_path = "sweep-cat/col/rain.grib"
        incoming_bucket = MagicMock()
        incoming_bucket.list_files.return_value = []
        sources_bucket = MagicMock()
        sources_bucket.list_files.return_value = [{"path": file_path}]

        def _bucket_side_effect(bucket_type):
            return sources_bucket if bucket_type == BT.SOURCES else incoming_bucket

        with (
            patch("georiva.ingestion.tasks.storage") as mock_storage,
            patch("georiva.ingestion.tasks.validate_path") as mock_vp,
            patch("georiva.ingestion.tasks.process_incoming_file") as mock_task,
        ):
            mock_storage.bucket.side_effect = _bucket_side_effect
            mock_vp.return_value = {"catalog": "sweep-cat", "reference_time": None}
            mock_task.run = MagicMock()

            sweep_unprocessed(sync=True)

        log = FileIngestion.objects.get(file_path=file_path, bucket=BT.SOURCES)
        self.assertIsNone(log.data_arrival)
        self.assertEqual(DataArrival.objects.count(), 0)
