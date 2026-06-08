from unittest.mock import MagicMock, patch

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


class ConsumerCreatesDataArrivalTests(TestCase):
    def setUp(self):
        Catalog.objects.create(name="Test", slug="test-catalog", file_format="grib2")
        self.file_path = "test-catalog/test-collection/somefile.grib2"
        self.ev = _make_event("georiva-sources", self.file_path)

    def test_direct_drop_creates_data_arrival_and_linked_file_ingestion(self):
        _run(self.ev)

        arrival = DataArrival.objects.get(file_path=self.file_path)
        self.assertEqual(arrival.trigger, DataArrival.Trigger.MANUAL_UPLOAD)
        self.assertEqual(arrival.status, DataArrival.Status.PENDING)

        log = FileIngestion.objects.get(file_path=self.file_path)
        self.assertEqual(log.data_arrival_id, arrival.pk)

    def test_pre_registered_uploading_arrival_transitions_to_pending(self):
        existing = DataArrival.objects.create(
            file_path=self.file_path,
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.UPLOADING,
        )

        _run(self.ev)

        existing.refresh_from_db()
        self.assertEqual(existing.status, DataArrival.Status.PENDING)
        self.assertEqual(DataArrival.objects.filter(file_path=self.file_path).count(), 1)

        log = FileIngestion.objects.get(file_path=self.file_path)
        self.assertEqual(log.data_arrival_id, existing.pk)
