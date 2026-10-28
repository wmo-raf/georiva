from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.models import FileIngestion
from georiva.organisations.testing import make_organisation


def _make_event(bucket_name: str, key: str) -> dict:
    return {"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}


def _run(ev, org_slug="test-org", catalog_slug="test-catalog", collection_slug="test-collection"):
    with (
        patch("georiva.ingestion.consumer.validate_path") as mock_vp,
        patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
        patch("georiva.ingestion.consumer.process_incoming_file") as mock_task,
    ):
        mock_vp.return_value = {
            "org": org_slug,
            "catalog": catalog_slug,
            "collection": collection_slug,
            "reference_time": None,
        }
        mock_task.delay = MagicMock()
        _handle_event(ev)


class ConsumerDirectFileIngestionTests(TestCase):
    def setUp(self):
        Catalog.objects.create(organisation=make_organisation(), name="Test", slug="test-catalog", file_format="grib2")
        self.file_path = "test-org/test-catalog/test-collection/somefile.grib2"
        self.ev = _make_event("georiva-sources", self.file_path)

    def test_direct_drop_creates_file_ingestion(self):
        _run(self.ev)
        self.assertTrue(FileIngestion.objects.filter(file_path=self.file_path).exists())

    def test_time_extraction_failure_marks_file_ingestion_failed(self):
        with (
            patch("georiva.ingestion.consumer.validate_path") as mock_vp,
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
            patch("georiva.ingestion.consumer.process_incoming_file") as mock_task,
            patch("georiva.ingestion.consumer._required_time_error", return_value="Could not extract valid time"),
        ):
            mock_vp.return_value = {
                "org": "test-org",
                "catalog": "test-catalog",
                "collection": "test-collection",
                "reference_time": None,
            }
            mock_task.delay = MagicMock()
            _handle_event(self.ev)

        log = FileIngestion.objects.get(file_path=self.file_path)
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertIn("valid time", log.error)


class SweepDirectFileIngestionTests(TestCase):
    def setUp(self):
        Catalog.objects.create(organisation=make_organisation(), name="Sweep", slug="sweep-cat", file_format="grib2")

    def test_sweep_registers_file(self):
        from georiva.core.storage import BucketType as BT
        from georiva.ingestion.tasks import sweep_unprocessed

        file_path = "test-org/sweep-cat/col/rain.grib"
        incoming_bucket = MagicMock()
        incoming_bucket.list_files.return_value = []
        sources_bucket = MagicMock()
        sources_bucket.list_files.return_value = [{"path": file_path}]

        def _bucket_side_effect(bucket_type):
            return sources_bucket if bucket_type == BT.SOURCES else incoming_bucket

        # The bucket scan lives in ingestion.unprocessed (issue #223); the
        # storage boundary is mocked there.
        with (
            patch("georiva.ingestion.unprocessed.storage") as mock_storage,
            patch("georiva.ingestion.unprocessed.validate_path") as mock_vp,
            patch("georiva.ingestion.tasks.process_incoming_file") as mock_task,
        ):
            mock_storage.bucket.side_effect = _bucket_side_effect
            mock_vp.return_value = {"org": "test-org", "catalog": "sweep-cat", "reference_time": None}
            mock_task.run = MagicMock()

            sweep_unprocessed(sync=True)

        self.assertTrue(FileIngestion.objects.filter(file_path=file_path, bucket=BT.SOURCES).exists())


class MachineDeliveryAttributionTests(TestCase):
    def setUp(self):
        from georiva.ingestion.models import PushFeed, PushSender

        org = make_organisation("test-org")
        self.catalog = Catalog.objects.create(organisation=org, name="WRF", slug="wrf", file_format="netcdf")
        self.feed = PushFeed.objects.create(catalog=self.catalog, name="WRF", valid_time_format="CONTENT")
        self.alpha = PushSender.objects.create(push_feed=self.feed, label="alpha", access_key="AKALPHA")
        self.beta = PushSender.objects.create(push_feed=self.feed, label="beta", access_key="AKBETA")
        other = Catalog.objects.create(organisation=org, name="Other", slug="other", file_format="netcdf")
        other_feed = PushFeed.objects.create(catalog=other, name="Other", valid_time_format="CONTENT")
        self.stranger = PushSender.objects.create(push_feed=other_feed, label="stranger", access_key="AKOTHER")
        self.key = "test-org/wrf/GR--20260101T0000--wrf.nc"

    def _arrive(self, principal, key=None):
        ev = _make_event("georiva-incoming", key or self.key)
        ev["userIdentity"] = {"principalId": principal}
        ev["s3"]["object"]["size"] = 42
        meta = {"org": "test-org", "catalog": "wrf", "collection": None, "reference_time": None}
        with (
            patch("georiva.ingestion.consumer.validate_path", return_value=meta),
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.INCOMING),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            _handle_event(ev)
        return task

    def test_each_sender_of_a_feed_is_matched(self):
        from georiva.ingestion.models import Delivery

        for sender, name in ((self.alpha, "a.nc"), (self.beta, "b.nc")):
            self._arrive(sender.access_key, f"test-org/wrf/{name}")
            delivery = Delivery.objects.get(file_path=f"test-org/wrf/{name}")
            self.assertEqual(delivery.push_sender, sender)
            self.assertEqual(delivery.status, Delivery.Status.ACCEPTED)
            self.assertEqual(delivery.bytes, 42)

    def test_delivery_starts_the_timeline_naming_the_sender(self):
        from georiva.ingestion.models import Delivery, DeliveryReceived

        task = self._arrive("AKALPHA")
        task.delay.assert_called_once()
        delivery = Delivery.objects.get()
        event = DeliveryReceived.objects.get(delivery=delivery)
        self.assertEqual(event.actor, "sender:alpha")
        self.assertEqual(event.correlation_id, delivery.correlation_id)
        self.assertEqual(event.sender, "alpha")
        log = FileIngestion.objects.get(file_path=self.key)
        self.assertEqual(log.status, FileIngestion.Status.PENDING)

    def test_principal_of_another_feed_is_not_matched(self):
        from georiva.ingestion.models import Delivery

        self._arrive("AKOTHER")
        self.assertFalse(Delivery.objects.exists())

    def test_unknown_writer_is_not_recorded(self):
        from georiva.ingestion.models import Delivery

        self._arrive("minioadmin")
        self.assertFalse(Delivery.objects.exists())

    def test_a_persons_upload_is_not_recorded_twice(self):
        from georiva.ingestion.models import Delivery

        Delivery.receive(push_feed=self.feed, filename="x.nc", file_path=self.key)
        self._arrive("minioadmin")
        self.assertEqual(Delivery.objects.count(), 1)

    def test_a_repeat_notification_while_in_flight_is_one_delivery(self):
        from georiva.ingestion.models import Delivery

        self._arrive("AKALPHA")
        self._arrive("AKALPHA")
        self.assertEqual(Delivery.objects.count(), 1)
