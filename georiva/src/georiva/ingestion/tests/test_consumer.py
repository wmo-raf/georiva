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
        patcher = patch("georiva.ingestion.quarantine.storage")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _arrive(self, principal, key=None, user_agent=None):
        ev = _make_event("georiva-incoming", key or self.key)
        ev["userIdentity"] = {"principalId": principal}
        ev["s3"]["object"]["size"] = 42
        if user_agent is not None:
            ev["source"] = {"host": "10.0.0.7", "port": "", "userAgent": user_agent}
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
        delivery = Delivery.objects.get()
        self.assertEqual(delivery.status, Delivery.Status.REJECTED)
        self.assertIsNone(delivery.push_sender)

    def test_a_persons_upload_is_not_recorded_twice(self):
        from django.contrib.auth.models import User

        from georiva.ingestion.models import Delivery

        person = User.objects.create(username="amina")
        Delivery.receive(push_feed=self.feed, filename="x.nc", file_path=self.key, sent_by=person)
        self._arrive("minioadmin")
        self.assertEqual(Delivery.objects.count(), 1)

    def test_a_repeat_notification_while_in_flight_is_one_delivery(self):
        from georiva.ingestion.models import Delivery

        self._arrive("AKALPHA")
        self._arrive("AKALPHA")
        self.assertEqual(Delivery.objects.count(), 1)

    def test_a_georiva_push_delivery_records_the_senders_version(self):
        from georiva.ingestion.models import Delivery

        self._arrive("AKALPHA", "test-org/wrf/a.nc", user_agent="georiva-push/1.0.0")
        self._arrive("AKBETA", "test-org/wrf/b.nc", user_agent="rclone/v1.68.1")
        self._arrive("AKBETA", "test-org/wrf/c.nc")
        versions = dict(Delivery.objects.values_list("original_filename", "sender_version"))
        self.assertEqual(versions, {"a.nc": "1.0.0", "b.nc": "", "c.nc": ""})

    def test_a_rejected_georiva_push_delivery_records_the_version_too(self):
        from georiva.ingestion.models import Delivery

        with patch("georiva.ingestion.consumer._required_time_error", return_value="no valid time"):
            self._arrive("AKALPHA", user_agent="georiva-push/1.0.0")
        delivery = Delivery.objects.get()
        self.assertEqual(delivery.status, Delivery.Status.REJECTED)
        self.assertEqual(delivery.sender_version, "1.0.0")


class QuarantineTests(TestCase):
    """A rejected file leaves ``incoming`` for quarantine, with a Delivery and a refused event (ADR 0034)."""

    def setUp(self):
        from georiva.ingestion.models import PushFeed, PushSender

        self.org = make_organisation("test-org")
        self.catalog = Catalog.objects.create(organisation=self.org, name="WRF", slug="wrf", file_format="netcdf")
        self.feed = PushFeed.objects.create(catalog=self.catalog, name="WRF", valid_time_format="CONTENT")
        self.sender = PushSender.objects.create(push_feed=self.feed, label="alpha", access_key="AKALPHA")
        patcher = patch("georiva.ingestion.quarantine.storage")
        self.storage = patcher.start()
        self.addCleanup(patcher.stop)

    def _arrive(self, key, principal="AKALPHA"):
        ev = _make_event("georiva-incoming", key)
        ev["userIdentity"] = {"principalId": principal}
        with (
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.INCOMING),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            _handle_event(ev)
        return task

    def _assert_quarantined(self, key):
        self.storage.move_between.assert_called_once_with(self.storage.incoming, self.storage.quarantine, key)

    def test_root_writing_into_a_feeds_area_is_quarantined_and_shown_on_the_feed(self):
        from georiva.ingestion.models import Delivery, DeliveryRejected

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        task = self._arrive(key, principal="minioadmin")

        task.delay.assert_not_called()
        self._assert_quarantined(key)
        self.assertFalse(FileIngestion.objects.exists())
        delivery = Delivery.objects.get()
        self.assertEqual(delivery.status, Delivery.Status.REJECTED)
        self.assertEqual(delivery.reason, "not written by a sender of this feed")
        event = DeliveryRejected.objects.get()
        self.assertEqual(event.organisation_id, self.org.pk)
        self.assertEqual(event.failure_group_key(), f"push-feed:{self.feed.pk}")
        self.assertEqual(event.outcome_class, "refused")
        self.assertEqual(event.correlation_id, delivery.correlation_id)

    def test_a_pull_fed_catalog_is_quarantined_without_a_delivery(self):
        from georiva.ingestion.models import Delivery, DeliveryRejected
        from georiva.sources.models import DataFeed

        pull = Catalog.objects.create(organisation=self.org, name="Pull", slug="pull", file_format="netcdf")
        DataFeed.objects.create(catalog=pull, name="Pull")
        key = "test-org/pull/x.nc"
        self._arrive(key)

        self._assert_quarantined(key)
        self.assertFalse(Delivery.objects.exists())
        event = DeliveryRejected.objects.get()
        self.assertEqual(event.organisation_id, self.org.pk)
        self.assertEqual(event.failure_group_key(), "catalog:test-org/pull")
        self.assertIn("pull feed", event.reason)

    def test_a_catalog_with_no_feed_is_quarantined(self):
        from georiva.ingestion.models import DeliveryRejected

        Catalog.objects.create(organisation=self.org, name="Bare", slug="bare", file_format="netcdf")
        self._arrive("test-org/bare/x.nc")
        self.assertIn("no push feed", DeliveryRejected.objects.get().reason)

    def test_an_unknown_organisation_is_unattributed(self):
        from georiva.ingestion.models import DeliveryRejected

        key = "nobody/wrf/x.nc"
        self._arrive(key)

        self._assert_quarantined(key)
        event = DeliveryRejected.objects.get()
        self.assertIsNone(event.organisation_id)
        self.assertIn("Unknown organisation", event.reason)

    def test_a_key_that_does_not_parse_is_quarantined(self):
        from georiva.ingestion.models import DeliveryRejected

        self._arrive("test-org/loose.nc")
        self._assert_quarantined("test-org/loose.nc")
        self.assertIn("Path check failed", DeliveryRejected.objects.get().reason)

    def test_a_machine_delivery_whose_time_cannot_be_extracted_is_quarantined(self):
        from georiva.ingestion.models import Delivery

        with patch("georiva.ingestion.consumer._required_time_error", return_value="Could not extract a time"):
            task = self._arrive("test-org/wrf/x.nc")

        task.delay.assert_not_called()
        delivery = Delivery.objects.get()
        self.assertEqual(delivery.status, Delivery.Status.REJECTED)
        self.assertEqual(delivery.push_sender, self.sender)
        self.assertEqual(delivery.reason, "Could not extract a time")

    def test_a_valid_delivery_is_not_quarantined(self):
        task = self._arrive("test-org/wrf/GR--20260101T0000--wrf.nc")
        task.delay.assert_called_once()
        self.storage.move_between.assert_not_called()

    def test_retry_moves_the_file_back_and_it_is_checked_again(self):
        from django.contrib.auth.models import User

        from georiva.ingestion.models import Delivery
        from georiva.ingestion.quarantine import retry

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        self._arrive(key, principal="minioadmin")
        rejected = Delivery.objects.get()
        self.storage.reset_mock()

        fresh = retry(rejected, User.objects.create(username="ops"))

        self.storage.move_between.assert_called_once_with(self.storage.quarantine, self.storage.incoming, key)
        self.assertEqual(fresh.status, Delivery.Status.RECEIVED)
        rejected.refresh_from_db()
        self.assertEqual(rejected.status, Delivery.Status.REJECTED)

        # The move back raises a notification written by GeoRiva's own credentials.
        self.storage.reset_mock()
        task = self._arrive(key, principal="minioadmin")
        task.delay.assert_called_once()
        self.storage.move_between.assert_not_called()
        fresh.refresh_from_db()
        self.assertEqual(fresh.status, Delivery.Status.ACCEPTED)

    def test_a_stale_received_delivery_does_not_admit_another_writer(self):
        from datetime import timedelta

        from django.contrib.auth.models import User
        from django.utils import timezone

        from georiva.ingestion.consumer import WAITING_WINDOW
        from georiva.ingestion.models import Delivery

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        old = Delivery.receive(
            push_feed=self.feed, filename="x.nc", file_path=key, sent_by=User.objects.create(username="amina")
        )
        Delivery.objects.filter(pk=old.pk).update(received_at=timezone.now() - WAITING_WINDOW - timedelta(minutes=1))

        task = self._arrive(key, principal="minioadmin")

        task.delay.assert_not_called()
        self._assert_quarantined(key)

    def test_a_file_cannot_be_retried_twice(self):
        from django.contrib.auth.models import User

        from georiva.ingestion.models import Delivery
        from georiva.ingestion.quarantine import retry

        self._arrive("test-org/wrf/GR--20260101T0000--wrf.nc", principal="minioadmin")
        rejected = Delivery.objects.get()
        user = User.objects.create(username="ops")
        retry(rejected, user)
        with self.assertRaises(ValueError):
            retry(rejected, user)

    def test_only_a_rejected_delivery_can_be_retried(self):
        from georiva.ingestion.models import Delivery
        from georiva.ingestion.quarantine import retry

        delivery = Delivery.receive(push_feed=self.feed, filename="x.nc", file_path="test-org/wrf/x.nc")
        with self.assertRaises(ValueError):
            retry(delivery, None)

    def test_quarantine_is_never_swept_or_notified(self):
        from georiva.core.management.commands.setup_minio import BUCKET_CONFIGS
        from georiva.ingestion.consumer import _get_ingest_buckets

        self.assertEqual(BUCKET_CONFIGS["quarantine"]["expiry_days"], 30)
        self.assertFalse(BUCKET_CONFIGS["quarantine"]["notify_on_create"])
        self.assertFalse(BUCKET_CONFIGS["quarantine"]["public_read"])
        self.assertNotIn("georiva-quarantine", _get_ingest_buckets())


class SweepFoundIncomingTests(TestCase):
    """A file the Sweep finds in ``incoming`` is attributed like a notification, minus the principal (ADR 0034)."""

    def setUp(self):
        from georiva.ingestion.models import PushFeed

        org = make_organisation("test-org")
        self.catalog = Catalog.objects.create(organisation=org, name="WRF", slug="wrf", file_format="netcdf")
        self.feed = PushFeed.objects.create(catalog=self.catalog, name="WRF", valid_time_format="CONTENT")
        patcher = patch("georiva.ingestion.quarantine.storage")
        self.quarantine_storage = patcher.start()
        self.addCleanup(patcher.stop)

    def _sweep(self, key):
        from georiva.ingestion.tasks import sweep_unprocessed

        incoming = MagicMock()
        incoming.list_files.return_value = [{"path": key}]
        sources = MagicMock()
        sources.list_files.return_value = []
        meta = {"org": key.split("/")[0], "catalog": key.split("/")[1], "reference_time": None}
        with (
            patch("georiva.ingestion.unprocessed.storage") as mock_storage,
            patch("georiva.ingestion.unprocessed.validate_path", return_value=meta),
            patch("georiva.ingestion.consumer.validate_path", return_value=meta),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            mock_storage.bucket.side_effect = lambda b: incoming if b == BucketType.INCOMING else sources
            task.run = MagicMock()
            sweep_unprocessed(sync=True)
        return task

    def test_a_file_in_a_push_feeds_area_becomes_a_found_by_sweep_delivery_and_is_ingested(self):
        from georiva.ingestion.models import Delivery

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        task = self._sweep(key)

        delivery = Delivery.objects.get(file_path=key)
        self.assertEqual(delivery.status, Delivery.Status.ACCEPTED)
        self.assertTrue(delivery.found_by_sweep)
        self.assertEqual(delivery.push_feed, self.feed)
        self.assertIsNone(delivery.push_sender)
        task.run.assert_called_once()
        self.assertTrue(FileIngestion.objects.filter(file_path=key, bucket=BucketType.INCOMING).exists())

    def test_a_file_under_a_key_with_no_push_feed_is_quarantined(self):
        from georiva.ingestion.models import Delivery, DeliveryRejected

        Catalog.objects.create(organisation=self.catalog.organisation, name="Pull", slug="pull", file_format="netcdf")
        key = "test-org/pull/a.nc"
        task = self._sweep(key)

        self.quarantine_storage.move_between.assert_called_once()
        task.run.assert_not_called()
        self.assertFalse(FileIngestion.objects.filter(file_path=key).exists())
        self.assertFalse(Delivery.objects.exists())
        self.assertEqual(DeliveryRejected.objects.count(), 1)

    def test_rerunning_the_sweep_creates_no_duplicate_delivery(self):
        from georiva.ingestion.models import Delivery

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        self._sweep(key)
        self._sweep(key)

        self.assertEqual(Delivery.objects.filter(file_path=key).count(), 1)

    def test_a_persons_waiting_upload_is_not_recorded_again(self):
        from django.contrib.auth.models import User

        from georiva.ingestion.models import Delivery

        key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        Delivery.receive(
            push_feed=self.feed, filename="wrf.nc", file_path=key, sent_by=User.objects.create(username="amina")
        )
        self._sweep(key)

        self.assertEqual(Delivery.objects.filter(file_path=key).count(), 1)

    def test_a_failing_quarantine_does_not_stop_the_sweep(self):
        Catalog.objects.create(organisation=self.catalog.organisation, name="Pull", slug="pull", file_format="netcdf")
        self.quarantine_storage.move_between.side_effect = RuntimeError("minio down")

        self._sweep("test-org/pull/a.nc")  # must not raise

        self.assertFalse(FileIngestion.objects.filter(file_path="test-org/pull/a.nc").exists())
