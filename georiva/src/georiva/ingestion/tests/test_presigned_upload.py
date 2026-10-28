"""A person's upload sent straight to storage on a presigned URL (ADR 0034)."""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.ingestion.abandoned import ABANDONED_REASON, reject_abandoned
from georiva.ingestion.consumer import WAITING_WINDOW, _handle_event
from georiva.ingestion.models import (
    Delivery,
    DeliveryRejected,
    FileIngestion,
    FileIngestionJob,
    PushFeed,
    UploadSession,
)
from georiva.ingestion.tests.test_upload_page import _geotiff_setup, _grib_setup
from georiva.ingestion.upload_views import MAX_SINGLE_PUT_BYTES, PRESIGN_TTL
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()

PREPARE_URL = "/admin/push-feeds/{}/prepare/"
COMPLETE_URL = "/admin/push-feeds/{}/complete/"

SIGNED_URL = "http://localhost:9000/georiva-incoming/key?X-Amz-Signature=abc"
GRIB_NAME = "GR--20250115T0600--wrf.grib2"
GRIB_KEY = "test-org/models/GR--20250115T0600--wrf.grib2"


def _bucket(is_s3=True, exists=True, size=123):
    bucket = MagicMock()
    bucket.is_s3 = is_s3
    bucket.get_upload_url.return_value = SIGNED_URL
    bucket.exists.return_value = exists
    bucket.size.return_value = size
    return bucket


@patch("georiva.core.storage.StorageManager.incoming", new_callable=PropertyMock)
class PrepareTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_pre", "p@u.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def _prepare(self, config, **extra):
        data = {"filenames": [GRIB_NAME], "sizes": [123_000_000], "times": [""], **extra}
        return self.client.post(PREPARE_URL.format(config.pk), data)

    def test_hands_back_a_presigned_url_for_each_file(self, mock_incoming):
        mock_incoming.return_value = bucket = _bucket()
        _, _, config, _ = _grib_setup()

        second = "GR--20250115T1200--wrf.grib2"
        response = self._prepare(config, filenames=[GRIB_NAME, second], sizes=[1, 2], times=["", ""])

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["mode"], "presigned")
        self.assertEqual([f["upload_url"] for f in data["files"]], [SIGNED_URL, SIGNED_URL])
        self.assertEqual(
            [call.args for call in bucket.get_upload_url.call_args_list],
            [(GRIB_KEY, PRESIGN_TTL), (f"test-org/models/{second}", PRESIGN_TTL)],
        )

    def test_records_one_waiting_delivery_per_file_in_one_active_session(self, mock_incoming):
        mock_incoming.return_value = _bucket()
        _, _, config, _ = _grib_setup()

        data = self._prepare(config).json()

        session = UploadSession.objects.get(pk=data["upload_session_id"])
        self.assertEqual(session.status, UploadSession.Status.ACTIVE)
        delivery = Delivery.objects.get(pk=data["files"][0]["delivery_id"])
        self.assertEqual(delivery.session, session)
        self.assertEqual(delivery.status, Delivery.Status.RECEIVED)
        self.assertTrue(delivery.awaits_completion)
        self.assertEqual(delivery.sent_by, self.user)
        self.assertEqual(delivery.file_path, GRIB_KEY)

    def test_writes_nothing_to_storage(self, mock_incoming):
        mock_incoming.return_value = bucket = _bucket()
        _, _, config, _ = _grib_setup()

        self._prepare(config)

        bucket.save.assert_not_called()
        self.assertFalse(FileIngestion.objects.exists())
        self.assertFalse(FileIngestionJob.objects.exists())

    def test_a_backend_with_nothing_to_sign_for_says_to_post_the_files(self, mock_incoming):
        mock_incoming.return_value = _bucket(is_s3=False)
        _, _, config, _ = _grib_setup()

        response = self._prepare(config)

        self.assertEqual(response.json(), {"mode": "direct"})
        self.assertFalse(UploadSession.objects.exists())
        self.assertFalse(Delivery.objects.exists())

    def test_a_file_over_the_single_put_limit_is_refused_before_anything_is_recorded(self, mock_incoming):
        mock_incoming.return_value = _bucket()
        _, _, config, _ = _grib_setup()

        response = self._prepare(config, sizes=[MAX_SINGLE_PUT_BYTES + 1])

        self.assertEqual(response.status_code, 400)
        self.assertIn(GRIB_NAME, response.json()["error"])
        self.assertFalse(UploadSession.objects.exists())

    def test_a_forecast_without_a_run_time_is_refused_before_anything_is_recorded(self, mock_incoming):
        mock_incoming.return_value = _bucket()
        _, _, config, _ = _grib_setup()

        response = self._prepare(config, filenames=["wrf.grib2"])

        self.assertEqual(response.status_code, 400)
        self.assertFalse(UploadSession.objects.exists())

    def test_a_geotiff_needs_a_variable_of_this_feed(self, mock_incoming):
        mock_incoming.return_value = _bucket()
        _, _, config, _ = _geotiff_setup()

        response = self.client.post(
            PREPARE_URL.format(config.pk),
            {"filenames": ["20250115.tif"], "sizes": [10], "variable_ids": ["999999"], "times": [""]},
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(Delivery.objects.exists())

    def test_sizes_must_match_the_files(self, mock_incoming):
        mock_incoming.return_value = _bucket()
        _, _, config, _ = _grib_setup()

        self.assertEqual(self._prepare(config, sizes=[]).status_code, 400)
        self.assertEqual(self._prepare(config, sizes=["big"]).status_code, 400)
        self.assertFalse(UploadSession.objects.exists())

    def test_get_is_not_allowed(self, mock_incoming):
        _, _, config, _ = _grib_setup()
        self.assertEqual(self.client.get(PREPARE_URL.format(config.pk)).status_code, 405)


@patch("georiva.ingestion.tasks.process_incoming_file")
@patch("georiva.core.storage.StorageManager.incoming", new_callable=PropertyMock)
class CompleteTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_cmp", "c@u.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _, _, self.config, _ = _grib_setup()
        self.session = UploadSession.objects.create(push_feed=self.config, user=self.user)
        self.delivery = Delivery.receive(
            push_feed=self.config,
            filename=GRIB_NAME,
            file_path=GRIB_KEY,
            session=self.session,
            sent_by=self.user,
            awaits_completion=True,
        )

    def _complete(self, delivery=None):
        return self.client.post(COMPLETE_URL.format(self.config.pk), {"delivery_id": (delivery or self.delivery).pk})

    def test_starts_ingestion_with_the_job_the_page_follows(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket(size=123)

        response = self._complete()

        self.assertEqual(response.status_code, 200)
        job = FileIngestionJob.objects.get(file_path=GRIB_KEY)
        self.assertEqual(response.json(), {"id": self.delivery.pk, "status": "accepted", "job_id": job.pk})
        mock_task.delay.assert_called_once_with(
            file_path=GRIB_KEY,
            origin_bucket=BucketType.INCOMING,
            reference_time="2025-01-15T06:00:00+00:00",
            job_id=job.pk,
        )

    def test_accepts_the_delivery_registers_the_file_and_settles_the_session(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket(size=123)

        self._complete()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.ACCEPTED)
        self.assertEqual(self.delivery.bytes, 123)
        self.assertFalse(self.delivery.awaits_completion)
        self.assertEqual(FileIngestion.objects.get(file_path=GRIB_KEY).status, FileIngestion.Status.PENDING)
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, UploadSession.Status.COMPLETED)

    def test_a_file_that_did_not_arrive_starts_nothing(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket(exists=False)

        response = self._complete()

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["status"], "missing")
        mock_task.delay.assert_not_called()
        self.assertFalse(FileIngestionJob.objects.exists())
        self.delivery.refresh_from_db()
        self.assertTrue(self.delivery.awaits_completion)
        self.assertEqual(self.delivery.status, Delivery.Status.RECEIVED)

    def test_a_rejected_delivery_says_why(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        self.delivery.mark_rejected("Could not extract a valid time")

        response = self._complete()

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["status"], "rejected")
        self.assertIn("valid time", response.json()["error"])
        mock_task.delay.assert_not_called()

    def test_calling_again_returns_the_same_job_and_enqueues_nothing_more(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        first = self._complete().json()

        second = self._complete().json()

        self.assertEqual(second["job_id"], first["job_id"])
        mock_task.delay.assert_called_once()
        self.assertEqual(FileIngestionJob.objects.count(), 1)

    def test_a_notification_that_got_there_first_still_leaves_one_enqueue(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        # The notification admitted the file and registered it, then waited for this call.
        self.delivery.mark_accepted(file_path=GRIB_KEY, bytes=123)
        FileIngestion.register(bucket=BucketType.INCOMING, file_path=GRIB_KEY)

        response = self._complete()

        self.assertEqual(response.status_code, 200)
        mock_task.delay.assert_called_once()
        self.assertEqual(mock_task.delay.call_args.kwargs["job_id"], response.json()["job_id"])

    def test_a_reupload_of_a_finished_file_is_ingested_again(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        log, _ = FileIngestion.register(bucket=BucketType.INCOMING, file_path=GRIB_KEY)
        FileIngestion.objects.filter(pk=log.pk).update(status=FileIngestion.Status.COMPLETED)

        self._complete()

        log.refresh_from_db()
        self.assertEqual(log.status, FileIngestion.Status.PENDING)
        mock_task.delay.assert_called_once()

    def test_someone_elses_delivery_is_not_found(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        other = User.objects.create_superuser("admin_other", "o@u.com", "pw")
        self.client.force_login(other)

        self.assertEqual(self._complete().status_code, 404)
        mock_task.delay.assert_not_called()

    def test_a_delivery_of_another_feed_is_not_found(self, mock_incoming, mock_task):
        mock_incoming.return_value = _bucket()
        other_feed = PushFeed.objects.create(
            catalog=Catalog.objects.create(
                organisation=self.config.catalog.organisation, name="Other", slug="other", file_format="grib2"
            ),
            name="Other",
        )

        response = self.client.post(COMPLETE_URL.format(other_feed.pk), {"delivery_id": self.delivery.pk})

        self.assertEqual(response.status_code, 404)

    def test_get_is_not_allowed(self, mock_incoming, mock_task):
        self.assertEqual(self.client.get(COMPLETE_URL.format(self.config.pk)).status_code, 405)


class ConsumerWaitsForCompletionTests(TestCase):
    """The notification admits a presigned upload but leaves starting it to the browser's call."""

    def setUp(self):
        self.user = User.objects.create(username="amina")
        self.org = make_organisation("test-org")
        catalog = Catalog.objects.create(organisation=self.org, name="WRF", slug="wrf", file_format="netcdf")
        self.feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        self.key = "test-org/wrf/GR--20260101T0000--wrf.nc"
        patcher = patch("georiva.ingestion.quarantine.storage")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _arrive(self):
        ev = {
            "s3": {"bucket": {"name": "georiva-incoming"}, "object": {"key": self.key, "size": 42}},
            "userIdentity": {"principalId": "minioadmin"},
        }
        with (
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.INCOMING),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            _handle_event(ev)
        return task

    def _receive(self, **extra):
        return Delivery.receive(push_feed=self.feed, filename="wrf.nc", file_path=self.key, sent_by=self.user, **extra)

    def test_admits_and_registers_the_file_but_does_not_enqueue_it(self):
        delivery = self._receive(awaits_completion=True)

        task = self._arrive()

        task.delay.assert_not_called()
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, Delivery.Status.ACCEPTED)
        self.assertEqual(FileIngestion.objects.get(file_path=self.key).status, FileIngestion.Status.PENDING)

    def test_a_posted_upload_is_enqueued_as_before(self):
        self._receive()

        self._arrive().delay.assert_called_once()

    def test_enqueues_once_the_browser_has_completed(self):
        self._receive(awaits_completion=False)

        self._arrive().delay.assert_called_once()

    def test_the_latest_delivery_for_a_key_decides(self):
        self._receive(awaits_completion=False)
        self._receive(awaits_completion=True)

        self._arrive().delay.assert_not_called()

    def test_a_prepared_delivery_still_admits_the_file_up_to_the_window(self):
        delivery = self._receive(awaits_completion=True)
        Delivery.objects.filter(pk=delivery.pk).update(
            received_at=timezone.now() - WAITING_WINDOW + timedelta(minutes=5)
        )

        self._arrive()

        delivery.refresh_from_db()
        self.assertEqual(delivery.status, Delivery.Status.ACCEPTED)


class AbandonedDeliveryTests(TestCase):
    """A prepared delivery no file ever answered is rejected once the consumer would stop waiting."""

    def setUp(self):
        self.user = User.objects.create(username="amina")
        catalog = Catalog.objects.create(
            organisation=make_organisation("test-org"), name="WRF", slug="wrf", file_format="netcdf"
        )
        self.feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        self.session = UploadSession.objects.create(push_feed=self.feed, user=self.user)

    def _delivery(self, name, age, **extra):
        delivery = Delivery.receive(
            push_feed=self.feed,
            filename=name,
            file_path=f"test-org/wrf/{name}",
            session=self.session,
            sent_by=self.user,
            awaits_completion=extra.pop("awaits_completion", True),
            **extra,
        )
        Delivery.objects.filter(pk=delivery.pk).update(received_at=timezone.now() - age)
        return delivery

    def test_rejects_a_stale_prepared_delivery_and_records_why(self):
        delivery = self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))

        self.assertEqual(reject_abandoned(), 1)

        delivery.refresh_from_db()
        self.assertEqual(delivery.status, Delivery.Status.REJECTED)
        self.assertEqual(delivery.reason, ABANDONED_REASON)
        event = DeliveryRejected.objects.get(delivery=delivery)
        self.assertEqual(event.reason, ABANDONED_REASON)
        self.assertEqual(event.correlation_id, delivery.correlation_id)

    def test_fails_a_session_none_of_whose_files_arrived(self):
        self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))

        reject_abandoned()

        self.session.refresh_from_db()
        self.assertEqual(self.session.status, UploadSession.Status.FAILED)

    def test_completes_a_session_when_the_other_files_were_accepted(self):
        self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))
        accepted = self._delivery("b.nc", timedelta(minutes=30))
        accepted.mark_accepted(check_session=False)

        reject_abandoned()

        self.session.refresh_from_db()
        self.assertEqual(self.session.status, UploadSession.Status.COMPLETED)

    def test_leaves_a_session_active_while_another_file_may_still_arrive(self):
        self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))
        waiting = self._delivery("b.nc", timedelta(minutes=30))

        reject_abandoned()

        self.session.refresh_from_db()
        self.assertEqual(self.session.status, UploadSession.Status.ACTIVE)
        waiting.refresh_from_db()
        self.assertEqual(waiting.status, Delivery.Status.RECEIVED)

    def test_leaves_a_delivery_still_within_the_window(self):
        delivery = self._delivery("a.nc", WAITING_WINDOW - timedelta(minutes=5))

        self.assertEqual(reject_abandoned(), 0)

        delivery.refresh_from_db()
        self.assertEqual(delivery.status, Delivery.Status.RECEIVED)

    def test_leaves_a_delivery_that_was_not_a_presigned_upload(self):
        delivery = self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1), awaits_completion=False)

        self.assertEqual(reject_abandoned(), 0)

        delivery.refresh_from_db()
        self.assertEqual(delivery.status, Delivery.Status.RECEIVED)

    def test_leaves_a_delivery_whose_file_arrived(self):
        delivery = self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))
        delivery.mark_accepted(check_session=False)

        self.assertEqual(reject_abandoned(), 0)

    def test_running_again_finds_nothing_more(self):
        self._delivery("a.nc", WAITING_WINDOW + timedelta(minutes=1))

        reject_abandoned()

        self.assertEqual(reject_abandoned(), 0)
