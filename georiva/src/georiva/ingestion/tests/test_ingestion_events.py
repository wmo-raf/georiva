import json

import redis
from django.conf import settings
from django.test import TestCase

from georiva.ingestion.events import CHANNEL


class IngestionEventsTestCase(TestCase):

    def setUp(self):
        self.r = redis.from_url(settings.REDIS_URL)
        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(CHANNEL)
        for _ in range(20):
            msg = self.pubsub.get_message(timeout=0.1)
            if msg and msg.get("type") == "subscribe":
                break

    def tearDown(self):
        self.pubsub.unsubscribe()
        self.pubsub.close()

    def _drain(self):
        while self.pubsub.get_message(ignore_subscribe_messages=True):
            pass

    def _next_event(self, timeout=2.0):
        msg = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
        if msg is None:
            return None
        return json.loads(msg["data"])


# =============================================================================
# Cycle 1: publish_event() delivers to the channel
# =============================================================================

class PublishEventTests(IngestionEventsTestCase):

    def test_published_event_is_received_on_channel(self):
        from georiva.ingestion.events import publish_event

        publish_event({"type": "test.ping", "value": 42})

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "test.ping")
        self.assertEqual(event["value"], 42)


# =============================================================================
# Cycle 2: FileIngestion creation publishes file_ingestion.created
# =============================================================================

class FileIngestionCreatedEventTests(IngestionEventsTestCase):

    def test_creation_publishes_created_event(self):
        from georiva.ingestion.models import FileIngestion
        fi, _ = FileIngestion.register(bucket="incoming", file_path="cat/file.grib2")

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "file_ingestion.created")
        self.assertEqual(event["id"], fi.pk)
        self.assertIn("status", event)
        self.assertIn("bucket", event)
        self.assertIn("file_path", event)

    def test_creation_event_is_not_status_changed(self):
        from georiva.ingestion.models import FileIngestion
        FileIngestion.register(bucket="incoming", file_path="cat/file2.grib2")

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertNotEqual(event["type"], "file_ingestion.status_changed")


# =============================================================================
# Cycle 3: FileIngestion status changes publish file_ingestion.status_changed
# =============================================================================

class FileIngestionStatusEventTests(IngestionEventsTestCase):

    def test_status_change_publishes_event(self):
        from georiva.ingestion.models import FileIngestion
        fi, _ = FileIngestion.register(bucket="incoming", file_path="cat/file3.grib2")
        self._drain()

        fi.status = "processing"
        fi.save(update_fields=["status"])

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "file_ingestion.status_changed")
        self.assertEqual(event["id"], fi.pk)
        self.assertEqual(event["status"], "processing")


# =============================================================================
# Cycle 4: FileIngestionJob state changes publish an event
# =============================================================================

class FileIngestionJobStateEventTests(IngestionEventsTestCase):

    def _make_job(self):
        from django.contrib.contenttypes.models import ContentType
        from georiva.ingestion.models import FileIngestionJob
        ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
        return FileIngestionJob.objects.create(
            user=None,
            content_type=ct,
            file_path="catalog/somefile.grib2",
            bucket="incoming",
        )

    def test_state_change_publishes_event(self):
        job = self._make_job()
        self._drain()

        job.mark_started()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "job.state_changed")
        self.assertEqual(event["id"], job.pk)
        self.assertEqual(event["state"], "started")

    def test_creation_does_not_publish_event(self):
        self._make_job()

        event = self._next_event(timeout=0.5)
        self.assertIsNone(event)


# =============================================================================
# Cycle 5: PublishingProgress._publish() emits a job.progress_updated event
# =============================================================================

class PublishingProgressEventTests(IngestionEventsTestCase):

    def test_increment_publishes_progress_event(self):
        from georiva.ingestion.progress import PublishingProgress

        progress = PublishingProgress(total=100, job_id=42)
        progress.increment(by=10, state="file opened")

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "job.progress_updated")
        self.assertEqual(event["job_id"], 42)
        self.assertEqual(event["state"], "file opened")
        self.assertIn("percentage", event)

    def test_no_job_id_does_not_publish(self):
        from georiva.ingestion.progress import PublishingProgress

        progress = PublishingProgress(total=100)
        progress.increment(by=10, state="file opened")

        event = self._next_event(timeout=0.5)
        self.assertIsNone(event)


# =============================================================================
# Cycle 6: Acquisition model events (FetchRun, UploadSession)
# =============================================================================

class FetchRunEventTests(IngestionEventsTestCase):

    def _make_feed(self):
        from georiva.core.models import Catalog
        from georiva.sources.models import DataFeed
        catalog = Catalog.objects.create(name="FR", slug="fr-ev", file_format="grib2")
        return DataFeed.objects.create(name="FR Feed", catalog=catalog)

    def test_fetch_run_creation_publishes_event(self):
        from georiva.sources.models import FetchRun
        feed = self._make_feed()
        run = FetchRun.objects.create(data_feed=feed)

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "fetch_run.created")
        self.assertEqual(event["id"], run.pk)

    def test_fetch_run_status_change_publishes_event(self):
        from georiva.sources.models import FetchRun
        feed = self._make_feed()
        run = FetchRun.objects.create(data_feed=feed)
        self._drain()

        run.mark_completed()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "fetch_run.status_changed")
        self.assertEqual(event["id"], run.pk)
        self.assertEqual(event["status"], "completed")


class UploadSessionEventTests(IngestionEventsTestCase):

    def _make_session(self):
        from georiva.core.models import Catalog
        from georiva.ingestion.models import UploadSession
        catalog = Catalog.objects.create(name="US", slug="us-ev", file_format="geotiff")
        return UploadSession.objects.create(catalog=catalog)

    def test_upload_session_creation_publishes_event(self):
        session = self._make_session()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "upload_session.created")
        self.assertEqual(event["id"], session.pk)

    def test_upload_session_status_change_publishes_event(self):
        session = self._make_session()
        self._drain()

        session.mark_failed()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "upload_session.status_changed")
        self.assertEqual(event["id"], session.pk)
        self.assertEqual(event["status"], "failed")


# =============================================================================
# Cycle 7: Per-file acquisition events (FetchedFile, UploadedFile)
# =============================================================================

class FetchedFileEventTests(IngestionEventsTestCase):

    def _make_run(self):
        from georiva.core.models import Catalog
        from georiva.sources.models import DataFeed, FetchRun
        catalog = Catalog.objects.create(name="FF", slug="ff-ev", file_format="grib2")
        feed = DataFeed.objects.create(name="FF Feed", catalog=catalog)
        return FetchRun.objects.create(data_feed=feed)

    def test_mark_fetching_publishes_status_changed_event(self):
        from georiva.sources.models import FetchedFile
        run = self._make_run()
        ff = FetchedFile.objects.create(fetch_run=run, file_path="cat/file.grib2")
        self._drain()

        ff.mark_fetching()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "fetched_file.status_changed")
        self.assertEqual(event["id"], ff.pk)
        self.assertEqual(event["fetch_run_id"], run.pk)
        self.assertEqual(event["status"], "fetching")
        self.assertEqual(event["file_path"], "cat/file.grib2")

    def test_mark_stored_publishes_status_changed_event(self):
        from georiva.sources.models import FetchedFile
        run = self._make_run()
        ff = FetchedFile.objects.create(fetch_run=run, file_path="cat/file2.grib2")
        self._drain()

        ff.mark_stored(bytes_transferred=1024)

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "fetched_file.status_changed")
        self.assertEqual(event["status"], "stored")

    def test_mark_failed_publishes_status_changed_event(self):
        from georiva.sources.models import FetchedFile
        run = self._make_run()
        ff = FetchedFile.objects.create(fetch_run=run, file_path="cat/file3.grib2")
        self._drain()

        ff.mark_failed(error="timeout")

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "fetched_file.status_changed")
        self.assertEqual(event["status"], "failed")

    def test_creation_does_not_publish_event(self):
        from georiva.sources.models import FetchedFile
        run = self._make_run()
        self._drain()
        FetchedFile.objects.create(fetch_run=run, file_path="cat/file4.grib2")

        event = self._next_event(timeout=0.5)
        self.assertIsNone(event)


class UploadedFileEventTests(IngestionEventsTestCase):

    def _make_session(self):
        from georiva.core.models import Catalog
        from georiva.ingestion.models import UploadSession
        catalog = Catalog.objects.create(name="UF", slug="uf-ev", file_format="geotiff")
        return UploadSession.objects.create(catalog=catalog)

    def test_mark_uploading_publishes_status_changed_event(self):
        from georiva.ingestion.models import UploadedFile
        session = self._make_session()
        uf = UploadedFile.objects.create(session=session, original_filename="rain.tif")
        self._drain()

        uf.mark_uploading()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "uploaded_file.status_changed")
        self.assertEqual(event["id"], uf.pk)
        self.assertEqual(event["session_id"], session.pk)
        self.assertEqual(event["status"], "uploading")
        self.assertEqual(event["filename"], "rain.tif")

    def test_mark_stored_publishes_status_changed_event(self):
        from georiva.ingestion.models import UploadedFile
        session = self._make_session()
        uf = UploadedFile.objects.create(session=session, original_filename="rain2.tif")
        self._drain()

        uf.mark_stored(file_path="cat/rain2.tif", bytes=512)

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "uploaded_file.status_changed")
        self.assertEqual(event["status"], "stored")

    def test_mark_failed_publishes_status_changed_event(self):
        from georiva.ingestion.models import UploadedFile
        session = self._make_session()
        uf = UploadedFile.objects.create(session=session, original_filename="rain3.tif")
        self._drain()

        uf.mark_failed(error="disk full")

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "uploaded_file.status_changed")
        self.assertEqual(event["status"], "failed")

    def test_creation_does_not_publish_event(self):
        from georiva.ingestion.models import UploadedFile
        session = self._make_session()
        self._drain()
        UploadedFile.objects.create(session=session, original_filename="rain4.tif")

        event = self._next_event(timeout=0.5)
        self.assertIsNone(event)
