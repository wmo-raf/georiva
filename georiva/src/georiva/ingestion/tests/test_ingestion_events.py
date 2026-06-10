import json

import redis
from django.conf import settings
from django.test import TestCase

from georiva.ingestion.events import CHANNEL
from georiva.ingestion.models import DataArrival

# =============================================================================
# Shared test base — subscribes to ingestion:events before each test
# =============================================================================

class IngestionEventsTestCase(TestCase):

    def setUp(self):
        self.r = redis.from_url(settings.REDIS_URL)
        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(CHANNEL)
        # Wait for the subscribe confirmation so we don't race with publish calls.
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
# Cycle 2: DataArrival status changes publish an event
# =============================================================================

class DataArrivalStatusEventTests(IngestionEventsTestCase):

    def _make_arrival(self, status="pending"):
        from georiva.ingestion.models import DataArrival
        return DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=status,
        )

    def test_status_change_publishes_event(self):
        arrival = self._make_arrival()
        self._drain()  # discard any creation events

        arrival.status = "processing"
        arrival.save(update_fields=["status"])

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "data_arrival.status_changed")
        self.assertEqual(event["id"], arrival.pk)
        self.assertEqual(event["status"], "processing")

    def test_creation_publishes_created_event(self):
        arrival = self._make_arrival()

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "data_arrival.created")
        self.assertEqual(event["id"], arrival.pk)
        self.assertEqual(event["trigger"], DataArrival.Trigger.MANUAL_UPLOAD)
        self.assertIn("status", event)
        self.assertIn("file_path", event)
        self.assertIn("started_at", event)
        self.assertIn("file_ingestions", event)

    def test_creation_does_not_publish_status_changed_event(self):
        self._make_arrival()
        event = self._next_event(timeout=0.5)
        # creation should publish data_arrival.created, not data_arrival.status_changed
        if event:
            self.assertNotEqual(event["type"], "data_arrival.status_changed")


# =============================================================================
# Cycle 3: FileIngestion status changes publish an event
# =============================================================================

class FileIngestionStatusEventTests(IngestionEventsTestCase):

    def _make_file_ingestion(self):
        from georiva.ingestion.models import DataArrival, FileIngestion
        arrival = DataArrival.objects.create(trigger=DataArrival.Trigger.MANUAL_UPLOAD)
        fi, _ = FileIngestion.register(
            bucket="incoming",
            file_path="catalog/somefile.grib2",
            catalog_slug="catalog",
            collection_slug="",
            data_arrival=arrival,
        )
        return fi

    def test_status_change_publishes_event(self):
        fi = self._make_file_ingestion()
        self._drain()

        fi.status = "processing"
        fi.save(update_fields=["status"])

        event = self._next_event()
        self.assertIsNotNone(event)
        self.assertEqual(event["type"], "file_ingestion.status_changed")
        self.assertEqual(event["id"], fi.pk)
        self.assertEqual(event["status"], "processing")

    def test_creation_does_not_publish_file_ingestion_event(self):
        self._make_file_ingestion()
        # drain the data_arrival.created event (DataArrival is created inside _make_file_ingestion)
        self._drain()

        event = self._next_event(timeout=0.5)
        self.assertIsNone(event)


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
