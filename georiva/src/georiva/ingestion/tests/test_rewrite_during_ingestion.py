"""A sender writes a key again while that key is being ingested (#548)."""

import shutil
import tempfile
from unittest.mock import MagicMock, patch

from django.contrib.contenttypes.models import ContentType
from django.core.files.storage import FileSystemStorage
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.core.storage.manager import Bucket
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.handlers.source_file_manager import SourceFileManager
from georiva.ingestion.models import Delivery, FileIngestion, FileIngestionJob, PushFeed, PushSender
from georiva.ingestion.service import IngestionResult
from georiva.organisations.testing import make_organisation

KEY = "test-org/wrf/GR--20260101T0000--wrf.nc"


def _result(success):
    result = IngestionResult(
        origin_file=KEY,
        origin_bucket=BucketType.INCOMING,
        catalog_slug="wrf",
        collection_slug="",
        success=success,
        timestamp=timezone.now(),
    )
    if success:
        result.items_created.append("1")
    else:
        result.add_error("could not read the file")
    return result


class RewriteDuringIngestionTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation("test-org"), name="WRF", slug="wrf", file_format="netcdf"
        )
        self.feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        PushSender.objects.create(push_feed=self.feed, label="alpha", access_key="AKALPHA")
        patcher = patch("georiva.ingestion.quarantine.storage")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _arrive(self, sequencer):
        """A Bucket notification for one write of KEY by sender alpha; returns the task it may queue."""
        ev = {
            "s3": {
                "bucket": {"name": "georiva-incoming"},
                "object": {"key": KEY, "size": 42, "sequencer": sequencer, "eTag": sequencer},
            },
            "userIdentity": {"principalId": "AKALPHA"},
        }
        meta = {"org": "test-org", "catalog": "wrf", "collection": None, "reference_time": None}
        with (
            patch("georiva.ingestion.consumer.validate_path", return_value=meta),
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.INCOMING),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            _handle_event(ev)
        return task

    def _ingest(self, *, while_processing=lambda: None, success=True, on_dispatch=None):
        """Run the ingestion job for KEY; ``while_processing`` runs while it holds the lock."""
        from georiva.ingestion.job_types import FileIngestionJobType

        def process_file(**kwargs):
            while_processing()
            result = _result(success)
            return result

        ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
        job = FileIngestionJob.objects.create(user=None, content_type=ct, file_path=KEY, bucket=BucketType.INCOMING)
        with (
            patch("georiva.ingestion.service.IngestionService.process_file", side_effect=process_file),
            patch("georiva.ingestion.progress.PublishingProgress"),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            task.delay.side_effect = on_dispatch
            try:
                FileIngestionJobType().run(job, MagicMock())
            except RuntimeError:
                pass
        return task

    def _status(self):
        return FileIngestion.objects.get(file_path=KEY).status

    def test_a_write_during_a_successful_ingestion_is_ingested_after_it(self):
        self._arrive("seq-1")
        queued_during = []

        def rewrite():
            queued_during.append(self._arrive("seq-2"))

        task = self._ingest(while_processing=rewrite)

        queued_during[0].delay.assert_not_called()
        task.delay.assert_called_once()
        self.assertEqual(task.delay.call_args.kwargs["file_path"], KEY)
        self.assertEqual(self._status(), FileIngestion.Status.PENDING)
        self.assertEqual(Delivery.objects.filter(status=Delivery.Status.ACCEPTED).count(), 2)

    def test_a_write_during_a_failed_ingestion_is_ingested_after_it(self):
        self._arrive("seq-1")

        task = self._ingest(while_processing=lambda: self._arrive("seq-2"), success=False)

        task.delay.assert_called_once()
        log = FileIngestion.objects.get(file_path=KEY)
        self.assertEqual(log.status, FileIngestion.Status.PENDING)
        self.assertEqual(log.retry_count, 0)

    def test_a_write_during_a_crashed_ingestion_is_ingested_after_it(self):
        self._arrive("seq-1")

        def rewrite_then_crash():
            self._arrive("seq-2")
            raise ValueError("worker blew up")

        with self.assertRaises(ValueError):
            self._ingest(while_processing=rewrite_then_crash)
        # The lock it held is released and the newer write is waiting for its own run.
        log = FileIngestion.objects.get(file_path=KEY)
        self.assertEqual(log.status, FileIngestion.Status.PENDING)
        self.assertEqual(log.locked_by, "")

    def test_the_ingestion_after_it_continues_the_newer_deliverys_chain(self):
        from georiva.events.context import current_context

        self._arrive("seq-1")
        seen = []

        self._ingest(
            while_processing=lambda: self._arrive("seq-2"),
            on_dispatch=lambda **kwargs: seen.append(current_context()),
        )

        newer = Delivery.objects.order_by("-id").first()
        self.assertEqual(seen[0].correlation_id, newer.correlation_id)

    def test_a_repeated_notification_for_the_write_being_ingested_starts_nothing(self):
        self._arrive("seq-1")

        task = self._ingest(while_processing=lambda: self._arrive("seq-1"))

        task.delay.assert_not_called()
        self.assertEqual(self._status(), FileIngestion.Status.COMPLETED)
        self.assertEqual(Delivery.objects.count(), 1)

    def test_an_ingestion_nobody_wrote_over_is_left_as_it_ended(self):
        self._arrive("seq-1")

        task = self._ingest()

        task.delay.assert_not_called()
        self.assertEqual(self._status(), FileIngestion.Status.COMPLETED)


class CleanupDeletesOnlyTheReadWriteTests(SimpleTestCase):
    def setUp(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        self.origin = Bucket(BucketType.INCOMING, "incoming")
        self.origin._storage = FileSystemStorage(location=root)
        self.catalog = MagicMock(archive_source_files=False)
        self.result = _result(True)

    def test_the_file_that_was_read_is_deleted(self):
        self.origin.save(KEY, b"first run")
        read = self.origin.version_of(KEY)

        SourceFileManager().cleanup(self.origin, KEY, self.catalog, self.result, read_version=read)

        self.assertFalse(self.origin.exists(KEY))

    def test_a_newer_write_is_kept(self):
        self.origin.save(KEY, b"first run")
        read = self.origin.version_of(KEY)
        self.origin.delete(KEY)
        self.origin.save(KEY, b"the corrected run")

        SourceFileManager().cleanup(self.origin, KEY, self.catalog, self.result, read_version=read)

        self.assertEqual(self.origin.read_bytes(KEY), b"the corrected run")
