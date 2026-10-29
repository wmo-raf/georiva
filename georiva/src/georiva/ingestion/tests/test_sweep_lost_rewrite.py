"""The Sweep finds a rewrite whose Bucket notification was lost (#549)."""

from unittest.mock import MagicMock, PropertyMock, patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.ingestion.models import Delivery, FileIngestion, FileIngestionJob, PushFeed
from georiva.ingestion.service import IngestionResult
from georiva.organisations.testing import make_organisation

KEY = "test-org/wrf/GR--20260101T0000--wrf.nc"


class SweepLostRewriteTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation("test-org"), name="WRF", slug="wrf", file_format="netcdf"
        )
        self.feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        patcher = patch("georiva.ingestion.quarantine.storage")
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = patch.object(FileIngestion, "has_live_data", new_callable=PropertyMock, return_value=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _completed(self, bucket=BucketType.INCOMING, read_version="v1", while_locked=lambda: None):
        """KEY ingested from ``bucket``, having read the write ``read_version``, after its delivery."""
        delivery = Delivery.receive(push_feed=self.feed, filename="wrf.nc", file_path=KEY)
        delivery.mark_accepted(file_path=KEY)
        locked_at = timezone.now()
        while_locked()
        FileIngestion.objects.create(
            bucket=bucket,
            file_path=KEY,
            status=FileIngestion.Status.COMPLETED,
            locked_at=locked_at,
            completed_at=timezone.now(),
            read_version=read_version,
        )

    def _sweep(self, stored_version, bucket=BucketType.INCOMING):
        """Run the Sweep with KEY in ``bucket``, holding the write ``stored_version``."""
        from georiva.ingestion.tasks import sweep_unprocessed

        def bucket_for(bucket_type):
            b = MagicMock()
            b.list_files.return_value = [{"path": KEY}] if bucket_type == bucket else []
            b.version_of.return_value = stored_version
            return b

        meta = {"org": "test-org", "catalog": "wrf", "reference_time": None}
        with (
            patch("georiva.ingestion.unprocessed.storage") as mock_storage,
            patch("georiva.ingestion.unprocessed.validate_path", return_value=meta),
            patch("georiva.ingestion.consumer.validate_path", return_value=meta),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            mock_storage.bucket.side_effect = bucket_for
            task.run = MagicMock()
            sweep_unprocessed(sync=True)
        return task

    def test_a_write_the_ingestion_never_read_is_found_recorded_and_ingested(self):
        self._completed(read_version="v1")

        task = self._sweep("v2")

        found = Delivery.objects.get(file_path=KEY, found_by_sweep=True)
        self.assertEqual(found.status, Delivery.Status.ACCEPTED)
        self.assertEqual(found.push_feed, self.feed)
        task.run.assert_called_once()
        self.assertEqual(task.run.call_args.kwargs["file_path"], KEY)
        self.assertEqual(FileIngestion.objects.get(file_path=KEY).status, FileIngestion.Status.PENDING)

    def test_the_file_a_partial_ingest_kept_is_not_ingested_again(self):
        self._completed(read_version="v1")

        for _ in range(2):
            task = self._sweep("v1")
            task.run.assert_not_called()

        self.assertFalse(Delivery.objects.filter(found_by_sweep=True).exists())
        self.assertEqual(FileIngestion.objects.get(file_path=KEY).status, FileIngestion.Status.COMPLETED)

    def test_an_ingestion_that_recorded_no_write_is_left_alone(self):
        # Completed before the read write was recorded: nothing to tell a rewrite from a partial ingest.
        self._completed(read_version="")

        task = self._sweep("v2")

        task.run.assert_not_called()
        self.assertFalse(Delivery.objects.filter(found_by_sweep=True).exists())

    def test_a_file_in_sources_is_unchanged(self):
        self._completed(bucket=BucketType.SOURCES, read_version="v1")

        task = self._sweep("v2", bucket=BucketType.SOURCES)

        task.run.assert_not_called()
        self.assertEqual(FileIngestion.objects.get(file_path=KEY).status, FileIngestion.Status.COMPLETED)

    def test_a_rewrite_whose_notification_did_arrive_is_not_recorded_again(self):
        self._completed(read_version="v1")
        rewrite = Delivery.receive(push_feed=self.feed, filename="wrf.nc", file_path=KEY)
        rewrite.mark_accepted(file_path=KEY)

        task = self._sweep("v2")

        task.run.assert_called_once()
        self.assertFalse(Delivery.objects.filter(found_by_sweep=True).exists())

    def test_a_rewrite_delivered_while_the_key_was_ingested_is_not_recorded_again(self):
        # The run has completed but not yet queued the write it saw arrive (#548).
        def rewrite():
            Delivery.receive(push_feed=self.feed, filename="wrf.nc", file_path=KEY).mark_accepted(file_path=KEY)

        self._completed(read_version="v1", while_locked=rewrite)

        task = self._sweep("v2")

        task.run.assert_called_once()
        self.assertFalse(Delivery.objects.filter(found_by_sweep=True).exists())

    def test_ingest_now_ingests_a_rewrite_found_under_the_feeds_catalog(self):
        from georiva.ingestion.unprocessed import UnprocessedFile, ingest_unprocessed

        self._completed(read_version="v1")
        meta = {"org": "test-org", "catalog": "wrf", "reference_time": None}
        with (
            patch("georiva.ingestion.consumer.validate_path", return_value=meta),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            queued = ingest_unprocessed([UnprocessedFile(BucketType.INCOMING, KEY, "rewritten")])

        self.assertEqual(queued, 1)
        task.delay.assert_called_once()
        self.assertTrue(Delivery.objects.filter(file_path=KEY, found_by_sweep=True).exists())
        self.assertEqual(FileIngestion.objects.get(file_path=KEY).status, FileIngestion.Status.PENDING)


class IngestionRecordsTheWriteItReadTests(TestCase):
    def test_a_completed_ingestion_records_the_write_it_read(self):
        from georiva.ingestion.job_types import FileIngestionJobType

        FileIngestion.register(bucket=BucketType.SOURCES, file_path=KEY)
        result = IngestionResult(
            origin_file=KEY,
            origin_bucket=BucketType.SOURCES,
            catalog_slug="wrf",
            collection_slug="",
            success=True,
            timestamp=timezone.now(),
            read_version="etag@2026-01-01",
        )
        result.items_created.append("1")
        ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
        job = FileIngestionJob.objects.create(user=None, content_type=ct, file_path=KEY, bucket=BucketType.SOURCES)

        with (
            patch("georiva.ingestion.service.IngestionService.process_file", return_value=result),
            patch("georiva.ingestion.progress.PublishingProgress"),
        ):
            FileIngestionJobType().run(job, MagicMock())

        log = FileIngestion.objects.get(file_path=KEY)
        self.assertEqual(log.status, FileIngestion.Status.COMPLETED)
        self.assertEqual(log.read_version, "etag@2026-01-01")
