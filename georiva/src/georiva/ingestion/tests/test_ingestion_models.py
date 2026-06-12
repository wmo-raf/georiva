from datetime import datetime

import pytz
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.ingestion.models import FileIngestion, FileIngestionJob


def _setup():
    catalog = Catalog.objects.create(name="wrf", slug="wrf", file_format="netcdf")
    collection = Collection.objects.create(
        catalog=catalog, name="Forecast", slug="wrf-forecast-collection-1",
    )
    log, _ = FileIngestion.register(
        bucket="incoming",
        file_path="wrf/file.nc",
    )
    return collection, log


class FileIngestionItemLinkTests(TestCase):
    """
    ItemHandler.delete_orphan() must not break subsequent timestamps.

    Regression: FileIngestion.item was on_delete=CASCADE; deleting an orphan
    Item mid-run cascade-deleted the FileIngestion record, causing every
    subsequent save() to fail. The item FK has since been removed — Items are
    now linked via Item.source_file. This test verifies that delete_orphan()
    is safe and that the next timestamp can still create an Item.
    """

    def test_orphan_deletion_does_not_break_next_timestamp(self):
        collection, log = _setup()
        handler = ItemHandler()
        kwargs = dict(
            collection=collection,
            reference_time=pytz.utc.localize(datetime(2023, 7, 1, 6)),
            source_file="incoming:wrf/file.nc",
            ingestion_log=log,
            bounds=[0.0, 0.0, 10.0, 10.0],
            width=10,
            height=10,
            crs="EPSG:4326",
        )

        # Timestamp 1: item created, then all variables fail and orphan is deleted.
        item1, _ = handler.get_or_create(
            timestamp=pytz.utc.localize(datetime(2023, 7, 1, 6)), **kwargs
        )
        handler.delete_orphan(item1)

        # Timestamp 2 must still succeed — FileIngestion must still exist.
        item2, created = handler.get_or_create(
            timestamp=pytz.utc.localize(datetime(2023, 7, 2, 6)), **kwargs
        )
        self.assertTrue(created)
        log.refresh_from_db()
        self.assertEqual(log.status, FileIngestion.Status.PENDING)


class FileIngestionJobLinkTests(TestCase):
    """
    Retries create a new FileIngestionJob per process_incoming_file
    invocation, all pointing at the same FileIngestion.

    Regression: file_ingestion was a OneToOneField, so the second run of a
    retried file failed with 'duplicate key value violates unique constraint
    georivaingestion_fileingestionjob_file_ingestion_id_key'.
    """

    def test_multiple_jobs_can_link_the_same_file_ingestion(self):
        _, log = _setup()
        ct = ContentType.objects.get_for_model(
            FileIngestionJob, for_concrete_model=False
        )

        for _run in range(2):
            job = FileIngestionJob.objects.create(
                user=None,
                content_type=ct,
                file_path=log.file_path,
                bucket=log.bucket,
            )
            job.file_ingestion = log
            job.save(update_fields=["file_ingestion"])

        self.assertEqual(log.jobs.count(), 2)


class JobCrashLockReleaseTests(TestCase):
    """
    An unexpected crash in a FileIngestionJob run must release the
    FileIngestion lock.

    Regression: on_error() only logged, so a crash after acquire() left the
    record stuck in 'processing' — with retries exhausted, unreclaimable
    even by the stale-lock sweep.
    """

    def _job_for(self, log):
        ct = ContentType.objects.get_for_model(
            FileIngestionJob, for_concrete_model=False
        )
        return FileIngestionJob.objects.create(
            user=None, content_type=ct,
            file_path=log.file_path, bucket=log.bucket,
        )

    def test_on_error_releases_own_lock(self):
        from georiva.ingestion.job_types import FileIngestionJobType

        _, log = _setup()
        job = self._job_for(log)
        self.assertTrue(
            FileIngestion.acquire(log.bucket, log.file_path, f"task-ferry-job-{job.id}")
        )

        FileIngestionJobType().on_error(job, RuntimeError("boom"))

        log.refresh_from_db()
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertEqual(log.locked_by, "")
        self.assertIn("boom", log.error)

    def test_on_error_does_not_clobber_another_workers_lock(self):
        from georiva.ingestion.job_types import FileIngestionJobType

        _, log = _setup()
        job = self._job_for(log)
        self.assertTrue(
            FileIngestion.acquire(log.bucket, log.file_path, "some-other-worker")
        )

        FileIngestionJobType().on_error(job, RuntimeError("boom"))

        log.refresh_from_db()
        self.assertEqual(log.status, FileIngestion.Status.PROCESSING)
        self.assertEqual(log.locked_by, "some-other-worker")



class FileIngestionSummaryFieldTests(TestCase):
    """Processing summary fields are stored and readable."""

    def test_summary_fields_persist(self):
        from datetime import datetime, timezone

        log, _ = FileIngestion.register(bucket="incoming", file_path="summary/file.nc")
        t_start = datetime(2024, 6, 1, 0, 0, tzinfo=timezone.utc)
        t_end = datetime(2024, 6, 30, 0, 0, tzinfo=timezone.utc)

        log.variables_discovered = 5
        log.valid_time_start = t_start
        log.valid_time_end = t_end
        log.timestep_count = 30
        log.save(update_fields=['variables_discovered', 'valid_time_start', 'valid_time_end', 'timestep_count'])

        log.refresh_from_db()
        self.assertEqual(log.variables_discovered, 5)
        self.assertEqual(log.valid_time_start, t_start)
        self.assertEqual(log.valid_time_end, t_end)
        self.assertEqual(log.timestep_count, 30)

    def test_mark_completed_writes_summary_fields(self):
        from datetime import datetime, timezone

        FileIngestion.register(bucket="incoming", file_path="mark/file.nc")
        t_start = datetime(2024, 7, 1, tzinfo=timezone.utc)
        t_end = datetime(2024, 7, 31, tzinfo=timezone.utc)

        FileIngestion.mark_completed(
            bucket="incoming",
            file_path="mark/file.nc",
            items_created=4,
            assets_created=8,
            variables_discovered=3,
            valid_time_start=t_start,
            valid_time_end=t_end,
            timestep_count=31,
        )

        log = FileIngestion.objects.get(file_path="mark/file.nc")
        self.assertEqual(log.status, FileIngestion.Status.COMPLETED)
        self.assertEqual(log.variables_discovered, 3)
        self.assertEqual(log.valid_time_start, t_start)
        self.assertEqual(log.valid_time_end, t_end)
        self.assertEqual(log.timestep_count, 31)
