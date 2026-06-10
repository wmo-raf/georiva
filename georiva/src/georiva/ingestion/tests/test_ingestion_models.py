from datetime import datetime

import pytz
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Item
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.ingestion.models import DataArrival, FileIngestion, FileIngestionJob


def _setup():
    catalog = Catalog.objects.create(name="wrf", slug="wrf", file_format="netcdf")
    collection = Collection.objects.create(
        catalog=catalog, name="Forecast", slug="wrf-forecast-collection-1",
    )
    arrival = DataArrival.objects.create(
        trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        status=DataArrival.Status.PENDING,
        file_path="wrf/file.nc",
    )
    log, _ = FileIngestion.register(
        bucket="incoming",
        file_path="wrf/file.nc",
        catalog_slug="wrf",
        data_arrival=arrival,
    )
    return collection, log


class FileIngestionItemLinkTests(TestCase):
    """
    Deleting an Item must never delete the FileIngestion lock/audit record.

    Regression: FileIngestion.item was on_delete=CASCADE, so
    ItemHandler.delete_orphan() (run when a timestamp produced no assets)
    cascade-deleted the FileIngestion mid-run, and every subsequent
    timestamp failed with 'Save with update_fields did not affect any rows.'
    """

    def test_item_delete_nulls_link_instead_of_cascading(self):
        collection, log = _setup()
        item = Item.objects.create(
            collection=collection,
            time=pytz.utc.localize(datetime(2023, 7, 1, 6)),
        )
        log.item = item
        log.save(update_fields=["item"])

        item.delete()

        log.refresh_from_db()
        self.assertIsNone(log.item)

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

        # Timestamp 1: item created and linked, then all variables fail
        # and the orphan item is deleted.
        item1, _ = handler.get_or_create(
            timestamp=pytz.utc.localize(datetime(2023, 7, 1, 6)), **kwargs
        )
        handler.delete_orphan(item1)

        # Timestamp 2 must still be able to link the same FileIngestion.
        item2, created = handler.get_or_create(
            timestamp=pytz.utc.localize(datetime(2023, 7, 2, 6)), **kwargs
        )
        self.assertTrue(created)
        log.refresh_from_db()
        self.assertEqual(log.item_id, item2.pk)


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
