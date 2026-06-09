from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.ingestion.models import DataArrival, FileIngestion
from georiva.sources.loader import LoaderRunResult
from georiva.sources.models import DataFeed


def _make_feed():
    return DataFeed.objects.create(name="Test Feed")


def _make_collection():
    catalog = Catalog.objects.create(name="Test", slug="test-cat", file_format="grib2")
    return Collection.objects.create(name="Test Col", slug="test-col", catalog=catalog)


class RecordRunCreatesDataArrivalTests(TestCase):
    def setUp(self):
        self.feed = _make_feed()
        self.collection = _make_collection()

    def test_successful_run_creates_scheduled_data_arrival(self):
        result = LoaderRunResult(
            files_requested=4,
            files_fetched=3,
            files_skipped=1,
            files_failed=0,
            bytes_transferred=2048,
        )
        result.finish()

        self.feed.record_run(result, self.collection)

        arrival = DataArrival.objects.get(
            trigger=DataArrival.Trigger.SCHEDULED,
            data_feed=self.feed,
        )
        self.assertEqual(arrival.status, DataArrival.Status.COMPLETED)
        self.assertEqual(arrival.files_requested, 4)
        self.assertEqual(arrival.files_fetched, 3)
        self.assertEqual(arrival.files_skipped, 1)
        self.assertEqual(arrival.files_failed, 0)
        self.assertEqual(arrival.bytes_transferred, 2048)
        self.assertEqual(arrival.collection, self.collection)

    def test_record_run_links_file_ingestions_to_arrival(self):
        paths = [
            "test-cat/test-col/file_a.grib2",
            "test-cat/test-col/file_b.grib2",
        ]
        preliminary = DataArrival.objects.create(trigger=DataArrival.Trigger.MANUAL_UPLOAD)
        for path in paths:
            FileIngestion.objects.create(
                bucket=BucketType.SOURCES,
                file_path=path,
                catalog_slug="test-cat",
                collection_slug="test-col",
                data_arrival=preliminary,
            )

        result = LoaderRunResult(files_fetched=2, stored_paths=paths)
        result.finish()

        self.feed.record_run(result, self.collection)

        arrival = DataArrival.objects.get(trigger=DataArrival.Trigger.SCHEDULED)
        linked = FileIngestion.objects.filter(data_arrival=arrival)
        self.assertEqual(linked.count(), 2)
        self.assertSetEqual(set(linked.values_list("file_path", flat=True)), set(paths))
