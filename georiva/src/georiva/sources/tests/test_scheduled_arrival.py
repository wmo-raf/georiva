"""
Tests for Loader-driven FetchRun creation after a scheduled data-feed run.

Replaces the old DataArrival-based tests that verified DataFeed.record_run().
"""
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, FetchRun
from georiva.sources.fetch.base import FetchResult


def _make_feed_and_collection():
    catalog = Catalog.objects.create(name="Sched", slug="sched", file_format="grib2")
    collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
    feed = DataFeed.objects.create(name="Sched Feed", catalog=catalog)
    return feed, collection


def _mock_request(filename):
    req = MagicMock()
    req.filename = filename
    req.reference_time = None
    req.expected_size = None
    return req


class ScheduledRunCreatesFetchRunTests(TestCase):
    def setUp(self):
        self.feed, self.collection = _make_feed_and_collection()

    def _run(self, requests, fetch_results):
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = requests
        loader.data_source.post_process_fetched_file.side_effect = lambda r, p: (p, None)
        fetch_iter = iter(fetch_results)
        with (
            patch.object(loader, '_already_exists', return_value=False),
            patch.object(loader, '_find_existing_catalog_path', return_value=None),
            patch.object(loader, '_fetch_and_store', side_effect=lambda r: next(fetch_iter)),
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            return loader.run()

    def test_successful_run_creates_completed_fetch_run(self):
        req = _mock_request("rain.grib")
        result = self._run(
            [req],
            [FetchResult(request=req, success=True, status="success",
                         bytes_transferred=4096)],
        )

        run = FetchRun.objects.get(data_feed=self.feed)
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.files_fetched, 1)
        self.assertEqual(run.bytes_transferred, 4096)
        self.assertEqual(run.files_failed, 0)

    def test_feed_stats_updated_after_run(self):
        req = _mock_request("temp.grib")
        self._run(
            [req],
            [FetchResult(request=req, success=True, status="success",
                         bytes_transferred=512)],
        )

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.total_runs, 1)
        self.assertEqual(self.feed.total_files_fetched, 1)
        self.assertIsNotNone(self.feed.last_run_at)

    def test_collection_link_last_run_at_updated(self):
        from georiva.sources.models import DataFeedCollectionLink
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=self.collection
        )
        req = _mock_request("wind.grib")
        self._run(
            [req],
            [FetchResult(request=req, success=True, status="success",
                         bytes_transferred=256)],
        )
        link = self.feed.collection_links.get(collection=self.collection)
        self.assertIsNotNone(link.last_run_at)
