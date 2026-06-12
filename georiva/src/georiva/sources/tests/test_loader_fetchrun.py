"""
Tests for Loader incremental FetchRun/FetchedFile tracking.

We mock fetch_strategy and storage to keep these fast and deterministic —
the point is to verify FetchRun/FetchedFile records are written correctly,
not to test network or storage I/O.
"""
from unittest.mock import MagicMock, patch, call

from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, FetchRun, FetchedFile
from georiva.sources.fetch.base import FetchResult


def _make_feed_and_collection():
    catalog = Catalog.objects.create(name="Test", slug="test", file_format="grib2")
    collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
    feed = DataFeed.objects.create(name="Feed", catalog=catalog)
    return feed, collection


def _mock_request(filename="file.grib", reference_time=None):
    req = MagicMock()
    req.filename = filename
    req.reference_time = reference_time
    req.expected_size = None
    return req


def _success_fetch_result(req, bytes_transferred=1024):
    return FetchResult(request=req, success=True, status="success",
                       bytes_transferred=bytes_transferred)


def _failed_fetch_result(req, error="timeout"):
    return FetchResult(request=req, success=False, status="failed", error=error)


class LoaderFetchRunCreationTests(TestCase):
    def setUp(self):
        self.feed, self.collection = _make_feed_and_collection()

    def _run_loader(self, requests, fetch_results, skip_existing=False):
        """Helper: run Loader with mocked strategy + storage, returning the run result."""
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = requests
        loader.data_source.post_process_fetched_file.side_effect = (
            lambda req, path: (path, None)
        )

        fetch_iter = iter(fetch_results)

        with (
            patch.object(loader, '_already_exists', return_value=False),
            patch.object(loader, '_find_existing_catalog_path', return_value=None),
            patch.object(loader, '_fetch_and_store',
                         side_effect=lambda req: next(fetch_iter)),
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            result = loader.run(skip_existing=skip_existing)
        return result

    def test_completed_run_creates_fetch_run(self):
        req = _mock_request("a.grib")
        self._run_loader([req], [_success_fetch_result(req)])

        self.assertEqual(FetchRun.objects.count(), 1)
        run = FetchRun.objects.get()
        self.assertEqual(run.data_feed, self.feed)
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.files_fetched, 1)
        self.assertEqual(run.files_skipped, 0)
        self.assertEqual(run.files_failed, 0)
        self.assertIsNotNone(run.finished_at)

    def test_failed_files_reflected_in_fetch_run(self):
        req = _mock_request("bad.grib")
        self._run_loader([req], [_failed_fetch_result(req)])

        run = FetchRun.objects.get()
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.files_fetched, 0)
        self.assertEqual(run.files_failed, 1)

    def test_no_data_feed_no_fetch_run(self):
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=None,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = []
        loader.data_source.post_process_fetched_file.side_effect = lambda r, p: (p, None)
        with (
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            loader.run()
        self.assertEqual(FetchRun.objects.count(), 0)


class LoaderFetchedFileTrackingTests(TestCase):
    def setUp(self):
        self.feed, self.collection = _make_feed_and_collection()

    def test_successful_fetch_creates_stored_fetched_file(self):
        req = _mock_request("rain.grib")
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = [req]
        loader.data_source.post_process_fetched_file.side_effect = lambda r, p: (p, None)

        with (
            patch.object(loader, '_already_exists', return_value=False),
            patch.object(loader, '_find_existing_catalog_path', return_value=None),
            patch.object(loader, '_fetch_and_store',
                         return_value=_success_fetch_result(req, bytes_transferred=2048)),
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            loader.run()

        ff = FetchedFile.objects.get()
        self.assertEqual(ff.status, "stored")
        self.assertEqual(ff.bytes_transferred, 2048)
        self.assertIsNotNone(ff.completed_at)

    def test_skipped_file_creates_skipped_fetched_file(self):
        req = _mock_request("existing.grib")
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = [req]
        loader.data_source.post_process_fetched_file.side_effect = lambda r, p: (p, None)

        with (
            patch.object(loader, '_already_exists', return_value=True),
            patch.object(loader, '_find_existing_catalog_path', return_value=None),
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            loader.run()

        ff = FetchedFile.objects.get()
        self.assertEqual(ff.status, "skipped")
        self.assertEqual(ff.skip_reason, "already exists")

    def test_failed_fetch_creates_failed_fetched_file(self):
        req = _mock_request("broken.grib")
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = [req]
        loader.data_source.post_process_fetched_file.side_effect = lambda r, p: (p, None)

        with (
            patch.object(loader, '_already_exists', return_value=False),
            patch.object(loader, '_find_existing_catalog_path', return_value=None),
            patch.object(loader, '_fetch_and_store',
                         return_value=_failed_fetch_result(req, error="FTP timeout")),
            patch.object(loader, '_cleanup_temp'),
            patch.object(loader.fetch_strategy, 'connect'),
            patch.object(loader.fetch_strategy, 'disconnect'),
        ):
            loader.run()

        ff = FetchedFile.objects.get()
        self.assertEqual(ff.status, "failed")
        self.assertEqual(ff.error, "FTP timeout")
