"""Fetch duration is bounded per file and per run (#479). Both limits are driven by a fake clock."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase, override_settings

from georiva.sources.fetch import FailureScope, FileRequest, FTPFetchStrategy, HTTPFetchStrategy
from georiva.sources.loader import Loader
from georiva.sources.models import FetchedFile, FetchRun

from .test_loader_fetchrun import _make_feed_and_collection, _mock_request, _success_fetch_result


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _chunks(clock, count, step):
    """A body that takes `step` seconds per chunk to arrive."""
    for _ in range(count):
        clock.advance(step)
        yield b"x" * 10


class MaxFileSecondsConfigTests(SimpleTestCase):
    def test_defaults_to_fifteen_minutes(self):
        self.assertEqual(HTTPFetchStrategy().max_file_seconds, 900)
        self.assertEqual(FTPFetchStrategy({"host": "h"}).max_file_seconds, 900)

    def test_overridable_in_the_strategy_config(self):
        self.assertEqual(HTTPFetchStrategy({"max_file_seconds": 60}).max_file_seconds, 60)
        self.assertEqual(FTPFetchStrategy({"host": "h", "max_file_seconds": 60}).max_file_seconds, 60)


class HTTPFileDeadlineTests(SimpleTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.clock = FakeClock()
        self.strategy = HTTPFetchStrategy({"max_file_seconds": 100})
        self.strategy.clock = self.clock
        self.strategy._session = MagicMock()

    def _fetch(self, count, step):
        response = MagicMock(status_code=200, headers={})
        response.iter_content.return_value = _chunks(self.clock, count, step)
        self.strategy._session.get.return_value = response
        request = FileRequest(identifier="f", filename="f.grib", params={"url": "http://x/f"})
        return self.strategy.fetch(request, self.tmp / "f.grib"), response

    def test_a_download_still_streaming_at_the_limit_stops_transient_and_leaves_no_file(self):
        result, response = self._fetch(count=50, step=10)

        self.assertFalse(result.success)
        self.assertEqual(result.failure_scope, FailureScope.TRANSIENT)
        self.assertIn("time limit", result.error)
        self.assertFalse((self.tmp / "f.grib").exists())
        response.close.assert_called()

    def test_a_download_inside_the_limit_succeeds(self):
        result, _ = self._fetch(count=5, step=10)

        self.assertTrue(result.success)
        self.assertTrue((self.tmp / "f.grib").exists())


class FTPFileDeadlineTests(SimpleTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.clock = FakeClock()
        self.strategy = FTPFetchStrategy({"host": "ftp.example.org", "max_file_seconds": 100})
        self.strategy.clock = self.clock
        self.strategy._connection = MagicMock()

    def _fetch(self, count, step):
        def retrbinary(_cmd, callback):
            for chunk in _chunks(self.clock, count, step):
                callback(chunk)

        self.strategy._connection.retrbinary.side_effect = retrbinary
        request = FileRequest(identifier="f", filename="f.grib", params={"remote_path": "/f.grib"})
        with patch.object(self.strategy, "_reset_connection") as reset:
            result = self.strategy.fetch(request, self.tmp / "f.grib")
        return result, reset

    def test_a_download_still_streaming_at_the_limit_stops_transient_and_leaves_no_file(self):
        result, reset = self._fetch(count=50, step=10)

        self.assertFalse(result.success)
        self.assertEqual(result.failure_scope, FailureScope.TRANSIENT)
        self.assertIn("time limit", result.error)
        self.assertFalse((self.tmp / "f.grib").exists())
        reset.assert_called_once()

    def test_a_download_inside_the_limit_succeeds(self):
        result, reset = self._fetch(count=5, step=10)

        self.assertTrue(result.success)
        reset.assert_not_called()


class RunDeadlineTests(TestCase):
    def setUp(self):
        self.feed, self.collection = _make_feed_and_collection()

    def _run(self, clock, names, seconds_per_file):
        requests = [_mock_request(n) for n in names]
        loader = Loader(data_source=MagicMock(), collection=self.collection, data_feed=self.feed, clock=clock)
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = requests
        loader.data_source.post_process_fetched_file.side_effect = lambda req, path: (path, None)

        def fetch(req):
            clock.advance(seconds_per_file)
            return _success_fetch_result(req)

        with (
            patch.object(loader, "_already_exists", return_value=False),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_fetch_and_store", side_effect=fetch) as fetch_mock,
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
        ):
            result = loader.run(skip_existing=False)
        return result, fetch_mock

    @override_settings(GEORIVA_FETCH_RUN_MAX_SECONDS=7200)
    def test_a_run_past_its_limit_starts_no_further_file_and_fails(self):
        result, fetch_mock = self._run(FakeClock(), ["a", "b", "c", "d"], seconds_per_file=3600)

        self.assertEqual(fetch_mock.call_count, 2)
        run = FetchRun.objects.get()
        self.assertEqual(run.status, "failed")
        self.assertIn("run exceeded its time limit (2h)", run.error_message)
        self.assertEqual(result.files_fetched, 2)
        rows = FetchedFile.objects.filter(failure_scope=FailureScope.NOT_ATTEMPTED)
        self.assertEqual(rows.count(), 2)

    @override_settings(GEORIVA_FETCH_RUN_MAX_SECONDS=7200)
    def test_a_run_inside_its_limit_completes(self):
        self._run(FakeClock(), ["a", "b"], seconds_per_file=3000)

        self.assertEqual(FetchRun.objects.get().status, "completed")
