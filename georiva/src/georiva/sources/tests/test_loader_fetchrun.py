"""
Tests for Loader incremental FetchRun/FetchedFile tracking.

We mock fetch_strategy and storage to keep these fast and deterministic —
the point is to verify FetchRun/FetchedFile records are written correctly,
not to test network or storage I/O.
"""

from contextlib import ExitStack
from unittest.mock import MagicMock, PropertyMock, patch

from django.db import IntegrityError, transaction
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import make_organisation
from georiva.sources.fetch.base import FetchResult, FileRequest
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, FetchedFile, FetchRun


def _make_feed_and_collection():
    catalog = Catalog.objects.create(organisation=make_organisation(), name="Test", slug="test", file_format="grib2")
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
    return FetchResult(request=req, success=True, status="success", bytes_transferred=bytes_transferred)


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
        loader.data_source.post_process_fetched_file.side_effect = lambda req, path: (path, None)

        fetch_iter = iter(fetch_results)

        with (
            patch.object(loader, "_already_exists", return_value=False),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_fetch_and_store", side_effect=lambda req: next(fetch_iter)),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
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
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
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
            patch.object(loader, "_already_exists", return_value=False),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_fetch_and_store", return_value=_success_fetch_result(req, bytes_transferred=2048)),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
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
            patch.object(loader, "_already_exists", return_value=True),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
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
            patch.object(loader, "_already_exists", return_value=False),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_fetch_and_store", return_value=_failed_fetch_result(req, error="FTP timeout")),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
        ):
            loader.run()

        ff = FetchedFile.objects.get()
        self.assertEqual(ff.status, "failed")
        self.assertEqual(ff.error, "FTP timeout")


class LoaderFetchPlanTests(TestCase):
    """The plan is persisted up front (#474): one PENDING row per request,
    created before any fetch, which every later step walks."""

    def setUp(self):
        self.feed, self.collection = _make_feed_and_collection()

    def _requests(self, *names):
        return [FileRequest(identifier=name, filename=name) for name in names]

    def _loader(self, requests):
        loader = Loader(data_source=MagicMock(), collection=self.collection, data_feed=self.feed)
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = requests
        return loader

    def _run(self, loader, *, exists=False, existing_path=None, fetch=None, **run_kwargs):
        """Run with storage and the fetch itself faked. `exists` is a bool or a
        per-request predicate; `fetch` maps a request to its FetchResult."""
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(loader, "_already_exists", side_effect=exists if callable(exists) else lambda r: exists)
            )
            stack.enter_context(patch.object(loader, "_find_existing_catalog_path", return_value=existing_path))
            stack.enter_context(patch.object(loader, "_fetch_and_store", side_effect=fetch or _success_fetch_result))
            stack.enter_context(patch.object(loader, "_cleanup_temp"))
            stack.enter_context(patch.object(loader.fetch_strategy, "connect"))
            stack.enter_context(patch.object(loader.fetch_strategy, "disconnect"))
            return loader.run(**run_kwargs)

    def _rows(self):
        return list(FetchRun.objects.get().fetched_files.order_by("id"))

    def test_every_request_has_a_pending_row_before_the_first_fetch(self):
        requests = self._requests("a.grib", "b.grib", "c.grib")
        seen = []

        def fetch(request):
            if not seen:
                seen.extend((ff.file_path, ff.status, ff.request_payload) for ff in self._rows())
            return _success_fetch_result(request)

        self._run(self._loader(requests), fetch=fetch)

        self.assertEqual([path.rsplit("/", 1)[-1] for path, _, _ in seen], ["a.grib", "b.grib", "c.grib"])
        self.assertEqual([status for _, status, _ in seen], ["fetching", "pending", "pending"])
        self.assertEqual([payload["filename"] for _, _, payload in seen], ["a.grib", "b.grib", "c.grib"])

    def test_a_skipped_request_is_its_planned_row_marked_skipped(self):
        requests = self._requests("old.grib", "new.grib")

        self._run(self._loader(requests), exists=lambda r: r.filename == "old.grib")

        rows = self._rows()
        self.assertEqual([ff.status for ff in rows], ["skipped", "stored"])
        self.assertEqual(rows[0].skip_reason, "already exists")

    def test_a_failed_copy_falls_back_to_download_on_the_same_row(self):
        requests = self._requests("shared.grib")
        loader = self._loader(requests)

        with patch.object(Loader, "_tier_bucket", new_callable=PropertyMock) as bucket:
            bucket.return_value.copy.side_effect = OSError("copy refused")
            self._run(loader, existing_path="sibling/shared.grib")

        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].status, "stored")

    def test_an_escaping_exception_fails_every_unfinished_row_and_keeps_the_sum(self):
        requests = self._requests("a.grib", "b.grib", "c.grib")
        results = iter([_success_fetch_result(requests[0])])

        def fetch(request):
            result = next(results, None)
            if result is None:
                raise RuntimeError("storage gone")
            return result

        self._run(self._loader(requests), fetch=fetch)

        rows = self._rows()
        self.assertEqual([ff.status for ff in rows], ["stored", "failed", "failed"])
        self.assertEqual(rows[1].error, "interrupted: the run stopped while fetching (RuntimeError: storage gone)")
        self.assertEqual(rows[2].error, "not attempted: the run stopped (RuntimeError: storage gone)")
        run = FetchRun.objects.get()
        self.assertEqual(run.files_requested, 3)
        self.assertEqual(run.files_fetched + run.files_skipped + run.files_failed, run.files_requested)

    def test_dry_run_writes_no_rows(self):
        self._run(self._loader(self._requests("a.grib")), dry_run=True)

        self.assertFalse(FetchedFile.objects.exists())

    def test_max_files_keeps_rows_only_for_requests_the_run_reaches(self):
        requests = self._requests("old.grib", "a.grib", "b.grib", "c.grib")

        self._run(self._loader(requests), exists=lambda r: r.filename == "old.grib", max_files=1)

        rows = self._rows()
        self.assertEqual([ff.status for ff in rows], ["skipped", "stored"])
        run = FetchRun.objects.get()
        self.assertEqual(run.files_requested, 2)
        self.assertEqual(run.files_fetched + run.files_skipped + run.files_failed, run.files_requested)

    def test_the_database_rejects_a_second_row_for_one_file_in_one_run(self):
        run = FetchRun.objects.create(data_feed=self.feed)
        FetchedFile.objects.create(fetch_run=run, file_path="test/col/a.grib")

        with self.assertRaises(IntegrityError), transaction.atomic():
            FetchedFile.objects.create(fetch_run=run, file_path="test/col/a.grib")

        # The same file in another run is history, not a duplicate.
        FetchedFile.objects.create(fetch_run=FetchRun.objects.create(data_feed=self.feed), file_path="test/col/a.grib")

    def test_pending_reads_as_waiting(self):
        self.assertEqual(FetchedFile(status=FetchedFile.Status.PENDING).get_status_display(), "Waiting")
