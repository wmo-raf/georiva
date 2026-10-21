"""
A fetch run stops as soon as it is clear the source cannot serve it (#478):
on the first `SOURCE` failure, or on a run of consecutive `TRANSIENT` ones.

Tested at the Loader seam (the fetch itself faked) and through the feed task.
"""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import make_organisation
from georiva.sources.fetch.base import FailureScope, FetchResult, FileRequest
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, DataFeedCollectionLink, FetchRun, FetchRunFinished
from georiva.sources.tasks import run_data_feed_loader

DNS_ERROR = "NameResolutionError for data.ecmwf.int"


def _catalog():
    return Catalog.objects.create(organisation=make_organisation(), name="Test", slug="test", file_format="grib2")


def _requests(count):
    return [FileRequest(identifier=f"f{i}", filename=f"f{i}.grib") for i in range(count)]


def ok(request):
    return FetchResult(request=request, success=True, status="complete", bytes_transferred=10)


def failed(scope, error="boom"):
    return lambda request: FetchResult(request=request).fail(error, scope)


class LoaderAbortTests(TestCase):
    def setUp(self):
        catalog = _catalog()
        self.collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
        self.feed = DataFeed.objects.create(name="Feed", catalog=catalog)

    def _run(self, outcomes, *, requests=None, generate=None):
        """Run the loader over `requests` (one per outcome by default); the
        n-th fetch answers with outcomes[n]. Returns (result, fetched filenames)."""
        requests = requests if requests is not None else _requests(len(outcomes))
        loader = Loader(data_source=MagicMock(), collection=self.collection, data_feed=self.feed)
        loader.data_source.name = "test"
        if generate is not None:
            loader.data_source.generate_requests_for_collection.side_effect = generate
        else:
            loader.data_source.generate_requests_for_collection.return_value = requests
        answers = iter(outcomes)
        attempted = []

        def fetch(request):
            attempted.append(request.filename)
            return next(answers)(request)

        with ExitStack() as stack:
            stack.enter_context(patch.object(loader, "_already_exists", return_value=False))
            stack.enter_context(patch.object(loader, "_find_existing_catalog_path", return_value=None))
            stack.enter_context(patch.object(loader, "_fetch_and_store", side_effect=fetch))
            stack.enter_context(patch.object(loader, "_cleanup_temp"))
            stack.enter_context(patch.object(loader.fetch_strategy, "connect"))
            stack.enter_context(patch.object(loader.fetch_strategy, "disconnect"))
            result = loader.run()
        return result, attempted

    def _fetch_run(self):
        return FetchRun.objects.get()

    def _assert_sum_holds(self, run):
        self.assertEqual(run.files_fetched + run.files_skipped + run.files_failed, run.files_requested)

    # ── SOURCE ──────────────────────────────────────────────────────────────

    def test_a_source_failure_stops_the_run(self):
        result, attempted = self._run([ok, failed(FailureScope.SOURCE, DNS_ERROR), ok, ok])

        self.assertEqual(attempted, ["f0.grib", "f1.grib"])
        run = self._fetch_run()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, f"Stopped: source unavailable — {DNS_ERROR}")
        self.assertEqual((run.files_requested, run.files_fetched, run.files_failed), (4, 1, 3))
        self._assert_sum_holds(run)
        self.assertEqual(result.source_unavailable, f"source unavailable — {DNS_ERROR}")

    def test_rows_after_an_abort_are_not_attempted_with_the_reason(self):
        self._run([failed(FailureScope.SOURCE, DNS_ERROR), ok])

        last = self._fetch_run().fetched_files.order_by("id").last()
        self.assertEqual(last.failure_scope, FailureScope.NOT_ATTEMPTED)
        self.assertEqual(last.error, f"not attempted: the run stopped (source unavailable — {DNS_ERROR})")

    def test_an_aborted_runs_event_records_failed(self):
        self._run([failed(FailureScope.SOURCE, DNS_ERROR)])

        self.assertEqual(FetchRunFinished.objects.get().outcome, "failed")

    # ── TRANSIENT circuit breaker ───────────────────────────────────────────

    def test_three_consecutive_transient_failures_stop_the_run(self):
        transient = failed(FailureScope.TRANSIENT, "Read timed out")
        result, attempted = self._run([transient, transient, transient, ok])

        self.assertEqual(len(attempted), 3)
        run = self._fetch_run()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, "Stopped: 3 consecutive transient failures — Read timed out")
        self._assert_sum_holds(run)
        self.assertTrue(result.source_unavailable)

    def test_an_unclassified_failure_counts_as_transient(self):
        unclassified = lambda request: FetchResult(request=request, success=False, status="failed", error="?")  # noqa: E731
        _, attempted = self._run([unclassified] * 4)

        self.assertEqual(len(attempted), 3)

    def test_a_success_resets_the_count(self):
        transient = failed(FailureScope.TRANSIENT)
        _, attempted = self._run([transient, transient, ok, transient, transient, ok])

        self.assertEqual(len(attempted), 6)
        self.assertEqual(self._fetch_run().status, FetchRun.Status.COMPLETED)

    def test_a_file_failure_resets_the_count(self):
        transient = failed(FailureScope.TRANSIENT)
        _, attempted = self._run([transient, transient, failed(FailureScope.FILE), transient, transient])

        self.assertEqual(len(attempted), 5)
        self.assertEqual(self._fetch_run().status, FetchRun.Status.COMPLETED)

    @override_settings(GEORIVA_FETCH_CIRCUIT_BREAKER_THRESHOLD=2)
    def test_the_threshold_comes_from_settings(self):
        transient = failed(FailureScope.TRANSIENT)
        _, attempted = self._run([transient, transient, ok])

        self.assertEqual(len(attempted), 2)
        self.assertEqual(self._fetch_run().status, FetchRun.Status.FAILED)

    # ── exceptions ──────────────────────────────────────────────────────────

    def test_an_exception_generating_the_plan_fails_the_run(self):
        def generate(collection):
            raise ConnectionError(DNS_ERROR)

        result, _ = self._run([], generate=generate)

        run = self._fetch_run()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, f"Stopped: ConnectionError: {DNS_ERROR}")
        self.assertTrue(result.source_unavailable)

    def test_an_exception_escaping_the_fetch_loop_fails_the_run(self):
        def explode(request):
            raise RuntimeError("database gone")

        result, _ = self._run([ok, explode, ok])

        run = self._fetch_run()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, "Stopped: RuntimeError: database gone")
        self._assert_sum_holds(run)
        # Our own failure, not the source's: the feed's other collections still run.
        self.assertIsNone(result.source_unavailable)

    # ── unchanged ───────────────────────────────────────────────────────────

    def test_a_run_that_walks_every_file_stays_completed_however_many_failed(self):
        self._run([failed(FailureScope.FILE)] * 4)

        run = self._fetch_run()
        self.assertEqual(run.status, FetchRun.Status.COMPLETED)
        self.assertEqual(run.error_message, "")

    def test_an_empty_plan_stays_completed(self):
        self._run([], requests=[])

        self.assertEqual(self._fetch_run().status, FetchRun.Status.COMPLETED)

    # ── run page ────────────────────────────────────────────────────────────

    def test_an_aborted_run_reads_as_stopped_early(self):
        self._run([failed(FailureScope.SOURCE, DNS_ERROR)])

        self.assertEqual(self._fetch_run().stopped_early, f"source unavailable — {DNS_ERROR}")

    def test_a_completed_run_did_not_stop_early(self):
        self._run([ok])

        self.assertIsNone(self._fetch_run().stopped_early)


class FeedShortCircuitTests(TestCase):
    """When one collection's run aborts, the feed tick attempts no other collection
    against the source: each gets a run that ends FAILED at once, with no plan."""

    def setUp(self):
        catalog = _catalog()
        self.feed = DataFeed.objects.create(name="Feed", catalog=catalog, is_active=True)
        self.collections = [
            Collection.objects.create(name=name, slug=name.lower(), catalog=catalog)
            for name in ("First", "Second", "Third")
        ]
        for collection in self.collections:
            DataFeedCollectionLink.objects.create(
                data_feed=self.feed, collection=collection, definition_key=collection.slug
            )
        self.sources = {}

    def _source_for(self, outcome):
        source = MagicMock()
        source.name = "test"
        source.generate_requests_for_collection.return_value = _requests(2)
        source.outcome = outcome
        return source

    def _tick(self, outcomes):
        """Run one feed tick; collection i's fetches answer with outcomes[i]."""
        by_slug = {c.slug: self._source_for(o) for c, o in zip(self.collections, outcomes, strict=True)}
        self.sources = by_slug

        def get_loader(feed, collection=None):
            source = by_slug[collection.slug]
            loader = Loader(data_source=source, collection=collection, data_feed=feed)
            loader._already_exists = lambda request: False
            loader._find_existing_catalog_path = lambda request: None
            loader._fetch_and_store = lambda request: source.outcome(request)
            loader._cleanup_temp = lambda: None
            loader.fetch_strategy = MagicMock()
            return loader

        with patch.object(DataFeed, "get_loader", autospec=True, side_effect=get_loader):
            run_data_feed_loader(self.feed.pk)

    def _runs(self):
        return list(FetchRun.objects.order_by("id"))

    def test_after_an_abort_the_remaining_collections_are_not_attempted(self):
        self._tick([ok, failed(FailureScope.SOURCE, DNS_ERROR), ok])

        first, second, third = self._runs()
        self.assertEqual(first.status, FetchRun.Status.COMPLETED)
        self.assertEqual(second.status, FetchRun.Status.FAILED)
        self.assertEqual(third.status, FetchRun.Status.FAILED)
        self.assertEqual(third.error_message, "not attempted: source unavailable — aborted in Second")
        self.assertEqual(third.files_requested, 0)
        self.assertFalse(third.fetched_files.exists())
        # Generating a plan may itself call the source: it is not asked.
        self.sources["third"].generate_requests_for_collection.assert_not_called()

    def test_a_not_attempted_run_still_stamps_its_collection(self):
        self._tick([failed(FailureScope.SOURCE, DNS_ERROR), ok, ok])

        stamped = DataFeedCollectionLink.objects.filter(data_feed=self.feed, last_run_at__isnull=False)
        self.assertEqual(stamped.count(), 3)

    def test_file_failures_do_not_short_circuit(self):
        self._tick([failed(FailureScope.FILE), ok, ok])

        self.assertEqual(
            [run.status for run in self._runs()],
            [FetchRun.Status.COMPLETED] * 3,
        )
