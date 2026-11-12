"""
The FetchRun lifecycle is a seam any loop can run inside (ADR 0031, ADR 0042 §4).

Driven here with loop bodies that fetch no files and plan no rows: what a
harvest will be. The file Loader's own use of the seam is covered by the
fetch-run, abort and retry families.
"""

from datetime import timedelta

from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import make_organisation
from georiva.sources.fetch_run_spine import RunResult, RunStopped, record_not_attempted, run_tracked
from georiva.sources.models import (
    DataFeed,
    DataFeedCollectionLink,
    FetchedFile,
    FetchRun,
    FetchRunFinished,
    FetchRunStarted,
)

UNREACHABLE = "source unavailable — upstream catalog unreachable"


class RunTrackedTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Test", slug="test", file_format="grib2"
        )
        self.collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
        self.feed = DataFeed.objects.create(name="Feed", catalog=catalog)
        self.link = DataFeedCollectionLink.objects.create(data_feed=self.feed, collection=self.collection)

    def _run(self, walk, **kwargs):
        return run_tracked(walk, result=RunResult(), data_feed=self.feed, collection=self.collection, **kwargs)

    # ── clean finish ────────────────────────────────────────────────────────

    def test_the_loop_runs_inside_a_running_run_that_has_started(self):
        seen = {}

        def walk(fetch_run):
            seen["status"] = fetch_run.status
            seen["started"] = FetchRunStarted.objects.filter(fetch_run=fetch_run).count()
            seen["collection"] = fetch_run.collection

        self._run(walk)

        self.assertEqual(seen, {"status": FetchRun.Status.RUNNING, "started": 1, "collection": self.collection})

    def test_a_loop_that_returns_completes_the_run(self):
        result = self._run(lambda fetch_run: None)

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.COMPLETED)
        self.assertEqual(run.error_message, "")
        self.assertIsNotNone(run.finished_at)
        self.assertIsNone(run.retry_due_at)
        self.assertFalse(run.parked)
        self.assertIsNone(result.stop_reason)
        self.assertIsNone(result.source_unavailable)
        self.assertIsNotNone(result.finished_at)

    def test_a_clean_run_records_its_start_and_a_completed_finish(self):
        self._run(lambda fetch_run: None)

        started = FetchRunStarted.objects.get()
        finished = FetchRunFinished.objects.get()
        self.assertEqual(started.collection_slug, "col")
        self.assertEqual((finished.outcome, finished.error), ("completed", ""))
        self.assertEqual(finished.caused_by_id, started.pk)

    def test_no_plan_rows_are_needed(self):
        self._run(lambda fetch_run: None)

        self.assertFalse(FetchedFile.objects.exists())
        run = FetchRun.objects.get()
        self.assertEqual((run.files_requested, run.files_fetched, run.files_skipped, run.files_failed), (0, 0, 0, 0))

    def test_the_feed_and_link_are_stamped_from_the_runs_start(self):
        self._run(lambda fetch_run: None)

        run = FetchRun.objects.get()
        self.link.refresh_from_db()
        self.feed.refresh_from_db()
        self.assertEqual(self.link.last_run_at, run.started_at)
        self.assertEqual((self.feed.last_run_status, self.feed.total_runs), ("success", 1))
        self.assertIsNotNone(self.feed.last_success_at)

    # ── a stop on a source failure ──────────────────────────────────────────

    def _stop(self, fetch_run):
        raise RunStopped(UNREACHABLE)

    def test_a_stop_fails_the_run_with_its_reason(self):
        result = self._run(self._stop)

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, f"Stopped: {UNREACHABLE}")
        self.assertEqual(run.stopped_early, UNREACHABLE)
        self.assertEqual(result.stop_reason, UNREACHABLE)
        self.assertEqual(result.source_unavailable, UNREACHABLE)
        self.assertEqual(result.errors, [UNREACHABLE])
        self.assertFalse(FetchedFile.objects.exists())

    def test_a_stopped_run_is_due_its_first_retry_five_minutes_after_it_finished(self):
        self._run(self._stop)

        run = FetchRun.objects.get()
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))

    def test_the_loops_retry_after_is_a_floor_under_the_backoff(self):
        def walk(fetch_run):
            result.retry_after = 3600
            raise RunStopped(UNREACHABLE)

        result = RunResult()
        run_tracked(walk, result=result, data_feed=self.feed, collection=self.collection)

        run = FetchRun.objects.get()
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(hours=1))

    def test_a_stopped_run_records_a_failed_finish(self):
        self._run(self._stop)

        finished = FetchRunFinished.objects.get()
        self.assertEqual((finished.outcome, finished.error), ("failed", f"Stopped: {UNREACHABLE}"))
        self.assertEqual(finished.caused_by_id, FetchRunStarted.objects.get().pk)

    def test_a_stopped_run_leaves_the_feed_failed(self):
        self._run(self._stop)

        self.feed.refresh_from_db()
        self.assertEqual((self.feed.last_run_status, self.feed.last_run_message), ("failed", UNREACHABLE))
        self.assertIsNone(self.feed.last_success_at)

    def test_a_retry_is_linked_to_the_run_it_retries(self):
        self._run(self._stop)
        earlier = FetchRun.objects.get()

        self._run(self._stop, resumed_from=earlier)

        retry = FetchRun.objects.exclude(pk=earlier.pk).get()
        self.assertEqual(retry.resumed_from, earlier)
        self.assertEqual(retry.correlation_id, earlier.correlation_id)
        self.assertEqual(retry.retry_due_at, retry.finished_at + timedelta(minutes=15))

    # ── an escaping exception ───────────────────────────────────────────────

    def _crash(self, fetch_run):
        raise ValueError("boom")

    def test_an_escaping_exception_fails_the_run_and_does_not_propagate(self):
        result = self._run(self._crash)

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.FAILED)
        self.assertEqual(run.error_message, "Stopped: ValueError: boom")
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertEqual(result.stop_reason, "ValueError: boom")
        self.assertEqual(result.errors, ["boom"])

    def test_an_escaping_exception_is_our_failure_not_the_sources(self):
        result = self._run(self._crash)

        self.assertIsNone(result.source_unavailable)

    def test_an_escaping_exception_records_a_failed_finish(self):
        self._run(self._crash)

        finished = FetchRunFinished.objects.get()
        self.assertEqual((finished.outcome, finished.error), ("failed", "Stopped: ValueError: boom"))

    # ── what the loop reports of itself ─────────────────────────────────────

    def test_settle_runs_once_the_loop_has_ended_and_is_told_why(self):
        calls = []

        def settle(fetch_run, stop_reason):
            calls.append((fetch_run.status, stop_reason))

        self._run(lambda fetch_run: None, settle=settle)
        self._run(self._stop, settle=settle)

        self.assertEqual(calls, [(FetchRun.Status.RUNNING, None), (FetchRun.Status.RUNNING, UNREACHABLE)])

    def test_the_results_counters_land_on_the_run(self):
        class Counted(RunResult):
            def counters(self):
                return {"files_fetched": 2, "files_failed": 1}

        run_tracked(self._stop, result=Counted(), data_feed=self.feed, collection=self.collection)

        run = FetchRun.objects.get()
        self.assertEqual((run.files_fetched, run.files_failed), (2, 1))
        self.assertEqual(FetchRunFinished.objects.get().files_fetched, 2)

    def test_without_a_feed_the_loop_runs_untracked(self):
        seen = []

        result = run_tracked(seen.append, result=RunResult(), data_feed=None, collection=self.collection)

        self.assertEqual(seen, [None])
        self.assertIsNotNone(result.finished_at)
        self.assertFalse(FetchRun.objects.exists())


class RecordNotAttemptedTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Test", slug="test", file_format="grib2"
        )
        self.collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
        self.feed = DataFeed.objects.create(name="Feed", catalog=catalog)

    def test_the_run_fails_at_once_with_the_reason_and_was_never_started(self):
        reason = "not attempted: source unavailable — aborted in Other"

        result = record_not_attempted(RunResult(), reason, data_feed=self.feed, collection=self.collection)

        run = FetchRun.objects.get()
        self.assertEqual((run.status, run.error_message), (FetchRun.Status.FAILED, reason))
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertEqual(result.stop_reason, reason)
        self.assertFalse(FetchRunStarted.objects.exists())
        self.assertEqual(FetchRunFinished.objects.get().outcome, "failed")
        self.assertFalse(FetchedFile.objects.exists())
