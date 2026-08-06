from datetime import timedelta
from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.ingestion.models import LoaderJob
from georiva.organisations.testing import make_organisation
from georiva.sources.acquisition_recovery import (
    INTERRUPTED_FILE_ERROR,
    MAX_AUTO_RESUMES,
    sweep_stale_fetch_runs,
)
from georiva.sources.acquisition_tracking import with_live_counters
from georiva.sources.models import DataFeed, FetchedFile, FetchRun


def _make_feed(slug="test"):
    catalog = Catalog.objects.create(
        organisation=make_organisation(),
        name=f"Test {slug}", slug=slug, file_format="grib2",
    )
    return DataFeed.objects.create(name=f"Test Feed {slug}", catalog=catalog)


def _make_run(feed, *, hours_ago=None, status="running", resumed_from=None):
    run = FetchRun.objects.create(
        data_feed=feed, status=status, resumed_from=resumed_from,
    )
    if hours_ago is not None:
        # started_at is auto_now_add — backdate via queryset update.
        FetchRun.objects.filter(pk=run.pk).update(
            started_at=timezone.now() - timedelta(hours=hours_ago),
        )
        run.refresh_from_db()
    return run


def _make_loader_job(feed, *, state, hours_ago=None, user=None):
    job = LoaderJob.objects.create(
        user=user,
        state=state,
        data_feed=feed,
        content_type=ContentType.objects.get_for_model(
            LoaderJob, for_concrete_model=False,
        ),
    )
    if hours_ago is not None:
        LoaderJob.objects.filter(pk=job.pk).update(
            updated_at=timezone.now() - timedelta(hours=hours_ago),
        )
        job.refresh_from_db()
    return job


@patch("task_ferry.handler.get_executor")
class SweepStaleFetchRunsTests(TestCase):
    def setUp(self):
        self.feed = _make_feed()

    def test_stale_run_marked_interrupted_with_truthful_counters(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        stored = FetchedFile.objects.create(fetch_run=run, file_path="c/a.tif")
        stored.mark_stored(bytes_transferred=100)
        dangling = FetchedFile.objects.create(fetch_run=run, file_path="c/b.tif")
        dangling.mark_fetching()

        result = sweep_stale_fetch_runs()

        run.refresh_from_db()
        dangling.refresh_from_db()
        self.assertEqual(result["swept"], 1)
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)
        self.assertIsNotNone(run.finished_at)
        self.assertIn("Interrupted", run.error_message)
        self.assertEqual(dangling.status, FetchedFile.Status.FAILED)
        self.assertEqual(dangling.error, INTERRUPTED_FILE_ERROR)
        # Counters frozen from file truth, not left at their stored zeros.
        self.assertEqual(run.files_fetched, 1)
        self.assertEqual(run.files_failed, 1)
        self.assertEqual(run.bytes_transferred, 100)

    def test_fresh_running_run_untouched(self, _executor):
        run = _make_run(self.feed, hours_ago=1)

        result = sweep_stale_fetch_runs()

        run.refresh_from_db()
        self.assertEqual(result["swept"], 0)
        self.assertEqual(run.status, FetchRun.Status.RUNNING)

    def test_feed_backfilled_so_first_run_is_not_never_run(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        self.assertIsNone(self.feed.last_run_at)

        sweep_stale_fetch_runs()

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.last_run_at, run.started_at)
        self.assertEqual(self.feed.last_run_status, "failed")

    def test_feed_backfill_never_clobbers_fresher_stats(self, _executor):
        _make_run(self.feed, hours_ago=7)
        newer_report = timezone.now() - timedelta(hours=2)
        DataFeed.objects.filter(pk=self.feed.pk).update(
            last_run_at=newer_report, last_run_status="success",
        )

        sweep_stale_fetch_runs()

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.last_run_at, newer_report)
        self.assertEqual(self.feed.last_run_status, "success")

    def test_resume_enqueued_with_lineage(self, _executor):
        run = _make_run(self.feed, hours_ago=7)

        result = sweep_stale_fetch_runs()

        self.assertEqual(result["resumed"], 1)
        resume_job = LoaderJob.objects.get(resume_of_run=run)
        self.assertEqual(resume_job.data_feed, self.feed)
        self.assertIsNone(resume_job.user)

    def test_resume_cap_stops_a_crash_loop(self, _executor):
        original = _make_run(self.feed, hours_ago=30, status="interrupted")
        first_resume = _make_run(
            self.feed, hours_ago=20, status="interrupted", resumed_from=original,
        )
        second_resume = _make_run(
            self.feed, hours_ago=7, resumed_from=first_resume,
        )
        self.assertEqual(second_resume.resume_generation(), MAX_AUTO_RESUMES)

        result = sweep_stale_fetch_runs()

        second_resume.refresh_from_db()
        self.assertEqual(second_resume.status, FetchRun.Status.INTERRUPTED)
        self.assertEqual(result["resumed"], 0)
        self.assertFalse(LoaderJob.objects.exists())

    def test_no_resume_when_a_newer_run_exists(self, _executor):
        _make_run(self.feed, hours_ago=7)
        _make_run(self.feed, hours_ago=1, status="completed")

        result = sweep_stale_fetch_runs()

        self.assertEqual(result["swept"], 1)
        self.assertEqual(result["resumed"], 0)
        self.assertFalse(LoaderJob.objects.exists())

    def test_no_resume_when_a_loader_job_is_already_queued(self, _executor):
        _make_run(self.feed, hours_ago=7)
        _make_loader_job(self.feed, state="pending")

        result = sweep_stale_fetch_runs()

        self.assertEqual(result["resumed"], 0)
        self.assertFalse(LoaderJob.objects.filter(resume_of_run__isnull=False).exists())

    def test_zombie_started_jobs_are_reaped(self, _executor):
        zombie = _make_loader_job(self.feed, state="started", hours_ago=8)
        fresh = _make_loader_job(self.feed, state="started", hours_ago=1)

        result = sweep_stale_fetch_runs()

        zombie.refresh_from_db()
        fresh.refresh_from_db()
        self.assertEqual(result["jobs_reaped"], 1)
        self.assertEqual(zombie.state, "failed")
        self.assertIn("interrupted", zombie.error)
        self.assertEqual(fresh.state, "started")

    def test_reaped_zombie_does_not_block_its_own_resume(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        _make_loader_job(self.feed, state="started", hours_ago=8)

        result = sweep_stale_fetch_runs()

        self.assertEqual(result["jobs_reaped"], 1)
        self.assertEqual(result["resumed"], 1)
        self.assertTrue(LoaderJob.objects.filter(resume_of_run=run).exists())


class LiveCountersTests(TestCase):
    def setUp(self):
        self.feed = _make_feed()

    def test_running_run_shows_derived_counters(self):
        run = _make_run(self.feed)
        f1 = FetchedFile.objects.create(fetch_run=run, file_path="c/a.tif")
        f1.mark_stored(bytes_transferred=100)
        f2 = FetchedFile.objects.create(fetch_run=run, file_path="c/b.tif")
        f2.mark_skipped(reason="already exists")

        run = with_live_counters(run)

        self.assertEqual(run.files_fetched, 1)
        self.assertEqual(run.files_skipped, 1)
        self.assertEqual(run.bytes_transferred, 100)
        # Overlay is display-only — nothing was persisted.
        self.assertEqual(FetchRun.objects.get(pk=run.pk).files_fetched, 0)

    def test_finished_run_keeps_its_frozen_summary(self):
        run = _make_run(self.feed)
        run.mark_completed(files_fetched=5, bytes_transferred=999)
        FetchedFile.objects.create(fetch_run=run, file_path="c/a.tif")

        run = with_live_counters(run)

        self.assertEqual(run.files_fetched, 5)
        self.assertEqual(run.bytes_transferred, 999)
