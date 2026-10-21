from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import LoaderJob
from georiva.organisations.testing import dial_org, make_organisation
from georiva.sources.acquisition_recovery import (
    INTERRUPTED_FILE_ERROR,
    dispatch_due_retries,
    sweep_stale_fetch_runs,
)
from georiva.sources.acquisition_tracking import with_live_counters
from georiva.sources.models import DataFeed, FetchedFile, FetchRun
from georiva.sources.retry_policy import max_retries


def _make_feed(slug="test"):
    catalog = Catalog.objects.create(
        organisation=make_organisation(),
        name=f"Test {slug}",
        slug=slug,
        file_format="grib2",
    )
    return DataFeed.objects.create(name=f"Test Feed {slug}", catalog=catalog)


def _make_run(feed, *, hours_ago=None, status="running", resumed_from=None):
    run = FetchRun.objects.create(
        data_feed=feed,
        status=status,
        resumed_from=resumed_from,
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
            LoaderJob,
            for_concrete_model=False,
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
            last_run_at=newer_report,
            last_run_status="success",
        )

        sweep_stale_fetch_runs()

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.last_run_at, newer_report)
        self.assertEqual(self.feed.last_run_status, "success")

    def _interrupt_and_come_due(self, run):
        """Sweep a stale run, then let its backoff pass."""
        sweep_stale_fetch_runs()
        FetchRun.objects.filter(pk=run.pk).update(retry_due_at=timezone.now() - timedelta(minutes=1))

    def test_interrupted_run_is_due_for_a_retry_after_the_first_backoff(self, _executor):
        run = _make_run(self.feed, hours_ago=7)

        result = sweep_stale_fetch_runs()

        run.refresh_from_db()
        self.assertEqual(result["resumed"], 0)
        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertFalse(LoaderJob.objects.exists())

    def test_due_retry_is_dispatched_once_with_lineage_and_scope(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        self._interrupt_and_come_due(run)

        first = sweep_stale_fetch_runs()
        second = sweep_stale_fetch_runs()

        job = LoaderJob.objects.get(resume_of_run=run)
        self.assertEqual((first["resumed"], second["resumed"]), (1, 0))
        self.assertEqual(job.data_feed, self.feed)
        self.assertIsNone(job.user)

    def test_dispatch_is_scoped_to_the_runs_collection(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        collection = Collection.objects.create(catalog=self.feed.catalog, name="C", slug="c")
        FetchRun.objects.filter(pk=run.pk).update(collection=collection)
        self._interrupt_and_come_due(run)

        sweep_stale_fetch_runs()

        self.assertEqual(LoaderJob.objects.get(resume_of_run=run).collection_id, collection.pk)

    def test_a_run_that_already_has_a_retry_is_not_dispatched_again(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        self._interrupt_and_come_due(run)
        _make_run(self.feed, hours_ago=0, resumed_from=run)

        self.assertEqual(dispatch_due_retries(), 0)
        self.assertFalse(LoaderJob.objects.exists())

    def test_not_due_yet_is_not_dispatched(self, _executor):
        _make_run(self.feed, hours_ago=7)
        sweep_stale_fetch_runs()

        self.assertEqual(dispatch_due_retries(), 0)

    def test_a_newer_regular_run_does_not_stop_the_retry(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        self._interrupt_and_come_due(run)
        _make_run(self.feed, hours_ago=1, status="completed")

        result = sweep_stale_fetch_runs()

        self.assertEqual(result["resumed"], 1)
        self.assertTrue(LoaderJob.objects.filter(resume_of_run=run).exists())

    def test_a_loader_job_for_the_feed_does_not_stop_the_retry(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        self._interrupt_and_come_due(run)
        _make_loader_job(self.feed, state="pending")

        self.assertEqual(sweep_stale_fetch_runs()["resumed"], 1)

    def test_a_lineage_at_the_cap_is_parked_not_scheduled(self, _executor):
        run = None
        for hours in (50, 40, 30, 20):
            run = _make_run(self.feed, hours_ago=hours, status="interrupted", resumed_from=run)
        last = _make_run(self.feed, hours_ago=7, resumed_from=run)
        self.assertEqual(last.resume_generation(), max_retries())

        result = sweep_stale_fetch_runs()

        last.refresh_from_db()
        self.assertEqual(last.status, FetchRun.Status.INTERRUPTED)
        self.assertIsNone(last.retry_due_at)
        self.assertTrue(last.parked)
        self.assertEqual(result["resumed"], 0)
        self.assertFalse(LoaderJob.objects.exists())

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

    def test_hard_sweep_with_zero_threshold(self, _executor):
        run = _make_run(self.feed, hours_ago=0.1)

        result = sweep_stale_fetch_runs(stale_hours=0)

        run.refresh_from_db()
        self.assertEqual(result["swept"], 1)
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)

    def test_targeted_sweep_ignores_age_and_only_touches_its_target(self, _executor):
        target = _make_run(self.feed, hours_ago=1)
        bystander = _make_run(self.feed, hours_ago=7)

        result = sweep_stale_fetch_runs(run_ids=[target.pk], resume=False)

        target.refresh_from_db()
        bystander.refresh_from_db()
        self.assertEqual(result["swept"], 1)
        self.assertEqual(target.status, FetchRun.Status.INTERRUPTED)
        self.assertIn("operator", target.error_message)
        self.assertEqual(bystander.status, FetchRun.Status.RUNNING)

    def test_no_resume_marks_interrupted_without_scheduling_a_retry(self, _executor):
        run = _make_run(self.feed, hours_ago=7)

        result = sweep_stale_fetch_runs(resume=False)

        run.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)
        self.assertIsNone(run.retry_due_at)
        self.assertEqual(result["resumed"], 0)
        self.assertFalse(LoaderJob.objects.exists())

    def test_reaped_zombie_does_not_block_its_own_retry(self, _executor):
        run = _make_run(self.feed, hours_ago=7)
        _make_loader_job(self.feed, state="started", hours_ago=8)
        self._interrupt_and_come_due(run)

        result = sweep_stale_fetch_runs()

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


@patch("task_ferry.handler.get_executor")
class RecoverRunViewTests(TestCase):
    """The operator Recover button: confirmation page + targeted recovery."""

    def setUp(self):
        user = get_user_model().objects.create_superuser(
            "admin_rec",
            "r@test.com",
            "pw",
        )
        dial_org(self.client)
        self.client.force_login(user)
        self.feed = _make_feed()

    def _detail_url(self, run):
        return reverse(
            "data_feed_fetch_run_detail",
            kwargs={"feed_pk": self.feed.pk, "run_pk": run.pk},
        )

    def _recover_url(self, run):
        return reverse(
            "data_feed_fetch_run_recover",
            kwargs={"feed_pk": self.feed.pk, "run_pk": run.pk},
        )

    def _stamp_stored(self, run, path, *, minutes_ago, duration_seconds):
        f = FetchedFile.objects.create(fetch_run=run, file_path=path)
        started = timezone.now() - timedelta(minutes=minutes_ago)
        FetchedFile.objects.filter(pk=f.pk).update(
            status=FetchedFile.Status.STORED,
            started_at=started,
            completed_at=started + timedelta(seconds=duration_seconds),
            bytes_transferred=1024,
        )
        return f

    def test_detail_page_renders_a_stuck_verdict_with_its_evidence(self, _executor):
        run = _make_run(self.feed, hours_ago=1)
        for i in range(3):
            self._stamp_stored(
                run,
                f"c/f{i}.tif",
                minutes_ago=55 - i,
                duration_seconds=40,
            )
        stuck = FetchedFile.objects.create(fetch_run=run, file_path="c/stuck.tif")
        FetchedFile.objects.filter(pk=stuck.pk).update(
            status=FetchedFile.Status.FETCHING,
            started_at=timezone.now() - timedelta(minutes=30),
        )

        response = self.client.get(self._detail_url(run))

        self.assertContains(response, "looks stuck")
        self.assertContains(response, "A typical file in this run takes")
        self.assertContains(response, "c/stuck.tif")

    def test_recover_page_pushes_back_when_the_run_looks_alive(self, _executor):
        run = _make_run(self.feed, hours_ago=1)
        for i in range(3):
            self._stamp_stored(
                run,
                f"c/f{i}.tif",
                minutes_ago=3 - i,
                duration_seconds=40,
            )

        response = self.client.get(self._recover_url(run))

        self.assertContains(response, "looks alive")
        self.assertContains(response, "Are you sure?")

    def test_detail_page_shows_recover_button_only_while_running(self, _executor):
        running = _make_run(self.feed)
        finished = _make_run(self.feed, status="completed")

        self.assertContains(
            self.client.get(self._detail_url(running)),
            "Recover stale run",
        )
        self.assertNotContains(
            self.client.get(self._detail_url(finished)),
            "Recover stale run",
        )

    def test_get_renders_confirmation_with_the_duplicate_warning(self, _executor):
        run = _make_run(self.feed)

        response = self.client.get(self._recover_url(run))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "duplicate fetch")
        run.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.RUNNING)

    def test_get_for_a_finished_run_redirects_without_touching_it(self, _executor):
        run = _make_run(self.feed, status="completed")

        response = self.client.get(self._recover_url(run))

        self.assertRedirects(response, self._detail_url(run))
        run.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.COMPLETED)

    def test_post_recovers_and_queues_the_resume(self, _executor):
        run = _make_run(self.feed, hours_ago=1)
        dangling = FetchedFile.objects.create(fetch_run=run, file_path="c/a.tif")
        dangling.mark_fetching()

        response = self.client.post(self._recover_url(run), follow=True)

        run.refresh_from_db()
        dangling.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)
        self.assertIn("operator", run.error_message)
        self.assertEqual(dangling.status, FetchedFile.Status.FAILED)
        self.assertTrue(LoaderJob.objects.filter(resume_of_run=run).exists())
        self.assertContains(response, "retry queued")

    def test_post_warns_when_the_run_is_already_being_retried(self, _executor):
        run = _make_run(self.feed, hours_ago=2)
        _make_loader_job(self.feed, state="pending", user=None)
        LoaderJob.objects.update(resume_of_run=run)

        response = self.client.post(self._recover_url(run), follow=True)

        run.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)
        self.assertContains(response, "already under way")

    def test_post_warns_when_the_retry_cap_is_reached(self, _executor):
        run = None
        for hours in (50, 40, 30, 20):
            run = _make_run(self.feed, hours_ago=hours, status="interrupted", resumed_from=run)
        run = _make_run(self.feed, hours_ago=1, resumed_from=run)

        response = self.client.post(self._recover_url(run), follow=True)

        run.refresh_from_db()
        self.assertEqual(run.status, FetchRun.Status.INTERRUPTED)
        self.assertFalse(LoaderJob.objects.exists())
        self.assertContains(response, "cap")


@patch("task_ferry.handler.get_executor")
class SweepFailureScopeTests(TestCase):
    """Crash recovery fails a dead run's dangling rows with a scope (#477)."""

    def test_a_waiting_row_was_not_attempted_and_a_fetching_one_is_transient(self, _executor):
        run = _make_run(_make_feed(), hours_ago=7)
        waiting = FetchedFile.objects.create(fetch_run=run, file_path="c/a.tif")
        fetching = FetchedFile.objects.create(fetch_run=run, file_path="c/b.tif")
        fetching.mark_fetching()

        sweep_stale_fetch_runs()

        waiting.refresh_from_db()
        fetching.refresh_from_db()
        self.assertEqual((waiting.status, waiting.failure_scope), ("failed", "not_attempted"))
        self.assertEqual((fetching.status, fetching.failure_scope), ("failed", "transient"))


class SweepScheduleTests(TestCase):
    def test_the_recovery_sweep_is_registered_every_five_minutes(self):
        from django_celery_beat.models import IntervalSchedule

        from georiva.sources.tasks import setup_stale_fetch_run_sweep_beat

        with patch("georiva.sources.tasks.register_periodic_tasks") as register:
            setup_stale_fetch_run_sweep_beat(sender=None)

        (specs,) = register.call_args.args
        (spec,) = specs
        self.assertEqual(spec.task, "georiva.sources.tasks.sweep_stale_fetch_runs")
        self.assertEqual((spec.every, spec.period), (5, IntervalSchedule.MINUTES))
