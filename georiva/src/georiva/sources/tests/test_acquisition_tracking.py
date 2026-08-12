"""
Acquisition Activity: feed-scoped FetchRun list (PRD #217, issue #218).

The read-side query module and the run-list page — the acquisition analogue of
the derived-product run tracking (derivation_tracking). Static pages: no SSE,
no polling. The run list is collection-agnostic (ADR-0003).
"""

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.sources.acquisition_tracking import feed_fetch_runs
from georiva.sources.models import DataFeed, FetchedFile, FetchRun
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()


def _feed(name="Rain Feed", slug="chirps"):
    catalog = Catalog.objects.create(organisation=make_organisation(), name=name, slug=slug, file_format="geotiff")
    return DataFeed.objects.create(name=name, catalog=catalog)


def _run(feed, status=FetchRun.Status.COMPLETED, *, started_ago=0, **fields):
    run = FetchRun.objects.create(data_feed=feed, status=status, **fields)
    # started_at is auto_now_add; set it explicitly so ordering is deterministic.
    FetchRun.objects.filter(pk=run.pk).update(started_at=timezone.now() - timedelta(minutes=started_ago))
    run.refresh_from_db()
    return run


class FeedFetchRunsTests(TestCase):
    """feed_fetch_runs: the query the run-list page renders."""

    def setUp(self):
        self.feed = _feed()

    def test_lists_only_the_feeds_runs_newest_first(self):
        older = _run(self.feed, started_ago=10)
        newer = _run(self.feed, started_ago=1)
        other_feed = _feed(name="Other", slug="other")
        _run(other_feed)

        runs = list(feed_fetch_runs(self.feed))

        self.assertEqual(runs, [newer, older])

    def test_status_filter_narrows_to_a_single_status(self):
        failed = _run(self.feed, FetchRun.Status.FAILED, started_ago=5)
        _run(self.feed, FetchRun.Status.COMPLETED, started_ago=1)

        runs = list(feed_fetch_runs(self.feed, status=FetchRun.Status.FAILED))

        self.assertEqual(runs, [failed])


class FetchRunListViewTests(TestCase):
    """The Acquisition Activity page: a thin view over feed_fetch_runs."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_acq", "a@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.feed = _feed()

    def _url(self):
        return reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})

    def test_lists_runs_with_status_counters_and_error(self):
        _run(
            self.feed,
            FetchRun.Status.FAILED,
            files_requested=7,
            files_fetched=4,
            files_skipped=2,
            files_failed=1,
            bytes_transferred=2048,
            error_message="source unreachable: connection timed out",
        )

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "failed")
        self.assertContains(response, "7")  # requested
        self.assertContains(response, "4")  # fetched
        self.assertContains(response, "source unreachable: connection timed out")

    def test_status_filter_querystring_narrows_the_list(self):
        _run(self.feed, FetchRun.Status.COMPLETED, started_ago=5, files_requested=90210)
        _run(self.feed, FetchRun.Status.FAILED, started_ago=1, files_requested=48151)

        both = self.client.get(self._url())
        self.assertContains(both, "90210")
        self.assertContains(both, "48151")

        only_failed = self.client.get(self._url(), {"status": FetchRun.Status.FAILED})
        self.assertContains(only_failed, "48151")
        self.assertNotContains(only_failed, "90210")

    def test_breadcrumbs_chain_back_through_the_feed(self):
        response = self.client.get(self._url())

        self.assertContains(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.assertContains(response, reverse("data_feed_list"))

    def test_each_run_row_links_to_its_detail_page(self):
        run = _run(self.feed)

        response = self.client.get(self._url())

        self.assertContains(
            response,
            reverse(
                "data_feed_fetch_run_detail",
                kwargs={"feed_pk": self.feed.pk, "run_pk": run.pk},
            ),
        )

    def test_run_list_is_paginated_at_25(self):
        for i in range(26):
            _run(self.feed, started_ago=i)

        first = self.client.get(self._url())
        self.assertEqual(first.context["page"].paginator.num_pages, 2)
        self.assertEqual(len(first.context["rows"]), 25)

        second = self.client.get(self._url(), {"page": 2})
        self.assertEqual(len(second.context["rows"]), 1)

    def test_requires_admin_login(self):
        self.client.logout()

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 302)


class FetchRunDetailViewTests(TestCase):
    """The FetchRun detail page (issue #221): one run's summary and its
    FetchedFile drill-down, scoped to the feed. Read-only in this slice."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_run", "d@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.feed = _feed()
        self.run = _run(
            self.feed,
            FetchRun.Status.COMPLETED,
            files_requested=3,
            files_fetched=1,
            files_skipped=1,
            files_failed=1,
        )

    def _url(self, run=None):
        return reverse(
            "data_feed_fetch_run_detail",
            kwargs={"feed_pk": self.feed.pk, "run_pk": (run or self.run).pk},
        )

    def test_shows_run_summary_and_per_file_status_error_and_skip_reason(self):
        FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/GR--20260714T0600--precip.tif",
            status=FetchedFile.Status.STORED,
            bytes_transferred=4096,
        )
        FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/GR--20260714T0600--precip2.tif",
            status=FetchedFile.Status.SKIPPED,
            skip_reason="already exists",
        )
        FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/GR--20260714T0600--precip3.tif",
            status=FetchedFile.Status.FAILED,
            error="read timed out after 30s",
        )

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "GR--20260714T0600--precip.tif")
        self.assertContains(response, "already exists")
        self.assertContains(response, "read timed out after 30s")
        self.assertContains(response, "Stored")
        self.assertContains(response, "Skipped")
        self.assertContains(response, "Failed")

    def test_breadcrumbs_chain_back_through_the_run_list(self):
        response = self.client.get(self._url())

        self.assertContains(response, reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk}))
        self.assertContains(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))

    def test_run_of_another_feed_is_not_reachable(self):
        other_feed = _feed(name="Other", slug="other")
        foreign_run = _run(other_feed)

        response = self.client.get(
            reverse(
                "data_feed_fetch_run_detail",
                kwargs={"feed_pk": self.feed.pk, "run_pk": foreign_run.pk},
            )
        )

        self.assertEqual(response.status_code, 404)

    def test_requires_admin_login(self):
        self.client.logout()

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 302)


class DataFeedDetailAcquisitionCardTests(TestCase):
    """The Acquisition Activity stat card at the top of the feed detail page:
    total runs, a last-run summary linking to that run, and a "View all"
    link. Actions (Run Now, checks) live on the list pages, not here."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from georiva.sources.tests.support import ensure_base_datafeed_viewset

        ensure_base_datafeed_viewset()

    def setUp(self):
        self.user = User.objects.create_superuser("admin_panel", "p@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.feed = _feed()

    def _detail_url(self):
        return reverse("data_feed_detail", kwargs={"pk": self.feed.pk})

    def test_card_shows_total_runs_and_last_run_summary(self):
        _run(self.feed, started_ago=60)
        _run(self.feed, started_ago=30)
        last = _run(
            self.feed,
            FetchRun.Status.FAILED,
            started_ago=1,
            files_requested=90210,
            files_failed=48151,
        )

        response = self.client.get(self._detail_url())

        self.assertEqual(response.context["acquisition_summary"]["total_runs"], 3)
        self.assertContains(response, "90210")  # last run requested
        self.assertContains(response, "48151")  # last run failed
        self.assertContains(response, "Failed")  # status badge
        self.assertContains(  # summary links to the run's detail page
            response,
            reverse(
                "data_feed_fetch_run_detail",
                kwargs={"feed_pk": self.feed.pk, "run_pk": last.pk},
            ),
        )
        self.assertContains(  # View all
            response, reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})
        )

    def test_card_with_no_runs_shows_an_empty_state(self):
        response = self.client.get(self._detail_url())

        self.assertContains(response, "No runs yet")

    def test_detail_page_carries_no_activity_actions_or_tables(self):
        _run(self.feed, files_requested=7)

        response = self.client.get(self._detail_url())

        self.assertNotContains(response, 'value="run_now"')
        self.assertNotContains(response, 'value="check_new_files"')
        self.assertNotContains(response, 'value="check_unprocessed"')


class RunLivenessTests(TestCase):
    """run_liveness: the stuck-vs-slow verdict behind the Recover button.

    Self-calibrating: silence since the run's last file activity is judged
    against the median duration of this run's own completed real fetches.
    """

    def setUp(self):
        self.now = timezone.now()
        self.feed = _feed()
        self.run = _run(self.feed, FetchRun.Status.RUNNING, started_ago=30)

    def _stored_file(self, path, *, started_ago, duration_seconds, bytes=1024):
        """A completed real fetch: started `started_ago` minutes before now,
        taking `duration_seconds`."""
        f = FetchedFile.objects.create(fetch_run=self.run, file_path=path)
        started = self.now - timedelta(minutes=started_ago)
        FetchedFile.objects.filter(pk=f.pk).update(
            status=FetchedFile.Status.STORED,
            started_at=started,
            completed_at=started + timedelta(seconds=duration_seconds),
            bytes_transferred=bytes,
        )
        return f

    def _fetching_file(self, path, *, started_ago_seconds):
        f = FetchedFile.objects.create(fetch_run=self.run, file_path=path)
        FetchedFile.objects.filter(pk=f.pk).update(
            status=FetchedFile.Status.FETCHING,
            started_at=self.now - timedelta(seconds=started_ago_seconds),
        )
        return f

    def _three_quick_files(self, duration_seconds=40):
        for i, ago in enumerate([25, 24, 23]):
            self._stored_file(
                f"c/f{i}.tif",
                started_ago=ago,
                duration_seconds=duration_seconds,
            )

    def test_stuck_when_silence_dwarfs_the_median(self):
        from georiva.sources.acquisition_tracking import run_liveness

        self._three_quick_files(duration_seconds=40)
        self._fetching_file("c/stuck.tif", started_ago_seconds=20 * 60)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "stuck")
        self.assertEqual(liveness["median_seconds"], 40)
        self.assertEqual(liveness["sample_count"], 3)
        self.assertEqual(liveness["silence_seconds"], 20 * 60)
        self.assertEqual(liveness["current_file"].file_path, "c/stuck.tif")

    def test_slow_between_two_and_five_times_the_median(self):
        from georiva.sources.acquisition_tracking import run_liveness

        self._three_quick_files(duration_seconds=60)
        self._fetching_file("c/slowish.tif", started_ago_seconds=150)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "slow")

    def test_normal_within_twice_the_median(self):
        from georiva.sources.acquisition_tracking import run_liveness

        self._three_quick_files(duration_seconds=60)
        self._fetching_file("c/fine.tif", started_ago_seconds=30)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "normal")

    def test_short_silence_is_never_stuck_even_on_a_fast_feed(self):
        # Median 5s, silence 30s = 6x the median — but under the 2-minute
        # floor, a fast feed's hiccup must not read as a death.
        from georiva.sources.acquisition_tracking import run_liveness

        self._three_quick_files(duration_seconds=5)
        self._fetching_file("c/hiccup.tif", started_ago_seconds=30)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "slow")

    def test_died_between_files_uses_last_completion_as_activity(self):
        from georiva.sources.acquisition_tracking import run_liveness

        # Last file completed 20 minutes ago; nothing in flight since.
        self._three_quick_files(duration_seconds=40)
        self._stored_file("c/last.tif", started_ago=21, duration_seconds=60)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "stuck")
        self.assertIsNone(liveness["current_file"])
        self.assertEqual(liveness["silence_seconds"], 20 * 60)

    def test_unknown_below_three_real_samples(self):
        from georiva.sources.acquisition_tracking import run_liveness

        self._stored_file("c/a.tif", started_ago=25, duration_seconds=40)
        self._stored_file("c/b.tif", started_ago=24, duration_seconds=40)
        self._fetching_file("c/c.tif", started_ago_seconds=20 * 60)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "unknown")
        self.assertIsNone(liveness["median_seconds"])
        self.assertEqual(liveness["silence_seconds"], 20 * 60)

    def test_near_instant_copies_do_not_poison_the_median(self):
        from georiva.sources.acquisition_tracking import run_liveness

        # Three cross-collection copies (sub-second, zero bytes) + two real
        # fetches: not enough real samples for a verdict.
        for i, ago in enumerate([25, 24, 23]):
            self._stored_file(
                f"c/copy{i}.tif",
                started_ago=ago,
                duration_seconds=0,
                bytes=0,
            )
        self._stored_file("c/real1.tif", started_ago=22, duration_seconds=40)
        self._stored_file("c/real2.tif", started_ago=21, duration_seconds=40)

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "unknown")
        self.assertEqual(liveness["sample_count"], 2)

    def test_finished_run_has_no_liveness(self):
        from georiva.sources.acquisition_tracking import run_liveness

        run = _run(self.feed, FetchRun.Status.COMPLETED)

        self.assertIsNone(run_liveness(run))

    def test_run_with_no_files_counts_silence_from_run_start(self):
        from georiva.sources.acquisition_tracking import run_liveness

        liveness = run_liveness(self.run, now=self.now)

        self.assertEqual(liveness["verdict"], "unknown")
        # _run() stamps started_at a hair after self.now — near enough.
        self.assertAlmostEqual(liveness["silence_seconds"], 30 * 60, delta=2)
