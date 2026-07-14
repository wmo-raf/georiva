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

User = get_user_model()


def _feed(name="Rain Feed", slug="chirps"):
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    return DataFeed.objects.create(name=name, catalog=catalog)


def _run(feed, status=FetchRun.Status.COMPLETED, *, started_ago=0, **fields):
    run = FetchRun.objects.create(data_feed=feed, status=status, **fields)
    # started_at is auto_now_add; set it explicitly so ordering is deterministic.
    FetchRun.objects.filter(pk=run.pk).update(
        started_at=timezone.now() - timedelta(minutes=started_ago)
    )
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
        self.assertContains(response, "7")   # requested
        self.assertContains(response, "4")   # fetched
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

        self.assertContains(
            response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk})
        )
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

        self.assertContains(
            response, reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})
        )
        self.assertContains(
            response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk})
        )

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


class DataFeedDetailAcquisitionPanelTests(TestCase):
    """The compact acquisition panel on the feed detail page (replacing the
    stale Recent Runs panel): recent runs + a "View all" link to the
    Acquisition Activity page."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from georiva.sources.tests.support import ensure_base_datafeed_viewset
        ensure_base_datafeed_viewset()

    def setUp(self):
        self.user = User.objects.create_superuser("admin_panel", "p@test.com", "pw")
        self.client.force_login(self.user)
        self.feed = _feed()

    def _detail_url(self):
        return reverse("data_feed_detail", kwargs={"pk": self.feed.pk})

    def test_links_to_the_acquisition_activity_page(self):
        response = self.client.get(self._detail_url())

        self.assertContains(
            response, reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})
        )

    def test_shows_recent_run_counters(self):
        _run(self.feed, files_requested=90210, files_fetched=48151)

        response = self.client.get(self._detail_url())

        self.assertContains(response, "90210")
        self.assertContains(response, "48151")

    def test_panel_offers_the_check_for_new_files_action(self):
        from georiva.core.models import Collection
        from georiva.sources.models import DataFeedCollectionLink

        collection = Collection.objects.create(
            name="Rainfall", slug="rainfall", catalog=self.feed.catalog
        )
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=collection
        )

        response = self.client.get(self._detail_url())

        self.assertContains(response, 'value="check_new_files"')
        self.assertContains(response, "Check for new files")
