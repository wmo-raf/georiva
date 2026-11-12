"""
A harvest's runs show on the same dashboard and run pages as any feed's, with
a harvest's own counts where a fetch shows file counts (ADR 0042 §4).

Driven through the admin's pages. Runs are made by harvesting the fake Upstream.
"""

from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.harvest.models import HarvestFeed
from georiva.harvest.tasks import run_harvest
from georiva.organisations.testing import dial_org
from georiva.sources.models import FetchRun, FetchRunFinished

from .support import UPSTREAM_URL, HarvestFeedMixin, day, rain_href

User = get_user_model()

#: What only a feed that fetches files has to say.
FILE_TALK = ("requested", "Bytes", "No file records", "Check for new files", "Ingestion Activity")


class HarvestRunPagesMixin(HarvestFeedMixin):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        patcher = mock.patch.object(HarvestFeed, "upstream_client", lambda feed: self.upstream)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client.force_login(User.objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client)

    def harvest_one_indexed_one_rejected(self):
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.upstream.add_item(day(2), {"rainfall": "https://elsewhere.example/chirps-02.tif"})
        self.harvest()
        return FetchRun.objects.get()

    def harvest_an_unreachable_upstream(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"
        self.harvest()
        self.upstream.unreachable = None
        return FetchRun.objects.get()

    def get(self, name, **kwargs):
        response = self.client.get(reverse(name, kwargs=kwargs))
        self.assertEqual(response.status_code, 200)
        return response

    def assertNoFileTalk(self, response):
        for words in FILE_TALK:
            self.assertNotContains(response, words)


class HarvestFeedDashboardTests(HarvestRunPagesMixin, TestCase):
    def test_the_last_run_shows_harvest_counts_not_file_counts(self):
        self.harvest_one_indexed_one_rejected()

        response = self.get("data_feed_detail", pk=self.feed.pk)

        self.assertContains(response, "Harvest Activity")
        self.assertContains(response, "1 indexed · 0 updated · 0 removed · 1 rejected")
        self.assertNoFileTalk(response)

    def test_it_shows_what_the_feed_is_set_up_to_index(self):
        response = self.get("data_feed_detail", pk=self.feed.pk)

        self.assertContains(response, UPSTREAM_URL)
        self.assertContains(response, "chirps-daily")
        self.assertContains(response, "stac.upstream.example")
        self.assertContains(response, "rainfall")
        self.assertNotContains(response, "No collection definitions")

    def test_a_feed_with_nothing_mapped_says_it_indexes_nothing(self):
        self.feed.variable_mappings.all().delete()

        response = self.get("data_feed_detail", pk=self.feed.pk)

        self.assertContains(response, "No asset key is mapped")

    def test_a_failed_last_run_shows_why_it_stopped_and_when_it_retries(self):
        self.harvest_an_unreachable_upstream()

        response = self.get("data_feed_detail", pk=self.feed.pk)

        self.assertContains(response, "Stopped early: source unavailable")
        self.assertContains(response, "Retry 1/")
        self.assertNoFileTalk(response)

    def test_a_feed_never_harvested_renders(self):
        response = self.get("data_feed_detail", pk=self.feed.pk)

        self.assertContains(response, "No runs yet.")

    def test_the_pull_feed_listing_lists_it(self):
        self.harvest_one_indexed_one_rejected()

        response = self.get("data_feed_list")

        self.assertContains(response, "CHIRPS upstream")
        self.assertNotContains(response, "Ingestions")


class HarvestRunListTests(HarvestRunPagesMixin, TestCase):
    def test_runs_are_listed_with_harvest_counts(self):
        self.harvest_one_indexed_one_rejected()

        response = self.get("data_feed_fetch_runs", feed_pk=self.feed.pk)

        for column in ("Indexed", "Updated", "Removed", "Rejected", "Harvest now"):
            self.assertContains(response, column)
        self.assertNoFileTalk(response)
        self.assertNotContains(response, "Fetch")

    def test_a_feed_never_harvested_has_no_runs_to_list(self):
        response = self.get("data_feed_fetch_runs", feed_pk=self.feed.pk)

        self.assertContains(response, "No harvest runs yet")

    def test_run_now_harvests(self):
        url = reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})

        with mock.patch.object(run_harvest, "delay") as delay:
            response = self.client.post(url, {"action": "fetch_now"}, follow=True)

        delay.assert_called_once_with(self.feed.pk, scheduled=False)
        self.assertContains(response, "Harvest started")

    def test_there_are_no_new_files_to_check_for(self):
        url = reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})

        response = self.client.post(url, {"action": "check_new_files"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Check results")


class HarvestRunPageTests(HarvestRunPagesMixin, TestCase):
    def run_page(self, run):
        return self.get("data_feed_fetch_run_detail", feed_pk=self.feed.pk, run_pk=run.pk)

    def test_a_run_shows_harvest_counts_and_no_file_table(self):
        response = self.run_page(self.harvest_one_indexed_one_rejected())

        self.assertContains(response, "Harvest run")
        self.assertContains(response, "1 indexed · 0 updated · 0 removed · 1 rejected")
        self.assertNoFileTalk(response)

    def test_rejected_assets_are_explained_where_they_are_counted(self):
        response = self.run_page(self.harvest_one_indexed_one_rejected())

        self.assertContains(response, "outside the asset host allowlist")

    def test_a_run_that_rejected_nothing_does_not_explain_rejection(self):
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.harvest()

        response = self.run_page(FetchRun.objects.get())

        self.assertContains(response, "1 indexed · 0 updated · 0 removed · 0 rejected")
        self.assertNotContains(response, "outside the asset host allowlist")

    def test_a_failed_run_shows_why_it_stopped_and_when_it_retries(self):
        response = self.run_page(self.harvest_an_unreachable_upstream())

        self.assertContains(response, "Stopped early: source unavailable")
        self.assertContains(response, "timed out")
        self.assertContains(response, "Retry 1/")
        self.assertNoFileTalk(response)

    def test_a_retried_run_links_to_its_retry_and_back(self):
        failed = self.harvest_an_unreachable_upstream()
        self.harvest(resumed_from=failed)
        retry = FetchRun.objects.exclude(pk=failed.pk).get()

        self.assertContains(self.run_page(failed), f"Retried as run #{retry.pk}")
        self.assertContains(self.run_page(retry), f"Retry of run #{failed.pk}")

    def test_its_history_speaks_of_assets_not_files(self):
        response = self.run_page(self.harvest_one_indexed_one_rejected())

        self.assertContains(response, "1 indexed, 0 updated, 0 removed, 1 rejected")
        self.assertNotContains(response, "0 fetched")

    def test_a_running_harvest_can_be_recovered_without_talk_of_files(self):
        run = FetchRun.begin(data_feed=self.feed, collection=self.feed.collection)

        response = self.run_page(run)

        self.assertContains(response, "Recover stale run")
        self.assertContains(response, "counted when the run ends")
        self.assertNotContains(response, "completed file")
        self.assertNotContains(response, "file activity")

        recover = self.get("data_feed_fetch_run_recover", feed_pk=self.feed.pk, run_pk=run.pk)
        self.assertContains(recover, "Recover harvest run")
        self.assertContains(recover, "fresh harvest of the same window")
        self.assertNotContains(recover, "requested")
        self.assertNotContains(recover, "unfinished file")
        self.assertNotContains(recover, "file activity")


class HarvestRunEventTests(HarvestRunPagesMixin, TestCase):
    def test_a_finished_harvest_is_summarised_in_assets(self):
        self.harvest_one_indexed_one_rejected()

        event = FetchRunFinished.objects.get()
        self.assertEqual((event.harvest_indexed, event.harvest_rejected), (1, 1))
        self.assertEqual(event.summary(), "CHIRPS upstream: 1 indexed, 0 updated, 0 removed, 1 rejected")

    def test_a_failed_harvest_is_summarised_with_why(self):
        self.harvest_an_unreachable_upstream()

        summary = FetchRunFinished.objects.get().summary()
        self.assertIn("0 indexed", summary)
        self.assertIn("source unavailable", summary)
        self.assertNotIn("fetched", summary)
