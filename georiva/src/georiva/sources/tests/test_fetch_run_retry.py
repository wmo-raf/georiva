"""
A fetch run that ended with failures retries soon, from its own plan (#480).

Tested at the FetchRun seam (when a retry falls due), the Loader seam (what the
retry fetches), the feed's link scheduling, the run page and the health chips.
"""

from contextlib import ExitStack
from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import dial_org, make_organisation
from georiva.sources.fetch.base import FailureScope, FetchResult, FileRequest
from georiva.sources.health import Health
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, DataFeedCollectionLink, FetchedFile, FetchRun


def _feed():
    catalog = Catalog.objects.create(organisation=make_organisation(), name="Test", slug="test", file_format="grib2")
    return DataFeed.objects.create(name="Feed", catalog=catalog)


def _run(feed, *, status="running", resumed_from=None, **kwargs):
    return FetchRun.objects.create(data_feed=feed, status=status, resumed_from=resumed_from, **kwargs)


class RetryScheduleTests(TestCase):
    def setUp(self):
        self.feed = _feed()

    def test_a_failed_run_is_due_after_the_first_backoff(self):
        run = _run(self.feed)
        run.mark_failed(error="Stopped: boom")

        self.assertEqual(run.retry_due_at, run.finished_at + timedelta(minutes=5))
        self.assertFalse(run.parked)

    def test_a_completed_run_with_failed_rows_is_due(self):
        run = _run(self.feed)
        run.mark_completed(files_fetched=3, files_failed=1)

        self.assertIsNotNone(run.retry_due_at)

    def test_a_clean_or_cancelled_run_is_never_due(self):
        clean, cancelled = _run(self.feed), _run(self.feed)
        clean.mark_completed(files_fetched=3, files_skipped=1)
        cancelled.mark_cancelled()

        for run in (clean, cancelled):
            run.refresh_from_db()
            self.assertIsNone(run.retry_due_at)
            self.assertFalse(run.parked)

    def test_backoff_follows_lineage_depth(self):
        expected = [5, 15, 45, 120]
        run = None
        for minutes in expected:
            run = _run(self.feed, resumed_from=run)
            run.mark_failed(error="x")
            self.assertEqual(run.retry_due_at - run.finished_at, timedelta(minutes=minutes))

    def test_the_fifth_run_in_a_lineage_is_parked(self):
        run = None
        for _ in range(4):
            run = _run(self.feed, resumed_from=run)
            run.mark_failed(error="x")
        fifth = _run(self.feed, resumed_from=run)
        fifth.mark_failed(error="x")

        fifth.refresh_from_db()
        self.assertIsNone(fifth.retry_due_at)
        self.assertTrue(fifth.parked)

    def test_retry_after_is_a_floor(self):
        long_hint, short_hint = _run(self.feed), _run(self.feed)
        long_hint.mark_failed(error="x", retry_after=3600)
        short_hint.mark_failed(error="x", retry_after=30)

        self.assertEqual(long_hint.retry_due_at - long_hint.finished_at, timedelta(hours=1))
        self.assertEqual(short_hint.retry_due_at - short_hint.finished_at, timedelta(minutes=5))

    def test_retrying_the_failed_files_by_hand_clears_a_gap(self):
        run = _run(self.feed, parked=True)
        row = FetchedFile.objects.create(fetch_run=run, file_path="a")
        row.mark_failed(error="x")
        row.mark_stored()

        run.recompute_counters()

        run.refresh_from_db()
        self.assertFalse(run.parked)


class RetryLoaderTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Test", slug="test", file_format="grib2"
        )
        self.collection = Collection.objects.create(name="Col", slug="col", catalog=catalog)
        self.other = Collection.objects.create(name="Other", slug="other", catalog=catalog)
        self.feed = DataFeed.objects.create(name="Feed", catalog=catalog)

    def _earlier(self, outcomes, *, payload=True):
        """A finished run with one row per outcome ('stored' / 'failed')."""
        run = _run(self.feed, collection=self.collection)
        for i, outcome in enumerate(outcomes):
            request = FileRequest(identifier=f"f{i}", filename=f"f{i}.grib")
            row = FetchedFile.objects.create(
                fetch_run=run, file_path=f"o/test/col/f{i}.grib", request_payload=request.to_dict() if payload else None
            )
            row.mark_stored() if outcome == "stored" else row.mark_failed(error="x", scope=FailureScope.FILE)
        run.mark_completed(files_failed=outcomes.count("failed"))
        return run

    def _retry(self, earlier, *, collection=None, fresh=()):
        loader = Loader(
            data_source=MagicMock(), collection=collection or self.collection, data_feed=self.feed, resumed_from=earlier
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = list(fresh)
        fetched = []

        def fetch(request):
            fetched.append(request.filename)
            return FetchResult(request=request, success=True, status="complete", bytes_transferred=1)

        with ExitStack() as stack:
            stack.enter_context(patch.object(loader, "_already_exists", return_value=False))
            stack.enter_context(patch.object(loader, "_find_existing_catalog_path", return_value=None))
            stack.enter_context(
                patch.object(loader, "_get_storage_path", side_effect=lambda r: f"o/test/col/{r.filename}")
            )
            stack.enter_context(patch.object(loader, "_fetch_and_store", side_effect=fetch))
            stack.enter_context(patch.object(loader, "_cleanup_temp"))
            stack.enter_context(patch.object(loader.fetch_strategy, "connect"))
            stack.enter_context(patch.object(loader.fetch_strategy, "disconnect"))
            loader.run()
        return loader, fetched

    def test_the_retry_fetches_exactly_the_failed_requests_without_asking_the_source(self):
        earlier = self._earlier(["stored", "failed", "failed"])

        loader, fetched = self._retry(earlier, fresh=[FileRequest(identifier="new", filename="new.grib")])

        retry = FetchRun.objects.get(resumed_from=earlier)
        self.assertEqual(fetched, ["f1.grib", "f2.grib"])
        loader.data_source.generate_requests_for_collection.assert_not_called()
        self.assertEqual(retry.fetched_files.count(), 2)
        self.assertEqual(retry.status, FetchRun.Status.COMPLETED)
        self.assertEqual(retry.collection, self.collection)

    def test_the_earlier_run_is_left_untouched(self):
        earlier = self._earlier(["failed"])

        self._retry(earlier)

        earlier.refresh_from_db()
        self.assertEqual(earlier.files_failed, 1)
        self.assertEqual(earlier.fetched_files.get().status, FetchedFile.Status.FAILED)

    def test_a_failed_row_without_a_stored_request_makes_the_retry_ask_the_source(self):
        earlier = self._earlier(["failed"], payload=False)
        fresh = [FileRequest(identifier="n", filename="n.grib")]

        loader, fetched = self._retry(earlier, fresh=fresh)

        loader.data_source.generate_requests_for_collection.assert_called_once()
        self.assertEqual(fetched, ["n.grib"])

    def test_a_run_with_no_plan_makes_the_retry_ask_the_source(self):
        earlier = _run(self.feed, collection=self.collection)
        earlier.mark_failed(error="Stopped: could not plan")
        fresh = [FileRequest(identifier="n", filename="n.grib")]

        loader, fetched = self._retry(earlier, fresh=fresh)

        loader.data_source.generate_requests_for_collection.assert_called_once()
        self.assertEqual(fetched, ["n.grib"])

    def test_a_plan_for_another_collection_is_not_used(self):
        earlier = self._earlier(["failed"])

        loader, _ = self._retry(earlier, collection=self.other)

        loader.data_source.generate_requests_for_collection.assert_called_once()

    def test_the_retry_after_hint_of_a_failed_fetch_floors_the_next_retry(self):
        loader = Loader(data_source=MagicMock(), collection=self.collection, data_feed=self.feed)
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = [FileRequest(identifier="a", filename="a")]
        busy = FetchResult(request=None).fail("429", FailureScope.SOURCE)
        busy.retry_after = 7200
        with ExitStack() as stack:
            stack.enter_context(patch.object(loader, "_already_exists", return_value=False))
            stack.enter_context(patch.object(loader, "_find_existing_catalog_path", return_value=None))
            stack.enter_context(patch.object(loader, "_fetch_and_store", return_value=busy))
            stack.enter_context(patch.object(loader, "_cleanup_temp"))
            stack.enter_context(patch.object(loader.fetch_strategy, "connect"))
            stack.enter_context(patch.object(loader.fetch_strategy, "disconnect"))
            loader.run()

        run = FetchRun.objects.get()
        self.assertEqual(run.retry_due_at - run.finished_at, timedelta(hours=2))

    def test_a_links_last_run_at_is_the_start_of_the_run(self):
        link = DataFeedCollectionLink.objects.create(data_feed=self.feed, collection=self.collection)
        loader = Loader(data_source=MagicMock(), collection=self.collection, data_feed=self.feed)
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = []
        with patch.object(loader.fetch_strategy, "connect"), patch.object(loader.fetch_strategy, "disconnect"):
            loader.run()

        link.refresh_from_db()
        self.assertEqual(link.last_run_at, FetchRun.objects.get().started_at)


class RetryVisibilityTests(TestCase):
    def setUp(self):
        self.feed = _feed()

    def _rank(self):
        return DataFeed.objects.with_health().get().health_rank

    def test_a_feed_with_a_retry_pending_is_retrying_until_it_is_retried(self):
        DataFeed.objects.update(last_run_status="failed", last_run_at=timezone.now())
        run = _run(self.feed)
        run.mark_failed(error="x")
        self.assertEqual(self._rank(), Health.RETRYING.rank)

        _run(self.feed, resumed_from=run)

        self.assertEqual(self._rank(), Health.FAILED.rank)

    def test_a_recently_parked_run_is_a_gap_even_after_a_later_success(self):
        DataFeed.objects.update(last_run_status="success", last_run_at=timezone.now())
        _run(self.feed, status="failed", parked=True, finished_at=timezone.now() - timedelta(hours=47))

        self.assertEqual(self._rank(), Health.GAPS.rank)

    def test_a_gap_ages_out_after_48_hours(self):
        DataFeed.objects.update(last_run_status="success", last_run_at=timezone.now())
        _run(self.feed, status="failed", parked=True, finished_at=timezone.now() - timedelta(hours=49))

        self.assertEqual(self._rank(), Health.OK.rank)

    def test_gaps_ranks_between_failed_and_partial(self):
        self.assertTrue(Health.FAILED.rank < Health.GAPS.rank < Health.PARTIAL.rank)

    def test_the_run_page_says_where_the_retry_stands_in_both_directions(self):
        user = get_user_model().objects.create_superuser("admin_retry", "r@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(user)

        def page(run):
            url = reverse("data_feed_fetch_run_detail", kwargs={"feed_pk": self.feed.pk, "run_pk": run.pk})
            return self.client.get(url)

        first = _run(self.feed)
        first.mark_failed(error="x")
        self.assertContains(page(first), "Retry 1/4 due at")

        second = _run(self.feed, resumed_from=first)
        self.assertContains(page(first), f"Retried as run #{second.pk}")
        self.assertContains(page(second), f"Retry of run #{first.pk}")

        parked = _run(self.feed, resumed_from=second)
        parked.parked = True
        parked.status = "failed"
        parked.save()
        self.assertContains(page(parked), "Retries exhausted (2/4)")
