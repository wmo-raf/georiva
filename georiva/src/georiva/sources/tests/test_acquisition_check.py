"""
"Check for new files" dry run + "Fetch now" (PRD #217, issue #222).

The check is synchronous and ephemeral: it asks the source what it would
fetch, classifies each candidate against storage, and persists nothing —
no FetchRun/FetchedFile, no feed counter changes. "Fetch now" is the
existing asynchronous run_now, unchanged.
"""
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.sources.loader import Loader
from georiva.sources.models import (
    DataFeed,
    DataFeedCollectionLink,
    FetchedFile,
    FetchRun,
)


User = get_user_model()


def _feed_and_collection(name="CHIRPS", slug="chirps"):
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    collection = Collection.objects.create(
        name="Rainfall", slug="rainfall", catalog=catalog
    )
    feed = DataFeed.objects.create(name=f"{name} Feed", catalog=catalog)
    return feed, collection


def _request(filename):
    req = MagicMock()
    req.filename = filename
    req.reference_time = None
    return req


class LoaderCheckNewFilesTests(TestCase):
    """Loader.check_new_files: the read-only dry run behind the check action."""

    def setUp(self):
        self.feed, self.collection = _feed_and_collection()

    def _check(self, requests, exists_by_filename):
        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.generate_requests_for_collection.return_value = requests

        with patch.object(
            loader, "_already_exists",
            side_effect=lambda req: exists_by_filename[req.filename],
        ):
            return loader.check_new_files()

    def test_classifies_candidates_as_new_or_already_existing(self):
        candidates = self._check(
            [_request("new.tif"), _request("old.tif")],
            {"new.tif": False, "old.tif": True},
        )

        by_name = {c.filename: c for c in candidates}
        self.assertEqual(len(candidates), 2)
        self.assertFalse(by_name["new.tif"].exists)
        self.assertTrue(by_name["old.tif"].exists)
        self.assertEqual(
            by_name["new.tif"].storage_path, "chirps/rainfall/new.tif"
        )

    def test_persists_no_acquisition_records(self):
        self._check([_request("new.tif")], {"new.tif": False})

        self.assertEqual(FetchRun.objects.count(), 0)
        self.assertEqual(FetchedFile.objects.count(), 0)


class DataFeedCheckNewFilesTests(TestCase):
    """DataFeed.check_new_files: the feed-level check the view action runs —
    one result group per linked collection."""

    def setUp(self):
        self.feed, self.rainfall = _feed_and_collection()
        self.wind = Collection.objects.create(
            name="Wind", slug="wind", catalog=self.feed.catalog
        )
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=self.rainfall
        )
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=self.wind
        )

    def test_groups_candidates_per_linked_collection(self):
        from georiva.sources.loader import CandidateFile

        def loader_for(collection=None):
            loader = MagicMock()
            loader.check_new_files.return_value = [
                CandidateFile(
                    filename=f"{collection.slug}.tif",
                    storage_path=f"chirps/{collection.slug}/{collection.slug}.tif",
                    exists=(collection == self.wind),
                )
            ]
            return loader

        with patch.object(DataFeed, "get_loader", side_effect=loader_for):
            results = self.feed.get_real_instance().check_new_files()

        by_collection = {r["collection"]: r for r in results}
        self.assertEqual(len(results), 2)
        self.assertEqual(
            [c.filename for c in by_collection[self.rainfall]["candidates"]],
            ["rainfall.tif"],
        )
        self.assertEqual(
            [c.filename for c in by_collection[self.wind]["candidates"]],
            ["wind.tif"],
        )

    def test_check_leaves_feed_stats_untouched(self):
        from georiva.sources.loader import CandidateFile

        def loader_for(collection=None):
            loader = MagicMock()
            loader.check_new_files.return_value = [
                CandidateFile("a.tif", "chirps/x/a.tif", exists=False)
            ]
            return loader

        with patch.object(DataFeed, "get_loader", side_effect=loader_for):
            self.feed.get_real_instance().check_new_files()

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.total_runs, 0)
        self.assertEqual(self.feed.total_files_fetched, 0)
        self.assertIsNone(self.feed.last_run_at)

    def test_a_failing_source_reports_per_collection_instead_of_raising(self):
        def loader_for(collection=None):
            loader = MagicMock()
            if collection == self.rainfall:
                loader.check_new_files.side_effect = ConnectionError("host down")
            else:
                loader.check_new_files.return_value = []
            return loader

        with patch.object(DataFeed, "get_loader", side_effect=loader_for):
            results = self.feed.get_real_instance().check_new_files()

        by_collection = {r["collection"]: r for r in results}
        self.assertIn("host down", by_collection[self.rainfall]["error"])
        self.assertIsNone(by_collection[self.wind]["error"])


class CheckNewFilesViewTests(TestCase):
    """The check/fetch actions on the Acquisition Activity page."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_check", "c@test.com", "pw")
        self.client.force_login(self.user)
        self.feed, self.rainfall = _feed_and_collection()
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=self.rainfall
        )

    def _url(self):
        return reverse("data_feed_fetch_runs", kwargs={"feed_pk": self.feed.pk})

    def test_check_renders_results_grouped_per_collection(self):
        from georiva.sources.loader import CandidateFile

        canned = [{
            "collection": self.rainfall,
            "candidates": [
                CandidateFile("fresh.tif", "chirps/rainfall/fresh.tif", exists=False),
                CandidateFile("stale.tif", "chirps/rainfall/stale.tif", exists=True),
            ],
            "error": None,
        }]

        with patch.object(DataFeed, "check_new_files", return_value=canned):
            response = self.client.post(self._url(), {"action": "check_new_files"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Rainfall")
        self.assertContains(response, "fresh.tif")
        self.assertContains(response, "stale.tif")

    def test_check_with_nothing_on_offer_shows_a_clear_empty_state(self):
        canned = [{"collection": self.rainfall, "candidates": [], "error": None}]

        with patch.object(DataFeed, "check_new_files", return_value=canned):
            response = self.client.post(self._url(), {"action": "check_new_files"})

        self.assertContains(response, "Source offered no files")

    def test_check_shows_a_source_error_instead_of_crashing(self):
        canned = [{
            "collection": self.rainfall, "candidates": [], "error": "host down",
        }]

        with patch.object(DataFeed, "check_new_files", return_value=canned):
            response = self.client.post(self._url(), {"action": "check_new_files"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "host down")

    def test_fetch_now_dispatches_the_feeds_async_run_and_confirms(self):
        with patch.object(DataFeed, "run_now") as run_now:
            response = self.client.post(
                self._url(), {"action": "fetch_now"}, follow=True
            )

        run_now.assert_called_once()
        self.assertEqual(run_now.call_args.kwargs.get("user"), self.user)
        self.assertRedirects(response, self._url())
        self.assertContains(response, "Fetch started")
