"""
Ingestion Activity: feed-scoped FileIngestion list (PRD #217, issue #219).

Scoping is by the feed's catalog path prefix — FileIngestion joins the
acquisition side via file_path, not FKs (ADR-0003) — so failed Ingestions
never associated with any Collection still appear on the feed's page.
"""
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.ingestion.ingestion_tracking import feed_file_ingestions
from georiva.ingestion.models import FileIngestion
from georiva.sources.models import DataFeed

User = get_user_model()


def _feed(name="CHIRPS", slug="chirps"):
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    feed = DataFeed.objects.create(name=f"{name} Feed", catalog=catalog)
    return feed


def _ingestion(feed, filename, *, status=FileIngestion.Status.COMPLETED,
               collections=(), **fields):
    record = FileIngestion.objects.create(
        bucket="sources",
        file_path=f"{feed.catalog.slug}/rainfall/{filename}",
        status=status,
        **fields,
    )
    record.collections.set(collections)
    return record


class FeedFileIngestionsTests(TestCase):
    """feed_file_ingestions: the query the Ingestion Activity page renders."""

    def setUp(self):
        self.feed = _feed()

    def test_scopes_by_catalog_prefix_and_includes_collectionless_failures(self):
        orphaned_failure = _ingestion(
            self.feed, "orphan.tif", status=FileIngestion.Status.FAILED,
        )
        linked = _ingestion(self.feed, "linked.tif")

        other_feed = _feed(name="Other", slug="other")
        _ingestion(other_feed, "foreign.tif")

        records = list(feed_file_ingestions(self.feed))

        self.assertCountEqual(records, [orphaned_failure, linked])

    def test_status_filter_narrows_to_a_single_status(self):
        failed = _ingestion(self.feed, "bad.tif", status=FileIngestion.Status.FAILED)
        _ingestion(self.feed, "good.tif")

        records = list(
            feed_file_ingestions(self.feed, status=FileIngestion.Status.FAILED)
        )

        self.assertEqual(records, [failed])

    def test_collection_filter_narrows_via_the_m2m_and_none_finds_orphans(self):
        from georiva.ingestion.ingestion_tracking import NO_COLLECTION

        rainfall = Collection.objects.create(
            name="Rainfall", slug="rainfall", catalog=self.feed.catalog
        )
        wind = Collection.objects.create(
            name="Wind", slug="wind", catalog=self.feed.catalog
        )
        rain_record = _ingestion(self.feed, "rain.tif", collections=[rainfall])
        _ingestion(self.feed, "wind.tif", collections=[wind])
        orphan = _ingestion(
            self.feed, "orphan.tif", status=FileIngestion.Status.FAILED,
        )

        self.assertEqual(
            list(feed_file_ingestions(self.feed, collection=rainfall)),
            [rain_record],
        )
        self.assertEqual(
            list(feed_file_ingestions(self.feed, collection=NO_COLLECTION)),
            [orphan],
        )


class IngestionActivityViewTests(TestCase):
    """The Ingestion Activity page: a thin view over feed_file_ingestions."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_ing", "i@test.com", "pw")
        self.client.force_login(self.user)
        self.feed = _feed()
        self.rainfall = Collection.objects.create(
            name="Rainfall", slug="rainfall", catalog=self.feed.catalog
        )

    def _url(self):
        return reverse("data_feed_ingestions", kwargs={"feed_pk": self.feed.pk})

    def test_lists_records_with_status_collections_retries_error_and_counts(self):
        _ingestion(
            self.feed, "bad.grib",
            status=FileIngestion.Status.FAILED,
            collections=[self.rainfall],
            retry_count=2,
            error="no variables matched the collection definition",
            items_created=0,
            assets_created=0,
        )

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "bad.grib")
        self.assertContains(response, "Failed")
        self.assertContains(response, "Rainfall")
        self.assertContains(response, "no variables matched the collection definition")
        self.assertContains(response, "2")  # retry count

    def test_filters_round_trip_via_get_params(self):
        from georiva.ingestion.ingestion_tracking import NO_COLLECTION

        _ingestion(self.feed, "linked-ok.grib", collections=[self.rainfall])
        _ingestion(
            self.feed, "orphan-bad.grib", status=FileIngestion.Status.FAILED,
        )

        by_status = self.client.get(self._url(), {"status": "failed"})
        self.assertContains(by_status, "orphan-bad.grib")
        self.assertNotContains(by_status, "linked-ok.grib")

        by_collection = self.client.get(
            self._url(), {"collection": self.rainfall.pk}
        )
        self.assertContains(by_collection, "linked-ok.grib")
        self.assertNotContains(by_collection, "orphan-bad.grib")

        orphans_only = self.client.get(self._url(), {"collection": NO_COLLECTION})
        self.assertContains(orphans_only, "orphan-bad.grib")
        self.assertNotContains(orphans_only, "linked-ok.grib")

    def test_list_is_paginated_at_25(self):
        for i in range(26):
            _ingestion(self.feed, f"file-{i}.grib")

        first = self.client.get(self._url())
        self.assertEqual(first.context["page"].paginator.num_pages, 2)
        self.assertEqual(len(first.context["records"]), 25)

        second = self.client.get(self._url(), {"page": 2})
        self.assertEqual(len(second.context["records"]), 1)

    def test_requires_admin_login(self):
        self.client.logout()

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 302)


class CheckUnprocessedViewTests(TestCase):
    """The check/ingest actions on the Ingestion Activity page (issue #223)."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_chk", "k@test.com", "pw")
        self.client.force_login(self.user)
        self.feed = _feed()

    def _url(self):
        return reverse("data_feed_ingestions", kwargs={"feed_pk": self.feed.pk})

    def _found(self, *paths, reason="untracked"):
        from georiva.ingestion.unprocessed import UnprocessedFile
        return [
            UnprocessedFile(bucket="sources", file_path=p, reason=reason)
            for p in paths
        ]

    def test_check_renders_the_scoped_scan_results(self):
        from unittest.mock import patch

        with patch(
            "georiva.ingestion.unprocessed.find_unprocessed",
            return_value=self._found("chirps/rainfall/stuck.grib"),
        ) as find:
            response = self.client.post(self._url(), {"action": "check_unprocessed"})

        find.assert_called_once_with(prefix="chirps/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "chirps/rainfall/stuck.grib")
        self.assertContains(response, "untracked")

    def test_check_with_a_clean_bucket_shows_a_clear_empty_state(self):
        from unittest.mock import patch

        with patch(
            "georiva.ingestion.unprocessed.find_unprocessed", return_value=[]
        ):
            response = self.client.post(self._url(), {"action": "check_unprocessed"})

        self.assertContains(response, "No unprocessed files")

    def test_ingest_now_registers_resets_and_dispatches_per_file(self):
        from unittest.mock import patch

        FileIngestion.objects.create(
            bucket="sources", file_path="chirps/rainfall/pending.grib",
            status=FileIngestion.Status.PENDING,
        )
        FileIngestion.objects.create(
            bucket="sources", file_path="chirps/rainfall/dead.grib",
            status=FileIngestion.Status.COMPLETED, force_reingest=True,
        )
        found = (
            self._found("chirps/rainfall/new.grib", reason="untracked")
            + self._found("chirps/rainfall/pending.grib", reason="pending")
            + self._found("chirps/rainfall/dead.grib", reason="reingest")
        )

        with (
            patch("georiva.ingestion.unprocessed.find_unprocessed",
                  return_value=found),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            response = self.client.post(
                self._url(), {"action": "ingest_now"}, follow=True
            )

        dispatched = {c.kwargs["file_path"] for c in task.delay.call_args_list}
        self.assertEqual(dispatched, {
            "chirps/rainfall/new.grib",
            "chirps/rainfall/pending.grib",
            "chirps/rainfall/dead.grib",
        })
        # The untracked file now has a FileIngestion; the dead one was reset.
        self.assertTrue(FileIngestion.objects.filter(
            file_path="chirps/rainfall/new.grib").exists())
        self.assertEqual(
            FileIngestion.objects.get(file_path="chirps/rainfall/dead.grib").status,
            FileIngestion.Status.PENDING,
        )
        self.assertRedirects(response, self._url())
        self.assertContains(response, "3 file(s) queued for ingestion")

    def test_ingest_now_with_nothing_found_explains_itself(self):
        from unittest.mock import patch

        with (
            patch("georiva.ingestion.unprocessed.find_unprocessed",
                  return_value=[]),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            response = self.client.post(
                self._url(), {"action": "ingest_now"}, follow=True
            )

        task.delay.assert_not_called()
        self.assertContains(response, "No unprocessed files")


class DataFeedDetailIngestionPanelTests(TestCase):
    """The compact ingestion panel on the feed detail page: recent records +
    a "View all" link to the Ingestion Activity page."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from georiva.sources.tests.support import ensure_base_datafeed_viewset
        ensure_base_datafeed_viewset()

    def setUp(self):
        self.user = User.objects.create_superuser("admin_ipanel", "ip@test.com", "pw")
        self.client.force_login(self.user)
        self.feed = _feed()

    def test_panel_shows_recent_records_and_links_to_the_full_page(self):
        _ingestion(
            self.feed, "recent-orphan.grib", status=FileIngestion.Status.FAILED,
        )

        response = self.client.get(
            reverse("data_feed_detail", kwargs={"pk": self.feed.pk})
        )

        self.assertContains(
            response,
            reverse("data_feed_ingestions", kwargs={"feed_pk": self.feed.pk}),
        )
        self.assertContains(response, "recent-orphan.grib")

    def test_panel_offers_the_check_unprocessed_action(self):
        response = self.client.get(
            reverse("data_feed_detail", kwargs={"pk": self.feed.pk})
        )

        self.assertContains(response, 'value="check_unprocessed"')
        self.assertContains(response, "Check unprocessed files")
