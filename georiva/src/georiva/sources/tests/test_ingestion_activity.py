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
