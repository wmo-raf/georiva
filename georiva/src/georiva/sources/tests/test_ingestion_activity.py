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


class ReingestUITests(TestCase):
    """Reingest affordances on the Ingestion Activity page (issue #224):
    per-row buttons and bulk checkboxes on failed rows only."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_rei", "re@test.com", "pw")
        self.client.force_login(self.user)
        self.feed = _feed()

    def _url(self):
        return reverse("data_feed_ingestions", kwargs={"feed_pk": self.feed.pk})

    def test_only_failed_rows_offer_reingest(self):
        failed = _ingestion(
            self.feed, "bad.grib", status=FileIngestion.Status.FAILED,
        )
        _ingestion(self.feed, "good.grib")  # completed
        _ingestion(self.feed, "busy.grib", status=FileIngestion.Status.PROCESSING)

        response = self.client.get(self._url())

        self.assertContains(response, 'name="reingest_id"', count=1)
        self.assertContains(response, 'type="checkbox" name="record_ids"', count=1)
        self.assertContains(response, f'value="{failed.pk}"', count=2)  # button + box

    def test_single_reingest_resets_the_record_and_queues_processing(self):
        from unittest.mock import patch

        failed = _ingestion(
            self.feed, "bad.grib",
            status=FileIngestion.Status.FAILED,
            error="boom", retry_count=3,
        )

        with patch("georiva.ingestion.tasks.process_incoming_file") as task:
            response = self.client.post(
                self._url(), {"reingest_id": failed.pk}, follow=True
            )

        failed.refresh_from_db()
        self.assertEqual(failed.status, FileIngestion.Status.PENDING)
        self.assertEqual(failed.error, "")
        task.delay.assert_called_once()
        self.assertEqual(
            task.delay.call_args.kwargs["file_path"], failed.file_path
        )
        self.assertRedirects(response, self._url())
        self.assertContains(response, "Reingestion queued for 1 file(s)")

    def test_bulk_reingest_queues_each_selected_failed_record(self):
        from unittest.mock import patch

        first = _ingestion(
            self.feed, "one.grib", status=FileIngestion.Status.FAILED,
        )
        second = _ingestion(
            self.feed, "two.grib", status=FileIngestion.Status.FAILED,
        )
        completed = _ingestion(self.feed, "fine.grib")

        with patch("georiva.ingestion.tasks.process_incoming_file") as task:
            response = self.client.post(
                self._url(),
                {"action": "reingest_selected",
                 "record_ids": [first.pk, second.pk, completed.pk, "junk"]},
                follow=True,
            )

        dispatched = {c.kwargs["file_path"] for c in task.delay.call_args_list}
        self.assertEqual(
            dispatched, {first.file_path, second.file_path}
        )
        completed.refresh_from_db()
        self.assertEqual(completed.status, FileIngestion.Status.COMPLETED)
        self.assertContains(response, "Reingestion queued for 2 file(s)")

    def test_crafted_reingest_outside_the_feed_or_unselected_queues_nothing(self):
        from unittest.mock import patch

        other_feed = _feed(name="Other", slug="other")
        foreign = _ingestion(
            other_feed, "foreign.grib", status=FileIngestion.Status.FAILED,
        )

        with patch("georiva.ingestion.tasks.process_incoming_file") as task:
            crafted = self.client.post(
                self._url(), {"reingest_id": foreign.pk}, follow=True
            )
            empty = self.client.post(
                self._url(), {"action": "reingest_selected"}, follow=True
            )

        task.delay.assert_not_called()
        foreign.refresh_from_db()
        self.assertEqual(foreign.status, FileIngestion.Status.FAILED)
        self.assertContains(crafted, "cannot be reingested")
        self.assertContains(empty, "No files selected")

    def test_page_wires_a_select_all_checkbox(self):
        _ingestion(self.feed, "bad.grib", status=FileIngestion.Status.FAILED)

        response = self.client.get(self._url())

        self.assertContains(response, 'id="ging-select-all"')
        self.assertContains(response, "getElementById('ging-select-all')")
        self.assertContains(response, "DOMContentLoaded")


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
