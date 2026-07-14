"""
Per-file re-fetch (PRD #217, issue #225).

A FetchedFile stores the serialized FileRequest that produced it; the retry
task rebuilds the request, re-fetches with skip-existing disabled, updates
the SAME record in place, and recomputes the parent FetchRun's counters.
Records without a stored request refuse retry gracefully.
"""
from datetime import datetime, timezone as dt_timezone
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.sources.fetch.base import FetchResult, FileRequest
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, FetchedFile, FetchRun


class FileRequestRoundTripTests(TestCase):
    """FileRequest ⇄ JSON-safe dict: the payload persisted on FetchedFile."""

    def test_round_trips_through_a_json_safe_dict(self):
        import json

        request = FileRequest(
            identifier="gfs-2026071406-t2m",
            filename="t2m.grib2",
            valid_time=datetime(2026, 7, 14, 12, 0, tzinfo=dt_timezone.utc),
            reference_time=datetime(2026, 7, 14, 6, 0, tzinfo=dt_timezone.utc),
            params={"level": "surface", "step": 6},
            expected_format="grib",
            variables=["t2m"],
        )

        payload = request.to_dict()
        json.dumps(payload)  # must be JSON-serializable as stored
        rebuilt = FileRequest.from_dict(payload)

        self.assertEqual(rebuilt, request)

    def test_round_trips_without_optional_temporal_context(self):
        request = FileRequest(identifier="static-ndvi", filename="ndvi.tif")

        rebuilt = FileRequest.from_dict(request.to_dict())

        self.assertEqual(rebuilt, request)
        self.assertIsNone(rebuilt.valid_time)


User = get_user_model()


def _feed_and_collection(name="CHIRPS", slug="chirps"):
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    collection = Collection.objects.create(
        name="Rainfall", slug="rainfall", catalog=catalog
    )
    feed = DataFeed.objects.create(name=f"{name} Feed", catalog=catalog)
    return feed, collection


class LoaderPersistsRequestPayloadTests(TestCase):
    """Every FetchedFile the Loader records carries the request that produced
    it — stored, skipped, and failed alike — so it can be re-fetched later."""

    def setUp(self):
        self.feed, self.collection = _feed_and_collection()

    def test_stored_skipped_and_failed_files_all_carry_the_request(self):
        stored_req = FileRequest(identifier="a", filename="stored.tif")
        skipped_req = FileRequest(identifier="b", filename="skipped.tif")
        failed_req = FileRequest(identifier="c", filename="failed.tif")

        loader = Loader(
            data_source=MagicMock(),
            collection=self.collection,
            data_feed=self.feed,
        )
        loader.data_source.name = "test"
        loader.data_source.generate_requests_for_collection.return_value = [
            stored_req, skipped_req, failed_req,
        ]
        loader.data_source.post_process_fetched_file.side_effect = (
            lambda req, path: (path, None)
        )

        def fetch(req):
            if req is stored_req:
                return FetchResult(request=req, success=True, status="success",
                                   bytes_transferred=10)
            return FetchResult(request=req, success=False, status="failed",
                               error="boom")

        with (
            patch.object(loader, "_already_exists",
                         side_effect=lambda req: req is skipped_req),
            patch.object(loader, "_find_existing_catalog_path", return_value=None),
            patch.object(loader, "_fetch_and_store", side_effect=fetch),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
        ):
            loader.run(skip_existing=True)

        payloads = {
            ff.file_path.rsplit("/", 1)[-1]: ff.request_payload
            for ff in FetchedFile.objects.all()
        }
        self.assertEqual(len(payloads), 3)
        for filename in ("stored.tif", "skipped.tif", "failed.tif"):
            self.assertIsNotNone(payloads[filename], filename)
            self.assertEqual(payloads[filename]["filename"], filename)


class RetryFetchTests(TestCase):
    """retry_fetch: re-fetch one failed FetchedFile in place and keep the
    parent run's counters truthful."""

    def setUp(self):
        self.feed, self.collection = _feed_and_collection()
        self.run = FetchRun.objects.create(
            data_feed=self.feed,
            status=FetchRun.Status.COMPLETED,
            files_requested=1,
            files_failed=1,
        )
        self.ff = FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/failed.tif",
            status=FetchedFile.Status.FAILED,
            error="timeout",
            request_payload=FileRequest(
                identifier="x", filename="failed.tif"
            ).to_dict(),
        )

    def _retry(self, fetch_result):
        from georiva.sources.acquisition_retry import retry_fetch

        loader = MagicMock()
        loader.fetch_one.return_value = fetch_result
        with patch.object(DataFeed, "get_loader", return_value=loader) as get_loader:
            retry_fetch(self.ff)
        return loader, get_loader

    def test_successful_retry_updates_the_same_record_and_run_counters(self):
        request = FileRequest(identifier="x", filename="failed.tif")
        loader, get_loader = self._retry(
            FetchResult(request=request, success=True, status="success",
                        bytes_transferred=55)
        )

        get_loader.assert_called_once_with(self.collection)
        self.assertEqual(loader.fetch_one.call_args.args[0], request)

        self.ff.refresh_from_db()
        self.assertEqual(self.ff.status, FetchedFile.Status.STORED)
        self.assertEqual(self.ff.error, "")  # stale failure story cleared
        self.assertEqual(self.ff.bytes_transferred, 55)
        self.assertEqual(FetchedFile.objects.count(), 1)  # same record, no new rows

        self.run.refresh_from_db()
        self.assertEqual(self.run.files_fetched, 1)
        self.assertEqual(self.run.files_failed, 0)

    def test_failed_retry_records_the_new_error_and_keeps_counters_truthful(self):
        request = FileRequest(identifier="x", filename="failed.tif")
        self._retry(
            FetchResult(request=request, success=False, status="failed",
                        error="still unreachable")
        )

        self.ff.refresh_from_db()
        self.assertEqual(self.ff.status, FetchedFile.Status.FAILED)
        self.assertEqual(self.ff.error, "still unreachable")

        self.run.refresh_from_db()
        self.assertEqual(self.run.files_failed, 1)
        self.assertEqual(self.run.files_fetched, 0)

    def test_record_without_a_stored_request_refuses_retry_untouched(self):
        from georiva.sources.acquisition_retry import RetryNotPossible, retry_fetch

        legacy = FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/legacy.tif",
            status=FetchedFile.Status.FAILED,
            error="old failure",
        )

        with self.assertRaises(RetryNotPossible):
            retry_fetch(legacy)

        legacy.refresh_from_db()
        self.assertEqual(legacy.status, FetchedFile.Status.FAILED)
        self.assertEqual(legacy.error, "old failure")


class RetryFetchedFileTaskTests(TestCase):
    """The Celery entry point: retry by id; an impossible retry is a logged
    no-op, not a worker crash."""

    def setUp(self):
        self.feed, self.collection = _feed_and_collection()
        self.run = FetchRun.objects.create(
            data_feed=self.feed, status=FetchRun.Status.COMPLETED, files_failed=1,
        )
        self.ff = FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/failed.tif",
            status=FetchedFile.Status.FAILED,
            request_payload=FileRequest(
                identifier="x", filename="failed.tif"
            ).to_dict(),
        )

    def test_task_retries_the_file_by_id(self):
        from georiva.sources.tasks import retry_fetched_file

        request = FileRequest(identifier="x", filename="failed.tif")
        loader = MagicMock()
        loader.fetch_one.return_value = FetchResult(
            request=request, success=True, status="success", bytes_transferred=9,
        )

        with patch.object(DataFeed, "get_loader", return_value=loader):
            retry_fetched_file(self.ff.pk)

        self.ff.refresh_from_db()
        self.assertEqual(self.ff.status, FetchedFile.Status.STORED)

    def test_task_is_a_noop_for_an_unretryable_record(self):
        from georiva.sources.tasks import retry_fetched_file

        legacy = FetchedFile.objects.create(
            fetch_run=self.run,
            file_path="chirps/rainfall/legacy.tif",
            status=FetchedFile.Status.FAILED,
        )

        retry_fetched_file(legacy.pk)  # must not raise

        legacy.refresh_from_db()
        self.assertEqual(legacy.status, FetchedFile.Status.FAILED)


class RunDetailRetryUITests(TestCase):
    """Retry affordances on the FetchRun detail page: per-row buttons on
    retryable failed rows, checkbox multi-select for bulk retry."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_retry", "r@test.com", "pw")
        self.client.force_login(self.user)
        self.feed, self.collection = _feed_and_collection()
        self.run = FetchRun.objects.create(
            data_feed=self.feed, status=FetchRun.Status.COMPLETED,
        )

    def _file(self, filename, status=FetchedFile.Status.FAILED, with_payload=True):
        payload = (
            FileRequest(identifier=filename, filename=filename).to_dict()
            if with_payload else None
        )
        return FetchedFile.objects.create(
            fetch_run=self.run,
            file_path=f"chirps/rainfall/{filename}",
            status=status,
            request_payload=payload,
        )

    def _url(self):
        return reverse(
            "data_feed_fetch_run_detail",
            kwargs={"feed_pk": self.feed.pk, "run_pk": self.run.pk},
        )

    def test_only_retryable_failed_rows_offer_retry(self):
        retryable = self._file("retryable.tif")
        self._file("legacy.tif", with_payload=False)
        self._file("fine.tif", status=FetchedFile.Status.STORED)

        response = self.client.get(self._url())

        # Exactly one per-row retry button and one bulk checkbox — both only
        # for the retryable failed row.
        self.assertContains(response, 'name="retry_file_id"', count=1)
        self.assertContains(response, f'value="{retryable.pk}"', count=2)
        self.assertContains(response, 'type="checkbox" name="file_ids"', count=1)

    def test_single_retry_queues_the_task_and_confirms(self):
        retryable = self._file("retryable.tif")

        with patch("georiva.sources.tasks.retry_fetched_file.delay") as delay:
            response = self.client.post(
                self._url(), {"retry_file_id": retryable.pk}, follow=True
            )

        delay.assert_called_once_with(retryable.pk)
        self.assertRedirects(response, self._url())
        self.assertContains(response, "Retry queued")

    def test_crafted_retry_of_an_unretryable_file_queues_nothing(self):
        stored = self._file("fine.tif", status=FetchedFile.Status.STORED)
        legacy = self._file("legacy.tif", with_payload=False)

        with patch("georiva.sources.tasks.retry_fetched_file.delay") as delay:
            for pk in (stored.pk, legacy.pk):
                self.client.post(self._url(), {"retry_file_id": pk})

        delay.assert_not_called()

    def test_bulk_retry_queues_each_selected_retryable_file(self):
        first = self._file("one.tif")
        second = self._file("two.tif")
        stored = self._file("fine.tif", status=FetchedFile.Status.STORED)

        with patch("georiva.sources.tasks.retry_fetched_file.delay") as delay:
            response = self.client.post(
                self._url(),
                {"action": "retry_selected",
                 "file_ids": [first.pk, second.pk, stored.pk]},
                follow=True,
            )

        queued = {call.args[0] for call in delay.call_args_list}
        self.assertEqual(queued, {first.pk, second.pk})  # crafted id ignored
        self.assertContains(response, "Retry queued for 2 file(s)")

    def test_bulk_retry_with_nothing_selected_explains_instead_of_queuing(self):
        self._file("one.tif")

        with patch("georiva.sources.tasks.retry_fetched_file.delay") as delay:
            response = self.client.post(
                self._url(), {"action": "retry_selected"}, follow=True
            )

        delay.assert_not_called()
        self.assertContains(response, "No files selected")

    def test_garbage_ids_in_a_crafted_post_are_rejected_not_a_crash(self):
        with patch("georiva.sources.tasks.retry_fetched_file.delay") as delay:
            response = self.client.post(
                self._url(),
                {"action": "retry_selected", "file_ids": ["abc", "1; DROP"]},
                follow=True,
            )

        delay.assert_not_called()
        self.assertEqual(response.status_code, 200)

    def test_page_wires_a_select_all_checkbox(self):
        self._file("one.tif")

        response = self.client.get(self._url())

        # The header checkbox plus the script that wires it.
        self.assertContains(response, 'id="gaqd-select-all"')
        self.assertContains(response, "DOMContentLoaded")
        self.assertContains(response, "getElementById('gaqd-select-all')")
