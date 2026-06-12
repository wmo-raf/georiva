from django.test import TestCase

from georiva.core.models import Catalog
from georiva.sources.models import DataFeed, FetchRun, FetchedFile


def _make_feed():
    catalog = Catalog.objects.create(name="Test", slug="test", file_format="grib2")
    return DataFeed.objects.create(name="Test Feed", catalog=catalog)


def _make_run(feed, status="running"):
    return FetchRun.objects.create(data_feed=feed, status=status)


class FetchRunTransitionTests(TestCase):
    def setUp(self):
        self.feed = _make_feed()

    def test_running_to_completed(self):
        run = _make_run(self.feed)
        run.mark_completed(files_fetched=3, bytes_transferred=1024)
        run.refresh_from_db()
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.files_fetched, 3)
        self.assertEqual(run.bytes_transferred, 1024)
        self.assertIsNotNone(run.finished_at)

    def test_running_to_failed(self):
        run = _make_run(self.feed)
        run.mark_failed(error="timeout connecting to FTP")
        run.refresh_from_db()
        self.assertEqual(run.status, "failed")
        self.assertEqual(run.error_message, "timeout connecting to FTP")
        self.assertIsNotNone(run.finished_at)

    def test_running_to_cancelled(self):
        run = _make_run(self.feed)
        run.mark_cancelled()
        run.refresh_from_db()
        self.assertEqual(run.status, "cancelled")
        self.assertIsNotNone(run.finished_at)


class FetchedFileTransitionTests(TestCase):
    def setUp(self):
        feed = _make_feed()
        self.run = _make_run(feed)

    def test_pending_to_fetching_to_stored(self):
        f = FetchedFile.objects.create(fetch_run=self.run, file_path="catalog/file.grib")
        f.mark_fetching()
        f.refresh_from_db()
        self.assertEqual(f.status, "fetching")
        self.assertIsNotNone(f.started_at)

        f.mark_stored(bytes_transferred=512)
        f.refresh_from_db()
        self.assertEqual(f.status, "stored")
        self.assertEqual(f.bytes_transferred, 512)
        self.assertIsNotNone(f.completed_at)

    def test_skipped_is_terminal_and_always_recorded(self):
        f = FetchedFile.objects.create(fetch_run=self.run, file_path="catalog/already.grib")
        f.mark_skipped(reason="already exists")
        f.refresh_from_db()
        self.assertEqual(f.status, "skipped")
        self.assertEqual(f.skip_reason, "already exists")
        self.assertTrue(FetchedFile.objects.filter(pk=f.pk).exists())

    def test_pending_to_failed(self):
        f = FetchedFile.objects.create(fetch_run=self.run, file_path="catalog/bad.grib")
        f.mark_failed(error="checksum mismatch")
        f.refresh_from_db()
        self.assertEqual(f.status, "failed")
        self.assertEqual(f.error, "checksum mismatch")
        self.assertIsNotNone(f.completed_at)
