from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog
from georiva.ingestion.models import UploadSession, UploadedFile

User = get_user_model()


def _catalog():
    return Catalog.objects.create(name="Upload Test", slug="upload-test", file_format="grib2")


def _session(catalog=None, status="active"):
    if catalog is None:
        catalog = _catalog()
    return UploadSession.objects.create(catalog=catalog, status=status)


class UploadSessionTransitionTests(TestCase):
    def setUp(self):
        self.catalog = _catalog()

    def test_active_to_failed(self):
        session = _session(self.catalog)
        session.mark_failed()
        session.refresh_from_db()
        self.assertEqual(session.status, "failed")
        self.assertIsNotNone(session.completed_at)

    def test_active_to_cancelled(self):
        session = _session(self.catalog)
        session.mark_cancelled()
        session.refresh_from_db()
        self.assertEqual(session.status, "cancelled")
        self.assertIsNotNone(session.completed_at)


class UploadedFileTransitionTests(TestCase):
    def setUp(self):
        self.session = _session()

    def test_pending_to_uploading_to_stored(self):
        f = UploadedFile.objects.create(
            session=self.session,
            original_filename="rain_2024.grib",
        )
        f.mark_uploading()
        f.refresh_from_db()
        self.assertEqual(f.status, "uploading")
        self.assertIsNotNone(f.started_at)

        f.mark_stored(file_path="catalog/rain_2024.grib", bytes=2048)
        f.refresh_from_db()
        self.assertEqual(f.status, "stored")
        self.assertEqual(f.file_path, "catalog/rain_2024.grib")
        self.assertEqual(f.bytes, 2048)
        self.assertIsNotNone(f.completed_at)

    def test_pending_to_failed(self):
        f = UploadedFile.objects.create(
            session=self.session,
            original_filename="bad.grib",
        )
        f.mark_failed(error="virus detected")
        f.refresh_from_db()
        self.assertEqual(f.status, "failed")
        self.assertEqual(f.error, "virus detected")
        self.assertIsNotNone(f.completed_at)


class UploadSessionAutoCompleteTests(TestCase):
    def setUp(self):
        self.session = _session()

    def test_auto_completes_when_all_files_stored(self):
        f1 = UploadedFile.objects.create(session=self.session, original_filename="a.grib")
        f2 = UploadedFile.objects.create(session=self.session, original_filename="b.grib")
        f1.mark_stored(file_path="a.grib", bytes=100)
        f2.mark_stored(file_path="b.grib", bytes=200)
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "completed")
        self.assertIsNotNone(self.session.completed_at)

    def test_auto_completes_when_mix_of_stored_and_failed(self):
        f1 = UploadedFile.objects.create(session=self.session, original_filename="a.grib")
        f2 = UploadedFile.objects.create(session=self.session, original_filename="b.grib")
        f1.mark_stored(file_path="a.grib", bytes=100)
        f2.mark_failed(error="bad file")
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "completed")

    def test_does_not_auto_complete_while_file_still_pending(self):
        f1 = UploadedFile.objects.create(session=self.session, original_filename="a.grib")
        UploadedFile.objects.create(session=self.session, original_filename="b.grib")
        f1.mark_stored(file_path="a.grib", bytes=100)
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "active")

    def test_does_not_auto_complete_while_file_uploading(self):
        f1 = UploadedFile.objects.create(session=self.session, original_filename="a.grib")
        f2 = UploadedFile.objects.create(session=self.session, original_filename="b.grib")
        f1.mark_stored(file_path="a.grib", bytes=100)
        f2.mark_uploading()
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "active")
