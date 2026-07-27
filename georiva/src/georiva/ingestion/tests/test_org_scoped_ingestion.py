"""Ingestion resolves its organisation from the path, or refuses the file (#267).

The rule these tests pin down: a file whose leading path segment names no
organisation — or whose catalog belongs to a *different* one — produces a loud
``FileIngestion`` failure and stays exactly where it is. Nothing is guessed into
a default organisation, because the cost of guessing wrong is one institution's
national data filed under another's prefix.
"""
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.ingestion.models import FileIngestion
from georiva.ingestion.service import IngestionService
from georiva.organisations.testing import dial_org, make_organisation, org_host


def _event(bucket_name, key):
    return {"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}


class ProcessFileOrgResolutionTests(TestCase):

    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.uganda = make_organisation("uganda")
        self.catalog = Catalog.objects.create(
            organisation=self.kenya, name="CHIRPS", slug="chirps",
            file_format="geotiff", clip_mode="none",
        )
        Collection.objects.create(
            catalog=self.catalog, name="Rainfall", slug="rainfall", is_active=True
        )

    def _process(self, file_path):
        """Run process_file with the storage boundary mocked, returning the
        result and the origin bucket so deletions can be asserted on."""
        origin = MagicMock()
        with (
            patch("georiva.ingestion.service.storage") as mock_storage,
            patch("georiva.ingestion.service.format_registry"),
        ):
            mock_storage.bucket.return_value = origin
            service = IngestionService()
            service._source_file_manager = MagicMock()
            result = service.process_file(file_path, origin_bucket=BucketType.SOURCES)
        return result, origin

    def test_unknown_org_fails_and_leaves_the_file_in_place(self):
        result, origin = self._process("atlantis/chirps/rainfall/rain.tif")

        self.assertFalse(result.success)
        self.assertIn("Unknown organisation 'atlantis'.", result.errors)
        origin.delete.assert_not_called()

    def test_catalog_of_another_org_is_not_ingested_under_that_org(self):
        result, origin = self._process("uganda/chirps/rainfall/rain.tif")

        self.assertFalse(result.success)
        self.assertIn(
            "Catalog 'chirps' does not belong to organisation 'uganda'.",
            result.errors,
        )
        origin.delete.assert_not_called()

    def test_a_valid_path_resolves_the_catalog_within_its_own_org(self):
        result, _ = self._process("kenya/chirps/rainfall/rain.tif")

        # It gets past resolution — the next failure is about the file's
        # content, not about who owns it.
        self.assertNotIn("Unknown organisation 'kenya'.", result.errors)
        self.assertFalse(
            any("does not belong to organisation" in e for e in result.errors)
        )


class ConsumerOrgResolutionTests(TestCase):
    """The MinIO consumer records the refusal instead of dropping it silently."""

    def setUp(self):
        self.kenya = make_organisation("kenya")
        make_organisation("uganda")
        Catalog.objects.create(
            organisation=self.kenya, name="CHIRPS", slug="chirps", file_format="grib2"
        )

    def _handle(self, key):
        from georiva.ingestion.consumer import _handle_event

        with (
            patch(
                "georiva.ingestion.consumer._resolve_origin",
                return_value=BucketType.SOURCES,
            ),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            task.delay = MagicMock()
            _handle_event(_event("georiva-sources", key))
        return task

    def test_unknown_org_is_recorded_as_a_failure_and_never_queued(self):
        key = "atlantis/chirps/rainfall/rain.grib2"
        task = self._handle(key)

        log = FileIngestion.objects.get(file_path=key)
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertIn("Unknown organisation 'atlantis'", log.error)
        task.delay.assert_not_called()

    def test_catalog_outside_the_paths_org_is_recorded_as_a_failure(self):
        key = "uganda/chirps/rainfall/rain.grib2"
        task = self._handle(key)

        log = FileIngestion.objects.get(file_path=key)
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertIn("does not belong to organisation 'uganda'", log.error)
        task.delay.assert_not_called()

    def test_a_file_in_its_own_org_is_queued(self):
        key = "kenya/chirps/rainfall/rain.grib2"
        task = self._handle(key)

        log = FileIngestion.objects.get(file_path=key)
        self.assertEqual(log.status, FileIngestion.Status.PENDING)
        task.delay.assert_called_once()


class SweepOrgResolutionTests(TestCase):
    """Recovery honours the same org rules as the live event path.

    ``sweep_unprocessed`` is the safety net for files the bucket event missed,
    so it is the obvious place for a "just ingest whatever is lying around"
    shortcut to creep in. It must reach the same refusal.
    """

    def setUp(self):
        make_organisation("kenya")
        Catalog.objects.create(
            organisation=make_organisation("uganda"), name="CHIRPS", slug="chirps",
            file_format="geotiff", clip_mode="none",
        )

    def test_a_swept_file_of_another_org_is_refused_by_the_same_resolver(self):
        from georiva.ingestion.job_types import FileIngestionJobType
        from georiva.ingestion.models import FileIngestionJob
        from georiva.ingestion.unprocessed import UnprocessedFile, ingest_unprocessed

        key = "kenya/chirps/rainfall/rain.tif"

        with patch("georiva.ingestion.tasks.process_incoming_file") as task:
            task.delay = MagicMock()
            ingest_unprocessed([
                UnprocessedFile(
                    bucket=BucketType.SOURCES, file_path=key, reason="untracked"
                )
            ])

        # The sweep registers and dispatches; the refusal happens where every
        # other path's does — inside the job that runs IngestionService.
        task.delay.assert_called_once()

        from django.contrib.contenttypes.models import ContentType

        job = FileIngestionJob.objects.create(
            user=None,
            content_type=ContentType.objects.get_for_model(
                FileIngestionJob, for_concrete_model=False
            ),
            file_path=key,
            bucket=BucketType.SOURCES,
        )

        with (
            patch("georiva.ingestion.service.storage") as mock_storage,
            patch("georiva.ingestion.service.format_registry"),
            self.assertRaises(RuntimeError) as raised,
        ):
            mock_storage.bucket.return_value = MagicMock()
            FileIngestionJobType().run(job, MagicMock())

        self.assertIn("does not belong to organisation 'kenya'", str(raised.exception))
        log = FileIngestion.objects.get(file_path=key)
        self.assertEqual(log.status, FileIngestion.Status.FAILED)
        self.assertIn("does not belong to organisation 'kenya'", log.error)


class ServerSideOrgSegmentTests(TestCase):
    """Writers derive the org segment from the catalog, never from a client."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation("kenya"), name="CHIRPS", slug="chirps",
            file_format="grib2",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog, name="Rainfall", slug="rainfall", is_active=True
        )

    def test_manual_upload_path_starts_with_the_catalogs_org(self):
        from georiva.ingestion.upload_views import _build_incoming_path

        config = MagicMock()
        config.catalog = self.catalog

        path = _build_incoming_path(
            config, variable=None, filename="rain.grib2",
            reference_time=None, valid_time=None,
        )
        self.assertEqual(path, "kenya/chirps/rain.grib2")

    def test_loader_path_starts_with_the_feeds_org(self):
        from georiva.sources.loader import Loader

        loader = Loader.__new__(Loader)
        loader.collection = self.collection

        request = MagicMock()
        request.filename = "rain.grib2"
        request.reference_time = None

        self.assertEqual(
            loader._get_storage_path(request), "kenya/chirps/rainfall/rain.grib2"
        )


class UploadWizardCatalogScopingTests(TestCase):
    """The wizard never lets an operator target another organisation's catalog.

    Deriving the org segment server-side is only half the guarantee: if the
    wizard will accept *any* catalog id, an operator on org A can still pick org
    B's catalog and have every later upload written under ``B/…``. Server-side
    derivation would faithfully misfile it.
    """

    STEP1_URL = "/admin/manual-uploads/wizard/step1/"
    SESSION_KEY = "georiva_upload_wizard"

    def setUp(self):
        dial_org(self.client)
        self.client.force_login(
            get_user_model().objects.create_superuser("orgadmin", "o@x.com", "pw")
        )
        self.client.defaults["HTTP_HOST"] = org_host("kenya")
        make_organisation("kenya")
        self.other_orgs_catalog = Catalog.objects.create(
            organisation=make_organisation("uganda"), name="Rain", slug="rain",
            file_format="grib2",
        )

    def test_another_orgs_catalog_is_not_offered(self):
        response = self.client.get(self.STEP1_URL)
        self.assertNotIn(self.other_orgs_catalog, response.context["all_catalogs"])

    def test_selecting_another_orgs_catalog_is_refused(self):
        response = self.client.post(self.STEP1_URL, {
            "catalog_mode": "select",
            "catalog_id": self.other_orgs_catalog.pk,
        })

        # Re-rendered with an error rather than advancing, and nothing stored.
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(self.client.session.get(self.SESSION_KEY))

    def test_the_same_slug_in_our_own_org_is_still_selectable(self):
        """Per-org slugs mean 'rain' is free here even though another org has it."""
        ours = Catalog.objects.create(
            organisation=make_organisation("kenya"), name="Rain", slug="rain",
            file_format="grib2",
        )

        response = self.client.post(self.STEP1_URL, {
            "catalog_mode": "select",
            "catalog_id": ours.pk,
        })

        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.client.session[self.SESSION_KEY]["catalog_id"], ours.pk)
