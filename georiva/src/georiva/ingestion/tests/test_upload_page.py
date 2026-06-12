from unittest.mock import MagicMock, PropertyMock, patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import (
    DataArrival,
    FileIngestion,
    FileIngestionJob,
    ManualUploadConfig,
    ManualUploadConfigVariable,
)

User = get_user_model()

PAGE_URL    = "/admin/manual-uploads/{}/upload/"
EXTRACT_URL = "/admin/manual-uploads/{}/upload/extract-times/"
SUBMIT_URL  = "/admin/manual-uploads/{}/upload/submit/"

INCOMING_BUCKET_NAME = "georiva-incoming"


def _geotiff_setup():
    """Observation GeoTIFF catalog: dated path, valid time from filename."""
    catalog = Catalog.objects.create(name="Imagery", slug="imagery", file_format="geotiff")
    collection = Collection.objects.create(catalog=catalog, name="NDVI", slug="ndvi")
    config = ManualUploadConfig.objects.create(
        catalog=catalog, name="NDVI uploads",
        is_forecast=False, valid_time_format="YYYYMMDD",
    )
    variable = ManualUploadConfigVariable.objects.create(
        config=config, collection=collection,
        variable_name="band_1", long_name="NDVI", units="",
    )
    return catalog, collection, config, variable


def _grib_setup():
    """Forecast GRIB catalog: flat path with GR-- prefix, time from content."""
    catalog = Catalog.objects.create(name="Models", slug="models", file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
    config = ManualUploadConfig.objects.create(
        catalog=catalog, name="Surface uploads",
        is_forecast=True, valid_time_format="CONTENT",
    )
    variable = ManualUploadConfigVariable.objects.create(
        config=config, collection=collection,
        variable_name="2t", long_name="2m temperature", units="K",
    )
    return catalog, collection, config, variable


def _mock_incoming_bucket():
    bucket = MagicMock()
    bucket.save.side_effect = lambda path, content: path
    return bucket


class UploadPageRenderTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_up", "u@p.com", "pw")
        self.client.force_login(self.user)

    def test_page_renders_with_variable_dropdown_and_file_picker(self):
        _, _, config, variable = _geotiff_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "NDVI")
        self.assertContains(response, 'type="file"')
        self.assertContains(response, 'type="datetime-local"')

    def test_time_label_is_observation_date_for_non_forecast(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertContains(response, "Observation date")

    def test_time_label_is_model_run_time_for_forecast(self):
        _, _, config, _ = _grib_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertContains(response, "Model run time")

    def test_unknown_config_returns_404(self):
        self.assertEqual(self.client.get(PAGE_URL.format(99999)).status_code, 404)

    # ------------------------------------------------------------------
    # Cycle 1: progress log structure
    # ------------------------------------------------------------------

    def test_page_has_progress_log_container(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertContains(response, 'id="progress-log"')

    def test_page_exposes_sse_url_to_js(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertContains(response, "SSE_URL")
        self.assertContains(response, "/admin/api/ingestion/events/")

    def test_upload_form_has_id_for_js_targeting(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.get(PAGE_URL.format(config.pk))
        self.assertContains(response, 'id="upload-form"')


class ExtractTimesTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_ex", "e@x.com", "pw")
        self.client.force_login(self.user)

    def test_prefill_from_filename_stem(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.post(EXTRACT_URL.format(config.pk), {"filename": "20250115.tif"})
        data = response.json()
        self.assertEqual(data["prefill"], "2025-01-15T00:00")
        self.assertIsNotNone(data["valid_time"])

    def test_prefill_reference_time_from_gr_prefix_for_forecast(self):
        _, _, config, _ = _grib_setup()
        response = self.client.post(
            EXTRACT_URL.format(config.pk),
            {"filename": "GR--20250115T0600--gfs.grib2"},
        )
        data = response.json()
        self.assertEqual(data["prefill"], "2025-01-15T06:00")

    def test_no_prefill_for_unparseable_filename(self):
        _, _, config, _ = _geotiff_setup()
        response = self.client.post(EXTRACT_URL.format(config.pk), {"filename": "random.tif"})
        self.assertIsNone(response.json()["prefill"])

    def test_get_not_allowed(self):
        _, _, config, _ = _geotiff_setup()
        self.assertEqual(self.client.get(EXTRACT_URL.format(config.pk)).status_code, 405)


@patch("georiva.ingestion.tasks.process_incoming_file")
@patch("georiva.core.storage.StorageManager.incoming", new_callable=PropertyMock)
class UploadSubmitTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_su", "s@u.com", "pw")
        self.client.force_login(self.user)

    def test_geotiff_submit_success_full_transition(self, mock_incoming, mock_task):
        bucket = _mock_incoming_bucket()
        mock_incoming.return_value = bucket
        catalog, collection, config, variable = _geotiff_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        arrival_id = response.json()["data_arrival_id"]

        expected_path = "imagery/ndvi/band_1/2025/01/15/20250115.tif"
        arrival = DataArrival.objects.get(pk=arrival_id)
        self.assertEqual(arrival.trigger, DataArrival.Trigger.MANUAL_UPLOAD)
        self.assertEqual(arrival.status, DataArrival.Status.PENDING)
        self.assertEqual(arrival.file_path, expected_path)
        self.assertEqual(arrival.catalog, catalog)
        self.assertEqual(arrival.files_fetched, 1)

        bucket.save.assert_called_once()
        self.assertEqual(bucket.save.call_args[0][0], expected_path)

        fi = FileIngestion.objects.get(file_path=expected_path)
        self.assertEqual(fi.bucket, "incoming")
        self.assertEqual(fi.data_arrival, arrival)

        call_kwargs = mock_task.delay.call_args.kwargs
        self.assertEqual(call_kwargs["file_path"], expected_path)
        self.assertEqual(call_kwargs["origin_bucket"], "incoming")
        self.assertIsNone(call_kwargs["reference_time"])
        self.assertIsInstance(call_kwargs["job_id"], int)

    def test_geotiff_date_from_form_when_filename_unparseable(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "2025-03-02T00:00",
            "file": SimpleUploadedFile("scene.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        arrival = DataArrival.objects.get(pk=response.json()["data_arrival_id"])
        self.assertEqual(arrival.file_path, "imagery/ndvi/band_1/2025/03/02/scene.tif")

    def test_grib_forecast_path_gets_gr_prefix(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _grib_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "2025-01-15T06:00",
            "file": SimpleUploadedFile("gfs.grib2", b"grib-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        arrival = DataArrival.objects.get(pk=response.json()["data_arrival_id"])
        self.assertEqual(arrival.file_path, "models/GR--20250115T0600--gfs.grib2")
        self.assertIsNotNone(arrival.catalog)

        mock_task.delay.assert_called_once()
        self.assertEqual(
            mock_task.delay.call_args[1]["reference_time"], "2025-01-15T06:00:00+00:00"
        )

    def test_grib_forecast_gr_prefix_in_filename_wins_over_form(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _grib_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "2025-06-01T00:00",
            "file": SimpleUploadedFile("GR--20250115T0600--gfs.grib2", b"grib-bytes"),
        })

        arrival = DataArrival.objects.get(pk=response.json()["data_arrival_id"])
        self.assertEqual(arrival.file_path, "models/GR--20250115T0600--gfs.grib2")

    def test_forecast_without_time_returns_400(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _grib_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("gfs.grib2", b"grib-bytes"),
        })

        self.assertEqual(response.status_code, 400)
        self.assertIn("Model run time", response.json()["error"])
        self.assertFalse(DataArrival.objects.exists())
        mock_task.delay.assert_not_called()

    def test_missing_file_returns_400(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()
        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk, "time": "2025-01-15T00:00",
        })
        self.assertEqual(response.status_code, 400)

    def test_variable_from_other_config_rejected(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, _ = _geotiff_setup()
        _, _, _, other_variable = _grib_setup()
        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": other_variable.pk,
            "time": "2025-01-15T00:00",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })
        self.assertEqual(response.status_code, 400)

    def test_reupload_resets_spent_file_ingestion(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()

        # A previous run exhausted its retries.
        old_arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.FAILED,
            file_path="imagery/ndvi/band_1/2025/01/15/20250115.tif",
        )
        spent, _ = FileIngestion.register(
            bucket="incoming",
            file_path="imagery/ndvi/band_1/2025/01/15/20250115.tif",
            data_arrival=old_arrival,
        )
        FileIngestion.objects.filter(pk=spent.pk).update(
            status=FileIngestion.Status.FAILED,
            retry_count=FileIngestion.MAX_RETRIES,
            error="old failure",
        )

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        spent.refresh_from_db()
        self.assertEqual(spent.status, FileIngestion.Status.PENDING)
        self.assertEqual(spent.retry_count, 0)
        self.assertEqual(spent.error, "")
        self.assertIsNotNone(spent.data_arrival_id)
        mock_task.delay.assert_called_once()

    def test_minio_failure_marks_arrival_failed_and_returns_500(self, mock_incoming, mock_task):
        bucket = MagicMock()
        bucket.save.side_effect = RuntimeError("minio down")
        mock_incoming.return_value = bucket
        _, _, config, variable = _geotiff_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 500)
        arrival = DataArrival.objects.get()
        self.assertEqual(arrival.status, DataArrival.Status.FAILED)
        self.assertIn("minio down", arrival.error_message)
        mock_task.delay.assert_not_called()

    def test_submit_response_includes_job_id(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("job_id", data)
        self.assertIsNotNone(data["job_id"])

    def test_file_ingestion_job_exists_before_task_is_enqueued(self, mock_incoming, mock_task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()

        response = self.client.post(SUBMIT_URL.format(config.pk), {
            "variable_id": variable.pk,
            "time": "",
            "file": SimpleUploadedFile("20250115.tif", b"tiff-bytes"),
        })

        self.assertEqual(response.status_code, 200)
        job_id = response.json()["job_id"]
        self.assertTrue(FileIngestionJob.objects.filter(pk=job_id).exists())


@patch("georiva.ingestion.consumer.process_incoming_file")
class DirectDropTimeValidationTests(TestCase):
    """Direct MinIO drops with unextractable required times must fail clearly."""

    def _event(self, key):
        return {
            "s3": {
                "bucket": {"name": INCOMING_BUCKET_NAME},
                "object": {"key": key},
            }
        }

    def test_unparseable_geotiff_drop_fails_file_ingestion(self, mock_task):
        from georiva.ingestion.consumer import _handle_event
        _geotiff_setup()

        _handle_event(self._event("imagery/random_name.tif"))

        fi = FileIngestion.objects.get(file_path="imagery/random_name.tif")
        self.assertEqual(fi.status, FileIngestion.Status.FAILED)
        self.assertIn("Could not extract a valid time", fi.error)
        self.assertIn("YYYYMMDD", fi.error)

        mock_task.delay.assert_not_called()

    def test_parseable_geotiff_drop_is_enqueued(self, mock_task):
        from georiva.ingestion.consumer import _handle_event
        _geotiff_setup()

        _handle_event(self._event("imagery/20250115.tif"))

        fi = FileIngestion.objects.get(file_path="imagery/20250115.tif")
        self.assertEqual(fi.status, FileIngestion.Status.PENDING)
        mock_task.delay.assert_called_once()

    def test_grib_drop_not_subject_to_filename_check(self, mock_task):
        from georiva.ingestion.consumer import _handle_event
        _grib_setup()

        _handle_event(self._event("models/any_name.grib2"))

        fi = FileIngestion.objects.get(file_path="models/any_name.grib2")
        self.assertEqual(fi.status, FileIngestion.Status.PENDING)
        mock_task.delay.assert_called_once()

    def test_consumer_applies_time_check_uniformly(self, mock_task):
        """Consumer checks time extractability for all drops; no DataArrival bypass."""
        from georiva.ingestion.consumer import _handle_event
        _geotiff_setup()

        # A pre-existing DataArrival no longer bypasses the time check.
        DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.UPLOADING,
            file_path="imagery/operator_named.tif",
        )

        _handle_event(self._event("imagery/operator_named.tif"))

        # FileIngestion is created but fails time extraction
        fi = FileIngestion.objects.get(file_path="imagery/operator_named.tif")
        self.assertEqual(fi.status, FileIngestion.Status.FAILED)
        # DataArrival is left untouched — consumer no longer manages it
        arrival = DataArrival.objects.get(file_path="imagery/operator_named.tif")
        self.assertEqual(arrival.status, DataArrival.Status.UPLOADING)
        mock_task.delay.assert_not_called()


# =============================================================================
# process_incoming_file task — job_id wiring
# =============================================================================

@patch("task_ferry.handler.JobHandler.run")
class ProcessIncomingFileJobIdTests(TestCase):

    def _make_job(self, file_path="chirps/rainfall/2024/01/15/file.tif", bucket="incoming"):
        from django.contrib.contenttypes.models import ContentType
        ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
        return FileIngestionJob.objects.create(
            user=None,
            content_type=ct,
            file_path=file_path,
            bucket=bucket,
        )

    def test_task_reuses_existing_job_when_job_id_provided(self, mock_run):
        from georiva.ingestion.tasks import process_incoming_file
        job = self._make_job()

        process_incoming_file.run(
            file_path=job.file_path,
            origin_bucket=job.bucket,
            job_id=job.pk,
        )

        self.assertEqual(FileIngestionJob.objects.count(), 1)
        mock_run.assert_called_once_with(job)

    def test_task_creates_new_job_when_no_job_id(self, mock_run):
        from georiva.ingestion.tasks import process_incoming_file

        process_incoming_file.run(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            origin_bucket="incoming",
        )

        self.assertEqual(FileIngestionJob.objects.count(), 1)
