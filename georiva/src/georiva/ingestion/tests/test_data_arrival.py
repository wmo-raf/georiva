from django.test import TestCase

from georiva.ingestion.models import DataArrival


class DataArrivalFindOrCreateTests(TestCase):

    def test_unknown_file_path_creates_new_arrival(self):
        arrival, created = DataArrival.find_or_create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        )

        self.assertTrue(created)
        self.assertEqual(arrival.file_path, "chirps/rainfall/2024/01/15/file.tif")
        self.assertEqual(arrival.trigger, DataArrival.Trigger.MANUAL_UPLOAD)

    def test_known_file_path_returns_existing_record(self):
        original, _ = DataArrival.find_or_create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        )

        found, created = DataArrival.find_or_create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        )

        self.assertFalse(created)
        self.assertEqual(found.pk, original.pk)
        self.assertEqual(DataArrival.objects.count(), 1)

    def test_uploading_status_is_not_reset_when_found(self):
        DataArrival.objects.create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.UPLOADING,
        )

        found, created = DataArrival.find_or_create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        )

        self.assertFalse(created)
        self.assertEqual(found.status, DataArrival.Status.UPLOADING)

    def test_fetch_stats_default_to_zero(self):
        arrival, _ = DataArrival.find_or_create(
            file_path="chirps/rainfall/2024/01/15/file.tif",
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        )

        self.assertEqual(arrival.files_requested, 0)
        self.assertEqual(arrival.files_fetched, 0)
        self.assertEqual(arrival.files_skipped, 0)
        self.assertEqual(arrival.files_failed, 0)
        self.assertEqual(arrival.files_queued, 0)
        self.assertEqual(arrival.bytes_transferred, 0)
