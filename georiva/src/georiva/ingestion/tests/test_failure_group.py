"""What a file ingestion's failure rolls up under on the dashboard (ADR 0033)."""

from django.test import SimpleTestCase

from georiva.ingestion.models import FileIngestionFinished


class FileIngestionFailureGroupTests(SimpleTestCase):
    def test_it_groups_under_the_first_collection_it_touched(self):
        event = FileIngestionFinished(file_path="kenya/a/b/f.tif", collections_touched=["gfs/wind", "gfs/rain"])
        self.assertEqual(event.failure_group_key(), "collection:gfs/rain")

    def test_without_a_touched_collection_it_reads_the_collection_off_the_key(self):
        event = FileIngestionFinished(file_path="kenya/chirps/rainfall/2026/f.tif", collections_touched=[])
        self.assertEqual(event.failure_group_key(), "collection:chirps/rainfall")

    def test_a_key_too_short_to_name_a_collection_groups_by_type(self):
        event = FileIngestionFinished(file_path="stray.tif", collections_touched=[])
        self.assertEqual(event.failure_group_key(), "")
