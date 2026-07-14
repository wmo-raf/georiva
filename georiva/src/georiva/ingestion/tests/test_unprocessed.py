"""
find_unprocessed (PRD #217, issue #223): the bucket-scan behind both the
periodic Sweep and the feed page's "Check unprocessed files" action.

Storage listing is mocked (like the sweep tests) — the behavior under test
is classification and prefix scoping, not MinIO I/O.
"""
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.storage import BucketType
from georiva.ingestion.models import FileIngestion
from georiva.ingestion.unprocessed import find_unprocessed


def _scan(listing_by_bucket, prefix=None):
    """Run find_unprocessed against a mocked storage layout:
    {BucketType.X: [path, ...]}."""
    def bucket_for(bucket_type):
        bucket = MagicMock()
        bucket.list_files.return_value = [
            {"path": p} for p in listing_by_bucket.get(bucket_type, [])
        ]
        return bucket

    with patch("georiva.ingestion.unprocessed.storage") as mock_storage:
        mock_storage.bucket.side_effect = bucket_for
        return find_unprocessed(prefix=prefix)


class FindUnprocessedClassificationTests(TestCase):
    def test_classifies_untracked_pending_and_reingest_and_skips_in_flight(self):
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES, file_path="cat/col/pending.tif",
            status=FileIngestion.Status.PENDING,
        )
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES, file_path="cat/col/reingest.tif",
            status=FileIngestion.Status.COMPLETED, force_reingest=True,
        )
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES, file_path="cat/col/busy.tif",
            status=FileIngestion.Status.PROCESSING,
        )

        found = _scan({
            BucketType.SOURCES: [
                "cat/col/untracked.tif",
                "cat/col/pending.tif",
                "cat/col/reingest.tif",
                "cat/col/busy.tif",
                ".keep",
            ],
        })

        by_path = {f.file_path: f.reason for f in found}
        self.assertEqual(by_path, {
            "cat/col/untracked.tif": "untracked",
            "cat/col/pending.tif": "pending",
            "cat/col/reingest.tif": "reingest",
        })
        self.assertEqual({f.bucket for f in found}, {BucketType.SOURCES})

    def test_force_reingest_wins_regardless_of_status(self):
        # The pre-refactor sweep re-ingested any force_reingest record, not
        # only completed ones — the refactor must not lose that.
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES, file_path="cat/col/forced-failed.tif",
            status=FileIngestion.Status.FAILED, force_reingest=True,
        )
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES, file_path="cat/col/forced-pending.tif",
            status=FileIngestion.Status.PENDING, force_reingest=True,
        )

        found = _scan({
            BucketType.SOURCES: [
                "cat/col/forced-failed.tif", "cat/col/forced-pending.tif",
            ],
        })

        self.assertEqual(
            {f.file_path: f.reason for f in found},
            {
                "cat/col/forced-failed.tif": "reingest",
                "cat/col/forced-pending.tif": "reingest",
            },
        )

    def test_prefix_scopes_the_scan_to_one_catalog(self):
        found = _scan(
            {
                BucketType.INCOMING: ["chirps/col/a.tif", "other/col/b.tif"],
                BucketType.SOURCES: ["chirps/col/c.tif"],
            },
            prefix="chirps/",
        )

        self.assertCountEqual(
            [f.file_path for f in found],
            ["chirps/col/a.tif", "chirps/col/c.tif"],
        )
