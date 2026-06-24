"""
Tests for sweep_staging — the staging-tier reconcile.

The staging path is event-driven (MinIO PUT → Redis → register_staging_file), so
an object that lands while the consumer is down, or survives a DB reset, has no
StagingItem. sweep_staging scans the bucket and registers exactly those, without
re-downloading. Mirror of sweep_unprocessed for the staging tier.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.core.models import Catalog
from georiva.core.storage import BucketType
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem


class SweepStagingTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="chirps-monthly", name="m"
        )

    def _already_registered(self, key):
        item = StagingItem.objects.create(collection=self.scol)
        StagingAsset.objects.create(item=item, href=key, roles=["source"])

    def _run_with_bucket(self, keys):
        with patch("georiva.ingestion.tasks.storage") as mock_storage, \
                patch("georiva.ingestion.tasks.process_staging_file") as mock_task:
            mock_storage.bucket.return_value.list_files.return_value = [
                {"path": k} for k in keys
            ]
            from georiva.ingestion.tasks import sweep_staging
            count = sweep_staging(sync=True)
        return count, mock_task

    def test_registers_only_objects_without_a_staging_item(self):
        self._already_registered("chirps/chirps-monthly/a.tif")
        count, mock_task = self._run_with_bucket([
            "chirps/chirps-monthly/a.tif",   # already registered → skip
            "chirps/chirps-monthly/b.tif",   # new → register
        ])

        self.assertEqual(count, 1)
        mock_task.run.assert_called_once_with(
            bucket=BucketType.STAGING, key="chirps/chirps-monthly/b.tif"
        )

    def test_skips_hidden_files(self):
        count, mock_task = self._run_with_bucket([
            "chirps/chirps-monthly/.keep",
        ])

        self.assertEqual(count, 0)
        mock_task.run.assert_not_called()
