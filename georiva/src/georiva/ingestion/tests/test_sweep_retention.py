"""The Sweep does not re-ingest what forecast retention pruned (#499).

``prune_forecast_items`` deletes a forecast collection's past Items every hour. The
source file stays in its bucket, so its completed FileIngestion looked like data that
was lost, and the Sweep re-ingested it — only for the next prune to delete it again,
once an hour, for as long as the file stayed in the bucket.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog, Collection, Item
from georiva.core.storage import BucketType
from georiva.core.tasks import prune_forecast_items
from georiva.ingestion.models import FileIngestion
from georiva.ingestion.unprocessed import find_unprocessed
from georiva.organisations.testing import make_organisation


def _scan(paths):
    def bucket_for(bucket_type):
        bucket = MagicMock()
        bucket.list_files.return_value = [{"path": p} for p in paths] if bucket_type == BucketType.SOURCES else []
        return bucket

    with patch("georiva.ingestion.unprocessed.storage") as mock_storage:
        mock_storage.bucket.side_effect = bucket_for
        return {f.file_path: f.reason for f in find_unprocessed()}


class SweepRetentionTests(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(organisation=make_organisation(), name="IFS", slug="ifs", file_format="grib2")
        self.collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface", is_forecast=True)
        self.now = timezone.now()

    def _ingested(self, name, valid_time, reference_time=None):
        """A completed ingestion of ``name`` that produced one Item at ``valid_time``."""
        reference_time = reference_time or valid_time - timedelta(hours=3)
        file_path = f"org/ifs/surface/{name}"
        record = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path=file_path,
            status=FileIngestion.Status.COMPLETED,
            completed_at=self.now,
            reference_time=reference_time,
            valid_time_start=valid_time,
            valid_time_end=valid_time,
        )
        record.collections.add(self.collection)
        Item.objects.create(
            collection=self.collection,
            time=valid_time,
            reference_time=reference_time,
            source_file=f"{BucketType.SOURCES}:{file_path}",
        )
        return file_path

    def test_a_file_whose_past_forecast_was_pruned_is_not_reingested(self):
        path = self._ingested("past.grib2", self.now - timedelta(days=60))

        prune_forecast_items()

        self.assertFalse(Item.objects.exists())
        self.assertEqual(_scan([path]), {})

    def test_a_file_from_a_run_retired_by_latest_run_only_is_not_reingested(self):
        self.collection.retain_latest_run_only = True
        self.collection.save()
        future = self.now + timedelta(days=2)
        old = self._ingested("old.grib2", future, reference_time=self.now - timedelta(hours=12))
        new = self._ingested("new.grib2", future + timedelta(hours=3), reference_time=self.now)

        prune_forecast_items()

        self.assertEqual(_scan([old, new]), {})

    def test_a_file_whose_items_vanished_while_retention_keeps_them_is_reingested(self):
        """Data loss the retention did not cause is still re-ingested."""
        path = self._ingested("future.grib2", self.now + timedelta(days=2))
        Item.objects.all().delete()

        self.assertEqual(_scan([path]), {path: "reingest"})

    def test_a_past_forecast_is_reingested_when_the_collection_retains_past_forecasts(self):
        self.collection.retain_past_forecasts = True
        self.collection.save()
        path = self._ingested("past.grib2", self.now - timedelta(days=60))
        Item.objects.all().delete()

        self.assertEqual(_scan([path]), {path: "reingest"})

    def test_a_past_forecast_is_reingested_when_its_collection_is_not_pruned(self):
        """An inactive collection is never pruned, so its missing Items were lost."""
        self.collection.is_active = False
        self.collection.save()
        path = self._ingested("past.grib2", self.now - timedelta(days=60))
        Item.objects.all().delete()

        self.assertEqual(_scan([path]), {path: "reingest"})

    def test_force_reingest_still_wins_over_retention(self):
        path = self._ingested("past.grib2", self.now - timedelta(days=60))
        prune_forecast_items()
        FileIngestion.objects.filter(file_path=path).update(force_reingest=True)

        self.assertEqual(_scan([path]), {path: "reingest"})
