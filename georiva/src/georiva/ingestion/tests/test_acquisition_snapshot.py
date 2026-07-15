"""
Acquisition Feed snapshot (issue #220): FetchedFile entries serialize via
file_path — the model has no `filename` attribute, and reading one crashed
the snapshot whenever a run had per-file records.
"""
from asgiref.sync import async_to_sync

from django.test import TestCase

from georiva.core.models import Catalog
from georiva.ingestion.acquisition_snapshot import build_acquisition_snapshot
from georiva.sources.models import DataFeed, FetchedFile, FetchRun


class AcquisitionSnapshotTests(TestCase):
    def test_snapshot_serializes_fetched_files_by_path(self):
        catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        feed = DataFeed.objects.create(name="Feed", catalog=catalog)
        run = FetchRun.objects.create(
            data_feed=feed, status=FetchRun.Status.COMPLETED,
        )
        FetchedFile.objects.create(
            fetch_run=run,
            file_path="chirps/rainfall/rain.tif",
            status=FetchedFile.Status.STORED,
        )

        snapshot = async_to_sync(build_acquisition_snapshot)()

        run_entry = next(i for i in snapshot if i["type"] == "fetch_run")
        self.assertEqual(
            run_entry["files"][0]["file_path"], "chirps/rainfall/rain.tif"
        )
