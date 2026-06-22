"""
Tests for the staging consumer's registration contract.

We mock storage and the format plugin to keep these fast — the point is to
verify that one raw file becomes exactly ONE StagingItem + ONE source asset
(no per-timestep shredding), with a temporal extent and a checksum.
"""
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.models import Catalog
from georiva.core.models.base import AbstractAsset
from georiva.core.storage import BucketType
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem


@contextmanager
def _fake_temp(content=b"netcdf-bytes" * 200):
    f = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
    f.write(content)
    f.close()
    try:
        yield Path(f.name)
    finally:
        os.unlink(f.name)


def _ts(*days):
    return [datetime(2020, 1, d, tzinfo=timezone.utc) for d in days]


class RegisterStagingFileTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="netcdf"
        )

    def _register(self, timestamps, key="cmip6/tas-ssp245/series.nc"):
        plugin = MagicMock()
        plugin.list_variables.return_value = [{"name": "tas"}]
        plugin.get_timestamps.return_value = timestamps
        plugin.get_metadata_for_variable.return_value = {
            "width": 10, "height": 5, "bounds": [0, 0, 1, 1], "crs": "EPSG:4326",
        }
        sfm = MagicMock()
        sfm.download_to_temp = lambda origin, key: _fake_temp()

        with patch("georiva.formats.registry.format_registry.get", return_value=plugin), \
                patch(
                    "georiva.ingestion.handlers.source_file_manager.SourceFileManager",
                    return_value=sfm,
                ), \
                patch("georiva.ingestion.staging_consumer.storage"):
            from georiva.ingestion.staging_consumer import register_staging_file
            return register_staging_file(BucketType.STAGING, key)

    def test_multi_temporal_file_makes_one_item_with_extent(self):
        item = self._register(_ts(1, 2, 3, 4, 5))

        # Exactly one item + one asset, regardless of timestep count.
        self.assertEqual(StagingItem.objects.count(), 1)
        self.assertEqual(StagingAsset.objects.count(), 1)

        # Range extent, no single datetime.
        self.assertIsNone(item.datetime)
        self.assertEqual(item.start_datetime, datetime(2020, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(item.end_datetime, datetime(2020, 1, 5, tzinfo=timezone.utc))
        self.assertEqual(item.source_file, "staging:cmip6/tas-ssp245/series.nc")
        self.assertEqual(item.bounds, [0, 0, 1, 1])

    def test_no_shredding_even_for_many_timesteps(self):
        self._register(_ts(*range(1, 29)))  # 28 timesteps
        self.assertEqual(StagingItem.objects.count(), 1)

    def test_single_timestamp_uses_datetime(self):
        item = self._register(_ts(7), key="cmip6/tas-ssp245/slice.nc")
        self.assertEqual(item.datetime, datetime(2020, 1, 7, tzinfo=timezone.utc))
        self.assertIsNone(item.start_datetime)

    def test_source_asset_has_role_format_and_checksum(self):
        item = self._register(_ts(1, 2))
        asset = item.assets.get()
        self.assertIn(AbstractAsset.Role.SOURCE, asset.roles)
        self.assertEqual(asset.format, AbstractAsset.Format.NETCDF)
        self.assertEqual(len(asset.checksum), 64)  # sha256 hex
        self.assertGreater(asset.file_size, 0)
        self.assertEqual(asset.href, "cmip6/tas-ssp245/series.nc")

    def test_creates_staging_collection_from_path(self):
        self._register(_ts(1))
        sc = StagingCollection.objects.get()
        self.assertEqual(sc.slug, "tas-ssp245")
        self.assertEqual(sc.catalog, self.catalog)

    def test_unknown_catalog_is_skipped(self):
        item = self._register(_ts(1), key="nope/coll/file.nc")
        self.assertIsNone(item)
        self.assertEqual(StagingItem.objects.count(), 0)
