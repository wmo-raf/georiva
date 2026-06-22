"""
Tests for store-only routing: a DataFeed with target_tier=staging stores
fetched files to the STAGING bucket (which the published consumer does not
watch), not to SOURCES.
"""
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed


class TargetTierRoutingTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="netcdf"
        )
        self.collection = Collection.objects.create(
            name="tas", slug="tas", catalog=self.catalog
        )

    def _loader(self, feed):
        ds = MagicMock()
        ds.name = "cmip6"
        ds.fetch_strategy.return_value = MagicMock()
        return Loader(data_source=ds, collection=self.collection, data_feed=feed)

    def test_staging_feed_routes_to_staging_bucket(self):
        feed = DataFeed.objects.create(
            name="Feed", catalog=self.catalog,
            target_tier=DataFeed.TargetTier.STAGING,
        )
        loader = self._loader(feed)
        self.assertEqual(loader._tier_bucket_type, BucketType.STAGING)

    def test_published_feed_routes_to_sources_bucket(self):
        feed = DataFeed.objects.create(
            name="Feed", catalog=self.catalog,
            target_tier=DataFeed.TargetTier.PUBLISHED,
        )
        loader = self._loader(feed)
        self.assertEqual(loader._tier_bucket_type, BucketType.SOURCES)

    def test_default_tier_is_published(self):
        feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.assertEqual(feed.target_tier, DataFeed.TargetTier.PUBLISHED)

    def test_no_feed_routes_to_sources(self):
        loader = self._loader(None)
        self.assertEqual(loader._tier_bucket_type, BucketType.SOURCES)

    def test_store_file_saves_to_staging_bucket(self):
        feed = DataFeed.objects.create(
            name="Feed", catalog=self.catalog,
            target_tier=DataFeed.TargetTier.STAGING,
        )
        loader = self._loader(feed)

        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.write(b"data" * 500)
        tmp.close()
        try:
            with patch("georiva.sources.loader.storage") as mock_storage:
                bucket = MagicMock()
                mock_storage.bucket.return_value = bucket
                loader._store_file(Path(tmp.name), "cmip6/tas/series.nc")

            mock_storage.bucket.assert_called_with(BucketType.STAGING)
            self.assertTrue(bucket.save.called)
        finally:
            os.unlink(tmp.name)
