"""
Store-only routing, auto-derived from products (ADR-0008, issue #146): a feed's
fetched files land in the STAGING bucket iff an enabled DerivedProduct consumes
that collection at the staging tier; otherwise they land in SOURCES (the
published path). Tier is computed from the product declarations, not a stored
DataFeed.target_tier field.
"""
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, DerivedProduct


def _staging_definition(collection_slug="tas"):
    return DerivedProductDefinition(
        key="anomaly", recipe_type="climatology", label="Anomaly", description="",
        config_schema=(),
        inputs=(InputRef(role="value", collection=collection_slug, tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection=f"{collection_slug}-anomaly"),),
        trigger_mode="scheduled",
    )


class TargetTierRoutingTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="netcdf"
        )
        self.collection = Collection.objects.create(
            name="tas", slug="tas", catalog=self.catalog
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def _loader(self, feed):
        ds = MagicMock()
        ds.name = "cmip6"
        ds.fetch_strategy.return_value = MagicMock()
        return Loader(data_source=ds, collection=self.collection, data_feed=feed)

    def _enable_staging_product(self):
        DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly",
            recipe_type="climatology", is_enabled=True,
        )

    def test_collection_consumed_at_staging_routes_to_staging_bucket(self):
        self._enable_staging_product()
        loader = self._loader(self.feed)

        with patch.object(DataFeed, "get_derived_products", return_value=[_staging_definition()]):
            self.assertEqual(loader._tier_bucket_type, BucketType.STAGING)

    def test_feed_with_no_consuming_product_routes_to_sources(self):
        loader = self._loader(self.feed)

        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            self.assertEqual(loader._tier_bucket_type, BucketType.SOURCES)

    def test_no_feed_routes_to_sources(self):
        loader = self._loader(None)
        self.assertEqual(loader._tier_bucket_type, BucketType.SOURCES)

    def test_store_file_saves_to_staging_bucket_when_consumed(self):
        self._enable_staging_product()
        loader = self._loader(self.feed)

        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.write(b"data" * 500)
        tmp.close()
        try:
            with (
                patch.object(DataFeed, "get_derived_products", return_value=[_staging_definition()]),
                patch("georiva.sources.loader.storage") as mock_storage,
            ):
                bucket = MagicMock()
                mock_storage.bucket.return_value = bucket
                loader._store_file(Path(tmp.name), "cmip6/tas/series.nc")

            mock_storage.bucket.assert_called_with(BucketType.STAGING)
            self.assertTrue(bucket.save.called)
        finally:
            os.unlink(tmp.name)
