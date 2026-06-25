"""
Auto-derived target tier (ADR-0008, issue #146).

A collection's storage tier is a computed consequence of the configured
products, not a stored field: it routes to staging iff some enabled
DerivedProduct consumes it at the staging tier; otherwise it publishes directly
("no derivation -> no staging"). This removes the target_tier-vs-products drift.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog
from georiva.sources.derivation_invocation import collection_routes_to_staging
from georiva.sources.models import DataFeed, DerivedProduct


def _definition(**overrides):
    kwargs = dict(
        key="anomaly",
        recipe_type="climatology",
        label="Rainfall anomaly",
        description="",
        config_schema=(),
        inputs=(InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode="scheduled",
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


class CollectionRoutesToStagingTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def _product(self, definition, **overrides):
        return DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key=definition.key,
            recipe_type=definition.recipe_type,
            is_enabled=overrides.get("is_enabled", True),
        )

    def test_collection_consumed_at_staging_routes_to_staging(self):
        definition = _definition()
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            self.assertTrue(collection_routes_to_staging(self.feed, "rainfall"))

    def test_collection_with_no_consuming_product_publishes(self):
        definition = _definition()
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            # A different collection that no product consumes -> not staging.
            self.assertFalse(collection_routes_to_staging(self.feed, "temperature"))

    def test_disabled_product_does_not_stage_its_collection(self):
        definition = _definition()
        self._product(definition, is_enabled=False)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            self.assertFalse(collection_routes_to_staging(self.feed, "rainfall"))

    def test_a_published_tier_input_does_not_route_to_staging(self):
        definition = _definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="published"),
        ))
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            self.assertFalse(collection_routes_to_staging(self.feed, "rainfall"))
