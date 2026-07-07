"""
Auto-derived target tier (ADR-0008, issue #146).

A collection's storage tier is a computed consequence of the configured
products, not a stored field: it routes to staging iff some enabled
DerivedProduct consumes it at the staging tier; otherwise it publishes directly
("no derivation -> no staging"). This removes the target_tier-vs-products drift.
"""
from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.sources.derivation_invocation import collection_routes_to_staging
from georiva.sources.models import (
    DataFeed,
    DerivedProduct,
    DerivedProductInput,
)


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
    """Auto-derived tier is now driven by the pinned staging-input binding rows,
    not by re-matching declarations (ADR-0010 §4)."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def _product(self, definition, **overrides):
        """Create the product and pin its input binding (a core Collection per
        input key + a DerivedProductInput carrying the FK), the way the enable
        path would."""
        product = DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key=definition.key,
            recipe_type=definition.recipe_type,
            is_enabled=overrides.get("is_enabled", True),
        )
        for ref in definition.inputs:
            col, _ = Collection.objects.get_or_create(
                catalog=self.catalog, slug=ref.collection,
                defaults={"name": ref.collection},
            )
            DerivedProductInput.objects.create(
                product=product, role=ref.role, tier=ref.tier,
                required=ref.required, source_key=ref.collection, collection=col,
            )
        return product

    def test_collection_consumed_at_staging_routes_to_staging(self):
        self._product(_definition())
        self.assertTrue(collection_routes_to_staging(self.feed, "rainfall"))

    def test_collection_with_no_consuming_product_publishes(self):
        self._product(_definition())
        # A different collection that no product consumes -> not staging.
        self.assertFalse(collection_routes_to_staging(self.feed, "temperature"))

    def test_disabled_product_does_not_stage_its_collection(self):
        self._product(_definition(), is_enabled=False)
        self.assertFalse(collection_routes_to_staging(self.feed, "rainfall"))

    def test_a_published_tier_input_does_not_route_to_staging(self):
        self._product(_definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="published"),
        )))
        self.assertFalse(collection_routes_to_staging(self.feed, "rainfall"))
