"""
Product readiness (ADR-0008, issue #151).

A coarse, product-level gate computed from the declared inputs — no recipe
execution: a product is ready iff every *required* declared input collection
exists and is non-empty. Gates the "Run now" button and names the blocker when
not ready. The engine's per-unit readiness + min-count guard are unchanged.
"""
from datetime import datetime, timezone
from unittest.mock import patch

from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.sources.derivation_tracking import product_readiness
from georiva.sources.models import DataFeed, DerivedProduct
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem

_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


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


class ProductReadinessTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def _add_staging(self, slug="rainfall"):
        scol = StagingCollection.objects.create(
            catalog=self.catalog, slug=slug, name=slug
        )
        si = StagingItem.objects.create(
            collection=scol, datetime=_TIME,
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=4, height=4,
        )
        StagingAsset.objects.create(
            item=si, href=f"chirps/{slug}/f.tif", roles=["source"],
            format="geotiff", checksum=f"{slug}-1",
        )
        return si

    def _readiness(self, definition):
        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            return product_readiness(self.product)

    def test_ready_when_all_required_inputs_present(self):
        self._add_staging("rainfall")

        verdict = self._readiness(_definition())

        self.assertTrue(verdict.ready)

    def test_blocked_when_a_required_input_is_empty(self):
        # The anomaly needs both raw rainfall (present) and normals (absent).
        self._add_staging("rainfall")
        definition = _definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="staging"),
            InputRef(role="normals", collection="rainfall-normals", tier="published"),
        ))

        verdict = self._readiness(definition)

        self.assertFalse(verdict.ready)
        self.assertEqual(verdict.blocked_by, "normals")
        self.assertIn("normals", verdict.reason)

    def test_an_empty_optional_input_does_not_block(self):
        self._add_staging("rainfall")
        definition = _definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="staging"),
            InputRef(role="pet", collection="pet", tier="published", required=False),
        ))

        verdict = self._readiness(definition)

        self.assertTrue(verdict.ready)
