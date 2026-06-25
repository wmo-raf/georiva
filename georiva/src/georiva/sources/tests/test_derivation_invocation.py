"""
Product-driven invocation (ADR-0008, issue #147).

An arriving input finds the enabled DerivedProducts whose declared inputs match
its collection/tier, builds a selector from each product's config, and calls the
engine's run(recipe, selector) — stamping the run with the product origin. This
application-layer dispatcher is the only place that knows about DerivedProduct;
the engine stays generic.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.processing.models import DerivationRun
from georiva.sources.derivation_invocation import (
    dispatch_for_input,
    product_origin,
    run_product_now,
)
from georiva.sources.models import DataFeed, DerivedProduct
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem


def _mock_writer():
    w = MagicMock()
    w.bucket.save.side_effect = lambda path, data: path
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    return w


def _definition(**overrides):
    kwargs = dict(
        key="serve-raw",
        recipe_type="promotion",
        label="Serve raw",
        description="",
        config_schema=(),
        inputs=(InputRef(role="source", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="served", collection="rainfall"),),
        trigger_mode="event",
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


def _staging_trigger(collection_slug="rainfall", staging_item_id=5):
    return {"staging_item_id": staging_item_id, "collection_slug": collection_slug}


class DispatchForInputTests(TestCase):
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
            config=overrides.get("config", {}),
            is_enabled=overrides.get("is_enabled", True),
        )

    def test_matching_enabled_product_runs_with_selector_and_origin(self):
        definition = _definition()
        product = self._product(definition, config={"baseline": "1991-2020"})

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            dispatch_for_input(_staging_trigger(), dispatch=False)

        run.assert_called_once()
        selector = run.call_args.args[1]
        # Selector carries the product's config merged with the event coordinates.
        self.assertEqual(selector["baseline"], "1991-2020")
        self.assertEqual(selector["staging_item_id"], 5)
        self.assertEqual(run.call_args.kwargs["origin"], f"derived_product:{product.pk}")

    def test_selector_carries_declared_inputs_and_outputs(self):
        # The recipe must be able to read the product's declared collections
        # from the selector (ADR-0008): a scheduled/manual recipe has no trigger
        # to learn them from, so the invocation layer injects the declaration.
        definition = _definition()
        self._product(definition)

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            dispatch_for_input(_staging_trigger(), dispatch=False)

        selector = run.call_args.args[1]
        self.assertEqual(
            selector["inputs"],
            [{"role": "source", "collection": "rainfall", "tier": "staging"}],
        )
        self.assertEqual(
            selector["outputs"],
            [{"role": "served", "collection": "rainfall"}],
        )

    def test_product_on_a_different_collection_is_not_dispatched(self):
        definition = _definition(inputs=(
            InputRef(role="source", collection="temperature", tier="staging"),
        ))
        self._product(definition)

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            dispatch_for_input(_staging_trigger(collection_slug="rainfall"), dispatch=False)

        run.assert_not_called()

    def test_disabled_product_is_not_dispatched(self):
        definition = _definition()
        self._product(definition, is_enabled=False)

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            dispatch_for_input(_staging_trigger(), dispatch=False)

        run.assert_not_called()

    def test_product_consuming_a_different_tier_is_not_dispatched(self):
        # The product wants rainfall at the published tier; a staging arrival
        # must not trigger it.
        definition = _definition(inputs=(
            InputRef(role="source", collection="rainfall", tier="published"),
        ))
        self._product(definition)

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            dispatch_for_input(_staging_trigger(), dispatch=False)

        run.assert_not_called()


class EndToEndPromotionTests(TestCase):
    """The mechanism end to end on a core recipe: a staging arrival routed
    through an enabled promotion product produces a Published item, and the run
    carries the product origin."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.pub_col = Collection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        self.unit_dim, _ = Unit.objects.get_or_create(
            symbol="mm", defaults={"name": "millimetre"}
        )
        self.variable = Variable.objects.create(
            collection=self.pub_col, slug="precip", name="Precipitation",
            unit=self.unit_dim, value_min=0, value_max=2000,
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        self.sitem = StagingItem.objects.create(
            collection=self.scol, datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )
        StagingAsset.objects.create(
            item=self.sitem, href="chirps/rainfall/f.tif", roles=["source"],
            format="geotiff", checksum="abc123", variable=self.variable,
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.definition = _definition()
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key=self.definition.key,
            recipe_type="promotion", config={}, is_enabled=True,
        )

    def test_staging_arrival_promotes_and_stamps_product_origin(self):
        trigger = {"staging_item_id": self.sitem.pk, "collection_slug": "rainfall"}

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[self.definition]),
            patch("georiva.core.storage.storage") as st,
            patch("georiva.ingestion.asset_writer.AssetWriter", return_value=_mock_writer()),
        ):
            st.bucket.return_value.read_bytes.return_value = b"GEOTIFFBYTES"
            dispatch_for_input(trigger, dispatch=False)

        item = Item.objects.get(collection=self.pub_col)
        self.assertEqual(item.time, datetime(2020, 1, 1, tzinfo=timezone.utc))

        run_rec = DerivationRun.objects.get(recipe_type="promotion")
        self.assertEqual(run_rec.status, DerivationRun.Status.COMPLETED)
        self.assertEqual(run_rec.origin, product_origin(self.product))


class StagingArrivalRoutesToProductsTests(TestCase):
    """The staging-arrival entry point is product-driven: registering a staging
    file dispatches the input to its consuming products."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        self.sitem = StagingItem.objects.create(
            collection=self.scol, datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )

    def test_process_staging_file_dispatches_input_to_products(self):
        from georiva.core.storage import BucketType
        from georiva.ingestion.tasks import process_staging_file

        with (
            patch(
                "georiva.ingestion.staging_consumer.register_staging_file",
                return_value=self.sitem,
            ),
            patch(
                "georiva.sources.derivation_invocation.dispatch_for_input"
            ) as dispatch,
        ):
            process_staging_file.apply(
                kwargs={"bucket": BucketType.STAGING, "key": "chirps/rainfall/f.tif"}
            )

        dispatch.assert_called_once()
        trigger = dispatch.call_args.args[0]
        self.assertEqual(trigger["staging_item_id"], self.sitem.pk)
        self.assertEqual(trigger["collection_slug"], "rainfall")

    def test_no_dispatch_when_registration_is_skipped(self):
        from georiva.core.storage import BucketType
        from georiva.ingestion.tasks import process_staging_file

        with (
            patch(
                "georiva.ingestion.staging_consumer.register_staging_file",
                return_value=None,
            ),
            patch(
                "georiva.sources.derivation_invocation.dispatch_for_input"
            ) as dispatch,
        ):
            process_staging_file.apply(
                kwargs={"bucket": BucketType.STAGING, "key": "bad/path.tif"}
            )

        dispatch.assert_not_called()


class RunProductNowTests(TestCase):
    """The manual / backfill overlay: run a product on demand with a wide
    selector (its config), so the recipe enumerates all its units."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="serve-raw",
            recipe_type="promotion", config={"baseline": "1991-2020"}, is_enabled=True,
        )

    def test_runs_recipe_with_config_selector_and_product_origin(self):
        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_definition()]),
            patch("georiva.processing.engine.run") as run,
        ):
            run_product_now(self.product, dispatch=False)

        run.assert_called_once()
        selector = run.call_args.args[1]
        # Wide selector = the product's config + declared binding (no event
        # trigger) -> backfill that can still read its collections.
        self.assertEqual(selector["baseline"], "1991-2020")
        self.assertEqual(
            selector["inputs"],
            [{"role": "source", "collection": "rainfall", "tier": "staging"}],
        )
        self.assertEqual(
            selector["outputs"],
            [{"role": "served", "collection": "rainfall"}],
        )
        self.assertEqual(run.call_args.kwargs["origin"], product_origin(self.product))
