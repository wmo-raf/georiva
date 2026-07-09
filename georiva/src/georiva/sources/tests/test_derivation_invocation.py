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
    collection_routes_to_staging,
    dispatch_due_scheduled_products,
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
    w.write_png.side_effect = lambda rgba, path, *a, **k: path
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


def _staging_trigger(collection_slug="rainfall", staging_item_id=5, collection_id=None):
    return {
        "staging_item_id": staging_item_id,
        "collection_id": collection_id,
        "collection_slug": collection_slug,
    }


def _pin(product, definition, catalog):
    """Pin binding rows for a product the way the enable path would (ADR-0010
    §2): a core Collection per referenced key, plus DerivedProductInput /
    DerivedProductOutput rows carrying the resolved collection FK. Dispatch now
    matches these rows by collection_id, so a test product must be bound to be
    dispatched."""
    from georiva.sources.models import DerivedProductInput, DerivedProductOutput

    def _col(slug):
        col, _ = Collection.objects.get_or_create(
            catalog=catalog, slug=slug, defaults={"name": slug}
        )
        return col

    for ref in definition.inputs:
        DerivedProductInput.objects.update_or_create(
            product=product, role=ref.role,
            defaults={
                "tier": ref.tier, "required": ref.required,
                "source_key": ref.collection, "collection": _col(ref.collection),
            },
        )
    for ref in definition.outputs:
        DerivedProductOutput.objects.update_or_create(
            product=product, role=ref.role,
            defaults={"output_key": ref.collection, "collection": _col(ref.collection)},
        )


class DispatchForInputTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def _product(self, definition, **overrides):
        product = DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key=definition.key,
            recipe_type=definition.recipe_type,
            config=overrides.get("config", {}),
            is_enabled=overrides.get("is_enabled", True),
        )
        _pin(product, definition, self.catalog)
        return product

    def _trigger(self, collection_slug="rainfall", staging_item_id=5):
        col, _ = Collection.objects.get_or_create(
            catalog=self.catalog, slug=collection_slug, defaults={"name": collection_slug}
        )
        return _staging_trigger(
            collection_slug=collection_slug, staging_item_id=staging_item_id,
            collection_id=col.pk,
        )

    def test_matching_enabled_product_runs_with_selector_and_origin(self):
        definition = _definition()
        product = self._product(definition, config={"baseline": "1991-2020"})

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(), dispatch=False)

        run.assert_called_once()
        selector = run.call_args.args[1]
        # Selector carries the product's config merged with the event coordinates.
        self.assertEqual(selector["baseline"], "1991-2020")
        self.assertEqual(selector["staging_item_id"], 5)
        self.assertEqual(run.call_args.kwargs["origin"], f"derived_product:{product.pk}")

    def test_selector_carries_declared_inputs_and_outputs(self):
        # The recipe reads the product's collections from the selector — now from
        # the pinned binding rows, so each entry also carries a resolved
        # collection_id (ADR-0010 §4) alongside the declared key + tier.
        definition = _definition()
        self._product(definition)

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(), dispatch=False)

        rain = Collection.objects.get(catalog=self.catalog, slug="rainfall")
        selector = run.call_args.args[1]
        self.assertEqual(
            selector["inputs"],
            [{"role": "source", "collection": "rainfall", "tier": "staging",
              "collection_id": rain.pk}],
        )
        self.assertEqual(
            selector["outputs"],
            [{"role": "served", "collection": "rainfall", "collection_id": rain.pk}],
        )

    def test_selector_binding_omits_output_display_metadata(self):
        # OutputRef's title/description/visibility drive catalog materialisation,
        # NOT recipe behaviour — they must never enter the selector, or a display
        # edit would change a recipe's unit identity.
        definition = _definition(outputs=(
            OutputRef(role="served", collection="rainfall",
                      title="Served rainfall", description="Raw, promoted.",
                      visibility="internal"),
        ))
        self._product(definition)

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(), dispatch=False)

        rain = Collection.objects.get(catalog=self.catalog, slug="rainfall")
        self.assertEqual(
            run.call_args.args[1]["outputs"],
            [{"role": "served", "collection": "rainfall", "collection_id": rain.pk}],
        )

    def test_product_on_a_different_collection_is_not_dispatched(self):
        # The product consumes 'temperature'; a 'rainfall' arrival (a distinct,
        # existing collection) must not match its binding.
        definition = _definition(inputs=(
            InputRef(role="source", collection="temperature", tier="staging"),
        ))
        self._product(definition)

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(collection_slug="rainfall"), dispatch=False)

        run.assert_not_called()

    def test_disabled_product_is_not_dispatched(self):
        definition = _definition()
        self._product(definition, is_enabled=False)

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(), dispatch=False)

        run.assert_not_called()

    def test_product_consuming_a_different_tier_is_not_dispatched(self):
        # The product wants rainfall at the published tier; a staging arrival
        # must not trigger it.
        definition = _definition(inputs=(
            InputRef(role="source", collection="rainfall", tier="published"),
        ))
        self._product(definition)

        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(self._trigger(), dispatch=False)

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
            catalog=self.catalog, slug="rainfall", name="Rainfall",
            collection=self.pub_col,
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
        _pin(self.product, self.definition, self.catalog)

    def test_staging_arrival_promotes_and_stamps_product_origin(self):
        trigger = {
            "staging_item_id": self.sitem.pk,
            "collection_id": self.pub_col.pk,
            "collection_slug": "rainfall",
        }

        import numpy as np

        from georiva.processing.recipes.promotion import PromotionRecipe

        data = np.full((10, 10), 5.0, dtype="float32")
        with (
            patch("georiva.ingestion.asset_writer.AssetWriter", return_value=_mock_writer()),
            patch.object(
                PromotionRecipe, "read_raster",
                return_value=(data, [0, 0, 1, 1], "EPSG:4326", 10, 10),
            ),
        ):
            dispatch_for_input(trigger, dispatch=False)

        item = Item.objects.get(collection=self.pub_col)
        self.assertEqual(item.time, datetime(2020, 1, 1, tzinfo=timezone.utc))
        # Promotion emits a served COG + a visual PNG under the shared ingestion
        # path scheme ({variable}_{HHMMSS}).
        cog = item.assets.get(format="cog")
        self.assertTrue(cog.href.endswith("/precip_000000.tif"), cog.href)
        png = item.assets.get(format="png")
        self.assertTrue(png.href.endswith("/precip_000000.png"), png.href)

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
        self.definition = _definition()
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="serve-raw",
            recipe_type="promotion", config={"baseline": "1991-2020"}, is_enabled=True,
        )
        _pin(self.product, self.definition, self.catalog)

    def test_runs_recipe_with_config_selector_and_product_origin(self):
        with (
            patch.object(DataFeed, "get_derived_products", return_value=[self.definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            run_product_now(self.product, dispatch=False)

        run.assert_called_once()
        selector = run.call_args.args[1]
        rain = Collection.objects.get(catalog=self.catalog, slug="rainfall")
        # Wide selector = the product's config + pinned binding (no event
        # trigger) -> backfill that can still read its collections, now by FK.
        self.assertEqual(selector["baseline"], "1991-2020")
        self.assertEqual(
            selector["inputs"],
            [{"role": "source", "collection": "rainfall", "tier": "staging",
              "collection_id": rain.pk}],
        )
        self.assertEqual(
            selector["outputs"],
            [{"role": "served", "collection": "rainfall", "collection_id": rain.pk}],
        )
        self.assertEqual(run.call_args.kwargs["origin"], product_origin(self.product))

    def test_manual_run_stamps_the_manual_rerun_reason(self):
        with (
            patch.object(DataFeed, "get_derived_products", return_value=[self.definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            run_product_now(self.product, dispatch=False)

        self.assertEqual(
            run.call_args.kwargs["reason"], DerivationRun.RetryReason.MANUAL_RERUN,
        )

    def test_disabled_product_dispatches_nothing(self):
        # A disabled product is inert on every path: event and scheduled already
        # filter on is_enabled, and the manual/backfill overlay must refuse it
        # too, so an unticked product fires no run when data arrives or on demand.
        self.product.is_enabled = False
        self.product.save(update_fields=["is_enabled"])

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[self.definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            result = run_product_now(self.product, dispatch=False)

        run.assert_not_called()
        self.assertEqual(result, [])


class OrphanExclusionTests(TestCase):
    """A row whose definition key the plugin no longer declares (removed/renamed
    by an upgrade) is inert on every path — definition_for returns None and each
    dispatcher skips it. This locks that in (issue #171)."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        # An enabled row for a definition the (patched-empty) declaration omits.
        self.orphan = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="serve-raw",
            recipe_type="promotion", is_enabled=True,
        )

    def _orphaned(self):
        return patch.object(DataFeed, "get_derived_products", return_value=[])

    def test_orphan_is_excluded_from_event_dispatch(self):
        # A real, existing collection — the orphan is skipped because it has no
        # binding row on it (unbound), not because the trigger is empty.
        col = Collection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        trigger = _staging_trigger(collection_id=col.pk)
        with self._orphaned(), patch("georiva.processing.engine.run") as run:
            result = dispatch_for_input(trigger, dispatch=False)
        run.assert_not_called()
        self.assertEqual(result, [])

    def test_orphan_is_excluded_from_scheduled_dispatch(self):
        with self._orphaned(), patch("georiva.processing.engine.run") as run:
            dispatched = dispatch_due_scheduled_products(dispatch=False)
        run.assert_not_called()
        self.assertEqual(dispatched, 0)

    def test_orphan_is_excluded_from_manual_run(self):
        with self._orphaned(), patch("georiva.processing.engine.run") as run:
            result = run_product_now(self.orphan, dispatch=False)
        run.assert_not_called()
        self.assertEqual(result, [])

    def test_orphan_does_not_route_its_collection_to_staging(self):
        # Its (absent) declaration can't opt any collection into staging.
        with self._orphaned():
            self.assertFalse(collection_routes_to_staging(self.feed, "rainfall"))


class CrossCatalogIsolationTests(TestCase):
    """Two feeds of the same shape in different catalogs share a collection slug;
    an arriving item in one catalog must trigger only its own feed's product,
    because dispatch matches the collection FK, not the slug (ADR-0010 §4 AC2)."""

    def _feed_with_product(self, catalog_slug):
        catalog = Catalog.objects.create(
            name=catalog_slug, slug=catalog_slug, file_format="geotiff"
        )
        feed = DataFeed.objects.create(name=f"Feed {catalog_slug}", catalog=catalog)
        definition = _definition()
        product = DerivedProduct.objects.create(
            data_feed=feed, definition_key=definition.key,
            recipe_type=definition.recipe_type, is_enabled=True,
        )
        _pin(product, definition, catalog)
        rainfall = Collection.objects.get(catalog=catalog, slug="rainfall")
        return product, rainfall

    def test_arrival_in_one_catalog_triggers_only_its_own_product(self):
        product_a, rainfall_a = self._feed_with_product("cat-a")
        product_b, rainfall_b = self._feed_with_product("cat-b")

        # An arrival for catalog A's rainfall collection (same slug as B's).
        trigger = _staging_trigger(collection_id=rainfall_a.pk)
        with patch("georiva.processing.engine.run") as run:
            dispatch_for_input(trigger, dispatch=False)

        run.assert_called_once()
        self.assertEqual(
            run.call_args.kwargs["origin"], product_origin(product_a)
        )


class UnboundProductSkippedTests(TestCase):
    """An enabled, still-declared product with no binding rows (e.g. its input
    collection was deleted) is inert on the event path — it simply never matches
    (ADR-0010 §4 AC4)."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.rainfall = Collection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )

    def test_enabled_product_without_bindings_is_not_dispatched(self):
        definition = _definition()
        # Enabled + declared, but never pinned -> no DerivedProductInput rows.
        DerivedProduct.objects.create(
            data_feed=self.feed, definition_key=definition.key,
            recipe_type=definition.recipe_type, is_enabled=True,
        )

        trigger = _staging_trigger(collection_id=self.rainfall.pk)
        with (
            patch.object(DataFeed, "get_derived_products", return_value=[definition]),
            patch("georiva.processing.engine.run") as run,
        ):
            result = dispatch_for_input(trigger, dispatch=False)

        run.assert_not_called()
        self.assertEqual(result, [])
