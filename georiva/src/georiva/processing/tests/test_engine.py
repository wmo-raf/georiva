"""
Engine tests — recipes are exercised *through* run()/run_unit(), with storage
and the AssetWriter mocked. Mirrors sources/tests/test_loader_fetchrun.py
(mock I/O, assert on produced records).
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from django.db import IntegrityError, transaction
from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.processing.engine import run, run_unit
from georiva.processing.models import DerivationRun
from georiva.processing.recipe import (
    BaseRecipe,
    OutputAsset,
    OutputItem,
    ResolvedInput,
)
from georiva.processing.recipes.promotion import PromotionRecipe
from georiva.staging.models import (
    DerivationLink,
    StagingAsset,
    StagingCollection,
    StagingItem,
)


def _mock_writer():
    w = MagicMock()
    w.bucket.save.side_effect = lambda path, data: path
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    w.write_png.side_effect = lambda rgba, path, *a, **k: path
    return w


class _PromotionFixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.pub_col = Collection.objects.create(
            catalog=self.catalog, slug="tas", name="tas"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")
        self.variable = Variable.objects.create(
            collection=self.pub_col, slug="tas", name="tas",
            unit=self.unit, value_min=0, value_max=50,
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="tas", name="tas"
        )
        self.sitem = StagingItem.objects.create(
            collection=self.scol,
            datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )
        self.sasset = StagingAsset.objects.create(
            item=self.sitem, href="cmip6/tas/f.tif", roles=["source"],
            format="geotiff", checksum="abc123", variable=self.variable,
        )

    def _unit(self):
        return {"staging_item_id": self.sitem.pk}

    def _run_promotion(self):
        import numpy as np

        recipe = PromotionRecipe()
        data = np.full((10, 10), 5.0, dtype="float32")
        with patch.object(
            PromotionRecipe, "read_raster",
            return_value=(data, [0, 0, 1, 1], "EPSG:4326", 10, 10),
        ):
            return run_unit(recipe, self._unit(), writer=_mock_writer())


class PromotionThroughEngineTests(_PromotionFixture):
    def test_promotion_produces_cog_and_png_assets_and_link(self):
        from georiva.core.models import Asset

        result = self._run_promotion()

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection, self.pub_col)
        self.assertEqual(item.time, datetime(2020, 1, 1, tzinfo=timezone.utc))

        # A served COG (data role) + a visual PNG — the pair the catalog needs.
        cog = item.assets.get(format=Asset.Format.COG)
        self.assertEqual(cog.variable, self.variable)
        self.assertEqual(cog.roles, ["data"])
        png = item.assets.get(format=Asset.Format.PNG)
        self.assertEqual(png.roles, ["visual"])

        link = DerivationLink.objects.get(derived_item=item)
        self.assertEqual(link.source_staging_item, self.sitem)
        self.assertIsNone(link.source_published_item)
        self.assertEqual(link.recipe_id, "promotion")
        self.assertEqual(link.recipe_version, "2")
        self.assertEqual(link.input_hash, result.input_hash)

    def test_derivation_run_records_lifecycle(self):
        result = self._run_promotion()
        run_rec = DerivationRun.objects.get(recipe_type="promotion")
        self.assertEqual(run_rec.status, DerivationRun.Status.COMPLETED)
        self.assertEqual(run_rec.produced_item_id, result.item_id)
        self.assertEqual(run_rec.input_hash, result.input_hash)

    def test_second_concurrent_acquire_is_blocked_by_lock(self):
        # First acquire takes the lock and leaves it RUNNING (not released).
        from georiva.processing.recipe import unit_hash
        uhash = unit_hash(self._unit())
        first = DerivationRun.acquire(
            recipe_type="promotion", recipe_version="1",
            unit_key=self._unit(), unit_hash=uhash,
        )
        self.assertIsNotNone(first)
        second = DerivationRun.acquire(
            recipe_type="promotion", recipe_version="1",
            unit_key=self._unit(), unit_hash=uhash,
        )
        self.assertIsNone(second)

    def test_rerun_unchanged_inputs_is_noop(self):
        first = self._run_promotion()
        self.assertEqual(first.status, "completed")

        second = self._run_promotion()
        self.assertEqual(second.status, "skipped")

        self.assertEqual(Item.objects.filter(collection=self.pub_col).count(), 1)
        self.assertEqual(DerivationLink.objects.count(), 1)

    def test_changed_checksum_recomputes_in_place(self):
        first = self._run_promotion()

        self.sasset.checksum = "DIFFERENT"
        self.sasset.save(update_fields=["checksum"])

        second = self._run_promotion()
        self.assertEqual(second.status, "completed")
        self.assertNotEqual(second.input_hash, first.input_hash)

        # Overwrite in place — still one item, one link.
        self.assertEqual(Item.objects.filter(collection=self.pub_col).count(), 1)
        self.assertEqual(DerivationLink.objects.count(), 1)
        item = Item.objects.get(pk=second.item_id)
        self.assertEqual(
            item.properties["derivation"]["input_hash"], second.input_hash
        )

    def test_dispatch_fans_out_one_task_per_unit(self):
        StagingItem.objects.create(
            collection=self.scol,
            datetime=datetime(2020, 1, 2, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )
        with patch("georiva.processing.tasks.run_unit_task") as task:
            run(PromotionRecipe(), {"collection_slug": "tas"}, dispatch=True)
        self.assertEqual(task.delay.call_count, 2)
        _, kwargs = task.delay.call_args
        self.assertEqual(kwargs["recipe_type"], "promotion")

    def test_promotion_targets_the_linked_core_collection_after_a_slug_rename(self):
        # The staging collection is linked to its core Collection (ADR-0010 §3).
        # Promotion resolves the output by that FK, so renaming the core
        # collection's slug afterwards doesn't spawn a second collection (§5 AC4).
        self.scol.collection = self.pub_col
        self.scol.save(update_fields=["collection"])
        self.pub_col.slug = "tas-renamed-by-operator"
        self.pub_col.save(update_fields=["slug"])

        result = self._run_promotion()

        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection, self.pub_col)
        # No stray collection created from the old slug.
        self.assertEqual(
            Collection.objects.filter(catalog=self.catalog).count(), 1
        )


class DerivationLinkConstraintTests(_PromotionFixture):
    def _item(self):
        return Item.objects.create(
            collection=self.pub_col,
            time=datetime(2021, 1, 1, tzinfo=timezone.utc),
        )

    def test_rejects_zero_sources(self):
        with self.assertRaises(IntegrityError), transaction.atomic():
            DerivationLink.objects.create(
                derived_item=self._item(),
                recipe_id="promotion", recipe_version="1", input_hash="x",
            )

    def test_rejects_two_sources(self):
        item = self._item()
        with self.assertRaises(IntegrityError), transaction.atomic():
            DerivationLink.objects.create(
                derived_item=item,
                source_staging_item=self.sitem,
                source_published_item=item,
                recipe_id="promotion", recipe_version="1", input_hash="x",
            )


class _FakeRecipe(BaseRecipe):
    """A trivial non-Promotion recipe — proves the engine is recipe-agnostic."""
    type = "fake"
    version = "1"

    def __init__(self, collection, variable, staging_item, staging_asset):
        self._c, self._v, self._si, self._sa = (
            collection, variable, staging_item, staging_asset
        )

    def enumerate_units(self, selector):
        return [{"n": 1}]

    def resolve_inputs(self, unit):
        return {"src": ResolvedInput("src", True, [self._si], [self._sa])}

    def outputs(self, unit):
        return OutputItem(
            collection=self._c,
            time=datetime(2022, 6, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=4, height=4,
        )

    def transform(self, unit, resolved):
        import numpy as np
        return [OutputAsset(
            variable=self._v, roles=["data"], format="cog",
            array=np.zeros((4, 4), dtype="float32"),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=4, height=4,
        )]


class EngineIsRecipeAgnosticTests(_PromotionFixture):
    def test_fake_recipe_runs_through_engine(self):
        recipe = _FakeRecipe(self.pub_col, self.variable, self.sitem, self.sasset)
        result = run_unit(recipe, {"n": 1}, writer=_mock_writer())

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.assets.get().format, "cog")
        self.assertEqual(DerivationLink.objects.filter(derived_item=item).count(), 1)
        self.assertEqual(
            DerivationRun.objects.get(recipe_type="fake").status,
            DerivationRun.Status.COMPLETED,
        )


class RegisterPngAssetTests(_PromotionFixture):
    """The engine writes a PNG (encoded RGBA) for an array OutputAsset declared
    with format='png', alongside the COG path for format='cog' (ADR: derived
    products emit COG + PNG like ingestion)."""

    def test_png_output_asset_is_encoded_and_written_as_png(self):
        import numpy as np

        from georiva.core.models import Asset
        from georiva.processing.engine import _register_asset
        from georiva.processing.recipe import OutputAsset

        writer = _mock_writer()
        item = Item.objects.create(
            collection=self.pub_col,
            time=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
        data = np.full((3, 2), 12.0, dtype="float32")

        asset = _register_asset(
            item,
            OutputAsset(variable=self.variable, roles=["visual"], format="png",
                        array=data, bounds=[0, 0, 1, 1], crs="EPSG:4326",
                        width=2, height=3),
            writer,
        )

        writer.write_png.assert_called_once()
        # The encoder turned the 2D data into an (H, W, 4) RGBA array.
        rgba = writer.write_png.call_args.args[0]
        self.assertEqual(rgba.shape, (3, 2, 4))
        self.assertEqual(asset.format, Asset.Format.PNG)
        self.assertEqual(asset.roles, ["visual"])
        writer.write_cog.assert_not_called()
