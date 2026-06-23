"""
Multi-input / cross-collection / cross-tier capability — proven through the
engine's ``run()`` seam with a *synthetic* fixture recipe and mocked I/O. This
is a capability slice, not a real product: the fixture recipe combines three
required inputs spanning a Staging collection, an ``internal`` Published
collection, and a ``public`` Published collection, plus one ``optional`` input.

The engine is generic — these tests assert it already handles the multi-input
join, optional-as-absent, cross-tier lineage, and recipe-side harmonization,
without any climate-specific knowledge. Mirrors ``test_engine.py`` (mock the
AssetWriter, assert produced records).

See issue #124 and docs/adr/0005-generic-derivation-engine.md.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
from django.test import TestCase
from rasterio.transform import from_bounds

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.geoprocessing import regrid_array
from georiva.processing.engine import run, run_unit
from georiva.processing.recipe import (
    BaseRecipe,
    OutputAsset,
    OutputItem,
    ResolvedInput,
)
from georiva.staging.models import (
    DerivationLink,
    StagingAsset,
    StagingCollection,
    StagingItem,
)

_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _mock_writer():
    w = MagicMock()
    w.bucket.save.side_effect = lambda path, data: path
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    return w


class _MultiInputFixtureRecipe(BaseRecipe):
    """
    A synthetic multi-input recipe: combine three required inputs spanning
    collections/tiers (+ one optional) onto a declared **target grid**.

    Declares only the engine's surface — selectors, enumeration, outputs, a
    pure transform. The transform harmonizes every input onto the target grid
    via ``geoprocessing.regrid_array`` before the join (a sum), so the engine
    never aligns anything.
    """

    type = "fixture_multi"
    version = "1"

    # The recipe's declared target grid of production.
    TARGET_SHAPE = (8, 8)             # (height, width)
    TARGET_BOUNDS = [0.0, 0.0, 8.0, 8.0]
    TARGET_CRS = "EPSG:4326"

    def __init__(self, *, output_collection, output_variable):
        self._out_col = output_collection
        self._out_var = output_variable

    # ---- declarative surface ------------------------------------------------

    def enumerate_units(self, selector):
        return [{"slice": "2020-01"}]

    def resolve_inputs(self, unit):
        return {
            "precip": self._select(StagingItem, "precip-staging", required=True),
            "soil": self._select(Item, "soil-internal", required=True),
            "veg": self._select(Item, "veg", required=True),
            "pet": self._select(Item, "pet", required=False),
        }

    def outputs(self, unit):
        return OutputItem(
            collection=self._out_col,
            time=_TIME,
            bounds=self.TARGET_BOUNDS,
            crs=self.TARGET_CRS,
            width=self.TARGET_SHAPE[1],
            height=self.TARGET_SHAPE[0],
        )

    def transform(self, unit, resolved):
        layers = [
            self._harmonized(resolved["precip"]),
            self._harmonized(resolved["soil"]),
            self._harmonized(resolved["veg"]),
        ]

        # Optional input: absent → passed to the transform as None, the unit
        # still computes (one fewer layer).
        pet = resolved.get("pet")
        pet_layer = self._harmonized(pet) if (pet and pet.present) else None
        if pet_layer is not None:
            layers.append(pet_layer)

        combined = np.nansum(np.stack(layers), axis=0).astype("float32")
        return [OutputAsset(
            variable=self._out_var, roles=["data"], format="cog",
            array=combined,
            bounds=self.TARGET_BOUNDS, crs=self.TARGET_CRS,
            width=self.TARGET_SHAPE[1], height=self.TARGET_SHAPE[0],
        )]

    # ---- harmonization (recipe-side, via geoprocessing) ---------------------

    def _harmonized(self, resolved_input):
        """Read one input's array on its own grid and regrid to the target."""
        item = resolved_input.items[0]
        arr, src_transform, src_crs = self.read_array(item)
        dst_transform = from_bounds(*self.TARGET_BOUNDS, *self.TARGET_SHAPE[::-1])
        return regrid_array(
            arr, src_transform, src_crs,
            dst_transform, self.TARGET_CRS, self.TARGET_SHAPE,
            resampling="nearest",
        )

    # ---- I/O seam (in-memory for tests) -------------------------------------

    def read_array(self, item):
        """
        The recipe's single I/O seam. Here it synthesises a constant array on
        the item's *own* grid (read from its bounds + dimensions), so inputs
        genuinely sit on different grids and the harmonization step has work to
        do. A real recipe would read bytes from storage instead.
        """
        h, w = item.height, item.width
        west, south, east, north = item.bounds
        transform = from_bounds(west, south, east, north, w, h)
        return np.ones((h, w), dtype="float32"), transform, item.crs

    # ---- helpers ------------------------------------------------------------

    @staticmethod
    def _select(model, collection_slug, *, required):
        items = list(
            model.objects
            .filter(collection__slug=collection_slug)
            .prefetch_related("assets")
        )
        assets = [a for it in items for a in it.assets.all()]
        name = collection_slug.split("-")[0]
        return ResolvedInput(name, required=required, items=items, assets=assets)


class _MultiInputFixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="Climate", slug="climate", file_format="geotiff"
        )
        self.unit = Unit.objects.create(name="dimensionless", symbol="x")

        # Output (public) collection + its variable.
        self.out_col = Collection.objects.create(
            catalog=self.catalog, slug="cdi", name="CDI",
            visibility=Collection.Visibility.PUBLIC,
        )
        self.out_var = Variable.objects.create(
            collection=self.out_col, slug="cdi", name="CDI",
            unit=self.unit, value_min=-3, value_max=3,
        )

        # precip — Staging tier.
        self.precip_scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="precip-staging", name="Precip"
        )
        # soil — Published, INTERNAL (harmonized intermediate, read as input).
        self.soil_col = Collection.objects.create(
            catalog=self.catalog, slug="soil-internal", name="Soil",
            visibility=Collection.Visibility.INTERNAL,
        )
        # veg — Published, public.
        self.veg_col = Collection.objects.create(
            catalog=self.catalog, slug="veg", name="Veg",
            visibility=Collection.Visibility.PUBLIC,
        )
        # pet — Published, public (optional input; created empty by default).
        self.pet_col = Collection.objects.create(
            catalog=self.catalog, slug="pet", name="PET",
            visibility=Collection.Visibility.PUBLIC,
        )

    # -- fixture builders (each input on a *different* grid) ------------------

    def _add_precip(self, checksum="precip-1"):
        si = StagingItem.objects.create(
            collection=self.precip_scol, datetime=_TIME,
            bounds=[0, 0, 8, 8], crs="EPSG:4326", width=4, height=4,
        )
        StagingAsset.objects.create(
            item=si, href="climate/precip/p.tif", roles=["source"],
            format="geotiff", checksum=checksum,
        )
        return si

    def _add_published(self, collection, href, checksum, *, w, h):
        item = Item.objects.create(
            collection=collection, time=_TIME,
            bounds=[0, 0, 8, 8], crs="EPSG:4326", width=w, height=h,
        )
        Asset.objects.create(
            item=item, variable=self.out_var, format="cog",
            href=href, roles=["data"], checksum=checksum, width=w, height=h,
        )
        return item

    def _add_soil(self, checksum="soil-1"):
        return self._add_published(self.soil_col, "climate/soil/s.tif", checksum, w=6, h=6)

    def _add_veg(self, checksum="veg-1"):
        return self._add_published(self.veg_col, "climate/veg/v.tif", checksum, w=5, h=5)

    def _add_pet(self, checksum="pet-1"):
        return self._add_published(self.pet_col, "climate/pet/e.tif", checksum, w=7, h=7)

    def _recipe(self):
        return _MultiInputFixtureRecipe(
            output_collection=self.out_col, output_variable=self.out_var
        )

    def _run(self):
        recipe = self._recipe()
        unit = next(iter(recipe.enumerate_units({})))
        self.writer = _mock_writer()
        return run_unit(recipe, unit, writer=self.writer)


class ReadinessJoinTests(_MultiInputFixture):
    def test_runs_only_when_all_required_inputs_present(self):
        self._add_precip()
        self._add_soil()
        self._add_veg()  # all three required inputs present

        result = self._run()

        self.assertEqual(result.status, "completed")

    def test_missing_required_input_is_not_ready(self):
        self._add_precip()
        self._add_soil()
        # veg (required) absent

        result = self._run()

        self.assertEqual(result.status, "not_ready")
        self.assertEqual(Item.objects.filter(collection=self.out_col).count(), 0)


class OptionalInputTests(_MultiInputFixture):
    def test_absent_optional_input_still_computes(self):
        self._add_precip()
        self._add_soil()
        self._add_veg()
        # pet (optional) absent — passed to the transform as absent (None)

        result = self._run()

        self.assertEqual(result.status, "completed")
        # Only the three required inputs contributed lineage edges.
        self.assertEqual(
            DerivationLink.objects.filter(derived_item_id=result.item_id).count(), 3
        )

    def test_present_optional_input_participates(self):
        self._add_precip()
        self._add_soil()
        self._add_veg()
        self._add_pet()  # optional input present this time

        result = self._run()

        self.assertEqual(result.status, "completed")
        self.assertEqual(
            DerivationLink.objects.filter(derived_item_id=result.item_id).count(), 4
        )


class CrossTierLineageTests(_MultiInputFixture):
    def test_links_span_collections_and_tiers(self):
        precip = self._add_precip()   # Staging tier
        soil = self._add_soil()       # Published, internal
        veg = self._add_veg()         # Published, public

        result = self._run()
        self.assertEqual(result.status, "completed")

        links = DerivationLink.objects.filter(derived_item_id=result.item_id)
        self.assertEqual(links.count(), 3)

        # One edge from the Staging tier…
        staging_links = links.filter(source_staging_item__isnull=False)
        self.assertEqual(staging_links.count(), 1)
        self.assertEqual(staging_links.get().source_staging_item, precip)

        # …two from the Published tier, spanning different collections.
        published = links.filter(source_published_item__isnull=False)
        self.assertEqual(
            {l.source_published_item_id for l in published}, {soil.pk, veg.pk}
        )

    def test_internal_collection_is_read_as_an_input(self):
        self._add_precip()
        soil = self._add_soil()
        self._add_veg()
        self.assertEqual(soil.collection.visibility, Collection.Visibility.INTERNAL)

        result = self._run()

        # The internal intermediate is consumed as an input and recorded in
        # lineage, even though it is never served.
        self.assertTrue(
            DerivationLink.objects.filter(
                derived_item_id=result.item_id, source_published_item=soil
            ).exists()
        )


class RunPrimitiveTests(_MultiInputFixture):
    """The capability funnels through the one ``run(recipe, selector)`` seam —
    enumerate units, then execute each — not a per-recipe harness."""

    def test_multi_input_recipe_completes_through_run(self):
        self._add_precip()
        self._add_soil()
        self._add_veg()

        with patch(
            "georiva.ingestion.asset_writer.AssetWriter", return_value=_mock_writer()
        ):
            results = run(self._recipe(), {}, dispatch=False)

        self.assertEqual([r.status for r in results], ["completed"])
        self.assertEqual(Item.objects.filter(collection=self.out_col).count(), 1)
        item = Item.objects.get(collection=self.out_col)
        self.assertEqual(
            DerivationLink.objects.filter(derived_item_id=item.pk).count(), 3
        )


class HarmonizationTests(_MultiInputFixture):
    def test_inputs_on_different_grids_harmonized_to_target_grid(self):
        # Inputs sit on 4x4, 6x6, 5x5 grids respectively.
        self._add_precip()
        self._add_soil()
        self._add_veg()

        result = self._run()
        self.assertEqual(result.status, "completed")

        # The COG written carries the recipe's target grid, not any input grid:
        # the join transform consumed pre-aligned (8x8) arrays.
        written_array = self.writer.write_cog.call_args[0][0]
        self.assertEqual(written_array.shape, _MultiInputFixtureRecipe.TARGET_SHAPE)

        item = Item.objects.get(pk=result.item_id)
        self.assertEqual((item.height, item.width), _MultiInputFixtureRecipe.TARGET_SHAPE)
