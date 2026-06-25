"""
Declaration-driven input resolution (ADR-0008, issue #143).

resolve_declared_inputs turns a product's declared InputRefs into the engine's
ResolvedInput objects, querying the StagingItem tier or the Published Item tier
by collection slug. This is the seam that lets resolve_inputs consume declared
inputs instead of hardcoded slugs — so readiness and the dependency graph are
computed from the declaration, not from running a recipe.

Mirrors the fixture style of test_multi_input.py (real catalog/staging rows,
asserting the resolved result).
"""
from datetime import datetime, timezone

from django.test import TestCase

from georiva.core.derived_products import InputRef
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.processing.recipe import BaseRecipe, resolve_declared_inputs
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem

_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


class ResolveDeclaredInputsTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.unit_dim, _ = Unit.objects.get_or_create(
            symbol="mm", defaults={"name": "millimetre"}
        )

        # Staging tier: a raw rainfall series.
        self.staging_col = StagingCollection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        # Published tier: a normals product.
        self.pub_col = Collection.objects.create(
            catalog=self.catalog, slug="rainfall-normals", name="Normals"
        )
        self.pub_var = Variable.objects.create(
            collection=self.pub_col, slug="normals", name="Normals",
            unit=self.unit_dim, value_min=0, value_max=2000,
        )

    def _add_staging(self):
        si = StagingItem.objects.create(
            collection=self.staging_col, datetime=_TIME,
            bounds=[0, 0, 8, 8], crs="EPSG:4326", width=4, height=4,
        )
        StagingAsset.objects.create(
            item=si, href="chirps/rainfall/r.tif", roles=["source"],
            format="geotiff", checksum="rain-1",
        )
        return si

    def _add_published(self):
        item = Item.objects.create(
            collection=self.pub_col, time=_TIME,
            bounds=[0, 0, 8, 8], crs="EPSG:4326", width=4, height=4,
        )
        Asset.objects.create(
            item=item, variable=self.pub_var, format="cog",
            href="chirps/normals/n.tif", roles=["data"], checksum="norm-1",
            width=4, height=4,
        )
        return item

    def test_staging_input_resolves_to_staging_items(self):
        si = self._add_staging()

        resolved = resolve_declared_inputs(
            [InputRef(role="value", collection="rainfall", tier="staging")]
        )

        ri = resolved["value"]
        self.assertTrue(ri.present)
        self.assertEqual([it.pk for it in ri.items], [si.pk])
        self.assertEqual(ri.checksums, ["rain-1"])

    def test_published_input_resolves_to_published_items(self):
        item = self._add_published()

        resolved = resolve_declared_inputs(
            [InputRef(role="normals", collection="rainfall-normals", tier="published")]
        )

        ri = resolved["normals"]
        self.assertTrue(ri.present)
        self.assertEqual([it.pk for it in ri.items], [item.pk])

    def test_required_flag_is_carried_through(self):
        resolved = resolve_declared_inputs([
            InputRef(role="value", collection="rainfall", tier="staging"),
            InputRef(role="normals", collection="rainfall-normals",
                     tier="published", required=False),
        ])

        self.assertTrue(resolved["value"].required)
        self.assertFalse(resolved["normals"].required)

    def test_empty_collection_resolves_to_absent_input(self):
        # No rows added: the declared input is keyed but not present, which is
        # how product readiness detects a blocked (empty-input) product.
        resolved = resolve_declared_inputs(
            [InputRef(role="value", collection="rainfall", tier="staging")]
        )

        self.assertIn("value", resolved)
        self.assertFalse(resolved["value"].present)

    def test_recipe_declaring_inputs_resolves_them_without_an_override(self):
        # A declaration-driven recipe sets declared_inputs and inherits the
        # default resolve_inputs — no hardcoded slugs in a bespoke override.
        si = self._add_staging()

        class _DeclarativeRecipe(BaseRecipe):
            type = "declarative_fixture"

            def declared_inputs(self, unit):
                return [InputRef(role="value", collection="rainfall", tier="staging")]

            def enumerate_units(self, selector):
                return [{}]

            def outputs(self, unit):
                return None

            def transform(self, unit, resolved):
                return []

        resolved = _DeclarativeRecipe().resolve_inputs({})

        self.assertEqual([it.pk for it in resolved["value"].items], [si.pk])
