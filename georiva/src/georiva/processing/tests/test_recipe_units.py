"""A recipe declares the unit it reads each input in (ADR 0046 §13; #745).

A recipe's output is stored bytes under a fixed label, so it reads its inputs in
units it names, never in whatever an input happens to be served in this week.
Nothing here opens a COG: an input is a Kelvin GeoTIFF ingested as it came, and
every value is read back through the engine's reading session.
"""

from datetime import UTC, datetime
from unittest import mock
from unittest.mock import MagicMock

import numpy as np
from django.test import SimpleTestCase

from georiva.core import values
from georiva.core.models import Collection, Item, Variable
from georiva.core.tests.test_values_units import KELVIN, IngestedInSourceUnit, unit
from georiva.processing.engine import run_unit
from georiva.processing.recipe import SOURCE_UNIT, BaseRecipe, OutputAsset, OutputItem, ResolvedInput
from georiva.processing.recipes.climatology import ClimatologyRecipe
from georiva.processing.recipes.promotion import PromotionRecipe
from georiva.processing.registry import RecipeRegistry, UndeclaredRecipeUnits


def _writer():
    """An AssetWriter that keeps the arrays it is handed instead of writing them."""
    w = MagicMock()
    w.bucket.save.side_effect = lambda path, data: path
    w.write_cog.side_effect = lambda arr, path, *a, **k: (path, None)
    return w


def _written(writer) -> np.ndarray:
    return np.asarray(writer.write_cog.call_args[0][0])


class _Declared(BaseRecipe):
    """Enough of a recipe to be registered."""

    def enumerate_units(self, selector):
        return []

    def outputs(self, unit):
        raise NotImplementedError

    def transform(self, unit, resolved):
        raise NotImplementedError

    def output_unit(self, unit, resolved):
        return "K"


class RegistrationTests(SimpleTestCase):
    """A recipe that does not declare its units is refused as it registers — at
    app start for a plugin's recipe — with the reason (ADR 0046 §13)."""

    def tearDown(self):
        RecipeRegistry._recipes.pop("units-test", None)

    def assertRefused(self, recipe_cls, reason):
        with self.assertRaisesMessage(UndeclaredRecipeUnits, reason):
            RecipeRegistry.register(recipe_cls)

    def test_a_recipe_that_declares_no_input_units_is_refused(self):
        class Undeclared(_Declared):
            type = "units-test"

        self.assertRefused(Undeclared, "declares no input units")
        self.assertNotIn("units-test", RecipeRegistry.all_types())

    def test_an_input_without_a_unit_is_refused_and_named(self):
        class HalfDeclared(_Declared):
            type = "units-test"
            input_units = {"value": "K", "baseline": ""}

        self.assertRefused(HalfDeclared, "'baseline'")
        self.assertNotIn("units-test", RecipeRegistry.all_types())

    def test_a_recipe_creating_its_output_variables_must_declare_their_unit(self):
        class NoOutputUnit(BaseRecipe):
            type = "units-test"
            input_units = {"value": "K"}

            def enumerate_units(self, selector):
                return []

            def outputs(self, unit):
                raise NotImplementedError

            def transform(self, unit, resolved):
                raise NotImplementedError

        self.assertRefused(NoOutputUnit, "output unit")
        self.assertNotIn("units-test", RecipeRegistry.all_types())

    def test_a_recipe_declaring_every_unit_is_registered(self):
        class Declared(_Declared):
            type = "units-test"
            input_units = {"value": "K", "baseline": "K"}

        RecipeRegistry.register(Declared)
        self.assertIn("units-test", RecipeRegistry.all_types())

    def test_the_core_recipes_declare_their_units(self):
        self.assertEqual(PromotionRecipe.input_units, {"source": SOURCE_UNIT})
        self.assertEqual(ClimatologyRecipe.input_units, {"value": SOURCE_UNIT, "baseline": SOURCE_UNIT})
        self.assertIn("promotion", RecipeRegistry.all_types())
        self.assertIn("climatology", RecipeRegistry.all_types())


class _Copy(BaseRecipe):
    """Writes its one input as it read it, in the unit it declares."""

    type = "copy-in-unit"
    version = "1"
    creates_output_variables = False

    def __init__(self, in_unit, asset, out_variable):
        self.input_units = {"value": in_unit}
        self.asset, self.out_variable = asset, out_variable
        self.sessions = []

    def enumerate_units(self, selector):
        return [{"n": 1}]

    def resolve_inputs(self, unit):
        return {"value": ResolvedInput("value", True, [self.asset.item], [self.asset])}

    def outputs(self, unit):
        return OutputItem(collection=self.out_variable.collection, time=datetime(2026, 3, 1, tzinfo=UTC))

    def transform(self, unit, resolved):
        ri = resolved["value"]
        self.sessions.append(ri.session)
        field = ri.field(ri.assets[0])
        return [
            OutputAsset(
                variable=self.out_variable,
                array=field.data,
                bounds=list(field.bounds),
                width=field.width,
                height=field.height,
            )
        ]


class ProductionUnitReadsInDeclaredUnitTests(IngestedInSourceUnit):
    """The input is held in K and served in °C."""

    def setUp(self):
        super().setUp()
        out = Collection.objects.create(catalog=self.variable.collection.catalog, name="Copy", slug="copy")
        self.out_variable = Variable.objects.create(
            collection=out, name="Copy", slug="copy", source_unit=unit("K"), unit=unit("K")
        )

    def _run(self, in_unit, version="1"):
        recipe = _Copy(in_unit, self.asset, self.out_variable)
        recipe.version = version
        writer = _writer()
        result = run_unit(recipe, {"n": 1}, writer=writer)
        self.assertEqual(result.status, "completed")
        return recipe, _written(writer)

    def test_an_input_is_read_in_its_declared_unit_not_its_served_one(self):
        _, written = self._run("K")
        np.testing.assert_allclose(written, KELVIN)

    def test_another_declared_unit_is_converted_to(self):
        _, written = self._run("degC")
        np.testing.assert_allclose(written, KELVIN - 273.15, rtol=1e-6)

    def test_changing_the_inputs_served_unit_changes_no_output_byte(self):
        _, before = self._run("K")

        self.variable.unit = unit("degF")
        self.variable.save()
        _, after = self._run("K", version="2")

        self.assertEqual(before.tobytes(), after.tobytes())

    def test_the_unit_is_read_in_one_session_that_ends_with_it(self):
        recipe, _ = self._run("K")
        (reading,) = recipe.sessions
        self.assertIsNotNone(reading)
        self.assertEqual(reading.open_stores, 0)
        with self.assertRaises(values.SessionClosed):
            reading.field(self.asset)

    def test_a_unit_the_input_cannot_be_read_in_fails_the_unit(self):
        with self.assertRaises(values.ConversionRefused):
            self._run("m")

    def test_an_input_whose_unit_is_not_declared_fails_the_unit_before_anything_is_read(self):
        """Never the served unit as a fallback: a role the recipe names no unit
        for refuses the production unit, and the recipe computes nothing."""
        recipe = _Copy("K", self.asset, self.out_variable)
        recipe.resolve_inputs = lambda unit: {
            "value": ResolvedInput("value", True, [self.asset.item], [self.asset]),
            "baseline": ResolvedInput("baseline", True, [self.asset.item], [self.asset]),
        }

        with self.assertRaisesRegex(values.ConversionRefused, "baseline"):
            run_unit(recipe, {"n": 1}, writer=_writer())
        self.assertEqual(recipe.sessions, [])


class _ClimatologyOverKelvin(IngestedInSourceUnit):
    """A climatology over the Kelvin input, read in its source unit."""

    def _run(self, quantity, *, source_unit="K", served_unit="degC"):
        from georiva.staging.models import StagingAsset, StagingCollection, StagingItem

        self.variable.unit = unit(served_unit)
        self.variable.correct_source_unit(unit(source_unit))
        self.variable.save()
        catalog = self.variable.collection.catalog
        scol = StagingCollection.objects.create(catalog=catalog, slug="t2m-series", name="t2m")
        sitem = StagingItem.objects.create(
            collection=scol,
            start_datetime=datetime(2011, 1, 1, tzinfo=UTC),
            bounds=[30, -4, 38, 4],
            crs="EPSG:4326",
            width=8,
            height=8,
        )
        StagingAsset.objects.create(item=sitem, href="t2m.nc", roles=["source"], variable=self.variable)
        series = self._series()
        writer = _writer()
        unit_ = {
            "source_collection": "t2m-series",
            "variable": "t2m",
            "period": [2011, 2011],
            "season": "annual",
            "quantity": quantity,
            "baseline": [2010, 2010] if quantity in ("anomaly", "relative_anomaly") else None,
        }
        with mock.patch.object(ClimatologyRecipe, "read_series", return_value=series):
            result = run_unit(ClimatologyRecipe(), unit_, writer=writer)
        self.assertEqual(result.status, "completed")
        return Item.objects.get(pk=result.item_id).assets.get().variable, _written(writer)

    def _series(self):
        import pandas as pd
        import xarray as xr

        time = pd.date_range("2010-01-01", periods=24, freq="MS")
        data = np.broadcast_to(KELVIN, (24, 8, 8)).copy()
        data[12:] += 2.0  # 2011 is two kelvin warmer than 2010
        return xr.DataArray(data, coords={"time": time}, dims=["time", "y", "x"])


class ClimatologyOutputUnitTests(_ClimatologyOverKelvin):
    def test_a_climatology_is_held_in_its_inputs_source_unit(self):
        variable, written = self._run("value")
        self.assertEqual(variable.source_unit.symbol, "K")
        self.assertEqual(variable.unit.symbol, "K")
        np.testing.assert_allclose(written, KELVIN + 2.0)

    def test_an_anomaly_of_a_unit_with_an_offset_is_a_difference_unit(self):
        variable, written = self._run("anomaly", source_unit="degC", served_unit="degF")
        self.assertEqual(variable.source_unit.symbol, "delta_degC")
        self.assertEqual(variable.unit.symbol, "delta_degC")
        np.testing.assert_allclose(written, 2.0)

    def test_an_anomaly_of_kelvin_is_kelvin(self):
        variable, _ = self._run("anomaly")
        self.assertEqual(variable.source_unit.symbol, "K")

    def test_a_relative_anomaly_is_dimensionless(self):
        variable, _ = self._run("relative_anomaly")
        self.assertEqual(variable.source_unit.symbol, "dimensionless")


class ClimatologyReadsItsSeriesInItsSourceUnitTests(IngestedInSourceUnit):
    """The staged series is a NetCDF file in K, served in °C, read through the session."""

    def test_the_series_is_read_as_stored_whatever_it_is_served_in(self):
        import os

        import pandas as pd
        import xarray as xr

        from georiva.core.storage import Bucket, BucketType
        from georiva.staging.models import StagingAsset, StagingCollection, StagingItem

        time = pd.date_range("2011-01-01", periods=12, freq="MS")
        data = np.broadcast_to(KELVIN, (12, 8, 8)).astype("float32")
        path = self.bucket.storage.path("staged/t2m.nc")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        xr.Dataset({"t2m": (("time", "y", "x"), data)}, coords={"time": time}).to_netcdf(path)

        catalog = self.variable.collection.catalog
        scol = StagingCollection.objects.create(catalog=catalog, slug="t2m-series", name="t2m")
        sitem = StagingItem.objects.create(
            collection=scol, start_datetime=datetime(2011, 1, 1, tzinfo=UTC), bounds=[30, -4, 38, 4], width=8, height=8
        )
        StagingAsset.objects.create(item=sitem, href="staged/t2m.nc", roles=["source"], variable=self.variable)
        staging = Bucket(BucketType.STAGING, "georiva-assets")  # the same temporary directory
        writer = _writer()
        production_unit = {
            "source_collection": "t2m-series",
            "variable": "t2m",
            "period": [2011, 2011],
            "season": "annual",
            "quantity": "value",
            "baseline": None,
        }
        with mock.patch("georiva.core.values.session.storage.bucket", return_value=staging):
            result = run_unit(ClimatologyRecipe(), production_unit, writer=writer)

        self.assertEqual(result.status, "completed")
        np.testing.assert_allclose(_written(writer), KELVIN, rtol=1e-6)
