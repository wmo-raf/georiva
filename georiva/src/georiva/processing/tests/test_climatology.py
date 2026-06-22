"""
ClimatologyRecipe tests — exercised *through* the engine's run()/run_unit()
seam, with the data-reading I/O (``read_series``) and the AssetWriter mocked.
The quantity math itself is covered by geoprocessing unit tests; here we assert
the declarative pieces (enumeration, outputs mapping, input resolution) and that
running a unit produces the right Published records.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import xarray as xr
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.processing.engine import run, run_unit
from georiva.processing.recipes.climatology import ClimatologyRecipe
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
    return w


def _cube(monthly_by_year, ny=3, nx=2, start="2011-01-01"):
    """(time, y, x) monthly cube; every pixel at time t equals monthly_by_year[t]."""
    time = pd.date_range(start, periods=len(monthly_by_year), freq="MS")
    data = np.broadcast_to(
        np.asarray(monthly_by_year, dtype="float32")[:, None, None],
        (len(time), ny, nx),
    )
    return xr.DataArray(data, coords={"time": time}, dims=["time", "y", "x"])


class _ClimatologyFixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="netcdf"
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="tas", name="tas"
        )
        self.sitem = StagingItem.objects.create(
            collection=self.scol,
            start_datetime=datetime(2011, 1, 1, tzinfo=timezone.utc),
            end_datetime=datetime(2012, 12, 31, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=2, height=3,
        )
        # The source variable the recipe mirrors onto its output products.
        self.unit_c = Unit.objects.create(name="Celsius", symbol="C")
        self.src_col = Collection.objects.create(
            catalog=self.catalog, slug="tas-src", name="tas src"
        )
        self.src_var = Variable.objects.create(
            collection=self.src_col, slug="tas", name="tas",
            unit=self.unit_c, value_min=0, value_max=50,
        )
        self.sasset = StagingAsset.objects.create(
            item=self.sitem, href="cmip6/tas/series.nc", roles=["source"],
            format="netcdf", checksum="abc123", variable=self.src_var,
        )

    def _unit(self, **over):
        unit = {
            "source_collection": "tas", "variable": "tas",
            "period": [2011, 2012], "season": "JJA",
            "quantity": "value", "baseline": None,
        }
        unit.update(over)
        return unit

    def _run(self, unit, cube):
        recipe = ClimatologyRecipe()
        self.writer = _mock_writer()
        with patch.object(ClimatologyRecipe, "read_series", return_value=cube):
            return run_unit(recipe, unit, writer=self.writer)

    def _written_array(self):
        """The (y,x) array the transform handed to the AssetWriter."""
        return np.asarray(self.writer.write_cog.call_args[0][0])

    def _yearly_jja_cube(self, jja_by_year, junk=999.0):
        """Contiguous monthly cube over the spanned years; JJA months carry the
        given per-year value, all other months carry junk (ignored by season)."""
        years = range(min(jja_by_year), max(jja_by_year) + 1)
        months = [
            jja_by_year.get(y, 0.0) if m in (6, 7, 8) else junk
            for y in years for m in range(1, 13)
        ]
        return _cube(months, start=f"{min(jja_by_year)}-01-01")


class ValueQuantityTests(_ClimatologyFixture):
    def test_value_unit_produces_item_asset_and_link(self):
        # JJA value over 2011-2012; all JJA pixels = 20.0.
        cube = _cube([20.0] * 24)
        result = self._run(self._unit(), cube)

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection.slug, "tas_jja_value")
        self.assertEqual(item.time, datetime(2011, 1, 1, tzinfo=timezone.utc))

        asset = item.assets.get()
        self.assertIn("data", asset.roles)

        link = DerivationLink.objects.get(derived_item=item)
        self.assertEqual(link.source_staging_item, self.sitem)
        self.assertEqual(link.recipe_id, "climatology")


class AssetStatsTests(_ClimatologyFixture):
    def test_derived_asset_carries_stats_from_result(self):
        cube = _cube([20.0] * 24)  # JJA climatology = 20.0 everywhere
        result = self._run(self._unit(), cube)

        asset = Item.objects.get(pk=result.item_id).assets.get()
        self.assertAlmostEqual(asset.stats_min, 20.0)
        self.assertAlmostEqual(asset.stats_max, 20.0)
        self.assertAlmostEqual(asset.stats_mean, 20.0)
        self.assertAlmostEqual(asset.stats_std, 0.0)


class OutputVariableMetadataTests(_ClimatologyFixture):
    def _out_variable(self, unit, cube):
        result = self._run(unit, cube)
        self.assertEqual(result.status, "completed")
        return Item.objects.get(pk=result.item_id).assets.get().variable

    def test_value_variable_mirrors_source(self):
        var = self._out_variable(self._unit(), _cube([20.0] * 24))
        self.assertEqual(var.unit, self.unit_c)
        self.assertAlmostEqual(var.value_min, 0.0)
        self.assertAlmostEqual(var.value_max, 50.0)

    def test_anomaly_variable_has_symmetric_range_and_mirrors_unit(self):
        cube = self._yearly_jja_cube({1981: 10.0, 2011: 13.0})
        unit = self._unit(
            season="JJA", quantity="anomaly",
            period=[2011, 2011], baseline=[1981, 1981],
        )
        var = self._out_variable(unit, cube)
        self.assertEqual(var.unit, self.unit_c)  # an anomaly keeps source units
        self.assertAlmostEqual(var.value_min, -25.0)  # span = (50-0)/2
        self.assertAlmostEqual(var.value_max, 25.0)
        self.assertIn("anomaly", var.name.lower())

    def test_relative_anomaly_variable_is_dimensionless(self):
        cube = self._yearly_jja_cube({1981: 10.0, 2011: 13.0})
        unit = self._unit(
            season="JJA", quantity="relative_anomaly",
            period=[2011, 2011], baseline=[1981, 1981],
        )
        var = self._out_variable(unit, cube)
        self.assertNotEqual(var.unit, self.unit_c)
        self.assertEqual(var.unit.symbol, "dimensionless")
        self.assertAlmostEqual(var.value_min, -1.0)
        self.assertAlmostEqual(var.value_max, 1.0)

    def test_trend_variable_names_per_year_and_keeps_unit(self):
        cube = self._yearly_jja_cube({2011: 10.0, 2012: 12.0, 2013: 14.0})
        unit = self._unit(season="JJA", quantity="trend", period=[2011, 2013])
        var = self._out_variable(unit, cube)
        self.assertEqual(var.unit, self.unit_c)
        self.assertIn("per year", var.name.lower())


class AnomalyQuantityTests(_ClimatologyFixture):
    def test_anomaly_uses_baseline_window_and_baseline_slug(self):
        # JJA value (2011) = 13; JJA baseline (1981) = 10; anomaly = 3.0.
        cube = self._yearly_jja_cube({1981: 10.0, 2011: 13.0})
        unit = self._unit(
            season="JJA", quantity="anomaly",
            period=[2011, 2011], baseline=[1981, 1981],
        )
        result = self._run(unit, cube)

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection.slug, "tas_jja_anomaly_1981-1981")
        np.testing.assert_array_almost_equal(
            self._written_array(), np.full((3, 2), 3.0)
        )

    def test_relative_anomaly_divides_by_baseline(self):
        cube = self._yearly_jja_cube({1981: 10.0, 2011: 13.0})
        unit = self._unit(
            season="JJA", quantity="relative_anomaly",
            period=[2011, 2011], baseline=[1981, 1981],
        )
        result = self._run(unit, cube)

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection.slug, "tas_jja_relative_anomaly_1981-1981")
        np.testing.assert_array_almost_equal(
            self._written_array(), np.full((3, 2), 0.3)
        )


class TrendQuantityTests(_ClimatologyFixture):
    def test_trend_computes_per_year_slope_with_no_baseline(self):
        # JJA rises 10 -> 12 -> 14 over 2011-2013: slope 2.0/year.
        cube = self._yearly_jja_cube({2011: 10.0, 2012: 12.0, 2013: 14.0})
        unit = self._unit(season="JJA", quantity="trend", period=[2011, 2013])
        result = self._run(unit, cube)

        self.assertEqual(result.status, "completed")
        item = Item.objects.get(pk=result.item_id)
        self.assertEqual(item.collection.slug, "tas_jja_trend")
        np.testing.assert_array_almost_equal(
            self._written_array(), np.full((3, 2), 2.0)
        )


class EnumerateUnitsTests(_ClimatologyFixture):
    def test_cartesian_product_with_baseline_only_on_anomalies(self):
        selector = {
            "source_collection": "tas", "variable": "tas",
            "periods": [[2011, 2040], [2041, 2070]],
            "seasons": ["DJF", "JJA"],
            "quantities": ["value", "anomaly", "trend"],
            "baselines": [[1981, 2010]],
        }
        units = list(ClimatologyRecipe().enumerate_units(selector))

        # 2 periods × 2 seasons × (value + trend + anomaly×1 baseline) = 12.
        self.assertEqual(len(units), 12)

        anomalies = [u for u in units if u["quantity"] == "anomaly"]
        self.assertEqual(len(anomalies), 4)  # 2 periods × 2 seasons
        self.assertTrue(all(u["baseline"] == [1981, 2010] for u in anomalies))

        non_anom = [u for u in units if u["quantity"] in ("value", "trend")]
        self.assertTrue(all(u["baseline"] is None for u in non_anom))

        self.assertIn(
            {"source_collection": "tas", "variable": "tas",
             "period": [2011, 2040], "season": "JJA",
             "quantity": "value", "baseline": None},
            units,
        )


class CalendarFromFileContentTests(_ClimatologyFixture):
    def test_slices_by_year_from_file_axis_not_staging_bounds(self):
        # Staging index bounds lie (say 2099); the authoritative time is the
        # file's own 360-day axis spanning 2011-2013.
        self.sitem.start_datetime = datetime(2099, 1, 1, tzinfo=timezone.utc)
        self.sitem.end_datetime = datetime(2099, 12, 31, tzinfo=timezone.utc)
        self.sitem.save()

        time = xr.date_range(
            "2011-01-01", periods=36, freq="MS",
            calendar="360_day", use_cftime=True,
        )
        months = [
            {2011: 10.0, 2012: 12.0, 2013: 14.0}[int(t.year)]
            if t.month in (6, 7, 8) else 999.0
            for t in time
        ]
        data = np.broadcast_to(
            np.asarray(months, dtype="float32")[:, None, None], (36, 3, 2)
        )
        cube = xr.DataArray(data, coords={"time": time}, dims=["time", "y", "x"])

        unit = self._unit(season="JJA", quantity="trend", period=[2011, 2013])
        result = self._run(unit, cube)

        self.assertEqual(result.status, "completed")
        np.testing.assert_array_almost_equal(
            self._written_array(), np.full((3, 2), 2.0)
        )


class RunThroughEngineTests(_ClimatologyFixture):
    def test_selector_produces_multiple_products_with_lineage(self):
        cube = self._yearly_jja_cube({2011: 10.0, 2012: 13.0})
        selector = {
            "source_collection": "tas", "variable": "tas",
            "periods": [[2012, 2012]], "seasons": ["JJA", "DJF"],
            "quantities": ["value", "anomaly"], "baselines": [[2011, 2011]],
        }
        with patch.object(ClimatologyRecipe, "read_series", return_value=cube), \
                patch("georiva.ingestion.asset_writer.AssetWriter") as AW:
            AW.return_value = _mock_writer()
            results = run(ClimatologyRecipe(), selector, dispatch=False)

        self.assertEqual(len(results), 4)
        self.assertTrue(all(r.status == "completed" for r in results))

        slugs = set(
            Collection.objects.filter(catalog=self.catalog)
            .values_list("slug", flat=True)
        )
        self.assertTrue({
            "tas_jja_value", "tas_jja_anomaly_2011-2011",
            "tas_djf_value", "tas_djf_anomaly_2011-2011",
        } <= slugs)

        self.assertEqual(Item.objects.filter(collection__slug__startswith="tas_").count(), 4)
        self.assertEqual(DerivationLink.objects.count(), 4)
        self.assertTrue(
            all(l.source_staging_item_id == self.sitem.pk
                for l in DerivationLink.objects.all())
        )
