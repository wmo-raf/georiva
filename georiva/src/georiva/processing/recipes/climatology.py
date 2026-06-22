"""
Climatology & Indices recipe family.

Turns a staging series (one long multi-temporal raster file per
``variable × experiment``) into Published climatology products across the
dimensions ``period × season × quantity``, where
``quantity ∈ {value, anomaly, relative_anomaly, trend}``. Anomaly quantities add
a ``baseline`` window.

The engine owns the run loop; this recipe only *declares* what to produce:

- ``enumerate_units`` — the cartesian product over the selector's declared
  dimensions (baseline attached only to the anomaly quantities).
- ``resolve_inputs`` — a ``value`` selector over the source staging series, plus
  a required ``baseline`` selector for the anomaly quantities.
- ``transform`` — slices the series to the period (and baseline) window **by
  year taken from the file's own time axis** (authoritative + calendar-safe),
  then computes the quantity via the pure ``geoprocessing`` library.
- ``outputs`` — maps the categorical coordinates (season, quantity, baseline)
  onto a Published Collection slug and the period onto the Item time key.

The actual numerical I/O (opening the staging file into an xarray DataArray) is
isolated in :meth:`read_series` so the declarative pieces and the transform are
testable with that one seam mocked. The quantity math is covered by the
``geoprocessing`` unit tests.

See docs/adr/0005-generic-derivation-engine.md and issue #123.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from georiva.geoprocessing import anomaly, climatology, trend
from georiva.processing.recipe import (
    BaseRecipe,
    OutputAsset,
    OutputItem,
    ProductionUnit,
    ResolvedInput,
)
from georiva.processing.registry import RecipeRegistry

# Quantities that compare a value window against a baseline window.
_ANOMALY_QUANTITIES = {"anomaly", "relative_anomaly"}


@RecipeRegistry.register
class ClimatologyRecipe(BaseRecipe):
    type = "climatology"
    version = "1"

    # ---- candidate generation ----------------------------------------------

    def enumerate_units(self, selector) -> Iterable[ProductionUnit]:
        selector = selector or {}
        source = selector["source_collection"]
        variable = selector.get("variable")
        periods = selector["periods"]
        seasons = selector.get("seasons", ["annual"])
        quantities = selector.get("quantities", ["value"])
        baselines = selector.get("baselines", [])

        for period in periods:
            for season in seasons:
                for quantity in quantities:
                    if quantity in _ANOMALY_QUANTITIES:
                        for baseline in baselines:
                            yield self._make_unit(
                                source, variable, period, season, quantity, baseline
                            )
                    else:
                        yield self._make_unit(
                            source, variable, period, season, quantity, None
                        )

    @staticmethod
    def _make_unit(source, variable, period, season, quantity, baseline) -> ProductionUnit:
        return {
            "source_collection": source, "variable": variable,
            "period": period, "season": season,
            "quantity": quantity, "baseline": baseline,
        }

    # ---- input resolution ---------------------------------------------------

    def resolve_inputs(self, unit: ProductionUnit) -> "dict[str, ResolvedInput]":
        items = self._staging_items(unit)
        assets = [a for si in items for a in si.assets.all()]
        resolved = {
            "value": ResolvedInput("value", required=True, items=items, assets=assets),
        }
        if unit.get("baseline"):
            # The baseline window is read from the same source series; it is a
            # distinct, required selector for the anomaly quantities.
            resolved["baseline"] = ResolvedInput(
                "baseline", required=True, items=items, assets=assets
            )
        return resolved

    # ---- outputs mapping ----------------------------------------------------

    def outputs(self, unit: ProductionUnit) -> OutputItem:
        si = self._staging_items(unit)[0]
        collection = self._published_collection(unit, si.collection.catalog)
        start = unit["period"][0]
        return OutputItem(
            collection=collection,
            time=datetime(start, 1, 1, tzinfo=timezone.utc),
            bounds=si.bounds, crs=si.crs, width=si.width, height=si.height,
            properties={"climatology": {
                "season": unit["season"], "quantity": unit["quantity"],
                "period": unit["period"], "baseline": unit.get("baseline"),
            }},
        )

    # ---- transform ----------------------------------------------------------

    def transform(self, unit: ProductionUnit, resolved) -> "list[OutputAsset]":
        season = unit["season"]
        quantity = unit["quantity"]

        series = self.read_series(resolved["value"].assets)
        value_window = self._slice_years(series, unit["period"])

        if quantity == "value":
            result = climatology(value_window, season=season)
        elif quantity == "trend":
            result = trend(value_window, season=season)
        elif quantity in _ANOMALY_QUANTITIES:
            baseline_series = self.read_series(resolved["baseline"].assets)
            baseline_window = self._slice_years(baseline_series, unit["baseline"])
            value = climatology(value_window, season=season)
            base = climatology(baseline_window, season=season)
            result = anomaly(value, base, relative=(quantity == "relative_anomaly"))
        else:
            raise ValueError(f"unknown quantity: {quantity!r}")

        si = resolved["value"].items[0]
        out_var = self._output_variable(unit, resolved)
        # The AssetWriter writes a 2D numpy array; the quantity ops return a
        # (y, x) DataArray once time is reduced out.
        import numpy as np
        array = np.asarray(result, dtype="float32")
        return [OutputAsset(
            variable=out_var, roles=["data"], format="cog",
            array=array, bounds=si.bounds, crs=si.crs,
            width=si.width, height=si.height,
            stats=self._array_stats(array),
        )]

    @staticmethod
    def _array_stats(array) -> dict:
        """NaN-aware min/max/mean/std for the derived raster (empty if all-NaN)."""
        import numpy as np

        if not np.isfinite(array).any():
            return {}
        return {
            "min": float(np.nanmin(array)),
            "max": float(np.nanmax(array)),
            "mean": float(np.nanmean(array)),
            "std": float(np.nanstd(array)),
        }

    # ---- I/O seam (mocked in tests) ----------------------------------------

    def read_series(self, assets):
        """
        Open the staging asset(s) into an xarray DataArray with a ``time`` dim.

        This is the only real I/O in the recipe and the recipe's single seam:
        unit tests patch it to return an in-memory array, so the declarative
        pieces and the transform math stay storage-free. Multiple assets are
        concatenated along time.

        The byte → xarray step is integration-level (format/engine handling is
        exercised end-to-end, not in unit tests).
        """
        import tempfile

        import xarray as xr

        from georiva.core.storage import BucketType, storage

        if not assets:
            raise ValueError("ClimatologyRecipe: no source assets to read")

        arrays = []
        for asset in assets:
            data = storage.bucket(BucketType.STAGING).read_bytes(asset.href)
            with tempfile.NamedTemporaryFile(suffix=".nc") as fh:
                fh.write(data)
                fh.flush()
                ds = xr.open_dataset(fh.name)
            arrays.append(self._pick_variable(ds, asset))

        if len(arrays) == 1:
            return arrays[0]
        return xr.concat(arrays, dim="time").sortby("time")

    @staticmethod
    def _pick_variable(ds, asset):
        """Select the asset's variable from a Dataset, or the sole data var."""
        if asset.variable and asset.variable.slug in ds.data_vars:
            return ds[asset.variable.slug]
        data_vars = list(ds.data_vars)
        if len(data_vars) != 1:
            raise ValueError(
                f"ClimatologyRecipe: cannot pick a variable from {data_vars} "
                f"for asset {asset.pk}"
            )
        return ds[data_vars[0]]

    # ---- helpers ------------------------------------------------------------

    @staticmethod
    def _slice_years(da, window, time_dim: str = "time"):
        """Restrict a series to calendar years in ``[start, end]`` (inclusive),
        using the year from the file's own time axis (authoritative, any calendar)."""
        start, end = window
        years = da[time_dim].dt.year
        return da.sel({time_dim: (years >= start) & (years <= end)})

    @staticmethod
    def _collection_slug(unit: ProductionUnit) -> str:
        season = unit["season"] or "annual"
        parts = [unit["source_collection"], season.lower(), unit["quantity"]]
        if unit.get("baseline"):
            b = unit["baseline"]
            parts.append(f"{b[0]}-{b[1]}")
        return "_".join(parts)

    def _published_collection(self, unit, catalog):
        from georiva.core.models import Collection

        slug = self._collection_slug(unit)
        collection, _ = Collection.objects.get_or_create(
            catalog=catalog, slug=slug, defaults={"name": slug},
        )
        return collection

    @staticmethod
    def _staging_items(unit) -> list:
        from georiva.staging.models import StagingItem

        return list(
            StagingItem.objects
            .filter(collection__slug=unit["source_collection"])
            .select_related("collection__catalog")
            .prefetch_related("assets")
        )

    def _output_variable(self, unit, resolved):
        """
        Derive the output Variable from the source, adjusting metadata to the
        quantity: ``value`` mirrors the source; ``anomaly``/``trend`` keep the
        source unit but use a symmetric range around zero; ``relative_anomaly``
        is dimensionless on [-1, 1]. The recipe creates these in the per-quantity
        output collection (one Variable per collection).
        """
        from georiva.core.models import Variable

        src = next(
            (a.variable for a in resolved["value"].assets if a.variable), None
        )
        if src is None:
            raise ValueError(
                "ClimatologyRecipe: source staging asset has no Variable to mirror"
            )
        si = resolved["value"].items[0]
        collection = self._published_collection(unit, si.collection.catalog)
        spec = self._variable_spec(src, unit["quantity"])
        out_var, _ = Variable.objects.get_or_create(
            collection=collection, slug=src.slug, defaults=spec,
        )
        return out_var

    def _variable_spec(self, src, quantity: str) -> dict:
        """Quantity-specific (name, unit, value_min, value_max) for the output."""
        span = (src.value_max - src.value_min) / 2.0
        if quantity == "value":
            return {"name": src.name, "unit": src.unit,
                    "value_min": src.value_min, "value_max": src.value_max}
        if quantity == "anomaly":
            return {"name": f"{src.name} anomaly", "unit": src.unit,
                    "value_min": -span, "value_max": span}
        if quantity == "relative_anomaly":
            return {"name": f"{src.name} relative anomaly",
                    "unit": self._dimensionless_unit(),
                    "value_min": -1.0, "value_max": 1.0}
        if quantity == "trend":
            return {"name": f"{src.name} trend (per year)", "unit": src.unit,
                    "value_min": -span, "value_max": span}
        raise ValueError(f"unknown quantity: {quantity!r}")

    @staticmethod
    def _dimensionless_unit():
        from georiva.core.models import Unit

        unit, _ = Unit.objects.get_or_create(
            symbol="dimensionless", defaults={"name": "Dimensionless"},
        )
        return unit
