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

from collections.abc import Iterable
from datetime import UTC, datetime

from georiva.core.unit_utils import difference_unit
from georiva.geoprocessing import anomaly, climatology, trend
from georiva.processing.recipe import (
    SOURCE_UNIT,
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
    # A mean, a difference and a slope hold in any unit, so the series is read as
    # it is held and the result written down in that unit (or its difference
    # unit): what an input is served in never reaches an archive (ADR 0046 §13).
    input_units = {"value": SOURCE_UNIT, "baseline": SOURCE_UNIT}

    # ---- candidate generation ----------------------------------------------

    def candidate_units(self, trigger) -> Iterable[ProductionUnit]:
        """
        Climatology is scheduled/manual, not event-driven: the product space
        (period × season × quantity × baseline) is not derivable from a bare
        arriving input. So a trigger lacking the explicit period config yields
        nothing — this recipe is invoked over an explicit selector instead.
        """
        trigger = trigger or {}
        if not (trigger.get("source_collection") and trigger.get("periods")):
            return []
        return self.enumerate_units(trigger)

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
                            yield self._make_unit(source, variable, period, season, quantity, baseline)
                    else:
                        yield self._make_unit(source, variable, period, season, quantity, None)

    @staticmethod
    def _make_unit(source, variable, period, season, quantity, baseline) -> ProductionUnit:
        return {
            "source_collection": source,
            "variable": variable,
            "period": period,
            "season": season,
            "quantity": quantity,
            "baseline": baseline,
        }

    # ---- input resolution ---------------------------------------------------

    def resolve_inputs(self, unit: ProductionUnit) -> dict[str, ResolvedInput]:
        items = self._staging_items(unit)
        assets = [a for si in items for a in si.assets.all()]
        resolved = {
            "value": ResolvedInput("value", required=True, items=items, assets=assets),
        }
        if unit.get("baseline"):
            # The baseline window is read from the same source series; it is a
            # distinct, required selector for the anomaly quantities.
            resolved["baseline"] = ResolvedInput("baseline", required=True, items=items, assets=assets)
        return resolved

    # ---- outputs mapping ----------------------------------------------------

    def outputs(self, unit: ProductionUnit) -> OutputItem:
        si = self._staging_items(unit)[0]
        collection = self._published_collection(unit, si.collection.catalog)
        start = unit["period"][0]
        return OutputItem(
            collection=collection,
            time=datetime(start, 1, 1, tzinfo=UTC),
            bounds=si.bounds,
            crs=si.crs,
            width=si.width,
            height=si.height,
            properties={
                "climatology": {
                    "season": unit["season"],
                    "quantity": unit["quantity"],
                    "period": unit["period"],
                    "baseline": unit.get("baseline"),
                }
            },
        )

    # ---- transform ----------------------------------------------------------

    def transform(self, unit: ProductionUnit, resolved) -> list[OutputAsset]:
        season = unit["season"]
        quantity = unit["quantity"]

        series = self.read_series(resolved["value"])
        value_window = self._slice_years(series, unit["period"])

        if quantity == "value":
            result = climatology(value_window, season=season)
        elif quantity == "trend":
            result = trend(value_window, season=season)
        elif quantity in _ANOMALY_QUANTITIES:
            baseline_series = self.read_series(resolved["baseline"])
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
        return [
            OutputAsset(
                variable=out_var,
                roles=["data"],
                format="cog",
                array=array,
                bounds=si.bounds,
                crs=si.crs,
                width=si.width,
                height=si.height,
                stats=self._array_stats(array),
            )
        ]

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

    def read_series(self, source: ResolvedInput):
        """
        The input's staged series as an xarray DataArray with a ``time`` dim,
        read in the input's declared unit through the production unit's
        reading session (``ResolvedInput.series``). Multiple assets are
        concatenated along time.

        This is the recipe's only real I/O and its single seam: unit tests patch
        it to return an in-memory array, so the declarative pieces and the
        transform math stay storage-free.
        """
        import xarray as xr

        if not source.assets:
            raise ValueError("ClimatologyRecipe: no source assets to read")

        arrays = [source.series(asset) for asset in source.assets]
        if len(arrays) == 1:
            return arrays[0]
        return xr.concat(arrays, dim="time").sortby("time")

    def output_unit(self, unit: ProductionUnit, resolved) -> str:
        """The series' source unit for a ``value``; its difference unit for an
        ``anomaly`` and a ``trend`` (a change per year), so they are scaled and
        never offset (ADR 0046 §7); ``dimensionless`` for a ``relative_anomaly``."""
        quantity = unit["quantity"]
        if quantity == "relative_anomaly":
            return "dimensionless"
        source = self._source_variable(resolved).source_unit.symbol
        if quantity == "value":
            return source
        if quantity in ("anomaly", "trend"):
            return difference_unit(source)
        raise ValueError(f"unknown quantity: {quantity!r}")

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
            catalog=catalog,
            slug=slug,
            defaults={"name": slug, "derived_by_recipe": self.type},
        )
        return collection

    @staticmethod
    def _staging_items(unit) -> list:
        from georiva.staging.models import StagingItem

        return list(
            StagingItem.objects.filter(collection__slug=unit["source_collection"])
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

        src = self._source_variable(resolved)
        si = resolved["value"].items[0]
        collection = self._published_collection(unit, si.collection.catalog)
        spec = self._variable_spec(src, unit["quantity"])
        # Its values are in the declared output unit, and are served in it too
        # until an operator chooses another.
        spec["source_unit"] = spec["unit"] = self.output_source_unit(unit, resolved)
        out_var, _ = Variable.objects.get_or_create(
            collection=collection,
            slug=src.slug,
            defaults=spec,
        )
        return out_var

    @staticmethod
    def _source_variable(resolved):
        src = next((a.variable for a in resolved["value"].assets if a.variable), None)
        if src is None:
            raise ValueError("ClimatologyRecipe: source staging asset has no Variable to mirror")
        return src

    @staticmethod
    def _variable_spec(src, quantity: str) -> dict:
        """Quantity-specific (name, value_min, value_max) for the output, in the
        source unit like every stored number (ADR 0046 §3)."""
        span = (src.value_max - src.value_min) / 2.0
        if quantity == "value":
            return {"name": src.name, "value_min": src.value_min, "value_max": src.value_max}
        if quantity == "anomaly":
            return {"name": f"{src.name} anomaly", "value_min": -span, "value_max": span}
        if quantity == "relative_anomaly":
            return {"name": f"{src.name} relative anomaly", "value_min": -1.0, "value_max": 1.0}
        if quantity == "trend":
            return {"name": f"{src.name} trend (per year)", "value_min": -span, "value_max": span}
        raise ValueError(f"unknown quantity: {quantity!r}")
