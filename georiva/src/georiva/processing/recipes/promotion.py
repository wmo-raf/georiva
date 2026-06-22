"""
Promotion — the identity derivation.

Turns a ready StagingItem into a Published Item with no real transform (the
degenerate base case proving the engine handles the 1:1 path). The source
artifact is copied into the assets bucket and registered as a Published Asset,
with a DerivationLink back to the staging input.

The output Published Collection mirrors the staging collection (same catalog,
same slug). The asset's Variable is the staging asset's variable if set, else
the target collection's first Variable.
"""
from __future__ import annotations

from typing import Iterable

from georiva.processing.recipe import (
    BaseRecipe,
    OutputAsset,
    OutputItem,
    ProductionUnit,
    ResolvedInput,
)
from georiva.processing.registry import RecipeRegistry


@RecipeRegistry.register
class PromotionRecipe(BaseRecipe):
    type = "promotion"
    version = "1"

    # ---- declarative surface ------------------------------------------------

    def enumerate_units(self, selector) -> Iterable[ProductionUnit]:
        from georiva.staging.models import StagingItem

        selector = selector or {}
        qs = StagingItem.objects.all()
        if selector.get("staging_item_ids"):
            qs = qs.filter(pk__in=selector["staging_item_ids"])
        if selector.get("collection_slug"):
            qs = qs.filter(collection__slug=selector["collection_slug"])
        for sid in qs.values_list("pk", flat=True):
            yield {"staging_item_id": sid}

    def resolve_inputs(self, unit: ProductionUnit) -> "dict[str, ResolvedInput]":
        from georiva.staging.models import StagingItem

        si = (
            StagingItem.objects
            .filter(pk=unit["staging_item_id"])
            .select_related("collection__catalog")
            .first()
        )
        items = [si] if si else []
        assets = list(si.assets.all()) if si else []
        return {"source": ResolvedInput("source", required=True, items=items, assets=assets)}

    def outputs(self, unit: ProductionUnit) -> OutputItem:
        from georiva.staging.models import StagingItem

        si = (
            StagingItem.objects
            .select_related("collection__catalog")
            .get(pk=unit["staging_item_id"])
        )
        collection = self._published_collection(si)
        time = si.datetime or si.start_datetime or si.end_datetime
        return OutputItem(
            collection=collection,
            time=time,
            reference_time=si.reference_time,
            bounds=si.bounds,
            crs=si.crs,
            width=si.width,
            height=si.height,
        )

    def transform(self, unit: ProductionUnit, resolved) -> "list[OutputAsset]":
        from georiva.core.storage import BucketType

        ri = resolved["source"]
        si = ri.items[0]
        collection = self._published_collection(si)

        out = []
        for asset in ri.assets:
            variable = asset.variable or self._fallback_variable(collection)
            if variable is None:
                raise ValueError(
                    f"Promotion: no Variable for staging asset {asset.pk}; "
                    f"set one on the asset or on collection '{collection.slug}'"
                )
            out.append(OutputAsset(
                variable=variable,
                roles=["data"],
                format=asset.format or "geotiff",
                passthrough=(BucketType.STAGING, asset.href),
                bounds=si.bounds,
                crs=si.crs,
                width=si.width,
                height=si.height,
                checksum=asset.checksum,
            ))
        return out

    # ---- helpers ------------------------------------------------------------

    @staticmethod
    def _published_collection(staging_item):
        from georiva.core.models import Collection

        sc = staging_item.collection
        collection, _ = Collection.objects.get_or_create(
            catalog=sc.catalog,
            slug=sc.slug,
            defaults={"name": sc.name or sc.slug},
        )
        return collection

    @staticmethod
    def _fallback_variable(collection):
        from georiva.core.models import Variable
        return Variable.objects.filter(collection=collection).first()
