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


def _array_stats(data) -> dict:
    """min/max/mean/std over the finite pixels of a raster band."""
    import numpy as np

    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return {}
    return {
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
        "mean": float(np.mean(finite)),
        "std": float(np.std(finite)),
    }


@RecipeRegistry.register
class PromotionRecipe(BaseRecipe):
    type = "promotion"
    # v2: promotion now materialises a COG + a visual PNG (was a raw-GeoTIFF
    # passthrough), so the served item is tile-servable and shows in the catalog.
    # Bumping the version re-derives already-promoted items on the next sweep.
    version = "2"

    # ---- declarative surface ------------------------------------------------

    def enumerate_units(self, selector) -> Iterable[ProductionUnit]:
        from georiva.processing.recipe import binding_input_collection_id
        from georiva.staging.models import StagingItem

        selector = selector or {}
        qs = StagingItem.objects.all()
        if selector.get("staging_item_ids"):
            qs = qs.filter(pk__in=selector["staging_item_ids"])
        # Prefer the pinned collection FK (catalog-scoped, rename-safe); fall
        # back to a slug filter for the CLI/backfill convenience path.
        collection_id = binding_input_collection_id(selector, "staging")
        if collection_id is not None:
            qs = qs.filter(collection__collection_id=collection_id)
        elif selector.get("collection_slug"):
            qs = qs.filter(collection__slug=selector["collection_slug"])
        for sid in qs.values_list("pk", flat=True):
            yield {"staging_item_id": sid}

    def candidate_units(self, trigger) -> Iterable[ProductionUnit]:
        """
        Map an arriving input back to the unit(s) it feeds.

        An event trigger carries a single ``staging_item_id`` → its 1:1
        promotion unit. Anything else is treated as a (possibly wide) selector
        and enumerated normally, so scheduled/backfill/manual stay unchanged.
        """
        trigger = trigger or {}
        if "staging_item_id" in trigger:
            return [{"staging_item_id": trigger["staging_item_id"]}]
        if "published_item_id" in trigger:
            # Promotion consumes Staging inputs only — ignore Published-item
            # (completion-chaining) triggers rather than mis-reading their
            # collection_slug as a staging filter.
            return []
        return self.enumerate_units(trigger)

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
            data, bounds, crs, width, height = self.read_raster(
                BucketType.STAGING, asset.href
            )
            stats = _array_stats(data)
            # A served COG (data) + a visual PNG (encoded by the engine), the
            # same pair ingestion writes — so promotion output is tile-servable
            # and shows in the public catalog (which is COG-gated).
            out.append(OutputAsset(
                variable=variable, roles=["data"], format="cog", array=data,
                bounds=bounds, crs=crs, width=width, height=height,
                stats=stats, checksum=asset.checksum,
            ))
            out.append(OutputAsset(
                variable=variable, roles=["visual"], format="png", array=data,
                bounds=bounds, crs=crs, width=width, height=height,
            ))
        return out

    # ---- I/O seam (mocked in tests) -----------------------------------------

    def read_raster(self, bucket_type, href):
        """Read a stored single-band raster into
        ``(data, bounds, crs, width, height)`` — the recipe's only real I/O,
        patched in unit tests. Nodata is mapped to NaN so the COG stats and the
        PNG alpha both skip it."""
        import numpy as np
        import rasterio
        from rasterio.io import MemoryFile

        from georiva.core.storage import storage

        raw = storage.bucket(bucket_type).read_bytes(href)
        with MemoryFile(raw) as memfile, memfile.open() as src:
            data = src.read(1).astype("float32")
            if src.nodata is not None:
                data = np.where(data == src.nodata, np.nan, data)
            bounds = list(src.bounds)
            crs = src.crs.to_string() if src.crs else "EPSG:4326"
            return data, bounds, crs, src.width, src.height

    # ---- helpers ------------------------------------------------------------

    @staticmethod
    def _published_collection(staging_item):
        """The served core Collection this staging item promotes into. Resolved
        by the StagingCollection -> core Collection FK (ADR-0010 §3/§5) when
        linked, so an operator slug rename never misroutes it or spawns a
        duplicate; falls back to a catalog+slug get-or-create for a staging
        collection registered before the link existed."""
        from georiva.core.models import Collection

        sc = staging_item.collection
        if sc.collection_id is not None:
            return sc.collection
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
