"""
AssetMaterializer — the shared "array in, served assets out" step.

Both pipelines that publish raster assets end at the same materialization
sequence: normalize bounds → apply the catalog boundary mask → write COG →
upsert Asset rows → expand the owning Collection's extent.
Ingestion (``handlers/asset_handler.py``) and the derivation engine
(``processing/engine.py``) both call this class, so derived items can no
longer drift from ingested ones — the drift is what left derived collections
extent-less.

Visual textures are not materialized here or anywhere: they are derived on
demand by Titiler's ``encoded-preview.png`` from the COG and the variable's
*current* render range (ADR 0021), so nothing render-config-dependent is ever
stored.

Extraction stays per-flow: ingestion reads windowed chunks from source files,
recipes compute arrays. This service only owns what happens after an array,
its bounds, and its Variable exist.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import numpy as np
from django.db import transaction
from wagtail import hooks

from georiva.core.models import Asset, Item
from georiva.core.storage import storage
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.ingestion.utils import compute_stats, normalize_bounds

if TYPE_CHECKING:
    from georiva.core.models import Variable
    from georiva.ingestion.clipper import BoundaryClipper

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WrittenCog:
    """A COG ``AssetMaterializer.write_cog`` has put in storage, and what its row will say of it."""

    href: str
    width: int
    height: int
    bounds: list
    stats: dict


class AssetMaterializer:
    """
    Persist one variable's raster array as its served COG asset and keep
    the catalog metadata honest.

    COG failure raises — the item is unservable without it.
    """

    def __init__(self, writer: AssetWriter):
        # Late import: handlers/__init__ imports asset_handler, which imports
        # this module — importing the subpackage at module level would cycle.
        from georiva.ingestion.handlers.extent_handler import CollectionExtentHandler

        self.writer = writer
        self.extent_handler = CollectionExtentHandler()

    # =========================================================================
    # Public entry points
    # =========================================================================

    def clip_array(
        self,
        data: np.ndarray,
        bounds: list | tuple,
        clipper: "BoundaryClipper",
    ) -> tuple[np.ndarray, list | tuple]:
        """
        Crop a full-grid array to the clipper's pixel-snapped window.

        Used by callers whose arrays cover the whole source grid (the
        derivation engine); ingestion crops at read time instead. A window
        that cannot be computed (no intersection) keeps the full grid.
        """
        if clipper is None or not clipper.is_active:
            return data, bounds
        height, width = data.shape[:2]
        try:
            window = clipper.compute_window(tuple(bounds), width, height)
        except ValueError as e:
            logger.warning("Clip window computation failed: %s — keeping full grid", e)
            return data, bounds
        if not window:
            return data, bounds
        y0, x0 = window["y_off"], window["x_off"]
        cropped = data[y0 : y0 + window["height"], x0 : x0 + window["width"]]
        return cropped, window["bounds"]

    def materialize_variable(
        self,
        *,
        item: Item,
        variable: "Variable",
        data: np.ndarray,
        bounds: list | tuple,
        crs: str,
        timestamp: datetime,
        clipper: Optional["BoundaryClipper"] = None,
        stats: dict | None = None,
        checksum: str = "",
    ) -> list[Asset]:
        """
        Run the shared materialization sequence for one variable's array:
        ``write_cog`` then ``record_cog``.

        ``clipper`` applies the precise boundary geometry mask — window
        cropping is the caller's job (``clip_array`` for full-grid arrays).
        """
        written = self.write_cog(
            item=item,
            variable=variable,
            data=data,
            bounds=bounds,
            crs=crs,
            timestamp=timestamp,
            clipper=clipper,
            stats=stats,
        )
        return self.record_cog(item=item, variable=variable, written=written, timestamp=timestamp, checksum=checksum)

    def write_cog(
        self,
        *,
        item: Item,
        variable: "Variable",
        data: np.ndarray,
        bounds: list | tuple,
        crs: str,
        timestamp: datetime,
        clipper: Optional["BoundaryClipper"] = None,
        stats: dict | None = None,
    ) -> "WrittenCog":
        """
        The storage half: mask the array, compute its statistics and write
        its COG to storage. Touches no row, so a caller may do it before
        taking a lock and record it after (the harvest's materialization does).
        """
        bounds = normalize_bounds(bounds)

        if clipper is not None and clipper.is_active:
            data = clipper.apply_geometry_mask(data, bounds, nodata=np.nan)

        if stats is None:
            stats = compute_stats(data)

        height, width = data.shape[:2]
        path = self.cog_path(item, variable, timestamp)
        try:
            stored = self.writer.write_cog(data, path, tuple(bounds), crs)
        except Exception as e:
            logger.error("COG save failed for %s: %s", variable.slug, e)
            raise
        return WrittenCog(href=stored, width=width, height=height, bounds=list(bounds), stats=stats)

    def record_cog(
        self,
        *,
        item: Item,
        variable: "Variable",
        written: "WrittenCog",
        timestamp: datetime,
        checksum: str = "",
    ) -> list[Asset]:
        """
        The database half: upsert the Asset row for a COG ``write_cog`` stored,
        run the after-save hooks and expand the owning Collection's extent.
        """
        stats = written.stats
        cog_defaults = {
            "href": written.href,
            "media_type": ("image/tiff; application=geotiff; profile=cloud-optimized"),
            "roles": ["data"],
            "file_size": self._get_file_size(written.href),
            "width": written.width,
            "height": written.height,
            "bands": 1,
            "stats_min": stats.get("min"),
            "stats_max": stats.get("max"),
            "stats_mean": stats.get("mean"),
            "stats_std": stats.get("std"),
            "extra_fields": {
                "compression": "deflate",
                "nodata": None,
            },
        }
        # Only stamp a checksum the caller actually supplied (derived
        # provenance) — never clobber an existing one with "".
        if checksum:
            cog_defaults["checksum"] = checksum
        try:
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults=cog_defaults,
            )
        except Exception as e:
            logger.error("COG save failed for %s: %s", variable.slug, e)
            raise
        self._after_save_asset(data_asset)

        self.extent_handler.expand(item.collection, timestamp, written.bounds)
        return [data_asset]

    def cog_path(self, item: Item, variable: "Variable", timestamp: datetime) -> str:
        """The key in the assets bucket a Variable's COG at ``timestamp`` on ``item`` is written to."""
        catalog = item.collection.catalog

        if item.reference_time:
            ref_str = item.reference_time.strftime("%Y%m%dT%H%M%S")
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}__ref{ref_str}"
        else:
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}"

        base_dir = storage.build_asset_path(
            org=catalog.organisation.slug,
            catalog=catalog.slug,
            collection=item.collection.slug,
            variable=variable.slug,
            timestamp=timestamp,
            filename="",
        ).rstrip("/")
        return f"{base_dir}/{base_name}.tif"

    # =========================================================================
    # Helpers
    # =========================================================================

    def _after_save_asset(self, asset: Asset) -> None:
        """Run every ``GEORIVA_AFTER_SAVE_ASSET`` hook (#715). Each is
        bookkeeping nothing waits on, so one that fails is logged and neither
        fails the asset nor keeps the hooks after it from running. Each runs in
        a savepoint: a derivation materializes inside a transaction, and a hook
        whose query fails would otherwise abort it for everything after."""
        for fn in hooks.get_hooks(GEORIVA_AFTER_SAVE_ASSET):
            try:
                with transaction.atomic():
                    fn(asset)
            except Exception as e:
                hook = getattr(fn, "__qualname__", fn)
                logger.warning("Post-save hook %s failed for asset %s: %s", hook, asset.pk, e)

    def _get_file_size(self, path: str) -> int | None:
        try:
            return int(self.writer.bucket.size(path))
        except Exception:
            return None
