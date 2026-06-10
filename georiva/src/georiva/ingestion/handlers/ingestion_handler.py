"""
IngestionHandler — orchestrates one (collection × timestamp) ingestion unit.

Can be called directly from management commands or tests to reprocess
a single timestamp without running the full pipeline.
"""
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from georiva.core.models import Collection, Item, Asset
from georiva.ingestion.handlers.asset_handler import AssetHandler
from georiva.ingestion.handlers.context import IngestionContext
from georiva.ingestion.handlers.extent_handler import CollectionExtentHandler
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.ingestion.utils import ensure_utc, normalize_bounds

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class IngestionHandler:
    """
    Processes all variables for a single (collection, timestamp) pair.

    Usage::

        handler = IngestionHandler(ctx)
        item, assets, clip_info, failed_vars = handler.process_timestamp(
            collection=collection,
            local_path=local_path,
            timestamp=ts,
            source_file="sources:ecmwf/2025/01/01/file.grib",
        )
    """
    
    def __init__(self, ctx: IngestionContext):
        self.ctx = ctx
        self.item_handler = ItemHandler()
        self.asset_handler = AssetHandler(ctx.writer, ctx.extractor, ctx.encoder)
        self.extent_handler = CollectionExtentHandler()
    
    # =========================================================================
    # Public entry point
    # =========================================================================
    
    def process_timestamp(
            self,
            *,
            collection: Collection,
            local_path: Path,
            timestamp: datetime,
            source_file: str,
            progress=None,
    ) -> tuple[Optional[Item], list[Asset], dict, list[str]]:
        """
        Process all Variables for *collection* at *timestamp*.

        Returns:
            (item, assets, clip_info, failed_variable_slugs)

        item is None if every variable failed (orphan Item is deleted).
        clip_info holds original_size / clipped_size for the first variable.
        """
        logger.info("Processing %s @ %s", collection, timestamp)
        
        variables = [v for v in collection.variables.all() if v.is_active]
        if not variables:
            raise ValueError(
                f"Collection '{collection.slug}' has no active variables"
            )
        
        ctx = self.ctx
        clipper = ctx.clipper
        
        # ── Spatial metadata from first variable ──────────────────────────────
        first_var = variables[0]
        meta = ctx.extractor.get_metadata(first_var, local_path, timestamp)
        src_width, src_height = meta["width"], meta["height"]
        src_bounds = tuple(meta["bounds"])
        
        if not src_bounds or len(src_bounds) < 4:
            raise ValueError(f"Invalid bounds from metadata: {src_bounds}")
        
        clip_info = {"original_size": (src_width, src_height), "clipped_size": None}
        clip_window = None
        
        # ── Clip window ───────────────────────────────────────────────────────
        if clipper.is_active:
            try:
                clip_window = clipper.compute_window(src_bounds, src_width, src_height)
                if clip_window:
                    width = clip_window["width"]
                    height = clip_window["height"]
                    bounds = clip_window["bounds"]
                    clip_info["clipped_size"] = (width, height)
                    reduction = 100 * (1 - (width * height) / (src_width * src_height))
                    logger.info(
                        "Clipping: %dx%d → %dx%d (%.1f%% reduction)",
                        src_width, src_height, width, height, reduction,
                    )
                else:
                    width, height, bounds = src_width, src_height, src_bounds
            except ValueError as e:
                logger.warning(
                    "Clip window computation failed: %s — using full extent", e
                )
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds
        
        crs = meta.get("crs", collection.crs or "EPSG:4326")
        ts_utc = ensure_utc(timestamp)
        bounds = normalize_bounds(bounds)
        
        # ── Item ──────────────────────────────────────────────────────────────
        item, created = self.item_handler.get_or_create(
            collection=collection,
            timestamp=ts_utc,
            reference_time=ctx.reference_time,
            source_file=source_file,
            ingestion_log=ctx.ingestion_log,
            bounds=bounds,
            width=width,
            height=height,
            crs=crs,
        )
        
        # ── Per-variable asset processing ─────────────────────────────────────
        assets: list[Asset] = []
        failed_variables: list[str] = []
        
        for variable in variables:
            try:
                variable_assets = self.asset_handler.process_variable(
                    item=item,
                    variable=variable,
                    local_path=local_path,
                    timestamp=ts_utc,
                    bounds=bounds,
                    crs=crs,
                    width=width,
                    height=height,
                    clipper=clipper,
                    clip_window=clip_window,
                )
                assets.extend(variable_assets)
                if progress is not None:
                    progress.increment(state=f"{variable.slug}: succeeded")
            except Exception as e:
                logger.error(
                    "Variable %s failed: %s\n%s",
                    variable.slug, e, traceback.format_exc(),
                )
                failed_variables.append(variable.slug)
                if progress is not None:
                    progress.increment(state=f"{variable.slug}: failed — {e}")
        
        # ── Extent update ─────────────────────────────────────────────────────
        self.extent_handler.expand(collection, ts_utc, bounds)
        
        # ── Orphan guard ──────────────────────────────────────────────────────
        if not assets:
            self.item_handler.delete_orphan(item)
            return None, [], clip_info, failed_variables
        
        if created:
            self.item_handler.increment_collection_item_count(collection)
        
        logger.info("Created Item %s with %d asset(s)", item.pk, len(assets))
        return item, assets, clip_info, failed_variables
