"""
IngestionHandler — orchestrates one (collection × timestamp) ingestion unit.

Can be called directly from management commands or tests to reprocess
a single timestamp without running the full pipeline.
"""

import logging
import traceback
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from django.utils import timezone

from georiva.core import retention
from georiva.core.models import Asset, Collection, Item
from georiva.ingestion.handlers.asset_handler import AssetHandler
from georiva.ingestion.handlers.context import IngestionContext
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.ingestion.materialization import ItemGone
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
        self.asset_handler = AssetHandler(ctx.writer, ctx.extractor)

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
    ) -> tuple[Item | None, list[Asset], dict, list[str]]:
        """
        Process all Variables for *collection* at *timestamp*.

        Returns:
            (item, assets, clip_info, failed_variables)

        item is None if every variable failed (orphan Item is deleted), or if
        retention (or anyone, with this file's assets in it) deleted the Item
        while its variables were being written. An Item a sibling file deleted
        as empty before this one recorded anything is made again (#769).
        clip_info holds original_size / clipped_size for the first variable.
        failed_variables holds one "<slug>: <reason>" entry per failed variable,
        so the reason reaches the ingestion's failure record.
        """
        logger.info("Processing %s @ %s", collection, timestamp)

        variables = [v for v in collection.variables.all() if v.is_active]
        if not variables:
            raise ValueError(f"Collection '{collection.slug}' has no active variables")

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
                        src_width,
                        src_height,
                        width,
                        height,
                        reduction,
                    )
                else:
                    width, height, bounds = src_width, src_height, src_bounds
            except ValueError as e:
                logger.warning("Clip window computation failed: %s — using full extent", e)
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds

        crs = meta.get("crs", collection.crs or "EPSG:4326")
        ts_utc = ensure_utc(timestamp)
        bounds = normalize_bounds(bounds)

        # ── Item ──────────────────────────────────────────────────────────────
        make_item = partial(
            self.item_handler.get_or_create,
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
        item, _ = make_item()

        # ── Per-variable asset processing ─────────────────────────────────────
        assets: list[Asset] = []
        failed_variables: list[str] = []

        for variable in variables:
            process = partial(
                self.asset_handler.process_variable,
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
            try:
                try:
                    variable_assets = process(item=item)
                except ItemGone:
                    if not self._item_may_be_made_again(collection, ts_utc, assets):
                        raise
                    # A sibling file for this timestamp deleted it as empty
                    # before this one recorded anything in it (#769). Made
                    # again once: a second ItemGone ends the timestamp.
                    logger.info("Item %s of %s @ %s is gone — making it again", item.pk, collection, ts_utc)
                    item, _ = make_item()
                    variable_assets = process(item=item)
                assets.extend(variable_assets)
                if progress is not None:
                    progress.increment(state=f"{variable.slug}: succeeded")
            except ItemGone:
                # Retention removed the Item, or someone removed it with this
                # file's assets in it (#704): nothing is left to add to, and
                # the timestamp ends here, failing nothing.
                logger.info("Item %s of %s @ %s is gone — stopping this timestamp", item.pk, collection, ts_utc)
                return None, [], clip_info, failed_variables
            except Exception as e:
                logger.error(
                    "Variable %s failed: %s\n%s",
                    variable.slug,
                    e,
                    traceback.format_exc(),
                )
                failed_variables.append(f"{variable.slug}: {e}")
                if progress is not None:
                    progress.increment(state=f"{variable.slug}: failed — {e}")

        # Extent expansion happens per successful variable inside the shared
        # AssetMaterializer, so an all-failed run no longer widens the extent.

        # ── Orphan guard ──────────────────────────────────────────────────────
        if not assets:
            self.item_handler.delete_orphan(item)
            return None, [], clip_info, failed_variables

        logger.info("Created Item %s with %d asset(s)", item.pk, len(assets))
        return item, assets, clip_info, failed_variables

    def _item_may_be_made_again(self, collection: Collection, timestamp: datetime, assets: list[Asset]) -> bool:
        """Whether an Item found gone mid-timestamp is made again: only while
        this file has recorded nothing in it — ``delete_orphan`` deletes only
        an empty Item — and retention does not remove the timestamp."""
        if assets:
            return False
        # ``retires`` judges a file by its last valid time; one timestamp is
        # judged as a file holding that timestamp alone.
        return not retention.retires(
            collection,
            valid_time_end=timestamp,
            reference_time=self.ctx.reference_time,
            now=timezone.now(),
        )
