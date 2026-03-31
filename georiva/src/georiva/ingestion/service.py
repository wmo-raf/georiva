import logging
import tempfile
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pytz
from django.conf import settings
from django.db.models import F

from georiva.core.filename import parse_path
from georiva.core.models import Catalog, Collection, Item, Asset
from georiva.core.storage import storage, BucketType, Bucket
from georiva.formats.registry import format_registry
from georiva.zarr_store.tasks import zarr_sync_store
from .asset_writer import AssetWriter
from .clipper import BoundaryClipper
from .encoder import VariableEncoder
from .extractor import VariableExtractor
from .models import IngestionLog
from .result import IngestionResult
from .utils import apply_unit_conversion, iter_windows

logger = logging.getLogger(__name__)


# =============================================================================
# Ingestion Service
# =============================================================================

class IngestionService:
    """
    Orchestrates the full ingestion pipeline for geospatial data files.

    Operates across GeoRiva's multi-bucket storage architecture:

        georiva-incoming  ──┐
                            ├──► process ──► georiva-assets  (COG + PNG + JSON)
        georiva-sources   ──┘
                            │
                            └──► georiva-archive             (raw copy, optional)

    Processing Flow:
    ─────────────────
    1.  Parse the file path → extract catalog slug, collection slug, reference time
    2.  Resolve the Catalog from the database (must exist and be active)
    3.  Resolve Collection(s):
          - If collection slug present in path → process that one collection
          - If absent → process ALL active collections under the catalog
    4.  Look up the format plugin (GRIB, NetCDF, GeoTIFF etc)
    5.  Initialize the boundary clipper if the catalog has a spatial boundary
    6.  Instantiate shared processing objects (writer, extractor, encoder) once
        per file — not per timestamp or variable
    7.  Download the source file once to a local temp directory
    8.  For each collection × timestamp:
          a. Compute spatial clip window from boundary bbox
          b. Get or create one Item for the collection + timestamp
          c. For each Variable in the collection:
               - Extract + transform source data → 2D array
               - Apply unit conversion
               - Apply geometry mask (if clip_mode = "mask")
               - Encode to RGBA PNG  (visual asset)
               - Write COG GeoTIFF   (data asset)
               - Write JSON sidecar  (metadata asset)
               - Create/update Asset records in DB
    9.  Update Collection spatial + temporal extent
    10. Archive raw file to georiva-archive (if configured)
    11. Delete from origin bucket (only if fully successful)

    Partial Failure Behaviour:
    ──────────────────────────
    If some variables fail but others succeed, the Item and successful
    Assets are kept. The source file is NOT deleted so it can be
    re-processed. This is intentional — a partial ingest is better than
    losing data silently.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("georiva.ingestion")
    
    # =========================================================================
    # Main Entry Point
    # =========================================================================
    
    def process_file(
            self,
            file_path: str,
            origin_bucket: str = BucketType.INCOMING,
            reference_time: datetime = None,
    ) -> IngestionResult:
        """
        Process a single incoming geospatial file end-to-end.

        The catalog and collection are resolved entirely from the file path.
        Reference time is extracted from the GR-- filename prefix if present,
        or can be supplied explicitly (e.g. from a Celery task argument).

        Archive and cleanup behaviour is driven by the Catalog model:
            catalog.archive_source_files = True  →  copy to georiva-archive
            Successful, fully-complete runs       →  delete from origin bucket
            Partial variable failures             →  keep in origin for retry

        Args:
            file_path:       Path relative to the bucket root.
            origin_bucket:   Which bucket the file came from.
            reference_time:  Explicit reference time (overrides GR-- prefix).

        Returns:
            IngestionResult summarising what was created and any errors.
        """
        origin = storage.bucket(origin_bucket)
        self.logger.info("Processing: %s/%s", origin.bucket_name, file_path)
        
        # --- Path parsing ---------------------------------------------------
        # Extract catalog slug, optional collection slug, and reference time
        # from the standardised GeoRiva filename convention.
        path_meta = parse_path(file_path)
        catalog_slug = path_meta.get("catalog")
        collection_slug = path_meta.get("collection")
        
        if reference_time is None:
            reference_time = path_meta.get("reference_time")
        
        result = IngestionResult(
            origin_file=file_path,
            origin_bucket=origin_bucket,
            catalog_slug=catalog_slug or "",
            collection_slug=collection_slug or "",
            success=False,
            timestamp=datetime.now(pytz.utc),
        )
        
        try:
            # --- Catalog resolution -----------------------------------------
            if not catalog_slug:
                result.add_error(f"Cannot determine catalog from path: {file_path}")
                return result
            
            try:
                catalog = Catalog.objects.select_related("boundary").get(
                    slug=catalog_slug, is_active=True
                )
            except Catalog.DoesNotExist:
                result.add_error(f"Catalog not found or inactive: {catalog_slug}")
                return result
            
            # --- Collection resolution ---------------------------------------
            # A file may target one specific collection (slug in path) or all
            # active collections under the catalog (slug absent).
            collections = self._resolve_collections(catalog, collection_slug)
            
            if not collections:
                if collection_slug:
                    result.add_error(
                        f"Collection not found or inactive: "
                        f"{catalog_slug}/{collection_slug}"
                    )
                else:
                    result.add_error(
                        f"No active collections found for catalog: {catalog_slug}"
                    )
                return result
            
            self.logger.info(
                "Processing against %d collection(s): %s",
                len(collections),
                ", ".join(c.slug for c in collections),
            )
            
            # --- Format plugin ----------------------------------------------
            plugin = format_registry.get(catalog.file_format)
            if not plugin:
                result.add_error(f"No format plugin for: {catalog.file_format}")
                return result
            
            # --- Boundary clipper -------------------------------------------
            # clip_mode = "none"  → no clipping at all
            # clip_mode = "bbox"  → spatial window crop only
            # clip_mode = "mask"  → crop + zero out pixels outside geometry
            clipper = BoundaryClipper(
                boundary=catalog.boundary if catalog.clip_mode != "none" else None,
                apply_mask=(catalog.clip_mode == "mask"),
            )
            
            if clipper.is_active:
                result.clipped = True
                result.clip_boundary = str(catalog.boundary)
                self.logger.info("Clipping enabled: %s", catalog.boundary)
            
            # --- IngestionLog reference ------------------------------------
            # Retrieved once so Items created from this file are linked back
            # to the log that triggered their ingestion.
            ingestion_log = IngestionLog.objects.filter(
                bucket=origin_bucket, file_path=file_path
            ).first()

            # --- Shared processing objects ----------------------------------
            # Instantiated once per file and passed through the call stack.
            writer = AssetWriter(storage.assets)
            extractor = VariableExtractor(plugin)
            encoder = VariableEncoder()
            
            try:
                with self._download_to_temp(origin, file_path) as local_path:
                    for collection in collections:
                        
                        # Timestamps are scoped to the first variable to avoid
                        # opening the file multiple times for the same info.
                        first_variable_name = self._get_first_variable_name(
                            collection
                        )
                        if not first_variable_name:
                            result.add_error(
                                f"No active variables for: {collection.slug}"
                            )
                            continue
                        
                        timestamps = plugin.get_timestamps(
                            local_path, first_variable_name
                        )
                        
                        if not timestamps:
                            result.add_error(
                                f"No timestamps found in: {file_path}"
                            )
                            continue
                        
                        self.logger.info(
                            "Found %d timestamp(s) for %s",
                            len(timestamps),
                            collection.slug,
                        )
                        result.collection_slug = collection.slug
                        
                        for ts in timestamps:
                            try:
                                item, assets, clip_info, failed_vars = (
                                    self._process_timestamp(
                                        collection=collection,
                                        writer=writer,
                                        extractor=extractor,
                                        encoder=encoder,
                                        local_path=local_path,
                                        timestamp=ts,
                                        source_file=f"{origin_bucket}:{file_path}",
                                        clipper=clipper,
                                        reference_time=reference_time,
                                        ingestion_log=ingestion_log,
                                    )
                                )
                                
                                if failed_vars:
                                    result.add_error(
                                        f"Partial failure for {collection.slug} "
                                        f"@ {ts}: variables failed: "
                                        f"{', '.join(failed_vars)}"
                                    )
                                
                                if item is None:
                                    continue
                                
                                result.items_created.append(str(item.pk))
                                result.assets_created.extend(
                                    [str(a.pk) for a in assets]
                                )
                                
                                # Record clip size from first successful timestamp
                                if clip_info and result.original_size is None:
                                    result.original_size = clip_info.get(
                                        "original_size"
                                    )
                                    result.clipped_size = clip_info.get(
                                        "clipped_size"
                                    )
                            
                            except Exception as e:
                                result.add_error(
                                    f"Failed {collection.slug} @ {ts}: {e}"
                                )
                
                result.success = len(result.items_created) > 0
            
            finally:
                # Release any file handles or cached datasets held by the plugin.
                if hasattr(plugin, "clear_cache"):
                    plugin.clear_cache()
            
            # --- Archive + cleanup ------------------------------------------
            # Only delete the source file if ALL variables succeeded.
            # Partial failures mean the file may need to be re-processed.
            has_partial_failures = any(
                "Partial failure" in e for e in result.errors
            )
            
            if result.success and not has_partial_failures:
                if catalog.archive_source_files:
                    archived = self._archive_source(origin, file_path)
                    result.archive_path = archived or ""
                origin.delete(file_path)
            
            elif result.success and has_partial_failures:
                self.logger.warning(
                    "Partial variable failures — keeping source file "
                    "for re-processing: %s",
                    file_path,
                )
            
            if result.clipped and result.size_reduction_percent:
                self.logger.info(
                    "Clipping reduced data size by %.1f%%",
                    result.size_reduction_percent,
                )
        
        except Exception as e:
            self.logger.exception("Ingestion failed: %s", file_path)
            result.add_error(str(e))
        
        return result
    
    # =========================================================================
    # Collection Resolution
    # =========================================================================
    
    def _resolve_collections(
            self,
            catalog: Catalog,
            collection_slug: str = None,
    ) -> list[Collection]:
        """
        Resolve which collections to process for a given catalog.

        Variables and their sources are prefetched here to avoid N+1 queries
        later when iterating over variables per timestamp.

        Returns a single-element list if collection_slug is given,
        or all active collections if not.
        """
        base_qs = Collection.objects.select_related("catalog").prefetch_related(
            "variables",
            "variables__sources",
        )
        
        if collection_slug:
            try:
                return [
                    base_qs.get(
                        catalog=catalog,
                        slug=collection_slug,
                        is_active=True,
                    )
                ]
            except Collection.DoesNotExist:
                return []
        
        return list(base_qs.filter(catalog=catalog, is_active=True))
    
    def _get_first_variable_name(self, collection: Collection) -> Optional[str]:
        """
        Return the source_name of the first active variable in the collection.

        Uses the prefetched variables and sources from _resolve_collections —
        no additional database queries are issued here.

        Used to scope get_timestamps() to a specific variable name,
        since some formats (GRIB, NetCDF) require a variable name to
        enumerate time steps.
        """
        for variable in collection.variables.all():
            if not variable.is_active:
                continue
            # Sort by sort_order to respect user-defined variable ordering
            sources = sorted(variable.sources.all(), key=lambda s: s.sort_order)
            if sources:
                return sources[0].source_name
        return None
    
    # =========================================================================
    # Bounds Normalisation
    # =========================================================================
    
    def _normalize_bounds(self, bounds: list | tuple) -> list:
        """
        Normalise bounds to valid WGS84 range.

        Handles:
          - 0–360 longitude convention (common in GRIB/ERA5) → -180 to 180
          - Latitude clamping to -90/90 (guards against floating point drift)
          - Longitude clamping to -180/180
        """
        west, south, east, north = bounds
        
        if west > 180:
            west -= 360
        if east > 180:
            east -= 360
        
        south = max(-90.0, min(90.0, south))
        north = max(-90.0, min(90.0, north))
        west = max(-180.0, min(180.0, west))
        east = max(-180.0, min(180.0, east))
        
        return [west, south, east, north]
    
    # =========================================================================
    # Timestamp Processing
    # =========================================================================
    
    def _process_timestamp(
            self,
            collection: Collection,
            writer: AssetWriter,
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            local_path: Path,
            timestamp: datetime,
            source_file: str,
            clipper: BoundaryClipper,
            reference_time: datetime = None,
            ingestion_log: "IngestionLog" = None,
    ) -> tuple[Optional[Item], list[Asset], dict, list]:
        """
        Process all Variables for a single timestamp within a collection.

        Creates or retrieves one Item record, then delegates per-variable
        processing to _process_variable. If no assets are created (all
        variables failed), the orphan Item is deleted and None is returned.

        Returns:
            (item, assets, clip_info, failed_variable_slugs)
        """
        self.logger.info("Processing %s @ %s", collection, timestamp)
        
        # Use prefetched variables — no extra DB query
        variables = [v for v in collection.variables.all() if v.is_active]
        
        if not variables:
            raise ValueError(
                f"Collection '{collection.slug}' has no active variables"
            )
        
        # --- Spatial metadata -----------------------------------------------
        # Derive source dimensions and bounds from the first variable.
        # All variables in a collection share the same spatial grid.
        first_var = variables[0]
        meta = extractor.get_metadata(first_var, local_path, timestamp)
        src_width, src_height = meta["width"], meta["height"]
        src_bounds = tuple(meta["bounds"])
        
        if not src_bounds or len(src_bounds) < 4:
            raise ValueError(f"Invalid bounds from metadata: {src_bounds}")
        
        clip_info = {
            "original_size": (src_width, src_height),
            "clipped_size": None,
        }
        clip_window = None
        
        # --- Clip window computation ----------------------------------------
        # Compute the pixel-space window that corresponds to the catalog
        # boundary. Subsequent extraction reads only those pixels,
        # avoiding loading the full global/continental grid into memory.
        if clipper.is_active:
            try:
                clip_window = clipper.compute_window(
                    src_bounds, src_width, src_height
                )
                
                if clip_window:
                    width = clip_window["width"]
                    height = clip_window["height"]
                    bounds = clip_window["bounds"]
                    clip_info["clipped_size"] = (width, height)
                    
                    reduction = 100 * (
                            1 - (width * height) / (src_width * src_height)
                    )
                    self.logger.info(
                        "Clipping: %dx%d → %dx%d (%.1f%% reduction)",
                        src_width, src_height, width, height, reduction,
                    )
                else:
                    # Boundary is outside the file extent — use full grid
                    width, height, bounds = src_width, src_height, src_bounds
            
            except ValueError as e:
                self.logger.warning(
                    "Clip window computation failed: %s — using full extent", e
                )
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds
        
        crs = meta.get("crs", collection.crs or "EPSG:4326")
        ts_utc = self._ensure_utc(timestamp)
        ref_utc = self._ensure_utc(reference_time) if reference_time else None
        bounds = self._normalize_bounds(bounds)
        
        # --- Item creation --------------------------------------------------
        # One Item per (collection, time, reference_time) tuple.
        # If it already exists (re-ingest scenario), update spatial fields
        # in case the source file has changed extent or resolution.
        item, created = Item.objects.get_or_create(
            collection=collection,
            time=ts_utc,
            reference_time=ref_utc,
            defaults={
                "source_file": source_file,
                "ingestion_log": ingestion_log,
                "bounds": list(bounds),
                "width": width,
                "height": height,
                "resolution_x": (
                    abs((bounds[2] - bounds[0]) / width) if width else 0
                ),
                "resolution_y": (
                    abs((bounds[3] - bounds[1]) / height) if height else 0
                ),
                "crs": crs,
            },
        )
        
        if not created:
            self.logger.info(
                "Item already exists for %s @ %s — updating assets", collection, ts_utc
            )
            update_fields = []
            if item.source_file != source_file:
                item.source_file = source_file
                update_fields.append("source_file")
            if list(item.bounds) != list(bounds):
                item.bounds = list(bounds)
                item.width = width
                item.height = height
                update_fields.extend(["bounds", "width", "height"])
            if ingestion_log and item.ingestion_log_id != ingestion_log.pk:
                item.ingestion_log = ingestion_log
                update_fields.append("ingestion_log")
            if update_fields:
                item.save(update_fields=update_fields)
        
        # --- Per-variable processing ----------------------------------------
        assets = []
        failed_variables = []
        
        for variable in variables:
            try:
                variable_assets = self._process_variable(
                    item=item,
                    variable=variable,
                    extractor=extractor,
                    encoder=encoder,
                    writer=writer,
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
            
            except Exception as e:
                self.logger.error(
                    "Variable %s failed: %s\n%s",
                    variable.slug, e, traceback.format_exc(),
                )
                failed_variables.append(variable.slug)
        
        self._update_collection_extent(collection, ts_utc, bounds)
        
        # --- Orphan guard ---------------------------------------------------
        # If every variable failed, delete the Item rather than leaving
        # an empty shell with no assets in the catalog.
        if not assets:
            self.logger.warning(
                "No assets created for Item %s — deleting orphan item", item.pk
            )
            item.delete()
            return None, [], clip_info, failed_variables
        
        if created:
            Collection.objects.filter(pk=collection.pk).update(
                item_count=F("item_count") + 1
            )
        
        self.logger.info(
            "Created Item %s with %d asset(s)", item.pk, len(assets)
        )
        return item, assets, clip_info, failed_variables
    
    # =========================================================================
    # Variable Processing
    # =========================================================================
    
    def _process_variable(
            self,
            item: Item,
            variable: "Variable",
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            writer: AssetWriter,
            local_path: Path,
            timestamp: datetime,
            bounds: tuple,
            crs: str,
            width: int,
            height: int,
            clipper: BoundaryClipper = None,
            clip_window: dict = None,
    ) -> list[Asset]:
        """
        Orchestrate the full processing pipeline for a single variable.

        Delegates to three focused sub-methods:
          _extract_and_encode  →  raw data + RGBA array
          _compute_stats       →  min/max/mean/std
          _save_assets         →  write files + create Asset records
        """
        self.logger.debug("Processing variable: %s", variable.slug)
        
        final_data, final_rgba = self._extract_and_encode(
            variable=variable,
            extractor=extractor,
            encoder=encoder,
            local_path=local_path,
            timestamp=timestamp,
            width=width,
            height=height,
            clip_window=clip_window,
            clipper=clipper,
            bounds=bounds,
        )
        
        # Stats are computed post-mask so they reflect only the valid spatial
        # domain (e.g. land pixels inside Ethiopia, not the full bbox).
        stats = self._compute_stats(final_data)
        
        assets = self._save_assets(
            item=item,
            variable=variable,
            writer=writer,
            final_data=final_data,
            final_rgba=final_rgba,
            stats=stats,
            bounds=bounds,
            crs=crs,
            width=width,
            height=height,
            timestamp=timestamp,
        )
        
        # Explicitly release large arrays — these can be 64MB+ for global data
        del final_data, final_rgba
        
        return assets
    
    def _extract_and_encode(
            self,
            variable: "Variable",
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
            clip_window: dict = None,
            clipper: BoundaryClipper = None,
            bounds: tuple = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Extract raw data from the source file and encode it to RGBA.

        Switches between two extraction strategies based on raster size:

        Direct extraction  — used for clipped or small rasters.
                             Reads the full (or windowed) array at once.

        Chunked extraction — used for large unclipped rasters above
                             GEORIVA_CHUNK_THRESHOLD_PIXELS. Processes the
                             grid in 2048×2048 blocks to avoid OOM on
                             continental or global datasets.

        After extraction, applies the boundary geometry mask if configured.
        The mask sets pixels outside the boundary polygon to nodata (NaN),
        so that statistics and COG outputs reflect only the valid domain.
        """
        # Chunked mode is only applicable when there is no clip window —
        # a clip window already limits the data to a manageable region.
        use_chunked = (
                width * height > settings.GEORIVA_CHUNK_THRESHOLD_PIXELS
                and clip_window is None
        )
        
        if use_chunked:
            self.logger.debug(
                "Using chunked extraction for %s (%dx%d)", variable.slug, width, height
            )
            final_data, final_rgba = self._process_variable_chunked(
                variable=variable,
                extractor=extractor,
                encoder=encoder,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
            )
        else:
            final_data, final_rgba = self._process_variable_direct(
                variable=variable,
                extractor=extractor,
                encoder=encoder,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
                clip_window=clip_window,
            )
        
        # Apply geometry mask after extraction so that pixels outside the
        # boundary polygon are zeroed in the visual and set to NaN in the
        # data — ensuring stats and COG nodata are consistent.
        if clipper and clipper.is_active:
            final_data = clipper.apply_geometry_mask(
                final_data, bounds, nodata=np.nan
            )
            final_rgba = clipper.apply_rgba_mask(final_rgba, bounds)
        
        return final_data, final_rgba
    
    def _compute_stats(self, data: np.ndarray) -> dict:
        """
        Compute basic descriptive statistics from a masked float array.

        Uses nanmin/nanmax/nanmean/nanstd so that NaN nodata pixels
        (introduced by clipping or source data) are excluded from
        the calculation.

        Returns None values on failure rather than raising — a stats
        computation error should not abort asset creation.
        """
        try:
            return {
                "min": float(np.nanmin(data)),
                "max": float(np.nanmax(data)),
                "mean": float(np.nanmean(data)),
                "std": float(np.nanstd(data)),
            }
        except Exception:
            return {"min": None, "max": None, "mean": None, "std": None}
    
    def _save_assets(
            self,
            item: Item,
            variable: "Variable",
            writer: AssetWriter,
            final_data: np.ndarray,
            final_rgba: np.ndarray,
            stats: dict,
            bounds: tuple,
            crs: str,
            width: int,
            height: int,
            timestamp: datetime,
    ) -> list[Asset]:
        """
        Write processed data to storage and create Asset records in the DB.

        Writes three asset types per variable per timestamp:
          COG  — Cloud-Optimized GeoTIFF, used by TiTiler and analysis layer (primary)
          PNG  — Encoded RGBA visual, used by GL web map clients for browser-side rendering
          JSON — metadata sidecar, used by the frontend and API responses

        The COG is the primary deliverable and is written first. If the COG
        write fails the exception is re-raised immediately, preventing PNG and
        JSON from being written. PNG and JSON failures are non-fatal.
        """
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        
        # Build a unique base filename that encodes the variable and time.
        # Reference time suffix is added for forecast products (e.g. NWP).
        if item.reference_time:
            ref_str = item.reference_time.strftime("%Y%m%dT%H%M%S")
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}__ref{ref_str}"
        else:
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}"
        
        base_dir = storage.build_asset_path(
            catalog=catalog_slug,
            collection=collection_slug,
            variable=variable.slug,
            timestamp=timestamp,
            filename="",
        ).rstrip("/")
        
        assets = []
        visual_asset = None
        
        # nodata value applied consistently across COG and metadata records.
        _nodata = None
        
        # --- COG (data asset) -----------------------------------------------
        # Written first. Failure raises immediately — PNG and JSON are skipped.
        cog_path = f"{base_dir}/{base_name}.tif"
        try:
            stored_cog = writer.write_cog(final_data, cog_path, bounds, crs)
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults={
                    "href": stored_cog,
                    "media_type": (
                        "image/tiff; application=geotiff; profile=cloud-optimized"
                    ),
                    "roles": ["data"],
                    "file_size": self._get_file_size(storage.assets, stored_cog),
                    "width": width,
                    "height": height,
                    "bands": 1,
                    "stats_min": stats.get("min"),
                    "stats_max": stats.get("max"),
                    "stats_mean": stats.get("mean"),
                    "stats_std": stats.get("std"),
                    "extra_fields": {
                        "compression": "deflate",
                        "nodata": _nodata,
                    },
                },
            )
            assets.append(data_asset)
            
            # Zarr sync: enqueue low-priority append task after COG is confirmed written.
            # Wrapped independently so a Zarr enqueue failure never blocks the COG record.
            try:
                self._enqueue_zarr_sync(item, variable)
            except Exception as zarr_exc:
                self.logger.warning(
                    "Zarr sync enqueue failed for %s: %s", variable.slug, zarr_exc
                )
        
        except Exception as e:
            self.logger.error("COG save failed for %s: %s", variable.slug, e)
            raise
        
        # --- Encoded PNG (visual asset) ---------------------------------------------
        png_path = f"{base_dir}/{base_name}.png"
        try:
            stored_png = writer.write_png(final_rgba, png_path)
            visual_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.PNG,
                defaults={
                    "href": stored_png,
                    "media_type": "image/png",
                    "roles": ["visual"],
                    "file_size": self._get_file_size(storage.assets, stored_png),
                    "width": width,
                    "height": height,
                    "bands": 4,
                    "stats_min": stats.get("min"),
                    "stats_max": stats.get("max"),
                    "stats_mean": stats.get("mean"),
                    "stats_std": stats.get("std"),
                    "extra_fields": {
                        # imageUnscale maps the 0-255 PNG range back to real
                        # physical units for rendering in WeatherLayers GL
                        "imageUnscale": [variable.value_min, variable.value_max],
                        "scale": variable.scale_type or "linear",
                    },
                },
            )
            assets.append(visual_asset)
        
        except Exception as e:
            self.logger.error("PNG save failed for %s: %s", variable.slug, e)
        
        # --- JSON (metadata sidecar) ----------------------------------------
        # Consumed by the frontend detail pages and the analysis layer
        # to understand variable semantics without opening the COG.
        meta_path = f"{base_dir}/{base_name}.json"
        try:
            metadata = {
                "variable": variable.slug,
                "name": variable.name,
                "units": variable.units or "",
                "timestamp": timestamp.isoformat(),
                "reference_time": (
                    item.reference_time.isoformat()
                    if item.reference_time
                    else None
                ),
                "bounds": list(bounds),
                "width": width,
                "height": height,
                "crs": crs,
                "transform": variable.transform_type,
                "imageUnscale": [variable.value_min, variable.value_max],
                "scale": variable.scale_type or "linear",
                "stats": stats,
            }
            
            # Include the colour palette reference if the visual asset was
            # created successfully — used by the frontend legend renderer.
            if visual_asset:
                metadata["color_map"] = visual_asset.variable.weather_layers_palette
            
            writer.write_metadata(metadata, meta_path)
        
        except Exception as e:
            # Metadata failure is non-fatal — warn rather than error,
            # since the COG and PNG are the primary deliverables.
            self.logger.warning(
                "Metadata save failed for %s: %s", variable.slug, e
            )
        
        return assets
    
    def _enqueue_zarr_sync(self, item: "Item", variable: "Variable") -> None:
        """
        Register a pending Zarr sync record and dispatch the sync task.

        Called after a COG asset has been successfully written. Skipped for
        forecast items (reference_time is not None) and when Zarr sync is
        disabled via settings.GEORIVA_ZARR_ENABLED.
        """
        from django.conf import settings as dj_settings
        from georiva.zarr_store.models import ZarrSyncLog
        
        if item.reference_time is not None:
            return  # Forecasts excluded from Zarr v1
        
        if not getattr(dj_settings, 'GEORIVA_ZARR_ENABLED', True):
            return
        
        store_path = (
            f"{item.collection.catalog.slug}"
            f"/{item.collection.slug}"
            f"/{variable.slug}.zarr"
        )
        
        ZarrSyncLog.objects.update_or_create(
            item=item,
            variable=variable,
            defaults={
                'store_path': store_path,
                'status': ZarrSyncLog.Status.PENDING,
                'error': '',
            },
        )
        
        zarr_sync_store.apply_async(args=[store_path], queue='georiva-ingestion')
    
    def _process_variable_chunked(
            self,
            variable: "Variable",
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Process a large variable by reading and encoding it in spatial blocks.

        Used when width × height exceeds GEORIVA_CHUNK_THRESHOLD_PIXELS and
        no clip window is active. Processes 2048×2048 pixel blocks sequentially,
        assembling the full arrays in memory block by block.

        This keeps peak memory usage bounded regardless of input raster size —
        critical for global datasets (7200×3600) running inside Celery workers
        with limited container memory.
        """
        final_data = np.zeros((height, width), dtype=np.float32)
        final_rgba = np.zeros((height, width, 4), dtype=np.uint8)
        
        for x, y, w, h in iter_windows(width, height, block_size=2048):
            window = (x, y, w, h)
            
            chunk = extractor.extract(variable, local_path, timestamp, window)
            chunk = apply_unit_conversion(chunk, variable.unit_conversion)
            
            final_data[y:y + h, x:x + w] = chunk
            final_rgba[y:y + h, x:x + w] = encoder.encode_to_rgba(chunk, variable)
            
            del chunk
        
        return final_data, final_rgba
    
    def _process_variable_direct(
            self,
            variable: "Variable",
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
            clip_window: dict = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Process a variable by reading the full (or windowed) array at once.

        Used for clipped rasters (where the clip window already limits the
        read to a small region) or for rasters below the chunk threshold.
        """
        # Translate the clip_window dict into the (x, y, w, h) tuple that
        # the extractor expects. None means read the full extent.
        if clip_window:
            window = (
                clip_window["x_off"],
                clip_window["y_off"],
                clip_window["width"],
                clip_window["height"],
            )
        else:
            window = None
        
        final_data = extractor.extract(variable, local_path, timestamp, window)
        final_data = apply_unit_conversion(final_data, variable.unit_conversion)
        final_rgba = encoder.encode_to_rgba(final_data, variable)
        
        return final_data, final_rgba
    
    # =========================================================================
    # Collection Extent Updates
    # =========================================================================
    
    def _update_collection_extent(
            self,
            collection: Collection,
            timestamp: datetime,
            bounds: tuple,
    ):
        """
        Expand the collection's temporal and spatial extent to include this item.

        Uses field-level saves (update_fields) to avoid overwriting concurrent
        updates from other ingestion workers processing the same collection.
        """
        update_fields = []
        
        # Temporal extent — expand to include this timestamp
        if collection.time_start is None or timestamp < collection.time_start:
            collection.time_start = timestamp
            update_fields.append("time_start")
        
        if collection.time_end is None or timestamp > collection.time_end:
            collection.time_end = timestamp
            update_fields.append("time_end")
        
        # Spatial extent — expand bbox to include this item's bounds
        current = collection.bounds
        if not current or len(current) < 4:
            collection.bounds = list(bounds)
            update_fields.append("bounds")
        else:
            expanded = [
                min(current[0], bounds[0]),  # west
                min(current[1], bounds[1]),  # south
                max(current[2], bounds[2]),  # east
                max(current[3], bounds[3]),  # north
            ]
            if expanded != current:
                collection.bounds = self._normalize_bounds(expanded)
                update_fields.append("bounds")
        
        if update_fields:
            collection.save(update_fields=update_fields)
    
    # =========================================================================
    # Helpers
    # =========================================================================
    
    def _ensure_utc(self, dt) -> Optional[datetime]:
        """
        Coerce any datetime-like value to a timezone-aware UTC datetime.

        Handles: str, pandas Timestamp, numpy datetime64, and Python datetime.
        Naive datetimes are assumed to be UTC.
        """
        if dt is None:
            return None
        
        if isinstance(dt, str):
            dt = pd.Timestamp(dt).to_pydatetime()
        
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        
        if isinstance(dt, np.datetime64):
            dt = pd.Timestamp(dt).to_pydatetime()
        
        if dt.tzinfo is None:
            return pytz.utc.localize(dt)
        
        return dt.astimezone(pytz.utc)
    
    def _get_file_size(self, bucket: Bucket, path: str) -> Optional[int]:
        """
        Return the stored size in bytes of a file in a given bucket.
        Returns None on failure — a missing file size should not abort ingestion.
        """
        try:
            return bucket.size(path)
        except Exception:
            return None
    
    @contextmanager
    def _download_to_temp(self, origin: Bucket, file_path: str):
        """
        Stream a file from an origin bucket to a local temporary directory.

        Yields the local Path and cleans up automatically on exit.
        Streams in 8 MB chunks to keep memory usage flat regardless of
        file size — important for large GRIB files from global models.
        """
        original_name = Path(file_path).name
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / original_name
            
            with origin.open(file_path, "rb") as src, open(tmp_path, "wb") as dst:
                while chunk := src.read(8 * 1024 * 1024):  # 8 MB chunks
                    dst.write(chunk)
            
            yield tmp_path
    
    def _archive_source(self, origin: Bucket, file_path: str) -> Optional[str]:
        """
        Copy the raw source file to georiva-archive before deletion.

        Archive failure is non-fatal — a warning is logged but ingestion
        is not aborted. The source file will still be deleted if the
        catalog is configured to do so.
        """
        try:
            archived = storage.archive_raw(origin, file_path)
            self.logger.info(
                "Archived: %s/%s → archive/%s",
                origin.bucket_name, file_path, archived,
            )
            return archived
        except Exception as e:
            self.logger.warning(
                "Archive failed: %s/%s — %s",
                origin.bucket_name, file_path, e,
            )
            return None
