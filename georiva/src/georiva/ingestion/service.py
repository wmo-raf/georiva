import gc
import logging
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from georiva.core.filename import parse_path
from georiva.core.models import Variable, Collection
from georiva.core.storage import storage, BucketType, Bucket
from .asset_writer import AssetWriter
from .clipper import BoundaryClipper
from .encoder import VariableEncoder
from .extractor import VariableExtractor
from .utils import apply_unit_conversion, iter_windows

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Result of processing a single incoming file."""
    
    origin_file: str
    origin_bucket: str
    catalog_slug: str
    collection_slug: str
    success: bool
    timestamp: datetime
    items_created: list = field(default_factory=list)
    assets_created: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    
    # Clipping info
    clipped: bool = False
    clip_boundary: str = ""
    original_size: tuple = None
    clipped_size: tuple = None
    
    # Archive info
    archive_path: str = ""
    
    def add_error(self, msg: str):
        self.errors.append(msg)
        logger.error(msg)
    
    @property
    def size_reduction_percent(self) -> Optional[float]:
        """Calculate storage reduction from clipping."""
        if self.original_size and self.clipped_size:
            original_pixels = self.original_size[0] * self.original_size[1]
            clipped_pixels = self.clipped_size[0] * self.clipped_size[1]
            return 100 * (1 - clipped_pixels / original_pixels)
        return None


# =============================================================================
# Ingestion Service
# =============================================================================


class IngestionService:
    """
    Main service for ingesting geospatial data.

    Operates across GeoRiva's multi-bucket storage:

        georiva-incoming  ──┐
                            ├──→ process ──→ georiva-assets
        georiva-sources   ──┘
                            │
                            └──→ georiva-archive (raw copy)

    Flow:
    1. Parse file path for catalog, collection, reference_time
    2. Resolve Catalog (must exist)
    3. Resolve Collection(s):
       - If collection slug in path → use that single collection
       - If no collection in path → use ALL active collections under catalog
    4. Get format plugin based on Catalog.file_format
    5. Initialize boundary clipper if Catalog has boundary
    6. Download file once, extract timestamps
    7. For each collection × timestamp:
       a. Compute clip window from boundary bbox
       b. Create one Item for the Collection
       c. For each Variable in Collection:
          - Extract + transform source data → single 2D array
          - Apply unit conversion
          - Apply geometry mask (if configured)
          - Encode to PNG (visual asset)
          - Write COG (data asset)
          - Create Asset records
    8. Update Collection extent
    9. Archive raw file to georiva-archive
    10. Optionally delete from origin bucket
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
        Process an incoming file from any source bucket.

        Resolves catalog and collection entirely from the file path.
        Reference time is extracted from the GR-- filename prefix if present,
        or can be passed explicitly.

        Archive and cleanup behavior is controlled by the Catalog model:
            catalog.archive_source_files = True → archive raw + delete from origin

        Args:
            file_path: Path relative to bucket root.
            origin_bucket: Which bucket the file came from.
            reference_time: Explicit reference time (overrides GR-- in filename).

        Returns:
            IngestionResult with status and created records.
        """
        from georiva.core.models import Catalog
        from georiva.formats.registry import format_registry
        
        origin = storage.bucket(origin_bucket)
        
        self.logger.info("Processing: %s/%s", origin.bucket_name, file_path)
        
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
            # 1. Resolve catalog
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
            
            # 2. Resolve collections
            collections = self._resolve_collections(catalog, collection_slug)
            
            if not collections:
                if collection_slug:
                    result.add_error(
                        f"Collection not found or inactive: {catalog_slug}/{collection_slug}"
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
            
            # 3. Get format plugin
            plugin = format_registry.get(catalog.file_format)
            if not plugin:
                result.add_error(f"No format plugin for: {catalog.file_format}")
                return result
            
            # 4. Initialize boundary clipper
            clipper = BoundaryClipper(
                boundary=catalog.boundary if catalog.clip_mode != "none" else None,
                apply_mask=(catalog.clip_mode == "mask"),
            )
            
            if clipper.is_active:
                result.clipped = True
                result.clip_boundary = str(catalog.boundary)
                self.logger.info("Clipping enabled: %s", catalog.boundary)
            
            # 5. Download from origin bucket to local temp (once for all collections)
            local_path = self._download_to_temp(origin, file_path)
            
            try:
                # 6. Process each collection × timestamp
                for collection in collections:
                    # 7. Get timestamps scoped to the first variable of the collection
                    first_variable_name = self._get_first_variable_name(collection)
                    if not first_variable_name:
                        result.add_error(
                            f"No active variables found in collection for: {collection.slug}"
                        )
                        continue
                    
                    timestamps = plugin.get_timestamps(local_path, first_variable_name)
                    
                    if not timestamps:
                        result.add_error(f"No timestamps found in: {file_path}")
                        continue
                    
                    self.logger.info("Found %d timestamps", len(timestamps))
                    
                    result.collection_slug = collection.slug
                    
                    for ts in timestamps:
                        try:
                            item, assets, clip_info = self._process_timestamp(
                                collection=collection,
                                plugin=plugin,
                                local_path=local_path,
                                timestamp=ts,
                                source_file=f"{origin_bucket}:{file_path}",
                                clipper=clipper,
                                reference_time=reference_time,
                            )
                            result.items_created.append(str(item.pk))
                            result.assets_created.extend(
                                [str(a.pk) for a in assets]
                            )
                            
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
                self._cleanup_temp(local_path)
            
            # 8. Archive raw file + delete from origin
            if result.success and catalog.archive_source_files:
                archived = self._archive_source(origin, file_path)
                result.archive_path = archived or ""
                
                if archived:
                    origin.delete(file_path)
                    self.logger.info(
                        "Archived and deleted source: %s/%s",
                        origin.bucket_name,
                        file_path,
                    )
            
            # Log clipping summary
            if result.clipped and result.size_reduction_percent:
                self.logger.info(
                    "Clipping reduced size by %.1f%%",
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
            self, catalog, collection_slug: str = None
    ) -> list["Collection"]:
        """
        Resolve collections to process for a given catalog.

        If collection_slug is provided, return just that one collection.
        If not, return ALL active collections under the catalog.
        """
        base_qs = Collection.objects.select_related("catalog").prefetch_related(
            "variables", "variables__sources"
        )
        
        if collection_slug:
            try:
                return [
                    base_qs.get(
                        catalog=catalog, slug=collection_slug, is_active=True
                    )
                ]
            except Collection.DoesNotExist:
                return []
        
        return list(base_qs.filter(catalog=catalog, is_active=True))
    
    def _get_first_variable_name(self, collection: Collection) -> Optional[str]:
        """
        Get the source_name of the first active variable of the collection.

        Used to scope get_timestamps() to a specific variable.
        """
        variables = collection.variables.filter(is_active=True).prefetch_related(
            "sources"
        )
        for variable in variables:
            sources = list(variable.sources.order_by("sort_order"))
            if sources:
                return sources[0].source_name
        return None
    
    def _normalize_bounds(self, bounds: list | tuple) -> list:
        """
        Normalize bounds to valid WGS84 range.

        Handles:
            - 0-360 longitude → -180 to 180
            - Clamping latitude to -90/90
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
            collection: "Collection",
            plugin,
            local_path: Path,
            timestamp: datetime,
            source_file: str,
            clipper: BoundaryClipper,
            reference_time: datetime = None,
    ) -> tuple["Item", list["Asset"], dict]:
        """
        Process all Variables for a single timestamp.

        Creates or retrieves one Item, then creates/updates Assets per Variable.
        """
        from georiva.core.models import Item
        
        self.logger.info("Processing %s @ %s", collection, timestamp)
        
        extractor = VariableExtractor(plugin)
        encoder = VariableEncoder()
        writer = AssetWriter(storage.assets)
        
        variables = list(
            collection.variables.filter(is_active=True).prefetch_related("sources")
        )
        
        if not variables:
            raise ValueError(
                f"Collection '{collection.slug}' has no active variables"
            )
        
        # Get spatial metadata from first variable
        first_var = variables[0]
        meta = extractor.get_metadata(first_var, local_path, timestamp)
        src_width, src_height = meta["width"], meta["height"]
        src_bounds = tuple(meta["bounds"])
        
        if not src_bounds or len(src_bounds) < 4:
            raise ValueError(f"Invalid bounds from metadata: {src_bounds}")
        
        # Compute clip window
        clip_info = {
            "original_size": (src_width, src_height),
            "clipped_size": None,
        }
        clip_window = None
        
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
                        src_width,
                        src_height,
                        width,
                        height,
                        reduction,
                    )
                else:
                    width, height, bounds = src_width, src_height, src_bounds
            except ValueError as e:
                self.logger.warning(
                    "Clip window failed: %s, using full extent", e
                )
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds
        
        crs = meta.get("crs", collection.crs or "EPSG:4326")
        
        ts_utc = self._ensure_utc(timestamp)
        ref_utc = self._ensure_utc(reference_time) if reference_time else None
        
        bounds = self._normalize_bounds(bounds)
        
        # Get or create Item
        item, created = Item.objects.get_or_create(
            collection=collection,
            time=ts_utc,
            reference_time=ref_utc,
            defaults={
                "source_file": source_file,
                "bounds": list(bounds),
                "width": width,
                "height": height,
                "resolution_x": abs((bounds[2] - bounds[0]) / width)
                if width
                else 0,
                "resolution_y": abs((bounds[3] - bounds[1]) / height)
                if height
                else 0,
                "crs": crs,
            },
        )
        
        if not created:
            self.logger.info(
                "Item already exists for %s @ %s, updating assets",
                collection,
                ts_utc,
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
            if update_fields:
                item.save(update_fields=update_fields)
        
        # Process each Variable
        assets = []
        
        for variable in variables:
            try:
                variable_assets = self._process_variable(
                    item=item,
                    variable=variable,
                    extractor=extractor,
                    encoder=encoder,
                    writer=writer,
                    local_path=local_path,
                    timestamp=timestamp,
                    bounds=bounds,
                    crs=crs,
                    width=width,
                    height=height,
                    clipper=clipper,
                    clip_window=clip_window,
                )
                assets.extend(variable_assets)
            
            except Exception as e:
                self.logger.error("Variable %s failed: %s", variable.slug, e)
        
        self._update_collection_extent(collection, ts_utc, bounds)
        
        self.logger.info("Created Item %s with %d assets", item.pk, len(assets))
        
        return item, assets, clip_info
    
    # =========================================================================
    # Variable Processing
    # =========================================================================
    
    def _process_variable(
            self,
            item: "Item",
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
    ) -> list["Asset"]:
        """
        Process a single Variable: extract, transform, encode, save.
        """
        from georiva.core.models import Asset
        
        self.logger.debug("Processing variable: %s", variable.slug)
        
        stats = extractor.compute_stats(
            variable, local_path, timestamp, window=clip_window
        )
        
        use_chunked = width * height > 4096 * 4096
        
        if use_chunked and clip_window is None:
            final_data, final_rgba = self._process_variable_chunked(
                variable=variable,
                extractor=extractor,
                encoder=encoder,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
                stats=stats,
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
                stats=stats,
                clip_window=clip_window,
            )
        
        if clipper and clipper.is_active:
            final_data = clipper.apply_geometry_mask(
                final_data, bounds, nodata=np.nan
            )
            final_rgba = clipper.apply_rgba_mask(final_rgba, bounds)
        
        # Build asset paths
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        time_str = timestamp.strftime("%H%M%S")
        base_name = f"{variable.slug}_{time_str}"
        
        base_dir = storage.build_asset_path(
            catalog=catalog_slug,
            collection=collection_slug,
            variable=variable.slug,
            timestamp=timestamp,
            filename="",
        ).rstrip("/")
        
        assets = []
        visual_asset = None
        
        # Save PNG (visual asset)
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
                        "imageUnscale": [
                            variable.value_min,
                            variable.value_max,
                        ],
                        "scale": variable.scale_type or "linear",
                    },
                },
            )
            assets.append(visual_asset)
        
        except Exception as e:
            self.logger.error("PNG save failed for %s: %s", variable.slug, e)
        
        # Save COG (data asset)
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
                        "image/tiff; application=geotiff; "
                        "profile=cloud-optimized"
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
                        "nodata": None,
                    },
                },
            )
            assets.append(data_asset)
        
        except Exception as e:
            self.logger.error("COG save failed for %s: %s", variable.slug, e)
        
        # Save metadata JSON
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
            
            if visual_asset:
                metadata["color_map"] = visual_asset.weather_layers_palette
            
            writer.write_metadata(metadata, meta_path)
        
        except Exception as e:
            self.logger.warning(
                "Metadata save failed for %s: %s", variable.slug, e
            )
        
        del final_data, final_rgba
        gc.collect()
        
        return assets
    
    def _process_variable_chunked(
            self,
            variable: "Variable",
            extractor: VariableExtractor,
            encoder: VariableEncoder,
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
            stats: dict,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Process variable in chunks for large datasets."""
        
        final_data = np.zeros((height, width), dtype=np.float32)
        final_rgba = np.zeros((height, width, 4), dtype=np.uint8)
        
        for x, y, w, h in iter_windows(width, height, block_size=2048):
            window = (x, y, w, h)
            
            chunk = extractor.extract(variable, local_path, timestamp, window)
            chunk = apply_unit_conversion(chunk, variable.unit_conversion)
            
            final_data[y: y + h, x: x + w] = chunk
            
            rgba_chunk = encoder.encode_to_rgba(chunk, variable)
            final_rgba[y: y + h, x: x + w] = rgba_chunk
            
            del chunk, rgba_chunk
        
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
            stats: dict,
            clip_window: dict = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Process variable with direct extraction (optionally clipped)."""
        
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
    # Collection Updates
    # =========================================================================
    
    def _update_collection_extent(
            self,
            collection: "Collection",
            timestamp: datetime,
            bounds: tuple,
    ):
        """Update collection's temporal and spatial extent."""
        from georiva.core.models import Item
        
        update_fields = []
        
        if collection.time_start is None or timestamp < collection.time_start:
            collection.time_start = timestamp
            update_fields.append("time_start")
        
        if collection.time_end is None or timestamp > collection.time_end:
            collection.time_end = timestamp
            update_fields.append("time_end")
        
        current = collection.bounds
        if not current or len(current) < 4:
            collection.bounds = list(bounds)
            update_fields.append("bounds")
        else:
            expanded = [
                min(current[0], bounds[0]),
                min(current[1], bounds[1]),
                max(current[2], bounds[2]),
                max(current[3], bounds[3]),
            ]
            if expanded != current:
                collection.bounds = self._normalize_bounds(expanded)
                update_fields.append("bounds")
        
        collection.item_count = Item.objects.filter(collection=collection).count()
        update_fields.append("item_count")
        
        if update_fields:
            collection.save(update_fields=update_fields)
    
    # =========================================================================
    # Helpers
    # =========================================================================
    
    def _ensure_utc(self, dt) -> Optional[datetime]:
        """Ensure datetime is UTC."""
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
        """Get file size from a specific bucket."""
        try:
            return bucket.size(path)
        except Exception:
            return None
    
    def _download_to_temp(self, origin: Bucket, file_path: str) -> Path:
        """Download file from an origin bucket to local temp."""
        suffix = Path(file_path).suffix
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        with origin.open(file_path, "rb") as src:
            tmp_path.write_bytes(src.read())
        
        return tmp_path
    
    def _cleanup_temp(self, path: Path):
        """Remove temp file."""
        try:
            path.unlink(missing_ok=True)
        except Exception as e:
            self.logger.warning("Temp cleanup failed: %s - %s", path, e)
    
    def _archive_source(self, origin: Bucket, file_path: str) -> Optional[str]:
        """
        Archive raw source file before processing.

        Copies to georiva-archive with origin prefix.
        """
        try:
            archived = storage.archive_raw(origin, file_path)
            self.logger.info(
                "Archived: %s/%s → archive/%s",
                origin.bucket_name,
                file_path,
                archived,
            )
            return archived
        except Exception as e:
            self.logger.warning(
                "Archive failed: %s/%s - %s",
                origin.bucket_name,
                file_path,
                e,
            )
            return None
