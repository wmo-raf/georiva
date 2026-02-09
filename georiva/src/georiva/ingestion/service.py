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

from georiva.core.models import Variable, Collection
from .asset_writer import AssetWriter
from .clipper import BoundaryClipper
from .encoder import VariableEncoder
from .extractor import VariableExtractor
from .utils import apply_unit_conversion, iter_windows

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Result of processing a single incoming file."""
    source_file: str
    catalog_slug: str
    collection_slug: str
    success: bool
    timestamp: datetime
    items_created: list = field(default_factory=list)
    assets_created: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    
    # Clipping info
    clipped: bool = False
    clip_boundary: str = ''
    original_size: tuple = None
    clipped_size: tuple = None
    
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
    
    Flow:
    1. Resolve Catalog + Collection from file path
    2. Get format plugin based on Catalog.file_format
    3. Initialize boundary clipper if Catalog has boundary
    4. Extract timestamps from file
    5. For each timestamp:
       a. Compute clip window from boundary bbox
       b. Create one Item for the Collection
       c. For each Variable in Collection:
          - Extract + transform source data → single 2D array
          - Apply unit conversion
          - Apply geometry mask (if configured)
          - Encode to PNG (visual asset)
          - Write COG (data asset)
          - Create Asset records
    6. Update Collection extent
    7. Archive source file
    """
    
    def __init__(self):
        self.logger = logging.getLogger("georiva.ingestion")
    
    @property
    def storage(self):
        from georiva.core.storage import storage_manager
        return storage_manager
    
    # =========================================================================
    # Main Entry Point
    # =========================================================================
    
    def process_file(
            self,
            file_path: str,
            catalog_slug: str = None,
            collection_slug: str = None,
            metadata: dict = None,
    ) -> IngestionResult:
        """
        Process an incoming file.
        
        Args:
            file_path: Path to source file in storage
            catalog_slug: Catalog slug (inferred from path if not provided)
            collection_slug: Collection slug (inferred from path if not provided)
            metadata: Optional metadata dict
        
        Returns:
            IngestionResult with status and created records
        """
        from georiva.core.models import Catalog, Collection
        from georiva.formats.registry import format_registry
        
        self.logger.info(f"Processing: {file_path}")
        
        reference_time = metadata.get('reference_time', None) if metadata else None
        
        result = IngestionResult(
            source_file=file_path,
            catalog_slug=catalog_slug or '',
            collection_slug=collection_slug or '',
            success=False,
            timestamp=datetime.now(pytz.utc),
        )
        
        try:
            # 1. Resolve catalog and collection
            if not catalog_slug or not collection_slug:
                inferred = self._infer_from_path(file_path)
                catalog_slug = catalog_slug or inferred.get('catalog')
                collection_slug = collection_slug or inferred.get('collection')
                result.catalog_slug = catalog_slug or ''
                result.collection_slug = collection_slug or ''
            
            if not catalog_slug or not collection_slug:
                result.add_error(f"Cannot determine catalog/collection for: {file_path}")
                return result
            
            # 2. Load models
            try:
                catalog = Catalog.objects.select_related('boundary').get(
                    slug=catalog_slug, is_active=True
                )
            except Catalog.DoesNotExist:
                result.add_error(f"Catalog not found or inactive: {catalog_slug}")
                return result
            
            try:
                collection = Collection.objects.select_related('catalog').prefetch_related(
                    'variables', 'variables__sources'
                ).get(catalog=catalog, slug=collection_slug, is_active=True)
            except Collection.DoesNotExist:
                result.add_error(f"Collection not found or inactive: {catalog_slug}/{collection_slug}")
                return result
            
            # 3. Get format plugin
            plugin = format_registry.get(catalog.file_format)
            if not plugin:
                result.add_error(f"No format plugin for: {catalog.file_format}")
                return result
            
            # 4. Initialize boundary clipper
            clipper = BoundaryClipper(
                boundary=catalog.boundary if catalog.clip_mode != 'none' else None,
                apply_mask=(catalog.clip_mode == 'mask')
            )
            
            if clipper.is_active:
                result.clipped = True
                result.clip_boundary = str(catalog.boundary)
                self.logger.info(f"Clipping enabled: {catalog.boundary}")
            
            # 5. Download and process
            local_path = self._download_to_temp(file_path)
            
            try:
                timestamps = plugin.get_timestamps(local_path)
                if not timestamps:
                    result.add_error(f"No timestamps found in: {file_path}")
                    return result
                
                self.logger.info(f"Found {len(timestamps)} timestamps")
                
                # 6. Process each timestamp
                for ts in timestamps:
                    try:
                        item, assets, clip_info = self._process_timestamp(
                            collection=collection,
                            plugin=plugin,
                            local_path=local_path,
                            timestamp=ts,
                            source_file=file_path,
                            clipper=clipper,
                            reference_time=reference_time,
                        )
                        result.items_created.append(str(item.pk))
                        result.assets_created.extend([str(a.pk) for a in assets])
                        
                        # Store clip info from first timestamp
                        if clip_info and result.original_size is None:
                            result.original_size = clip_info.get('original_size')
                            result.clipped_size = clip_info.get('clipped_size')
                    
                    except Exception as e:
                        result.add_error(f"Failed at {ts}: {e}")
                
                result.success = len(result.items_created) > 0
            
            finally:
                self._cleanup_temp(local_path)
            
            # 7. Archive if successful
            if result.success and catalog.archive_source_files:
                self._archive_source(file_path, catalog_slug, collection_slug)
            
            # Log clipping summary
            if result.clipped and result.size_reduction_percent:
                self.logger.info(
                    f"Clipping reduced size by {result.size_reduction_percent:.1f}%"
                )
        
        except Exception as e:
            self.logger.exception(f"Ingestion failed: {file_path}")
            result.add_error(str(e))
        
        return result
    
    # =========================================================================
    # Timestamp Processing
    # =========================================================================
    
    def _process_timestamp(
            self,
            collection: 'Collection',
            plugin,
            local_path: Path,
            timestamp: datetime,
            source_file: str,
            clipper: BoundaryClipper,
            reference_time: datetime = None,
    ) -> tuple['Item', list['Asset'], dict]:
        """
        Process all Variables for a single timestamp.
        
        Creates or retrieves one Item, then creates/updates Assets per Variable.
        Handles re-ingestion gracefully by updating existing records.
        
        Returns:
            Tuple of (Item, list of Assets, clip_info dict)
        """
        from georiva.core.models import Item
        
        self.logger.info(f"Processing {collection} @ {timestamp}")
        
        # Create extractor once for this timestamp
        extractor = VariableExtractor(plugin)
        encoder = VariableEncoder()
        writer = AssetWriter(self.storage)
        
        # Get active variables
        variables = list(
            collection.variables.filter(is_active=True).prefetch_related('sources')
        )
        
        if not variables:
            raise ValueError(f"Collection '{collection.slug}' has no active variables")
        
        # Get spatial metadata from first variable
        first_var = variables[0]
        meta = extractor.get_metadata(first_var, local_path, timestamp)
        src_width, src_height = meta['width'], meta['height']
        src_bounds = tuple(meta['bounds'])
        
        # Validate bounds
        if not src_bounds or len(src_bounds) < 4:
            raise ValueError(f"Invalid bounds from metadata: {src_bounds}")
        
        # Compute clip window if clipper is active
        clip_info = {
            'original_size': (src_width, src_height),
            'clipped_size': None,
        }
        clip_window = None
        
        if clipper.is_active:
            try:
                clip_window = clipper.compute_window(src_bounds, src_width, src_height)
                if clip_window:
                    width = clip_window['width']
                    height = clip_window['height']
                    bounds = clip_window['bounds']
                    clip_info['clipped_size'] = (width, height)
                    
                    reduction = 100 * (1 - (width * height) / (src_width * src_height))
                    self.logger.info(
                        f"Clipping: {src_width}x{src_height} → {width}x{height} "
                        f"({reduction:.1f}% reduction)"
                    )
                else:
                    width, height, bounds = src_width, src_height, src_bounds
            except ValueError as e:
                self.logger.warning(f"Clip window failed: {e}, using full extent")
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds
        
        crs = meta.get('crs', collection.crs or 'EPSG:4326')
        
        # Ensure UTC
        ts_utc = self._ensure_utc(timestamp)
        ref_utc = self._ensure_utc(reference_time) if reference_time else None
        
        # Get or create Item (handles re-ingestion)
        item, created = Item.objects.get_or_create(
            collection=collection,
            time=ts_utc,
            reference_time=ref_utc,
            defaults={
                'source_file': source_file,
                'bounds': list(bounds),
                'width': width,
                'height': height,
                'resolution_x': abs((bounds[2] - bounds[0]) / width) if width else 0,
                'resolution_y': abs((bounds[3] - bounds[1]) / height) if height else 0,
                'crs': crs,
            }
        )
        
        if not created:
            self.logger.info(f"Item already exists for {collection} @ {ts_utc}, updating assets")
            # Update item metadata if changed
            update_fields = []
            if item.source_file != source_file:
                item.source_file = source_file
                update_fields.append('source_file')
            if list(item.bounds) != list(bounds):
                item.bounds = list(bounds)
                item.width = width
                item.height = height
                update_fields.extend(['bounds', 'width', 'height'])
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
                self.logger.error(f"Variable {variable.slug} failed: {e}")
                # Continue with other variables
        
        # Update collection extent
        self._update_collection_extent(collection, ts_utc, bounds)
        
        self.logger.info(f"Created Item {item.pk} with {len(assets)} assets")
        
        return item, assets, clip_info
    
    # =========================================================================
    # Variable Processing
    # =========================================================================
    
    def _process_variable(
            self,
            item: 'Item',
            variable: 'Variable',
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
    ) -> list['Asset']:
        """
        Process a single Variable: extract, transform, encode, save.
        
        Returns list of created Assets (visual + data).
        """
        from georiva.core.models import Asset
        
        self.logger.debug(f"Processing variable: {variable.slug}")
        
        # Compute global stats (on clipped region if applicable)
        stats = extractor.compute_stats(variable, local_path, timestamp, window=clip_window)
        
        # Determine if we're using chunked processing or single extraction
        use_chunked = width * height > 4096 * 4096  # Use chunks for large data
        
        if use_chunked and clip_window is None:
            # Chunked processing for large unclipped data
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
            # Direct extraction (with optional clip window)
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
        
        # Apply geometry mask if clipper is active
        if clipper and clipper.is_active:
            final_data = clipper.apply_geometry_mask(final_data, bounds, nodata=np.nan)
            final_rgba = clipper.apply_rgba_mask(final_rgba, bounds)
        
        # Generate output paths
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        date_path = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M%S')
        base_dir = f"processed/{catalog_slug}/{collection_slug}/{variable.slug}/{date_path}"
        base_name = f"{variable.slug}_{time_str}"
        
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
                    'href': stored_png,
                    'media_type': 'image/png',
                    'roles': ['visual'],
                    'file_size': self._get_file_size(stored_png),
                    'width': width,
                    'height': height,
                    'bands': 4,
                    'stats_min': stats.get('min'),
                    'stats_max': stats.get('max'),
                    'stats_mean': stats.get('mean'),
                    'stats_std': stats.get('std'),
                    'extra_fields': {
                        'imageUnscale': [
                            variable.value_min,
                            variable.value_max
                        ],
                        'scale': variable.scale_type or 'linear',
                    },
                }
            )
            assets.append(visual_asset)
        
        except Exception as e:
            self.logger.error(f"PNG save failed for {variable.slug}: {e}")
        
        # Save COG (data asset)
        cog_path = f"{base_dir}/{base_name}.tif"
        try:
            stored_cog = writer.write_cog(final_data, cog_path, bounds, crs)
            
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults={
                    'href': stored_cog,
                    'media_type': 'image/tiff; application=geotiff; profile=cloud-optimized',
                    'roles': ['data'],
                    'file_size': self._get_file_size(stored_cog),
                    'width': width,
                    'height': height,
                    'bands': 1,
                    'stats_min': stats.get('min'),
                    'stats_max': stats.get('max'),
                    'stats_mean': stats.get('mean'),
                    'stats_std': stats.get('std'),
                    'extra_fields': {
                        'compression': 'deflate',
                        'nodata': None
                    },
                }
            )
            assets.append(data_asset)
        
        except Exception as e:
            self.logger.error(f"COG save failed for {variable.slug}: {e}")
        
        # Save metadata JSON
        meta_path = f"{base_dir}/{base_name}.json"
        try:
            metadata = {
                'variable': variable.slug,
                'name': variable.name,
                'units': variable.units or '',
                'timestamp': timestamp.isoformat(),
                'reference_time': item.reference_time.isoformat() if item.reference_time else None,
                'bounds': list(bounds),
                'width': width,
                'height': height,
                'crs': crs,
                'transform': variable.transform_type,
                'imageUnscale': [
                    variable.value_min,
                    variable.value_max
                ],
                'scale': variable.scale_type or 'linear',
                'stats': stats,
            }
            
            if visual_asset:
                metadata['color_map'] = visual_asset.weather_layers_palette
            
            writer.write_metadata(metadata, meta_path)
        
        except Exception as e:
            self.logger.warning(f"Metadata save failed for {variable.slug}: {e}")
        
        # Cleanup
        del final_data, final_rgba
        gc.collect()
        
        return assets
    
    def _process_variable_chunked(
            self,
            variable: 'Variable',
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
            
            # Extract chunk
            chunk = extractor.extract(variable, local_path, timestamp, window)
            
            # Apply unit conversion
            chunk = apply_unit_conversion(chunk, variable.unit_conversion)
            
            # Store raw data
            final_data[y:y + h, x:x + w] = chunk
            
            # Encode for PNG
            rgba_chunk = encoder.encode_to_rgba(chunk, variable)
            final_rgba[y:y + h, x:x + w] = rgba_chunk
            
            del chunk, rgba_chunk
        
        return final_data, final_rgba
    
    def _process_variable_direct(
            self,
            variable: 'Variable',
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
        
        # Build extraction window
        if clip_window:
            window = (
                clip_window['x_off'],
                clip_window['y_off'],
                clip_window['width'],
                clip_window['height'],
            )
        else:
            window = None
        
        # Extract full region at once
        final_data = extractor.extract(variable, local_path, timestamp, window)
        
        # Apply unit conversion
        final_data = apply_unit_conversion(final_data, variable.unit_conversion)
        
        # Encode to RGBA
        final_rgba = encoder.encode_to_rgba(final_data, variable)
        
        return final_data, final_rgba
    
    # =========================================================================
    # Collection Updates
    # =========================================================================
    
    def _update_collection_extent(
            self,
            collection: 'Collection',
            timestamp: datetime,
            bounds: tuple,
    ):
        """Update collection's temporal and spatial extent."""
        from georiva.core.models import Item
        
        update_fields = []
        
        # Time range
        if collection.time_start is None or timestamp < collection.time_start:
            collection.time_start = timestamp
            update_fields.append('time_start')
        
        if collection.time_end is None or timestamp > collection.time_end:
            collection.time_end = timestamp
            update_fields.append('time_end')
        
        # Spatial bounds (expand to encompass new data)
        current = collection.bounds
        if not current or len(current) < 4:
            collection.bounds = list(bounds)
            update_fields.append('bounds')
        else:
            expanded = [
                min(current[0], bounds[0]),
                min(current[1], bounds[1]),
                max(current[2], bounds[2]),
                max(current[3], bounds[3]),
            ]
            if expanded != current:
                collection.bounds = expanded
                update_fields.append('bounds')
        
        # Item count
        collection.item_count = Item.objects.filter(collection=collection).count()
        update_fields.append('item_count')
        
        if update_fields:
            collection.save(update_fields=update_fields)
    
    # =========================================================================
    # Helpers
    # =========================================================================
    
    def _ensure_utc(self, dt) -> Optional[datetime]:
        """Ensure datetime is UTC, handling strings and naive datetimes."""
        if dt is None:
            return None
        
        # Handle string timestamps
        if isinstance(dt, str):
            dt = pd.Timestamp(dt).to_pydatetime()
        
        # Handle pandas Timestamp
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        
        # Handle numpy datetime64
        if isinstance(dt, np.datetime64):
            dt = pd.Timestamp(dt).to_pydatetime()
        
        if dt.tzinfo is None:
            return pytz.utc.localize(dt)
        
        return dt.astimezone(pytz.utc)
    
    def _get_file_size(self, path: str) -> Optional[int]:
        """Get file size from storage."""
        try:
            return self.storage.size(path)
        except Exception:
            return None
    
    def _download_to_temp(self, file_path: str) -> Path:
        """Download file from storage to local temp."""
        suffix = Path(file_path).suffix
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        with self.storage.open(file_path, 'rb') as src:
            tmp_path.write_bytes(src.read())
        
        return tmp_path
    
    def _cleanup_temp(self, path: Path):
        """Remove temp file."""
        try:
            path.unlink(missing_ok=True)
        except Exception as e:
            self.logger.warning(f"Temp cleanup failed: {path} - {e}")
    
    def _archive_source(self, file_path: str, catalog: str, collection: str):
        """Archive processed source file."""
        name = Path(file_path).name
        archive_path = f"archive/{catalog}/{collection}/{name}"
        try:
            self.storage.move(file_path, archive_path)
        except Exception as e:
            self.logger.warning(f"Archive failed: {file_path} - {e}")
    
    def _infer_from_path(self, file_path: str) -> dict:
        """
        Infer catalog/collection from path.
        
        Expected: incoming/{catalog}/{collection}/...
        """
        parts = Path(file_path).parts
        result = {'catalog': None, 'collection': None}
        
        if 'incoming' in parts:
            idx = parts.index('incoming')
            if idx + 1 < len(parts):
                result['catalog'] = parts[idx + 1]
            if idx + 2 < len(parts):
                result['collection'] = parts[idx + 2]
        
        return result
