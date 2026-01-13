import gc
import logging
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

import numpy as np
import pytz

from georiva.core.models import Variable, Collection
from .asset_writer import AssetWriter
from .encoder import VariableEncoder
from .extractor import VariableExtractor

logger = logging.getLogger(__name__)

UNIT_CONVERSIONS = {
    'K_to_C': lambda x: x - 273.15,
    'Pa_to_hPa': lambda x: x * 0.01,
    'm_to_mm': lambda x: x * 1000.0,
    'ms_to_kmh': lambda x: x * 3.6,
    'kgm2s_to_mm': lambda x: x * 3600.0,
}


def apply_unit_conversion(data: np.ndarray, conversion: str) -> np.ndarray:
    """Apply unit conversion in-place where possible."""
    if not conversion or conversion not in UNIT_CONVERSIONS:
        return data
    return UNIT_CONVERSIONS[conversion](data)


def iter_windows(
        width: int,
        height: int,
        block_size: int = 2048
) -> Generator[tuple[int, int, int, int], None, None]:
    """
    Yield (x_offset, y_offset, width, height) windows for chunked processing.
    """
    for y in range(0, height, block_size):
        h = min(block_size, height - y)
        for x in range(0, width, block_size):
            w = min(block_size, width - x)
            yield x, y, w, h


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
    
    def add_error(self, msg: str):
        self.errors.append(msg)
        logger.error(msg)


class IngestionService:
    """
    Main service for ingesting geospatial data.
    
    Flow:
    1. Resolve Catalog + Collection from file path
    2. Get format plugin based on Catalog.file_format
    3. Extract timestamps from file
    4. For each timestamp:
       a. Create one Item for the Collection
       b. For each Variable in Collection:
          - Extract + transform source data → single 2D array
          - Apply unit conversion
          - Encode to PNG (visual asset)
          - Write COG (data asset)
          - Create Asset records
    5. Update Collection extent
    6. Archive source file
    """
    
    def __init__(self):
        self.logger = logging.getLogger("georiva.ingestion")
        self._zarr_manager = None
    
    @property
    def storage(self):
        from georiva.core.storage import storage_manager
        return storage_manager
    
    @property
    def zarr_manager(self):
        if self._zarr_manager is None:
            from georiva.core.zarr_manager import ZarrPyramidManager
            self._zarr_manager = ZarrPyramidManager(self.storage)
        return self._zarr_manager
    
    # =========================================================================
    # Main Entry Point
    # =========================================================================
    
    def process_file(
            self,
            file_path: str,
            catalog_slug: str = None,
            collection_slug: str = None,
    ) -> IngestionResult:
        """
        Process an incoming file.
        
        Args:
            file_path: Path to source file in storage
            catalog_slug: Catalog slug (inferred from path if not provided)
            collection_slug: Collection slug (inferred from path if not provided)
        
        Returns:
            IngestionResult with status and created records
        """
        from georiva.core.models import Catalog, Collection
        from georiva.formats.registry import format_registry
        
        self.logger.info(f"Processing: {file_path}")
        
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
                catalog = Catalog.objects.get(slug=catalog_slug, is_active=True)
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
            
            # 4. Download and process
            local_path = self._download_to_temp(file_path)
            
            try:
                timestamps = plugin.get_timestamps(local_path)
                if not timestamps:
                    result.add_error(f"No timestamps found in: {file_path}")
                    return result
                
                self.logger.info(f"Found {len(timestamps)} timestamps")
                
                # 5. Process each timestamp
                for ts in timestamps:
                    try:
                        item, assets = self._process_timestamp(
                            collection=collection,
                            plugin=plugin,
                            local_path=local_path,
                            timestamp=ts,
                            source_file=file_path,
                        )
                        result.items_created.append(str(item.pk))
                        result.assets_created.extend([str(a.pk) for a in assets])
                    
                    except Exception as e:
                        result.add_error(f"Failed at {ts}: {e}")
                
                result.success = len(result.items_created) > 0
            
            finally:
                self._cleanup_temp(local_path)
            
            # 6. Archive if successful
            if result.success and catalog.archive_source_files:
                self._archive_source(file_path, catalog_slug, collection_slug)
        
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
            reference_time: datetime = None,
    ) -> tuple['Item', list['Asset']]:
        """
        Process all Variables for a single timestamp.
        
        Creates or retrieves one Item, then creates/updates Assets per Variable.
        Handles re-ingestion gracefully by updating existing records.
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
        # (All variables in a collection share the same spatial extent)
        first_var = variables[0]
        meta = extractor.get_metadata(first_var, local_path, timestamp)
        width, height = meta['width'], meta['height']
        bounds = tuple(meta['bounds'])
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
            # Optionally update item metadata if source changed
            if item.source_file != source_file:
                item.source_file = source_file
                item.save(update_fields=['source_file'])
        
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
                )
                assets.extend(variable_assets)
            
            except Exception as e:
                self.logger.error(f"Variable {variable.slug} failed: {e}")
                # Continue with other variables
        
        # Update collection extent
        self._update_collection_extent(collection, ts_utc, bounds)
        
        self.logger.info(f"Created Item {item.pk} with {len(assets)} assets")
        
        return item, assets
    
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
    ) -> list['Asset']:
        """
        Process a single Variable: extract, transform, encode, save.
        
        Returns list of created Assets (visual + data).
        """
        from georiva.core.models import Asset
        
        self.logger.debug(f"Processing variable: {variable.slug}")
        
        # Compute global stats first (needed for consistent encoding)
        stats = extractor.compute_stats(variable, local_path, timestamp)
        
        # Allocate output arrays
        final_data = np.zeros((height, width), dtype=np.float32)
        final_rgba = np.zeros((height, width, 4), dtype=np.uint8)
        
        # Process in chunks
        for x, y, w, h in iter_windows(width, height, block_size=2048):
            window = (x, y, w, h)
            
            # Extract + transform → single 2D array
            chunk = extractor.extract(variable, local_path, timestamp, window)
            
            # Apply unit conversion
            chunk = apply_unit_conversion(chunk, variable.unit_conversion)
            
            # Store raw data
            final_data[y:y + h, x:x + w] = chunk
            
            # Encode for PNG
            rgba_chunk = encoder.encode_to_rgba(chunk, variable, stats)
            final_rgba[y:y + h, x:x + w] = rgba_chunk
            
            del chunk, rgba_chunk
        
        # Generate output paths
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        date_path = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M%S')
        base_dir = f"processed/{catalog_slug}/{collection_slug}/{variable.slug}/{date_path}"
        base_name = f"{variable.slug}_{time_str}"
        
        assets = []
        
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
                            variable.value_min if variable.value_min is not None else stats.get('min'),
                            variable.value_max if variable.value_max is not None else stats.get('max'),
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
                'bounds': list(bounds),
                'width': width,
                'height': height,
                'crs': crs,
                'transform': variable.transform_type,
                'imageUnscale': [
                    variable.value_min if variable.value_min is not None else stats.get('min'),
                    variable.value_max if variable.value_max is not None else stats.get('max'),
                ],
                'scale': variable.scale_type or 'linear',
                'stats': stats,
            }
            writer.write_metadata(metadata, meta_path)
        
        except Exception as e:
            self.logger.warning(f"Metadata save failed for {variable.slug}: {e}")
        
        # Update Zarr store
        try:
            self.zarr_manager.append_timestep(
                collection=item.collection,
                variable=variable,
                timestamp=timestamp,
                data=final_data,
                bounds=bounds,
            )
        except Exception as e:
            self.logger.warning(f"Zarr update failed for {variable.slug}: {e}")
        
        # Cleanup
        del final_data, final_rgba
        gc.collect()
        
        return assets
    
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
        if collection.bounds is None:
            collection.bounds = list(bounds)
            update_fields.append('bounds')
        else:
            current = collection.bounds
            expanded = [
                min(current[0], bounds[0]),  # west
                min(current[1], bounds[1]),  # south
                max(current[2], bounds[2]),  # east
                max(current[3], bounds[3]),  # north
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
    
    def _ensure_utc(self, dt: datetime) -> datetime:
        """Ensure datetime is UTC."""
        if dt is None:
            return None
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
