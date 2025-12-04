"""
GeoRiva Ingestion Service

Processes incoming geospatial files through a pipeline:
   - Extract data from source files (GRIB, NetCDF, GeoTIFF)
   - Apply unit conversions
   - Encode to PNG (0-255)
   - Save PNG + metadata to storage
   - Create Item and Asset records
"""

import gc
import io
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

import numpy as np
from PIL import Image

from georiva.core.models import Collection, Dataset
from georiva.core.models.item import Item, Asset
from georiva.formats.registry import format_registry, ExtractedVariable

logger = logging.getLogger(__name__)


def get_windows(width: int, height: int, block_size: int = 2048) -> Generator[tuple, None, None]:
    """
    Yields window tuples (x_off, y_off, x_size, y_size) for chunked processing.
    """
    for y in range(0, height, block_size):
        y_size = min(block_size, height - y)
        for x in range(0, width, block_size):
            x_size = min(block_size, width - x)
            yield x, y, x_size, y_size


@dataclass
class IngestionResult:
    """Result of processing a single incoming file."""
    source_file: str
    collection_id: str
    success: bool
    timestamp: datetime
    datasets_processed: list = field(default_factory=list)
    items_created: list = field(default_factory=list)
    errors: list = field(default_factory=list)


@dataclass
class ProcessingContext:
    """Context for processing data (either a chunk or full dataset)."""
    dataset: Dataset
    timestamp: datetime
    
    # extracted is optional now as we might create context just for saving
    extracted: Optional[ExtractedVariable] = None
    
    # Working data
    data: np.ndarray = None
    companion_data: np.ndarray = None
    mask: np.ndarray = None
    
    # Spatial info
    bounds: tuple = None
    width: int = 0
    height: int = 0
    crs: str = "EPSG:4326"
    
    # Statistics (Global stats passed down to chunks)
    stats_min: float = None
    stats_max: float = None
    stats_mean: float = None
    stats_std: float = None
    stats_speed_max: float = None  # For vector data
    
    # Encoded output
    encoded: np.ndarray = None
    
    # Output paths
    output_png: str = None
    output_metadata: str = None


class IngestionService:
    """
    Service for ingesting and processing geospatial data.
    Uses chunked processing to handle large files with low memory usage.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("georiva.ingestion")
    
    @property
    def storage(self):
        """Get the storage manager."""
        from georiva.core.storage import storage_manager
        return storage_manager
    
    def process_incoming_file(
            self,
            file_path: str,
            collection_id: Optional[str] = None,
    ) -> IngestionResult:
        """
        Process a single incoming file through the complete pipeline.
        """
        self.logger.info(f"Processing incoming file: {file_path}")
        
        result = IngestionResult(
            source_file=file_path,
            collection_id=collection_id or '',
            success=False,
            timestamp=datetime.now(),
        )
        
        try:
            # Infer collection from path if not provided
            if not collection_id:
                collection_id = self._infer_collection_from_path(file_path)
                result.collection_id = collection_id
            
            if not collection_id:
                result.errors.append(f"Could not determine collection for {file_path}")
                return result
            
            # Get collection
            try:
                collection = Collection.objects.get(id=collection_id)
            except Collection.DoesNotExist:
                result.errors.append(f"Collection '{collection_id}' not found")
                return result
            
            if not collection.is_active:
                result.errors.append(f"Collection '{collection_id}' is not active")
                return result
            
            # Get format plugin
            format_plugin = format_registry.get(collection.file_format)
            if not format_plugin:
                result.errors.append(
                    f"No format plugin found for '{collection.file_format}'"
                )
                return result
            
            # Download file to local temp for processing
            local_path = self._download_to_temp(file_path)
            
            try:
                # Get timestamps in the file
                timestamps = format_plugin.get_timestamps(local_path)
                if not timestamps:
                    result.errors.append(f"No timestamps found in file {file_path}")
                    return result
                
                self.logger.info(f"Found {len(timestamps)} timestamps in file {file_path}")
                
                # Process each active dataset
                for dataset in collection.datasets.filter(is_active=True):
                    try:
                        items = self._process_dataset(
                            dataset=dataset,
                            format_plugin=format_plugin,
                            local_path=local_path,
                            timestamps=timestamps,
                            source_file=file_path,
                        )
                        
                        result.datasets_processed.append(dataset.id)
                        result.items_created.extend([str(item.pk) for item in items])
                    
                    except Exception as e:
                        error_msg = f"Failed to process dataset '{dataset.id}': {e}"
                        self.logger.exception(error_msg)
                        result.errors.append(error_msg)
                
                result.success = len(result.datasets_processed) > 0
            
            finally:
                # Clean up temp file
                self._cleanup_temp(local_path)
            
            # Archive or delete the source file
            if result.success:
                self._archive_source_file(file_path, collection_id)
        
        except Exception as e:
            self.logger.exception(f"Ingestion failed for {file_path}")
            result.errors.append(str(e))
        
        return result
    
    def _process_dataset(
            self,
            dataset: Dataset,
            format_plugin,
            local_path: Path,
            timestamps: list[datetime],
            source_file: str,
    ) -> list[Item]:
        """
        Process a single dataset using chunked reading.
        
        Returns list of created Item objects.
        """
        items = []
        
        for timestamp in timestamps:
            self.logger.info(f"Processing {dataset.slug} for {timestamp} (Chunked)")
            
            # 1. Light extraction (Metadata only)
            meta_info = format_plugin.get_metadata(local_path, dataset)
            full_width, full_height = meta_info['width'], meta_info['height']
            crs = meta_info.get('crs', 'EPSG:4326')
            bounds = meta_info['bounds']
            
            # 2. Compute Global Stats (Lazy Load)
            global_stats = self._compute_global_stats(
                format_plugin, local_path, dataset, timestamp
            )
            
            # 3. Pre-allocate the final output image
            final_encoded = np.zeros((full_height, full_width, 4), dtype=np.uint8)
            
            # 4. Process in Chunks
            for x, y, w, h in get_windows(full_width, full_height, block_size=2048):
                window = (x, y, w, h)
                
                # Extract ONLY this chunk
                chunk_extracted = format_plugin.extract_dataset(
                    file_path=local_path,
                    dataset=dataset,
                    timestamp=timestamp,
                    window=window
                )
                
                # Create a context for this chunk
                ctx = ProcessingContext(
                    dataset=dataset,
                    extracted=chunk_extracted,
                    timestamp=timestamp,
                    data=chunk_extracted.data,
                    companion_data=chunk_extracted.secondary_data,
                    bounds=chunk_extracted.bounds,
                    width=w,
                    height=h,
                    crs=crs,
                    # Pass down the global stats
                    stats_min=global_stats['min'],
                    stats_max=global_stats['max'],
                    stats_speed_max=global_stats.get('speed_max')
                )
                
                # Run pipeline on chunk
                ctx = self._process_chunk_pipeline(ctx)
                
                # Write chunk result into main buffer
                final_encoded[y:y + h, x:x + w] = ctx.encoded
                
                # Explicit cleanup to free memory immediately
                del ctx
                del chunk_extracted
            
            # Force GC periodically
            gc.collect()
            
            # 5. Save the assembled image and create Item with Assets
            output_ctx = ProcessingContext(
                dataset=dataset,
                timestamp=timestamp,
                encoded=final_encoded,
                width=full_width,
                height=full_height,
                bounds=bounds,
                crs=crs,
                stats_min=global_stats['min'],
                stats_max=global_stats['max'],
                stats_mean=global_stats['mean'],
                stats_std=global_stats['std'],
                stats_speed_max=global_stats.get('speed_max')
            )
            
            # Save files to storage
            output_ctx = self._save(output_ctx)
            
            # Create Item record
            item = self._create_item(
                dataset=dataset,
                timestamp=timestamp,
                source_file=source_file,
                ctx=output_ctx,
            )
            
            items.append(item)
            
            # Cleanup final buffer
            del final_encoded
            gc.collect()
            
            self.logger.info(f"Created Item {item.pk} for {dataset.slug} @ {timestamp}")
        
        return items
    
    def _create_item(
            self,
            dataset: Dataset,
            timestamp: datetime,
            source_file: str,
            ctx: ProcessingContext,
    ) -> Item:
        """
        Create an Item with its associated Assets.
        """
        # Calculate resolution
        bounds = ctx.bounds
        resolution_x = (bounds[2] - bounds[0]) / ctx.width if ctx.width > 0 else 0
        resolution_y = (bounds[3] - bounds[1]) / ctx.height if ctx.height > 0 else 0
        
        # Create Item (TimescaleDB hypertable)
        # 'time' field comes from TimescaleModel
        item = Item(
            time=timestamp,
            dataset=dataset,
            source_file=source_file,
            bounds=list(bounds),
            width=ctx.width,
            height=ctx.height,
            resolution_x=abs(resolution_x),
            resolution_y=abs(resolution_y),
            # crs=ctx.crs,
            stats_min=ctx.stats_min,
            stats_max=ctx.stats_max,
            stats_mean=ctx.stats_mean,
            stats_std=ctx.stats_std,
            metadata={
                'variable_name': dataset.primary_variable,
                'units': dataset.units,
                'is_vector': dataset.is_vector,
            },
        )
        
        # Save Item first to get PK
        item.save()
        
        # Compute file size
        png_size = self._get_file_size(ctx.output_png)
        
        # Create Visual Asset (PNG)
        visual_asset = Asset(
            item=item,
            key='visual',
            title=f"{dataset.name} - {timestamp.strftime('%Y-%m-%d %H:%M')}",
            href=ctx.output_png,
            media_type='image/png',
            file_size=png_size,
            roles=[Asset.Role.VISUAL],
            format=Asset.Format.PNG,
            data_type=Asset.DataType.UINT8,
            width=ctx.width,
            height=ctx.height,
            bands=4,  # RGBA
            proj_epsg=self._parse_epsg(ctx.crs),
            proj_bbox=list(bounds),
            extra_fields={
                'imageUnscale': [
                    float(dataset.value_min) if dataset.value_min is not None else ctx.stats_min,
                    float(dataset.value_max) if dataset.value_max is not None else ctx.stats_max,
                ],
                'scale': dataset.scale_type or 'linear',
                'palette': dataset.palette.name if dataset.palette else None,
            },
        )
        visual_asset.save()
        
        # Create Metadata Asset (JSON)
        if ctx.output_metadata:
            json_size = self._get_file_size(ctx.output_metadata)
            metadata_asset = Asset(
                item=item,
                key='metadata',
                title=f"Metadata for {dataset.name}",
                href=ctx.output_metadata,
                media_type='application/json',
                file_size=json_size,
                roles=[Asset.Role.METADATA],
                format=Asset.Format.JSON,
            )
            metadata_asset.save()
        
        # Update dataset time range
        self._update_dataset_time_range(dataset, timestamp)
        
        return item
    
    def _get_file_size(self, storage_path: str) -> Optional[int]:
        """Get file size from storage, return None if not available."""
        try:
            return self.storage.size(storage_path)
        except Exception:
            return None
    
    def _update_dataset_time_range(self, dataset: Dataset, timestamp: datetime):
        """Update the dataset's time_start and time_end fields."""
        update_fields = []
        
        if dataset.time_start is None or timestamp < dataset.time_start:
            dataset.time_start = timestamp
            update_fields.append('time_start')
        
        if dataset.time_end is None or timestamp > dataset.time_end:
            dataset.time_end = timestamp
            update_fields.append('time_end')
        
        # Increment item count
        dataset.item_count = Item.objects.filter(dataset=dataset).count()
        update_fields.append('item_count')
        
        if update_fields:
            dataset.save(update_fields=update_fields)
    
    def _parse_epsg(self, crs: str) -> Optional[int]:
        """Extract EPSG code from CRS string."""
        if not crs:
            return None
        if crs.upper().startswith('EPSG:'):
            try:
                return int(crs.split(':')[1])
            except (IndexError, ValueError):
                return None
        return None
    
    def _compute_global_stats(self, plugin, path, dataset, timestamp) -> dict:
        """
        Compute stats efficiently without loading full data into RAM.
        Uses the lazy loading capabilities of the format plugins.
        """
        # Get lazy object (dask-backed xarray)
        lazy_var = plugin.get_lazy_dataset(path, dataset, timestamp)
        
        # Apply unit conversions on the lazy object
        if dataset.unit_conversion == 'K_to_C':
            lazy_var = lazy_var - 273.15
        elif dataset.unit_conversion == 'Pa_to_hPa':
            lazy_var = lazy_var * 0.01
        elif dataset.unit_conversion == 'm_to_mm':
            lazy_var = lazy_var * 1000.0
        elif dataset.unit_conversion == 'ms_to_kmh':
            lazy_var = lazy_var * 3.6
        elif dataset.unit_conversion == 'kgm2s_to_mm':
            lazy_var = lazy_var * 3600.0
        
        # Compute stats - ensure Python floats
        stats = {
            'min': float(lazy_var.min().compute()),
            'max': float(lazy_var.max().compute()),
            'mean': float(lazy_var.mean().compute()),
            'std': float(lazy_var.std().compute()),
        }
        
        return stats
    
    def _process_chunk_pipeline(self, ctx: ProcessingContext) -> ProcessingContext:
        """Simplified pipeline for a single chunk."""
        
        # 1. Validation
        if ctx.data is None:
            raise ValueError("Chunk has no data")
        
        # 2. Ensure data is float32 for processing
        ctx.data = np.asarray(ctx.data, dtype=np.float32)
        if ctx.companion_data is not None:
            ctx.companion_data = np.asarray(ctx.companion_data, dtype=np.float32)
        
        # 3. Masking
        ctx.mask = np.isnan(ctx.data)
        if ctx.companion_data is not None:
            ctx.mask |= np.isnan(ctx.companion_data)
        
        # 4. Unit Conversion (In-Place)
        ctx = self._apply_unit_conversions(ctx)
        
        # 5. Encode (using global stats stored in ctx)
        ctx = self._encode(ctx)
        
        return ctx
    
    def _apply_unit_conversions(self, ctx: ProcessingContext) -> ProcessingContext:
        """Apply unit conversions IN-PLACE to save memory."""
        unit_conversion = ctx.dataset.unit_conversion
        if not unit_conversion:
            return ctx
        
        if unit_conversion == 'K_to_C':
            ctx.data -= 273.15
        elif unit_conversion == 'Pa_to_hPa':
            ctx.data *= 0.01
        elif unit_conversion == 'm_to_mm':
            ctx.data *= 1000.0
        elif unit_conversion == 'ms_to_kmh':
            ctx.data *= 3.6
        elif unit_conversion == 'kgm2s_to_mm':
            ctx.data *= 3600.0
        
        return ctx
    
    def _encode(self, ctx: ProcessingContext) -> ProcessingContext:
        """Encode data to PNG format (0-255)."""
        if ctx.dataset.is_vector:
            return self._encode_vector(ctx)
        else:
            return self._encode_scalar(ctx)
    
    def _encode_scalar(self, ctx: ProcessingContext) -> ProcessingContext:
        """Encode scalar data to RGBA PNG using in-place operations."""
        dataset = ctx.dataset
        
        # Get value bounds - prioritize dataset config, fall back to computed stats
        vmin = dataset.value_min
        vmax = dataset.value_max
        
        # Fall back to computed stats if dataset bounds not set
        if vmin is None:
            vmin = ctx.stats_min
        if vmax is None:
            vmax = ctx.stats_max
        
        # Final fallback to data range if still None
        if vmin is None:
            vmin = np.nanmin(ctx.data)
        if vmax is None:
            vmax = np.nanmax(ctx.data)
        
        # CRITICAL: Cast to Python float to avoid dtype('O') issues
        vmin = float(vmin)
        vmax = float(vmax)
        
        scale_type = dataset.scale_type or 'linear'
        
        # Work on data - ensure float32
        data = ctx.data
        if data.dtype != np.float32:
            data = data.astype(np.float32)
        
        # Handle edge case where vmin == vmax
        if vmax <= vmin:
            vmax = vmin + 1.0
        
        # Apply scaling
        if scale_type == 'log':
            shift = 1.0 - min(0.0, vmin)
            np.clip(data, vmin, vmax, out=data)
            data += shift
            np.log10(data, out=data)
            
            log_min = np.log10(vmin + shift)
            log_max = np.log10(vmax + shift)
            
            data -= log_min
            denom = log_max - log_min
            if denom > 0:
                data /= denom
        
        elif scale_type == 'sqrt':
            np.clip(data, max(0.0, vmin), vmax, out=data)
            np.sqrt(data, out=data)
            
            sqrt_min = np.sqrt(max(0.0, vmin))
            sqrt_max = np.sqrt(vmax)
            
            data -= sqrt_min
            denom = sqrt_max - sqrt_min
            if denom > 0:
                data /= denom
        
        elif scale_type == 'diverging':
            abs_max = max(abs(vmin), abs(vmax))
            if abs_max > 0:
                data += abs_max
                data /= (2.0 * abs_max)
            else:
                data[:] = 0.5
        
        else:  # linear
            data -= vmin
            denom = vmax - vmin
            if denom > 0:
                data /= denom
        
        # Convert to 0-255
        data *= 255.0
        np.clip(data, 0, 255, out=data)
        
        # Create RGBA image
        ctx.encoded = np.zeros((ctx.height, ctx.width, 4), dtype=np.uint8)
        ctx.encoded[:, :, 0] = data.astype(np.uint8)
        
        # Alpha channel: 255 for valid, 0 for masked
        ctx.encoded[:, :, 3] = np.where(ctx.mask, 0, 255).astype(np.uint8)
        
        # Clean up heavy float array
        del ctx.data
        ctx.data = None
        
        return ctx
    
    def _encode_vector(self, ctx: ProcessingContext) -> ProcessingContext:
        """
        Encode vector (U/V) data to RGBA PNG.
        R = magnitude (normalized)
        G = direction (normalized 0-360 -> 0-255)
        A = alpha mask
        """
        dataset = ctx.dataset
        
        # Get max speed for normalization
        max_speed = dataset.value_max
        if max_speed is None:
            max_speed = ctx.stats_speed_max
        if max_speed is None:
            u = ctx.data.astype(np.float32)
            v = ctx.companion_data.astype(np.float32)
            max_speed = float(np.nanmax(np.hypot(u, v)))
        
        max_speed = float(max_speed)
        if max_speed <= 0:
            max_speed = 1.0
        
        # Cast to float32
        u = ctx.data.astype(np.float32, copy=False)
        v = ctx.companion_data.astype(np.float32, copy=False)
        
        # Calculate Magnitude
        magnitude = np.hypot(u, v)
        
        # Calculate Direction
        direction = np.arctan2(u, v)
        np.degrees(direction, out=direction)
        direction += 360.0
        np.mod(direction, 360.0, out=direction)
        
        # Free inputs
        del ctx.data
        del ctx.companion_data
        ctx.data = None
        ctx.companion_data = None
        
        # Normalize magnitude
        magnitude /= max_speed
        magnitude *= 255.0
        np.clip(magnitude, 0, 255, out=magnitude)
        
        # Normalize direction
        direction /= 360.0
        direction *= 255.0
        
        # Output
        ctx.encoded = np.zeros((ctx.height, ctx.width, 4), dtype=np.uint8)
        ctx.encoded[:, :, 0] = magnitude.astype(np.uint8)
        ctx.encoded[:, :, 1] = direction.astype(np.uint8)
        ctx.encoded[:, :, 3] = np.where(ctx.mask, 0, 255).astype(np.uint8)
        
        return ctx
    
    def _save(self, ctx: ProcessingContext) -> ProcessingContext:
        """Save processed PNG and metadata to storage."""
        # Generate output path
        date_path = ctx.timestamp.strftime('%Y/%m/%d')
        time_str = ctx.timestamp.strftime('%H%M%S')
        
        # Use dataset slug for path
        output_dir = f"processed/{ctx.dataset.collection.slug}/{ctx.dataset.slug}/{date_path}"
        output_path = f"{output_dir}/{ctx.dataset.slug}_{time_str}.png"
        
        # Save PNG
        image = Image.fromarray(ctx.encoded, mode='RGBA')
        ctx.output_png = self._save_png(output_path, image)
        
        # Build metadata
        metadata = {
            'type': ctx.dataset.variable_type,
            'display_name': ctx.dataset.name,
            'units': ctx.dataset.units or '',
            'timestamp': ctx.timestamp.isoformat(),
            'bounds': list(ctx.bounds) if ctx.bounds else None,
            'width': ctx.width,
            'height': ctx.height,
            'crs': ctx.crs,
            'imageUnscale': [
                float(ctx.dataset.value_min) if ctx.dataset.value_min is not None else ctx.stats_min,
                float(ctx.dataset.value_max) if ctx.dataset.value_max is not None else ctx.stats_max,
            ],
            'scale': ctx.dataset.scale_type or 'linear',
            'stats': {
                'min': float(ctx.stats_min) if ctx.stats_min is not None else None,
                'max': float(ctx.stats_max) if ctx.stats_max is not None else None,
                'mean': float(ctx.stats_mean) if ctx.stats_mean is not None else None,
                'std': float(ctx.stats_std) if ctx.stats_std is not None else None,
            },
        }
        
        if ctx.dataset.is_vector:
            metadata['stats']['speed_max'] = float(ctx.stats_speed_max) if ctx.stats_speed_max else None
            metadata['direction_range'] = [0, 360]
        
        # Save metadata
        meta_path = output_path.replace('.png', '.json')
        ctx.output_metadata = self._save_json(meta_path, metadata)
        
        return ctx
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _save_png(self, path: str, image: Image.Image) -> str:
        """Save PIL Image as PNG to storage."""
        buffer = io.BytesIO()
        image.save(buffer, format='PNG', optimize=True)
        buffer.seek(0)
        return self.storage.save_bytes(path, buffer.read())
    
    def _save_json(self, path: str, data: dict) -> str:
        """Save dict as JSON to storage."""
        content = json.dumps(data, indent=2, default=str).encode('utf-8')
        return self.storage.save_bytes(path, content)
    
    def _infer_collection_from_path(self, file_path: str) -> Optional[str]:
        """Infer collection ID from the incoming file path."""
        parts = file_path.split('/')
        if len(parts) >= 2 and parts[0] == 'incoming':
            return parts[1]
        return None
    
    def _download_to_temp(self, storage_path: str) -> Path:
        """Download a file from storage to local temp."""
        import tempfile
        
        ext = Path(storage_path).suffix
        fd, tmp_path = tempfile.mkstemp(suffix=ext)
        
        try:
            content = self.storage.read_bytes(storage_path)
            with open(tmp_path, 'wb') as f:
                f.write(content)
        except Exception:
            import os
            os.close(fd)
            os.unlink(tmp_path)
            raise
        
        import os
        os.close(fd)
        
        return Path(tmp_path)
    
    def _cleanup_temp(self, local_path: Path):
        """Remove a temporary file."""
        try:
            local_path.unlink()
        except Exception as e:
            self.logger.warning(f"Failed to clean up temp file {local_path}: {e}")
    
    def _archive_source_file(self, file_path: str, collection_id: str):
        """Move source file to archive location."""
        filename = Path(file_path).name
        date_path = datetime.now().strftime('%Y/%m')
        archive_path = f"archive/{collection_id}/{date_path}/{filename}"
        
        try:
            self.storage.move(file_path, archive_path)
            self.logger.info(f"Archived {file_path} to {archive_path}")
        except Exception as e:
            self.logger.warning(f"Failed to archive {file_path}: {e}")
