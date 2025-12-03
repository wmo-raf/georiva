"""
GeoRiva Ingestion Service

Pipeline that handles the complete flow from incoming file to processed output
using chunked processing to minimize memory footprint.

1. File arrives in incoming/{collection_id}/
2. Match to Collection, load Format Plugin
3. For each Dataset in Collection:
   - Extract variable(s) from source file
   - Validate data
   - Clip to bounds (if specified)
   - Compute statistics
   - Apply unit conversions
   - Encode to PNG (0-255)
   - Save PNG + metadata to storage
   - Create RasterAsset record
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
    assets_created: list = field(default_factory=list)
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
                
                # Process each active dataset
                for dataset in collection.datasets.filter(is_active=True):
                    try:
                        assets = self._process_dataset(
                            dataset=dataset,
                            format_plugin=format_plugin,
                            local_path=local_path,
                            timestamps=timestamps,
                        )
                        
                        result.datasets_processed.append(dataset.id)
                        result.assets_created.extend([str(a.id) for a in assets])
                    
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
    ) -> list:
        """
        Process a single dataset using chunked reading.
        """
        from georiva.core.models import RasterFileAsset
        
        assets = []
        
        for timestamp in timestamps:
            self.logger.info(f"Processing {dataset.id} for {timestamp} (Chunked)")
            
            # 1. Light extraction (Metadata only)
            # This uses the new get_metadata method we added to plugins
            meta_info = format_plugin.get_metadata(local_path, dataset)
            full_width, full_height = meta_info['width'], meta_info['height']
            
            # 2. Compute Global Stats (Lazy Load)
            # We must compute global min/max first to ensure consistent coloring across chunks
            global_stats = self._compute_global_stats(
                format_plugin, local_path, dataset, timestamp
            )
            
            # 3. Pre-allocate the final output image
            # RGBA (4 bytes) is much smaller than float64 inputs
            final_encoded = np.zeros((full_height, full_width, 4), dtype=np.uint8)
            
            # 4. Process in Chunks
            # Iterate through the image in manageable blocks
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
                    data=chunk_extracted.data,  # Takes ownership (no copy)
                    companion_data=chunk_extracted.secondary_data,
                    bounds=chunk_extracted.bounds,
                    width=w,
                    height=h,
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
            
            # Force GC periodically (good for tight memory)
            gc.collect()
            
            # 5. Save the assembled image and create Asset
            # Create a context just for saving metadata
            output_ctx = ProcessingContext(
                dataset=dataset,
                timestamp=timestamp,
                encoded=final_encoded,
                width=full_width,
                height=full_height,
                bounds=meta_info['bounds'],
                stats_min=global_stats['min'],
                stats_max=global_stats['max'],
                stats_mean=global_stats['mean'],
                stats_std=global_stats['std'],
                stats_speed_max=global_stats.get('speed_max')
            )
            
            output_ctx = self._save(output_ctx)
            
            # Create RasterAsset record
            asset = RasterFileAsset.objects.create(
                dataset=dataset,
                time=timestamp,
                source_file=str(local_path),
                processed_file=output_ctx.output_png,
                bounds=list(output_ctx.bounds),
                width=output_ctx.width,
                height=output_ctx.height,
                resolution_x=(output_ctx.bounds[2] - output_ctx.bounds[0]) / output_ctx.width,
                resolution_y=(output_ctx.bounds[3] - output_ctx.bounds[1]) / output_ctx.height,
                crs=meta_info['crs'],
                status=RasterFileAsset.Status.READY,
                stats_min=output_ctx.stats_min,
                stats_max=output_ctx.stats_max,
                stats_mean=output_ctx.stats_mean,
                stats_std=output_ctx.stats_std,
                metadata={
                    'variable_name': dataset.primary_variable,
                    'units': dataset.units,
                    'is_vector': dataset.is_vector,
                    'metadata_file': output_ctx.output_metadata,
                },
            )
            
            assets.append(asset)
            
            # Cleanup final buffer
            del final_encoded
            gc.collect()
            
            self.logger.info(f"Created asset {asset.id} for {dataset.id}")
        
        return assets
    
    def _compute_global_stats(self, plugin, path, dataset, timestamp):
        """
        Compute stats efficiently without loading full data into RAM.
        Uses the lazy loading capabilities of the format plugins.
        """
        # Get lazy object (dask-backed xarray)
        lazy_var = plugin.get_lazy_dataset(path, dataset, timestamp)
        
        # Apply unit conversions on the lazy object if possible?
        # Note: Xarray handles lazy math.
        # Ideally, we mirror the _apply_unit_conversions logic here lazily.
        if dataset.unit_conversion == 'K_to_C':
            lazy_var = lazy_var - 273.15
        elif dataset.unit_conversion == 'Pa_to_hPa':
            lazy_var = lazy_var * 0.01
        elif dataset.unit_conversion == 'm_to_mm':
            lazy_var = lazy_var * 1000.0
        elif dataset.unit_conversion == 'ms_to_kmh':
            lazy_var = lazy_var * 3.6
        
        stats = {
            'min': float(lazy_var.min().compute()),
            'max': float(lazy_var.max().compute()),
            'mean': float(lazy_var.mean().compute()),
            'std': float(lazy_var.std().compute()),
        }
        
        return stats
    
    def _process_chunk_pipeline(self, ctx: ProcessingContext) -> ProcessingContext:
        """Simplified pipeline for a single chunk."""
        
        # 1. Validation (Lightweight)
        if ctx.data is None:
            raise ValueError("Chunk has no data")
        
        # 2. Masking
        ctx.mask = np.isnan(ctx.data)
        if ctx.companion_data is not None:
            ctx.mask |= np.isnan(ctx.companion_data)
        
        # 3. Unit Conversion (In-Place)
        # We do this BEFORE encoding so values match the stats
        ctx = self._apply_unit_conversions(ctx)
        
        # 4. Encode (using global stats stored in ctx)
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
        # Use Global Stats if available (from ctx), else dataset defaults
        vmin = dataset.value_min
        vmax = dataset.value_max
        scale_type = dataset.scale_type
        
        # Work on data reference directly
        data = ctx.data
        
        # Apply scaling
        if scale_type == 'log':
            shift = 1 - min(0, vmin)
            # In-place modifications
            np.clip(data, vmin, vmax, out=data)
            data += shift
            np.log10(data, out=data)
            
            log_min = np.log10(vmin + shift)
            log_max = np.log10(vmax + shift)
            
            data -= log_min
            data /= (log_max - log_min)
        
        elif scale_type == 'sqrt':
            np.clip(data, max(0, vmin), vmax, out=data)
            np.sqrt(data, out=data)
            
            sqrt_min = np.sqrt(max(0, vmin))
            sqrt_max = np.sqrt(vmax)
            
            data -= sqrt_min
            data /= (sqrt_max - sqrt_min)
        
        elif scale_type == 'diverging':
            abs_max = max(abs(vmin), abs(vmax))
            data += abs_max
            data /= (2 * abs_max)
        
        else:  # linear
            data -= vmin
            data /= (vmax - vmin)
        
        # Convert to 0-255
        data *= 255
        np.clip(data, 0, 255, out=data)
        
        # Create RGBA image
        ctx.encoded = np.zeros((ctx.height, ctx.width, 4), dtype=np.uint8)
        ctx.encoded[:, :, 0] = data.astype(np.uint8)
        
        # Clean up heavy float array
        del ctx.data
        ctx.data = None
        
        ctx.encoded[:, :, 3] = np.where(ctx.mask, 0, 255).astype(np.uint8)
        
        return ctx
    
    def _encode_vector(self, ctx: ProcessingContext) -> ProcessingContext:
        """
        Encode vector (U/V) data to RGBA PNG.
        Uses float32 to save memory and deletes inputs early.
        """
        dataset = ctx.dataset
        max_speed = dataset.value_max
        
        # Cast to float32 to save 50% RAM compared to float64
        u = ctx.data.astype(np.float32, copy=False)
        v = ctx.companion_data.astype(np.float32, copy=False)
        
        # Calculate Magnitude
        magnitude = np.hypot(u, v)
        
        # Calculate Direction
        direction = np.arctan2(u, v)
        np.degrees(direction, out=direction)
        direction += 360
        np.mod(direction, 360, out=direction)
        
        # Free inputs
        del ctx.data
        del ctx.companion_data
        ctx.data = None
        ctx.companion_data = None
        
        # Normalize
        magnitude /= max_speed
        magnitude *= 255
        np.clip(magnitude, 0, 255, out=magnitude)
        
        direction /= 360
        direction *= 255
        
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
        output_path = f"{ctx.dataset.processed_path}{date_path}/{ctx.dataset.id}_{time_str}.png"
        
        # Save PNG
        image = Image.fromarray(ctx.encoded, mode='RGBA')
        ctx.output_png = self._save_png(output_path, image)
        
        # Build metadata
        metadata = {
            'type': ctx.dataset.variable_type,
            'display_name': ctx.dataset.name,
            'units': ctx.dataset.units or '',
            'timestamp': ctx.timestamp.isoformat(),
            'bounds': list(ctx.bounds),
            'width': ctx.width,
            'height': ctx.height,
            'imageUnscale': [ctx.dataset.value_min, ctx.dataset.value_max],
            'scale': ctx.dataset.scale_type,
            'stats': {
                'min': ctx.stats_min,
                'max': ctx.stats_max,
                'mean': ctx.stats_mean,
                'std': ctx.stats_std,
            },
        }
        
        if ctx.dataset.is_vector:
            metadata['stats']['speed_max'] = ctx.stats_speed_max
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
        content = json.dumps(data, indent=2).encode('utf-8')
        return self.storage.save_bytes(path, content)
    
    def _infer_collection_from_path(self, file_path: str) -> Optional[str]:
        """Infer collection ID from the incoming file path."""
        parts = file_path.split('/')
        if len(parts) >= 2 and parts[0] == 'incoming':
            return parts[1]
        return None
    
    def _download_to_temp(self, s3_path: str) -> Path:
        """Download a file from S3 to local temp storage."""
        import tempfile
        
        ext = Path(s3_path).suffix
        fd, tmp_path = tempfile.mkstemp(suffix=ext)
        
        try:
            content = self.storage.read_bytes(s3_path)
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
