import io
import json
import logging
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image

from georiva.core.storage import Bucket

logger = logging.getLogger(__name__)


class AssetWriter:
    """
    Writes processed data to storage as various formats.
    """
    
    def __init__(self, bucket: Bucket):
        self.bucket = bucket
        self.logger = logging.getLogger("georiva.writer")
    
    def write_png(
            self,
            rgba: np.ndarray,
            output_path: str,
    ) -> str:
        """Write RGBA array as PNG."""
        image = Image.fromarray(rgba, mode='RGBA')
        
        buffer = io.BytesIO()
        image.save(buffer, format='PNG', optimize=True)
        buffer.seek(0)
        
        return self.bucket.save(output_path, buffer.read())
    
    def write_cog(
            self,
            data: np.ndarray,
            output_path: str,
            bounds: tuple,
            crs: str = "EPSG:4326",
            nodata: float = None,
    ) -> str:
        """
        Write data as Cloud-Optimized GeoTIFF.
        
        Args:
            data: 2D float32 array
            output_path: Storage path
            bounds: (west, south, east, north)
            crs: Coordinate reference system
            nodata: NoData value (default: NaN)
        
        Returns:
            Final storage path
        """
        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.enums import Resampling
        
        height, width = data.shape
        minx, miny, maxx, maxy = bounds
        transform = from_bounds(minx, miny, maxx, maxy, width, height)
        
        profile = {
            'driver': 'GTiff',
            'dtype': 'float32',
            'width': width,
            'height': height,
            'count': 1,
            'crs': crs,
            'transform': transform,
            'nodata': nodata if nodata is not None else np.nan,
            'compress': 'deflate',
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256,
        }
        
        with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            with rasterio.open(tmp_path, 'w', **profile) as dst:
                dst.write(data.astype(np.float32), 1)
                
                # Build overviews for COG
                overview_levels = []
                level = 2
                while min(width, height) // level >= 256:
                    overview_levels.append(level)
                    level *= 2
                
                if overview_levels:
                    dst.build_overviews(overview_levels, Resampling.average)
                    dst.update_tags(ns='rio_overview', resampling='average')
            
            with open(tmp_path, 'rb') as f:
                return self.bucket.save(output_path, f)
        
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    
    def write_metadata(
            self,
            metadata: dict,
            output_path: str,
    ) -> str:
        """Write metadata as JSON."""
        content = json.dumps(metadata, indent=2).encode('utf-8')
        return self.bucket.save(output_path, content)
