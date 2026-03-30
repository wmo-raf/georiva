import io
import json
import logging
import tempfile
from pathlib import Path

import numpy as np
import rasterio
from PIL import Image
from django.conf import settings
from rasterio.transform import from_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from georiva.core.storage import Bucket

logger = logging.getLogger(__name__)


class AssetWriter:
    """
    Writes processed geospatial data to object storage in multiple formats.

    Handles three asset types per variable per timestamp:
      PNG  — Encoded RGBA for map rendering
      COG  — Cloud-Optimized GeoTIFF for TiTiler serving and analysis
      JSON — Metadata sidecar for API responses and frontend rendering

    COG output is fully adaptive:
      - Block size scales with raster dimensions (country → global)
      - Overview levels are computed from dimensions and block size
      - Compression predictor is derived from data dtype
      - nodata default is derived from data dtype
    """
    
    def __init__(self, bucket: Bucket):
        self.bucket = bucket
        self.logger = logging.getLogger("georiva.writer")
    
    # =========================================================================
    # Public Interface
    # =========================================================================
    
    def write_png(self, rgba: np.ndarray, output_path: str) -> str:
        """
        Write an RGBA numpy array to storage as a PNG.

        Args:
            rgba:        H×W×4 uint8 array in RGBA order.
            output_path: Destination path in the bucket.

        Returns:
            Final stored path.
        """
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
        Write a 2D numpy array to storage as a Cloud-Optimized GeoTIFF.

        The output is fully adaptive to the input data:
          - dtype is derived from data.dtype (float32, int16, uint8 etc)
          - compression predictor follows dtype (3 for floats, 2 for ints)
          - nodata defaults to a sensible value per dtype if not supplied
          - block size and overview levels scale with raster dimensions

        Two-pass approach:
          Pass 1 — write raw data to a temp GeoTIFF (no overviews)
          Pass 2 — cog_translate builds overviews and rewrites in true
                   COG layout (overviews first, full-res last), which
                   minimises HTTP range requests from TiTiler and QGIS.

        Args:
            data:        2D numpy array of any numeric dtype.
            output_path: Destination path in the bucket.
            bounds:      (west, south, east, north) in the given CRS.
            crs:         Coordinate reference system (default EPSG:4326).
            nodata:      NoData value. If None, derived from dtype.

        Returns:
            Final stored path.
        """
        height, width = data.shape
        dtype = data.dtype
        
        transform = from_bounds(*bounds, width, height)
        blocksize = self._blocksize(width, height)
        overview_levels = self._overview_levels(width, height, blocksize)
        _nodata = nodata if nodata is not None else self._default_nodata(dtype)
        _predictor = self._predictor(dtype)
        
        # Copy the shared profile object before mutating — cog_profiles.get()
        # returns a reference to a module-level dict. Mutating it directly
        # would affect all subsequent calls within the same worker process.
        output_profile = cog_profiles.get("deflate").copy()
        output_profile.update({
            "blockxsize": blocksize,
            "blockysize": blocksize,
            "predictor": _predictor,
        })
        
        raw_profile = {
            'driver': 'GTiff',
            'dtype': np.dtype(dtype).name,  # rasterio expects string e.g. 'float32'
            'width': width,
            'height': height,
            'count': 1,
            'crs': crs,
            'transform': transform,
            'nodata': _nodata,
        }
        
        tmp_path = None
        cog_path = None
        
        try:
            with tempfile.NamedTemporaryFile(
                    suffix='.tif',
                    delete=False,
                    dir=settings.GEORIVA_TEMP_DIR,
            ) as tmp:
                tmp_path = tmp.name
            cog_path = tmp_path.replace('.tif', '_cog.tif')
            
            # Pass 1 — write raw data as a plain GeoTIFF
            # No overviews here — cog_translate handles that in pass 2.
            with rasterio.open(tmp_path, 'w', **raw_profile) as dst:
                dst.write(data.astype(dtype), 1)
            
            # Pass 2 — build overviews and rewrite in true COG byte order.
            # overview_resampling="average" is appropriate for continuous
            # fields (precipitation, temperature). Use "nearest" for
            # categorical data (land cover, alert levels).
            cog_translate(
                tmp_path,
                cog_path,
                output_profile,
                overview_level=overview_levels,
                overview_resampling="average",
                nodata=_nodata,
                forward_band_tags=True,  # preserve band-level CF metadata
                quiet=True,  # suppress progress bar in production
            )
            
            with open(cog_path, 'rb') as f:
                return self.bucket.save(output_path, f)
        
        finally:
            # Always clean up temp files — even if an exception is raised.
            # These can be 64MB+ for global datasets so leaking them matters.
            if tmp_path:
                Path(tmp_path).unlink(missing_ok=True)
            if cog_path:
                Path(cog_path).unlink(missing_ok=True)
    
    def write_metadata(self, metadata: dict, output_path: str) -> str:
        """
        Serialise a metadata dict to JSON and write it to storage.

        Args:
            metadata:    Dict of variable/asset metadata.
            output_path: Destination path in the bucket.

        Returns:
            Final stored path.
        """
        content = json.dumps(metadata, indent=2).encode('utf-8')
        return self.bucket.save(output_path, content)
    
    # =========================================================================
    # Private Helpers
    # =========================================================================
    
    def _blocksize(self, width: int, height: int) -> int:
        """
        Derive internal tile block size from raster dimensions.

        Larger rasters need larger blocks to keep the total tile count
        manageable for TiTiler HTTP range requests. Smaller rasters
        need smaller blocks so that partial reads are actually useful.

        Country-level  (<512px)  → 128
        Regional       (<2048px) → 256
        Continental/Global       → 512
        """
        min_dim = min(width, height)
        if min_dim < 512:
            return 128
        elif min_dim < 2048:
            return 256
        else:
            return 512
    
    def _overview_levels(self, width: int, height: int, blocksize: int) -> int:
        """
        Compute the number of overview levels to build.

        Builds levels until the smallest overview fits within ~2 block
        widths — beyond that point TiTiler serves the overview directly
        and additional levels add no benefit.

        Always returns at least 1 so TiTiler has a low-resolution
        fallback even for small country-level rasters.

        Examples at blocksize=128:
          Ethiopia  (300×229)  → 1 level  [2]
          E. Africa (800×700)  → 2 levels [2, 4]
          Africa    (2000×800) → 3 levels [2, 4, 8]
          Global    (7200×3600)→ 5 levels [2, 4, 8, 16, 32]
        """
        levels = 0
        min_dim = min(width, height)
        level = 2
        while min_dim // level >= blocksize * 2:
            levels += 1
            level *= 2
        return max(levels, 1)
    
    def _predictor(self, dtype: np.dtype) -> int:
        """
        Derive the optimal deflate compression predictor for a given dtype.

        predictor=3  Floating-point predictor. Reorders float bytes to group
                     sign/exponent/mantissa bits across adjacent pixels,
                     giving deflate long runs of similar bytes to compress.
                     Use for float32/float64 (precipitation, temperature etc).

        predictor=2  Horizontal differencing. Stores pixel deltas rather than
                     raw values. Effective for integer data where adjacent
                     pixels are numerically close.
                     Use for int16/uint8/uint16 (elevation, land cover etc).

        predictor=1  No prediction. Raw values passed to deflate as-is.
                     Fallback for boolean masks or other non-numeric types.
        """
        dtype = np.dtype(dtype)
        if np.issubdtype(dtype, np.floating):
            return 3
        elif np.issubdtype(dtype, np.integer):
            return 2
        return 1
    
    def _default_nodata(self, dtype: np.dtype):
        """
        Return a sensible default nodata value for a given dtype.

        Called when the caller does not supply an explicit nodata value.
        np.nan is invalid for integer dtypes — rasterio will raise if
        you try to set nodata=nan on an int16 or uint8 raster.

        float32 / float64  → NaN   (standard for continuous fields)
        uint8              → 255   (max value, avoids valid data range)
        uint16             → 65535 (max value)
        int16              → -9999 (WMO convention for missing data)
        other              → None  (let the caller decide explicitly)
        """
        dtype = np.dtype(dtype)
        if np.issubdtype(dtype, np.floating):
            return np.nan
        if dtype == np.uint8:
            return 255
        if dtype == np.uint16:
            return 65535
        if dtype == np.int16:
            return -9999
        return None
