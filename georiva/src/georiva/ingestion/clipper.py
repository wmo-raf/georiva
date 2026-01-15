import logging
from typing import Optional, Tuple

import numpy as np
from adminboundarymanager.models import AdminBoundary
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds

logger = logging.getLogger(__name__)


class BoundaryClipper:
    """
    Handles spatial clipping using AdminBoundary geometry.
    
    Uses bbox for efficient windowed extraction,
    optionally applies precise geometry mask.
    """
    
    def __init__(self, boundary: 'AdminBoundary' = None, apply_mask: bool = True):
        """
        Args:
            boundary: AdminBoundary instance with PostGIS geometry
            apply_mask: Whether to apply precise geometry mask (vs bbox only)
        """
        self.boundary = boundary
        self.apply_mask = apply_mask
        self._shapely_geom = None
        self._logger = logging.getLogger("georiva.ingestion.clipper")
    
    @property
    def is_active(self) -> bool:
        return self.boundary is not None
    
    @property
    def bbox(self) -> Optional[Tuple[float, float, float, float]]:
        """Get bounding box [west, south, east, north]."""
        if not self.boundary:
            return None
        return tuple(self.boundary.bbox)
    
    @property
    def shapely_geom(self):
        """Convert PostGIS geometry to Shapely for masking."""
        if self._shapely_geom is None and self.boundary:
            from shapely import wkt
            self._shapely_geom = wkt.loads(self.boundary.geom.wkt)
        return self._shapely_geom
    
    def compute_window(
            self,
            src_bounds: Tuple[float, float, float, float],
            src_width: int,
            src_height: int,
    ) -> Optional[dict]:
        """
        Compute pixel window for clipped extraction.
        
        Args:
            src_bounds: Source data bounds [west, south, east, north]
            src_width: Source raster width in pixels
            src_height: Source raster height in pixels
        
        Returns:
            dict with x_off, y_off, width, height, bounds
            or None if no clipping needed
        """
        if not self.bbox:
            return None
        
        src_west, src_south, src_east, src_north = src_bounds
        clip_west, clip_south, clip_east, clip_north = self.bbox
        
        # Check intersection
        if (clip_east <= src_west or clip_west >= src_east or
                clip_north <= src_south or clip_south >= src_north):
            raise ValueError(
                f"Boundary bbox {self.bbox} does not intersect "
                f"source bounds {src_bounds}"
            )
        
        # Compute intersection
        int_west = max(src_west, clip_west)
        int_south = max(src_south, clip_south)
        int_east = min(src_east, clip_east)
        int_north = min(src_north, clip_north)
        
        # Pixel resolution
        res_x = (src_east - src_west) / src_width
        res_y = (src_north - src_south) / src_height
        
        # Convert to pixel offsets (y is flipped in raster coordinates)
        x_off = int((int_west - src_west) / res_x)
        y_off = int((src_north - int_north) / res_y)
        win_width = int(np.ceil((int_east - int_west) / res_x))
        win_height = int(np.ceil((int_north - int_south) / res_y))
        
        # Clamp to source dimensions
        x_off = max(0, min(x_off, src_width - 1))
        y_off = max(0, min(y_off, src_height - 1))
        win_width = min(win_width, src_width - x_off)
        win_height = min(win_height, src_height - y_off)
        
        # Recompute exact bounds from pixel window
        exact_west = src_west + x_off * res_x
        exact_north = src_north - y_off * res_y
        exact_east = exact_west + win_width * res_x
        exact_south = exact_north - win_height * res_y
        
        self._logger.debug(
            f"Clip window: ({x_off}, {y_off}) {win_width}x{win_height} "
            f"from {src_width}x{src_height}"
        )
        
        return {
            'x_off': x_off,
            'y_off': y_off,
            'width': win_width,
            'height': win_height,
            'bounds': (exact_west, exact_south, exact_east, exact_north),
            'resolution': (res_x, res_y),
        }
    
    def create_mask(
            self,
            bounds: Tuple[float, float, float, float],
            width: int,
            height: int,
    ) -> np.ndarray:
        """
        Create a boolean mask from the boundary geometry.
        
        Args:
            bounds: Data bounds [west, south, east, north]
            width: Raster width
            height: Raster height
        
        Returns:
            Boolean array where True = inside boundary
        """
        if not self.shapely_geom:
            return np.ones((height, width), dtype=bool)
        
        transform = from_bounds(*bounds, width, height)
        
        # geometry_mask returns True where geometry is NOT
        # So we invert to get True where geometry IS
        mask = ~geometry_mask(
            [self.shapely_geom],
            out_shape=(height, width),
            transform=transform,
            invert=False
        )
        
        return mask
    
    def apply_geometry_mask(
            self,
            data: np.ndarray,
            bounds: Tuple[float, float, float, float],
            nodata: float = np.nan,
    ) -> np.ndarray:
        """
        Mask data to boundary geometry.
        
        Args:
            data: 2D array to mask
            bounds: Data bounds
            nodata: Value to use outside geometry
        
        Returns:
            Masked array (copy)
        """
        if not self.apply_mask or not self.shapely_geom:
            return data
        
        height, width = data.shape[:2]
        mask = self.create_mask(bounds, width, height)
        
        result = data.copy()
        result[~mask] = nodata
        
        return result
    
    def apply_rgba_mask(
            self,
            rgba: np.ndarray,
            bounds: Tuple[float, float, float, float],
    ) -> np.ndarray:
        """
        Mask RGBA array by setting alpha=0 outside boundary.
        
        Args:
            rgba: RGBA array (height, width, 4)
            bounds: Data bounds
        
        Returns:
            Masked RGBA array (copy)
        """
        if not self.apply_mask or not self.shapely_geom:
            return rgba
        
        height, width = rgba.shape[:2]
        mask = self.create_mask(bounds, width, height)
        
        result = rgba.copy()
        result[~mask, 3] = 0  # Set alpha to 0 outside boundary
        
        return result
