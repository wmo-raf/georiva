import logging

import numpy as np

from georiva.core.models import Variable

logger = logging.getLogger(__name__)


class VariableEncoder:
    """
    Encodes Variable data to visual formats (PNG).
    
    Takes a 2D float32 array and produces RGBA uint8 output.
    Uses Variable's visualization settings (value_min/max, scale_type, palette).
    """
    
    def encode_to_rgba(
            self,
            data: np.ndarray,
            variable: 'Variable',
            stats: dict,
    ) -> np.ndarray:
        """
        Encode data to RGBA format for PNG output.
        
        Args:
            data: 2D float32 array (height, width)
            variable: Variable with visualization settings
            stats: Computed statistics for fallback ranges
        
        Returns:
            4D uint8 array (height, width, 4) - RGBA
        """
        height, width = data.shape
        
        # Create mask before normalization
        mask = np.isnan(data)
        
        # Get value range
        vmin = variable.value_min if variable.value_min is not None else stats.get('min')
        vmax = variable.value_max if variable.value_max is not None else stats.get('max')
        
        if vmin is None:
            vmin = float(np.nanmin(data))
        if vmax is None:
            vmax = float(np.nanmax(data))
        
        vmin, vmax = float(vmin), float(vmax)
        if vmax <= vmin:
            vmax = vmin + 1.0
        
        # Normalize based on scale type
        normalized = self._normalize(data, vmin, vmax, variable.scale_type)
        
        # Replace NaN with 0 before scaling (these pixels will have alpha=0 anyway)
        normalized = np.nan_to_num(normalized, nan=0.0)
        
        # Scale to 0-255
        scaled = np.clip(normalized * 255.0, 0, 255).astype(np.uint8)
        
        # Build RGBA output
        # R = data value, G = 0, B = 0, A = mask (255=valid, 0=nodata)
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        rgba[:, :, 0] = scaled
        rgba[:, :, 3] = np.where(mask, 0, 255)
        
        return rgba
    
    def _normalize(
            self,
            data: np.ndarray,
            vmin: float,
            vmax: float,
            scale_type: str,
    ) -> np.ndarray:
        """Normalize data to 0-1 range based on scale type."""
        
        if not scale_type or scale_type == 'linear':
            return (data - vmin) / (vmax - vmin)
        
        elif scale_type == 'log':
            # Shift to ensure positive values
            shift = 1.0 - min(0.0, vmin)
            data_shifted = np.clip(data, vmin, vmax) + shift
            log_data = np.log10(data_shifted)
            log_min = np.log10(vmin + shift)
            log_max = np.log10(vmax + shift)
            return (log_data - log_min) / (log_max - log_min)
        
        elif scale_type == 'sqrt':
            data_clipped = np.clip(data, max(0.0, vmin), vmax)
            sqrt_data = np.sqrt(data_clipped)
            sqrt_min = np.sqrt(max(0.0, vmin))
            sqrt_max = np.sqrt(vmax)
            return (sqrt_data - sqrt_min) / (sqrt_max - sqrt_min)
        
        elif scale_type == 'diverging':
            # Center at zero, symmetric range
            abs_max = max(abs(vmin), abs(vmax))
            if abs_max > 0:
                return (data + abs_max) / (2.0 * abs_max)
            return np.full_like(data, 0.5)
        
        else:
            # Fallback to linear
            return (data - vmin) / (vmax - vmin)
