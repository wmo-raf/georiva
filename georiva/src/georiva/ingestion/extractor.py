import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

from georiva.core.models import Variable, VariableSource
from georiva.formats.registry import BaseFormatPlugin

logger = logging.getLogger(__name__)


@dataclass
class VariableData:
    """
    Extracted and transformed data for a Variable.
    
    Always a single 2D array - transforms have already been applied.
    """
    data: np.ndarray  # Shape: (height, width), dtype: float32
    stats: dict = field(default_factory=dict)


class VariableExtractor:
    """
    Extracts and transforms data for a Variable from source files.
    
    Each Variable has one or more VariableSources that define what to read.
    The transform_type determines how sources are combined into a single output.
    
    Works with BaseFormatPlugin interface:
        - extract_variable(file_path, variable_name, timestamp, secondary_variable, window, dim_selectors)
        - get_metadata_for_variable(file_path, variable_name, timestamp, dim_selectors)
        - get_lazy_variable(file_path, variable_name, timestamp, dim_selectors)
    
    Examples:
        PASSTHROUGH: temperature_2m reads TMP_2maboveground directly
        VECTOR_MAGNITUDE: wind_speed reads UGRD + VGRD, outputs √(u² + v²)
        BAND_MATH: ndvi reads B04 + B08, outputs (nir - red) / (nir + red)
    """
    
    def __init__(self, format_plugin: 'BaseFormatPlugin'):
        self.plugin = format_plugin
        self.logger = logging.getLogger("georiva.extractor")
    
    def extract(
            self,
            variable: 'Variable',
            file_path: Path,
            timestamp: datetime,
            window: tuple[int, int, int, int] = None,
    ) -> np.ndarray:
        """
        Extract data for a Variable, applying its transform.
        
        Args:
            variable: Variable with its sources
            file_path: Local path to source file
            timestamp: Timestamp to extract
            window: Optional (x, y, w, h) for chunked reading
        
        Returns:
            2D numpy array (height, width) of float32 values
        """
        sources = list(variable.sources.order_by('sort_order'))
        
        if not sources:
            raise ValueError(f"Variable '{variable.slug}' has no sources defined")
        
        transform = variable.transform_type
        
        # Dispatch based on transform type
        if transform == variable.TransformType.PASSTHROUGH:
            return self._extract_passthrough(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.UNIT_CONVERT:
            # Same as passthrough - unit conversion applied later
            return self._extract_passthrough(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.VECTOR_MAGNITUDE:
            return self._extract_vector_magnitude(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.VECTOR_DIRECTION:
            return self._extract_vector_direction(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.BAND_MATH:
            return self._extract_band_math(
                sources, variable.transform_expression, file_path, timestamp, window
            )
        
        elif transform == variable.TransformType.THRESHOLD:
            return self._extract_threshold(
                sources, variable.transform_expression, file_path, timestamp, window
            )
        
        elif transform == variable.TransformType.RGB_COMPOSITE:
            raise NotImplementedError("RGB_COMPOSITE not yet implemented")
        
        else:
            raise ValueError(f"Unknown transform type: {transform}")
    
    def get_metadata(
            self,
            variable: 'Variable',
            file_path: Path,
            timestamp: datetime = None,
    ) -> dict:
        """
        Get spatial metadata for a Variable.
        
        Returns:
            dict with 'width', 'height', 'bounds', 'crs'
        """
        sources = list(variable.sources.order_by('sort_order'))
        if not sources:
            raise ValueError(f"Variable '{variable.slug}' has no sources")
        
        primary = self._get_primary_source(sources)
        dim_selectors = self._build_dim_selectors(primary)
        
        return self.plugin.get_metadata_for_variable(
            file_path=file_path,
            variable_name=primary.source_name,
            timestamp=timestamp,
            dim_selectors=dim_selectors,
        )
    
    # =========================================================================
    # Source Helpers
    # =========================================================================
    
    def _get_primary_source(
            self,
            sources: list['VariableSource']
    ) -> 'VariableSource':
        """Get the primary source (or first if no primary role)."""
        for s in sources:
            if s.role == 'primary':
                return s
        return sources[0]
    
    def _get_source_by_role(
            self,
            sources: list['VariableSource'],
            role: str
    ) -> 'VariableSource':
        """Find a source by its role."""
        for s in sources:
            if s.role == role:
                return s
        raise ValueError(f"No source with role '{role}' found")
    
    def _build_dim_selectors(
            self,
            source: 'VariableSource'
    ) -> Optional[dict]:
        """
        Build dim_selectors dict from VariableSource fields.
        
        Maps VariableSource attributes to plugin's dim_selectors format:
            - vertical_dimension + vertical_value → {vertical_dimension: vertical_value}
            - band_index → {'band': band_index}
        """
        selectors = {}
        
        if source.vertical_dimension and source.vertical_value is not None:
            selectors[source.vertical_dimension] = source.vertical_value
        
        if source.band_index is not None:
            selectors['band'] = source.band_index
        
        return selectors if selectors else None
    
    def _extract_source(
            self,
            source: 'VariableSource',
            file_path: Path,
            timestamp: datetime,
            window: tuple = None,
    ) -> np.ndarray:
        """
        Extract data for a single source using the format plugin.
        
        Args:
            source: VariableSource defining what to read
            file_path: Path to source file
            timestamp: Timestamp to extract
            window: Optional spatial subset (x, y, w, h)
        
        Returns:
            2D numpy array (float32)
        """
        dim_selectors = self._build_dim_selectors(source)
        
        extracted = self.plugin.extract_variable(
            file_path=file_path,
            variable_name=source.source_name,
            timestamp=timestamp,
            window=window,
            dim_selectors=dim_selectors,
        )
        
        return np.asarray(extracted.data, dtype=np.float32)
    
    # =========================================================================
    # Transform Implementations
    # =========================================================================
    
    def _extract_passthrough(
            self,
            sources: list['VariableSource'],
            file_path: Path,
            timestamp: datetime,
            window: tuple
    ) -> np.ndarray:
        """Direct read from primary source."""
        primary = self._get_primary_source(sources)
        return self._extract_source(primary, file_path, timestamp, window)
    
    def _extract_vector_magnitude(
            self,
            sources: list['VariableSource'],
            file_path: Path,
            timestamp: datetime,
            window: tuple
    ) -> np.ndarray:
        """
        Compute √(u² + v²) from u and v components.
        
        Extracts each component separately, then combines.
        """
        u_source = self._get_source_by_role(sources, 'u_component')
        v_source = self._get_source_by_role(sources, 'v_component')
        
        # Extract each component separately
        u = self._extract_source(u_source, file_path, timestamp, window)
        v = self._extract_source(v_source, file_path, timestamp, window)
        
        magnitude = np.hypot(u, v)
        
        del u, v
        return magnitude
    
    def _extract_vector_direction(
            self,
            sources: list['VariableSource'],
            file_path: Path,
            timestamp: datetime,
            window: tuple
    ) -> np.ndarray:
        """
        Compute wind direction from u and v components.
        
        Convention: meteorological direction (where wind comes FROM),
        0° = North, 90° = East, measured clockwise.
        
        Extracts each component separately, then combines.
        """
        u_source = self._get_source_by_role(sources, 'u_component')
        v_source = self._get_source_by_role(sources, 'v_component')
        
        # Extract each component separately
        u = self._extract_source(u_source, file_path, timestamp, window)
        v = self._extract_source(v_source, file_path, timestamp, window)
        
        # Meteorological convention: direction wind is FROM
        # atan2(u, v) gives direction wind is going TO, so add 180°
        direction = np.degrees(np.arctan2(u, v)) + 180.0
        direction = np.mod(direction, 360.0)
        
        del u, v
        return direction
    
    def _extract_band_math(
            self,
            sources: list['VariableSource'],
            expression: str,
            file_path: Path,
            timestamp: datetime,
            window: tuple
    ) -> np.ndarray:
        """
        Evaluate a band math expression.
        
        Expression uses source roles as variable names.
        Example: "(nir - red) / (nir + red)" for NDVI
        
        Each source is extracted independently, then combined via expression.
        """
        if not expression:
            raise ValueError("BAND_MATH transform requires an expression")
        
        # Build namespace from sources - each source extracted separately
        namespace = {}
        for source in sources:
            data = self._extract_source(source, file_path, timestamp, window)
            namespace[source.role] = data
        
        # Add safe math functions
        namespace.update({
            'sqrt': np.sqrt,
            'log': np.log,
            'log10': np.log10,
            'exp': np.exp,
            'abs': np.abs,
            'where': np.where,
            'clip': np.clip,
            'nan': np.nan,
            'minimum': np.minimum,
            'maximum': np.maximum,
        })
        
        try:
            with np.errstate(divide='ignore', invalid='ignore'):
                result = eval(expression, {"__builtins__": {}}, namespace)
            return np.asarray(result, dtype=np.float32)
        except Exception as e:
            raise ValueError(f"Band math failed: {expression!r} - {e}")
    
    def _extract_threshold(
            self,
            sources: list['VariableSource'],
            expression: str,
            file_path: Path,
            timestamp: datetime,
            window: tuple
    ) -> np.ndarray:
        """
        Apply threshold to create a binary mask.
        
        Expression example: "data > 0.5" or "data >= 273.15"
        The primary source data is available as 'data' in the expression.
        """
        if not expression:
            raise ValueError("THRESHOLD transform requires an expression")
        
        primary = self._get_primary_source(sources)
        data = self._extract_source(primary, file_path, timestamp, window)
        
        namespace = {'data': data}
        
        try:
            result = eval(expression, {"__builtins__": {}}, namespace)
            return np.asarray(result, dtype=np.float32)
        except Exception as e:
            raise ValueError(f"Threshold expression failed: {e}")
    
    # =========================================================================
    # Statistics
    # =========================================================================
    
    def compute_stats(
            self,
            variable: 'Variable',
            file_path: Path,
            timestamp: datetime,
    ) -> dict:
        """
        Compute global statistics for a Variable.
        
        For simple transforms (PASSTHROUGH, UNIT_CONVERT), uses lazy loading
        if the format plugin supports it. For complex transforms, falls back
        to full extraction.
        """
        try:
            sources = list(variable.sources.order_by('sort_order'))
            if not sources:
                return {'min': None, 'max': None, 'mean': None, 'std': None}
            
            # For passthrough, try lazy loading
            if variable.transform_type in (
                    variable.TransformType.PASSTHROUGH,
                    variable.TransformType.UNIT_CONVERT,
            ):
                try:
                    stats = self._compute_stats_lazy(variable, sources, file_path, timestamp)
                    if stats:
                        return stats
                except NotImplementedError:
                    pass  # Fall through to full extraction
            
            # Full extraction for complex transforms or if lazy not supported
            data = self.extract(variable, file_path, timestamp)
            data = apply_unit_conversion(data, variable.unit_conversion)
            
            return {
                'min': float(np.nanmin(data)),
                'max': float(np.nanmax(data)),
                'mean': float(np.nanmean(data)),
                'std': float(np.nanstd(data)),
            }
        
        except Exception as e:
            self.logger.warning(f"Stats computation failed for {variable.slug}: {e}")
            return {'min': None, 'max': None, 'mean': None, 'std': None}
    
    def _compute_stats_lazy(
            self,
            variable: 'Variable',
            sources: list['VariableSource'],
            file_path: Path,
            timestamp: datetime,
    ) -> Optional[dict]:
        """
        Compute stats using lazy loading (dask/xarray).
        
        Returns None if lazy loading not supported.
        """
        primary = self._get_primary_source(sources)
        dim_selectors = self._build_dim_selectors(primary)
        
        lazy_result = self.plugin.get_lazy_variable(
            file_path=file_path,
            variable_name=primary.source_name,
            timestamp=timestamp,
            dim_selectors=dim_selectors,
        )
        
        # Handle (lazy_obj, closer_callable) pattern
        closer = None
        if isinstance(lazy_result, tuple):
            lazy_data, closer = lazy_result
        else:
            lazy_data = lazy_result
        
        try:
            # Apply unit conversion if possible
            conversion = variable.unit_conversion
            if conversion:
                if conversion == 'K_to_C':
                    lazy_data = lazy_data - 273.15
                elif conversion == 'Pa_to_hPa':
                    lazy_data = lazy_data * 0.01
                elif conversion == 'm_to_mm':
                    lazy_data = lazy_data * 1000.0
                elif conversion == 'ms_to_kmh':
                    lazy_data = lazy_data * 3.6
                elif conversion == 'kgm2s_to_mm':
                    lazy_data = lazy_data * 3600.0
            
            if hasattr(lazy_data, 'compute'):
                # Dask array
                return {
                    'min': float(lazy_data.min().compute()),
                    'max': float(lazy_data.max().compute()),
                    'mean': float(lazy_data.mean().compute()),
                    'std': float(lazy_data.std().compute()),
                }
            else:
                # Regular numpy array from lazy load
                return {
                    'min': float(np.nanmin(lazy_data)),
                    'max': float(np.nanmax(lazy_data)),
                    'mean': float(np.nanmean(lazy_data)),
                    'std': float(np.nanstd(lazy_data)),
                }
        
        finally:
            if closer and callable(closer):
                closer()
