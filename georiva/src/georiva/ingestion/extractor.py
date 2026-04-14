import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

from georiva.core.models import Variable
from georiva.formats.base import BaseFormatPlugin
from georiva.ingestion.utils import apply_unit_conversion

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

    Each Variable has one or more sources that define what to read.
    The transform_type determines how sources are combined into a single output.

    Unit conversion is driven by variable.source_unit → variable.unit via pint

    Transform types:
        PASSTHROUGH:      reads primary source directly
        VECTOR_MAGNITUDE: √(u² + v²) from u_component + v_component
        VECTOR_DIRECTION: meteorological direction from u_component + v_component
    """
    
    def __init__(self, format_plugin: BaseFormatPlugin):
        self.plugin = format_plugin
        self.logger = logging.getLogger("georiva.extractor")
    
    def extract(
            self,
            variable: "Variable",
            file_path: Path,
            timestamp: datetime,
            window: tuple[int, int, int, int] = None,
    ) -> np.ndarray:
        """
        Extract data for a Variable, applying its transform and unit conversion.

        Args:
            variable:  Variable instance with sources StreamField populated
            file_path: Local path to source file
            timestamp: Timestamp to extract
            window:    Optional (x, y, w, h) for chunked/windowed reading

        Returns:
            2D numpy array (height, width) of float32 values in variable.unit
        """
        sources = list(variable.sources)
        
        if not sources:
            raise ValueError(f"Variable '{variable.slug}' has no sources defined")
        
        transform = variable.transform_type
        
        if transform == variable.TransformType.PASSTHROUGH:
            data = self._extract_passthrough(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.VECTOR_MAGNITUDE:
            data = self._extract_vector_magnitude(sources, file_path, timestamp, window)
        
        elif transform == variable.TransformType.VECTOR_DIRECTION:
            data = self._extract_vector_direction(sources, file_path, timestamp, window)
        
        else:
            raise ValueError(f"Unknown transform type: {transform}")
        
        return apply_unit_conversion(data, variable.source_unit, variable.unit)
    
    def get_metadata(
            self,
            variable: "Variable",
            file_path: Path,
            timestamp: datetime = None,
    ) -> dict:
        """
        Get spatial metadata for a Variable from its primary source.

        Returns:
            dict with 'width', 'height', 'bounds', 'crs'
        """
        sources = list(variable.sources)
        if not sources:
            raise ValueError(f"Variable '{variable.slug}' has no sources")
        
        primary = self._get_primary_source(sources)
        kwargs = self._build_plugin_kwargs(primary)
        
        return self.plugin.get_metadata_for_variable(
            file_path=file_path,
            variable_name=primary['source_name'],
            timestamp=timestamp,
            **kwargs,
        )
    
    # =========================================================================
    # Source Helpers
    # =========================================================================
    
    def _get_primary_source(self, sources: list) -> "StructValue":
        """
        Get the primary source block value.
        Falls back to first block if no primary block exists.
        """
        for block in sources:
            if block.block_type == 'primary':
                return block.value
        raise ValueError("No primary source found in variable sources")
    
    def _get_source_by_role(self, sources: list, role: str) -> "StructValue":
        """Get a source block value by its block_type (role)."""
        for block in sources:
            if block.block_type == role:
                return block.value
        raise ValueError(f"No source with role '{role}' found")
    
    def _build_plugin_kwargs(self, source) -> dict:
        """
        Build format-specific kwargs from a source StructValue.

        For GRIB sources, passes a VariableKey built from vertical_dimension
        and vertical_value if present — enables deterministic message selection
        when multiple levels exist in the same GRIB file.

        Returns:
            Dict of kwargs passed to plugin methods via **kwargs.
        """
        kwargs = {}
        
        vertical_dimension = source.get('vertical_dimension') or ''
        vertical_value = source.get('vertical_value')
        
        if vertical_dimension and vertical_value is not None:
            from georiva.formats.grib import VariableKey
            kwargs["key"] = VariableKey(
                short_name=source['source_name'],
                type_of_level=vertical_dimension,
                level=vertical_value,
            )
        
        return kwargs
    
    def _extract_source(
            self,
            source,
            file_path: Path,
            timestamp: datetime,
            window: tuple = None,
    ) -> np.ndarray:
        """
        Extract raw data for a single source StructValue using the format plugin.

        No unit conversion here — conversion is applied once on the final
        output in extract(), after any transform has been applied.

        Args:
            source:    StructValue with source_name, vertical_dimension, vertical_value
            file_path: Path to source file
            timestamp: Timestamp to extract
            window:    Optional spatial subset (x, y, w, h)

        Returns:
            2D numpy array (float32) in the source file's native units
        """
        kwargs = self._build_plugin_kwargs(source)
        
        extracted = self.plugin.extract_variable(
            file_path=file_path,
            variable_name=source['source_name'],
            timestamp=timestamp,
            window=window,
            **kwargs,
        )
        
        return np.asarray(extracted.data, dtype=np.float32)
    
    # =========================================================================
    # Transform Implementations
    # =========================================================================
    
    def _extract_passthrough(self, sources, file_path, timestamp, window) -> np.ndarray:
        """Direct read from primary source with no computation."""
        primary = self._get_primary_source(sources)
        return self._extract_source(primary, file_path, timestamp, window)
    
    def _extract_vector_magnitude(self, sources, file_path, timestamp, window) -> np.ndarray:
        """
        Compute wind speed as √(u² + v²) from U and V components.

        Both components are assumed to be in the same units (typically m/s).
        Unit conversion is applied to the magnitude after computation.
        """
        u_source = self._get_source_by_role(sources, 'u_component')
        v_source = self._get_source_by_role(sources, 'v_component')
        
        u = self._extract_source(u_source, file_path, timestamp, window)
        v = self._extract_source(v_source, file_path, timestamp, window)
        
        magnitude = np.hypot(u, v)
        del u, v
        return magnitude
    
    def _extract_vector_direction(self, sources, file_path, timestamp, window) -> np.ndarray:
        """
        Compute meteorological wind direction from U and V components.

        Convention: direction wind comes FROM, 0 = North, clockwise.
        Output is always in degrees (0–360) — unit conversion on direction
        is a no-op and should not be configured on VECTOR_DIRECTION variables.
        """
        u_source = self._get_source_by_role(sources, 'u_component')
        v_source = self._get_source_by_role(sources, 'v_component')
        
        u = self._extract_source(u_source, file_path, timestamp, window)
        v = self._extract_source(v_source, file_path, timestamp, window)
        
        direction = np.degrees(np.arctan2(u, v)) + 180.0
        direction = np.mod(direction, 360.0)
        del u, v
        return direction
    
    # =========================================================================
    # Statistics
    # =========================================================================
    
    def compute_stats(
            self,
            variable: "Variable",
            file_path: Path,
            timestamp: datetime,
            window: dict = None,
    ) -> dict:
        """
        Compute global statistics for a Variable in its output units.

        For PASSTHROUGH without a clip window, uses lazy/dask-backed loading
        to avoid materialising the full array in memory. Falls back to full
        extraction for VECTOR transforms or windowed reads.

        Args:
            variable:  Variable to compute stats for
            file_path: Path to source file
            timestamp: Timestamp to extract
            window:    Optional clip window dict with x_off, y_off, width, height
        """
        try:
            sources = list(variable.sources)
            if not sources:
                return {"min": None, "max": None, "mean": None, "std": None}
            
            window_tuple = None
            if window:
                window_tuple = (
                    window["x_off"],
                    window["y_off"],
                    window["width"],
                    window["height"],
                )
            
            # Lazy path: PASSTHROUGH only, no window
            if window_tuple is None and variable.transform_type == variable.TransformType.PASSTHROUGH:
                try:
                    stats = self._compute_stats_lazy(variable, sources, file_path, timestamp)
                    if stats:
                        return stats
                except (NotImplementedError, ValueError):
                    pass  # fall through to full extraction
            
            # Full extraction path
            data = self.extract(variable, file_path, timestamp, window=window_tuple)
            # unit conversion already applied inside extract()
            
            return {
                "min": float(np.nanmin(data)),
                "max": float(np.nanmax(data)),
                "mean": float(np.nanmean(data)),
                "std": float(np.nanstd(data)),
            }
        
        except Exception as e:
            self.logger.warning(f"Stats computation failed for {variable.slug}: {e}")
            return {"min": None, "max": None, "mean": None, "std": None}
    
    def _compute_stats_lazy(
            self,
            variable: "Variable",
            sources: list,
            file_path: Path,
            timestamp: datetime,
    ) -> Optional[dict]:
        """
        Compute stats using open_variable() for lazy/dask-backed access.

        Applies unit conversion via pint on the lazy array before computing
        stats — no full array materialisation, dask streams in chunks.
        Returns None if the plugin does not support lazy loading.
        """
        primary = self._get_primary_source(sources)
        kwargs = self._build_plugin_kwargs(primary)
        
        with self.plugin.open_variable(
                file_path=file_path,
                variable_name=primary['source_name'],
                timestamp=timestamp,
                **kwargs,
        ) as var_info:
            lazy_data = var_info.data
            
            # Apply unit conversion on the lazy array using pint
            if variable.source_unit and variable.unit and variable.source_unit != variable.unit:
                try:
                    factor = (
                        variable.source_unit.pint_unit
                        .to(variable.unit.pint_unit)
                        .magnitude
                    )
                    # pint offset units (e.g. K→°C) can't use a simple factor —
                    # fall back to full extraction for those
                    lazy_data = lazy_data * factor
                except Exception:
                    return None  # triggers fallback to full extraction
            
            return {
                "min": float(lazy_data.min().values),
                "max": float(lazy_data.max().values),
                "mean": float(lazy_data.mean().values),
                "std": float(lazy_data.std().values),
            }
