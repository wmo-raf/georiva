"""
GeoRiva Format Plugin System

Format plugins handle parsing different file formats (GRIB2, NetCDF, GeoTIFF)
and extracting variables for datasets.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import numpy as np

from georiva.utils.path import PathLike

logger = logging.getLogger(__name__)


@dataclass
class ExtractedVariable:
    """Represents a variable extracted from a source file."""
    
    # The data array
    data: np.ndarray
    
    # Spatial information
    bounds: tuple[float, float, float, float]  # west, south, east, north
    crs: str
    width: int
    height: int
    resolution: tuple[float, float]  # x, y
    
    # Temporal information
    timestamp: datetime
    
    # Variable metadata
    variable_name: str
    units: Optional[str] = None
    
    # Additional metadata
    metadata: Optional[dict] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class BaseFormatPlugin(ABC):
    """
    Base class for file format plugins.
    """
    
    # Plugin identification
    name: str = "base"
    display_name: str = "Base Format"
    extensions: list[str] = []
    
    def __init__(self):
        self.logger = logging.getLogger(f"georiva.formats.{self.name}")
    
    @abstractmethod
    def can_handle(self, file_path: PathLike) -> bool:
        """Check if this plugin can handle the given file."""
        raise NotImplementedError
    
    @abstractmethod
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List available variables in the file.

        Returns:
            List of dicts with variable info:
            [
                {'name': 't2m', 'long_name': '2 metre temperature', 'units': 'K'},
                ...
            ]
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_timestamps(self, file_path: PathLike) -> list[datetime]:
        """
        Get available timestamps in the file.

        Returns:
            List of datetime objects
        """
        raise NotImplementedError
    
    @abstractmethod
    def extract_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> ExtractedVariable:
        """
        Extract a variable (or a specific window of it) from the file.

        Args:
            file_path: Path to the source file
            variable_name: Primary variable name to extract
            timestamp: Specific timestamp to extract (if file has multiple)
            window: Spatial subset: (x_offset, y_offset, width, height)
            dim_selectors: Non-spatial selection mapping: {dim_or_coord_name: value}

                Examples:
                    {'heightAboveGround': 10}
                    {'isobaricInhPa': 850}
                    {'number': 0}  # ensemble member
                    {'step': np.timedelta64(6, 'h')}  # forecast lead time
        """
        raise NotImplementedError
    
    def get_metadata_for_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> dict:
        """
        Scan to get dimensions and bounds without reading full data.

        Default fallback implementation (inefficient):
        extracts a 1x1 window to infer bounds/CRS and uses metadata for full size if present.

        Subclasses SHOULD override for efficiency.
        """
        
        file_path = Path(file_path)
        
        var = self.extract_variable(
            file_path=file_path,
            variable_name=variable_name,
            timestamp=timestamp,
            window=(0, 0, 1, 1),
            dim_selectors=dim_selectors,
        )
        return {
            "width": var.metadata.get("full_width", var.width),
            "height": var.metadata.get("full_height", var.height),
            "bounds": var.bounds,
            "crs": var.crs,
        }
    
    def get_lazy_variable(
            self,
            file_path: Path,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> Any:
        """
        Return a lazy-loaded object (e.g. xarray DataArray) for global stats computation.

        Default behavior: not supported.
        Subclasses may override.

        Note:
        - If you need resource cleanup, plugins may return (lazy_obj, closer_callable).
          This base signature is "Any" to allow that pattern.
        """
        raise NotImplementedError("Plugin does not support lazy loading")
