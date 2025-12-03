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
from typing import Optional, Type

import numpy as np

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
    
    # For vector data - the second component
    secondary_data: Optional[np.ndarray] = None
    
    # Additional metadata
    metadata: dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def is_vector(self) -> bool:
        """Check if this is vector data (has secondary component)."""
        return self.secondary_data is not None


class BaseFormatPlugin(ABC):
    """
    Base class for file format plugins.
    
    A format plugin knows how to:
    1. Open files of a specific format
    2. List available variables in the file
    3. Extract specific variables as numpy arrays with metadata
    """
    
    # Plugin identification
    name: str = "base"
    display_name: str = "Base Format"
    extensions: list[str] = []
    
    def __init__(self):
        self.logger = logging.getLogger(f"georiva.formats.{self.name}")
    
    @abstractmethod
    def can_handle(self, file_path: Path) -> bool:
        """Check if this plugin can handle the given file."""
        pass
    
    @abstractmethod
    def list_variables(self, file_path: Path) -> list[dict]:
        """
        List available variables in the file.
        
        Returns:
            List of dicts with variable info:
            [
                {'name': 't2m', 'long_name': '2 metre temperature', 'units': 'K'},
                ...
            ]
        """
        pass
    
    @abstractmethod
    def get_timestamps(self, file_path: Path) -> list[datetime]:
        """
        Get available timestamps in the file.
        
        Returns:
            List of datetime objects
        """
        pass
    
    @abstractmethod
    def extract_variable(
            self,
            file_path: Path,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            secondary_variable: Optional[str] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            vertical_selection: Optional[dict] = None,
    ) -> ExtractedVariable:
        """
        Extract a variable (or a specific window of it) from the file.
        
        Args:
            file_path: Path to the source file
            variable_name: Primary variable name to extract
            timestamp: Specific timestamp to extract (if file has multiple)
            secondary_variable: For vector data, the V component variable name
            window: format (x_offset, y_offset, width, height)
            vertical_selection: dict with 'dim' and 'value' for vertical slicing
        
        Returns:
            ExtractedVariable with the data and metadata
        """
        pass
    
    def extract_dataset(
            self,
            file_path: Path,
            dataset,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
    ) -> ExtractedVariable:
        """Extract data for a Dataset definition."""
        
        # Prepare vertical selection if configured
        vertical_selection = None
        if dataset.vertical_dimension and dataset.vertical_value is not None:
            vertical_selection = {
                'dim': dataset.vertical_dimension,
                'value': dataset.vertical_value
            }
        
        return self.extract_variable(
            file_path=file_path,
            variable_name=dataset.primary_variable,
            timestamp=timestamp,
            secondary_variable=dataset.secondary_variable if dataset.is_vector else None,
            window=window,
            vertical_selection=vertical_selection
        )
    
    def get_metadata(self, file_path: Path, dataset) -> dict:
        """
        Scan to get dimensions and bounds without reading data.
        """
        # Default implementation (inefficient fallback)
        # Subclasses should override this!
        var = self.extract_variable(file_path, dataset.primary_variable, window=(0, 0, 1, 1))
        return {
            'width': var.metadata.get('full_width', var.width),
            'height': var.metadata.get('full_height', var.height),
            'bounds': var.bounds,
            'crs': var.crs
        }
    
    def get_lazy_dataset(self, file_path: Path, dataset, timestamp=None):
        """
        Return a lazy-loaded object (e.g. xarray DataArray) for global stats computation.
        """
        raise NotImplementedError("Plugin does not support lazy loading")


class FormatRegistry:
    """
    Registry for format plugins.
    
    Plugins are registered by format name and can be looked up
    by name or by file extension.
    """
    
    _plugins: dict[str, Type[BaseFormatPlugin]] = {}
    _extension_map: dict[str, str] = {}  # extension -> format name
    
    @classmethod
    def register(cls, plugin_class: Type[BaseFormatPlugin]) -> Type[BaseFormatPlugin]:
        """
        Decorator to register a format plugin.
        
        Usage:
            @FormatRegistry.register
            class GRIBFormatPlugin(BaseFormatPlugin):
                name = "grib2"
                ...
        """
        cls._plugins[plugin_class.name] = plugin_class
        
        # Register extensions
        for ext in plugin_class.extensions:
            ext_lower = ext.lower().lstrip('.')
            cls._extension_map[ext_lower] = plugin_class.name
        
        logger.info(f"Registered format plugin: {plugin_class.name}")
        return plugin_class
    
    @classmethod
    def get(cls, name: str) -> Optional[BaseFormatPlugin]:
        """Get a plugin instance by format name."""
        plugin_class = cls._plugins.get(name)
        if plugin_class:
            return plugin_class()
        return None
    
    @classmethod
    def get_by_extension(cls, extension: str) -> Optional[BaseFormatPlugin]:
        """Get a plugin instance by file extension."""
        ext_lower = extension.lower().lstrip('.')
        format_name = cls._extension_map.get(ext_lower)
        if format_name:
            return cls.get(format_name)
        return None
    
    @classmethod
    def get_for_file(cls, file_path: Path) -> Optional[BaseFormatPlugin]:
        """
        Get the appropriate plugin for a file.
        
        First tries by extension, then asks each plugin if it can handle the file.
        """
        # Try by extension first
        plugin = cls.get_by_extension(file_path.suffix)
        if plugin and plugin.can_handle(file_path):
            return plugin
        
        # Fall back to asking each plugin
        for plugin_class in cls._plugins.values():
            plugin = plugin_class()
            if plugin.can_handle(file_path):
                return plugin
        
        return None
    
    @classmethod
    def all(cls) -> dict[str, Type[BaseFormatPlugin]]:
        """Get all registered plugins."""
        return cls._plugins.copy()
    
    @classmethod
    def choices(cls) -> list[tuple[str, str]]:
        """Get choices for Django model field."""
        return [
            (name, plugin.display_name)
            for name, plugin in cls._plugins.items()
        ]


# Convenience alias
format_registry = FormatRegistry
