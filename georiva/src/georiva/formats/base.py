"""
GeoRiva Format Plugin System

Format plugins handle parsing different file formats (GRIB2, NetCDF, GeoTIFF)
and extracting variables for datasets.

Lazy-first design:
- open_variable()  is the primary interface — returns a context manager yielding
  a lazy (dask-backed) xarray DataArray. No data is loaded into RAM until you
  explicitly compute.
- extract_variable() is a convenience that materializes data into a numpy array
  via open_variable(). Use this when you need the pixels.
- get_metadata_for_variable() reads bounds/size without touching pixel data.
  Default implementation uses open_variable(); plugins may override for speed.

Plugin contract:
1. can_handle()       — detect if a file is this format
2. list_variables()   — list available variables
3. get_timestamps()   — get available time steps
4. open_variable()    — lazy access (context manager, primary interface)
5. extract_variable() — materialize to numpy (calls open_variable)
6. get_metadata_for_variable() — lightweight bounds/size scan
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Generator

import numpy as np
import xarray as xr

from georiva.utils.path import PathLike


@dataclass
class VariableInfo:
    """
    Metadata returned by open_variable().

    Carries spatial/temporal info alongside the lazy DataArray,
    so callers can inspect bounds, CRS, etc. without computing.
    """
    
    data: xr.DataArray  # lazy (dask-backed)
    bounds: tuple[float, float, float, float]  # west, south, east, north
    crs: str
    width: int
    height: int
    resolution: tuple[float, float]  # x_res, y_res
    timestamp: datetime
    variable_name: str
    units: str = ""
    needs_flip: bool = False  # True if data is south-to-north and needs flipud on materialize
    metadata: dict = field(default_factory=dict)
    
    def compute(self) -> np.ndarray:
        """Materialize to numpy with correct image orientation."""
        data = self.data.values.squeeze()
        if self.needs_flip:
            data = np.flipud(data)
        return data


@dataclass
class ExtractedVariable:
    """Materialized variable — numpy array with spatial metadata."""
    
    data: np.ndarray
    bounds: tuple[float, float, float, float]  # west, south, east, north
    crs: str
    width: int
    height: int
    resolution: tuple[float, float]  # x, y
    timestamp: datetime
    variable_name: str
    units: Optional[str] = None
    metadata: dict = field(default_factory=dict)


class BaseFormatPlugin(ABC):
    """
    Base class for file format plugins.

    Subclasses must implement: can_handle, list_variables, get_timestamps, open_variable.
    extract_variable and get_metadata_for_variable have default implementations.
    """
    
    name: str = "base"
    display_name: str = "Base Format"
    extensions: list[str] = []
    
    def __init__(self):
        self.logger = logging.getLogger(f"georiva.formats.{self.name}")
    
    @abstractmethod
    def can_handle(self, file_path: PathLike) -> bool:
        """Check if this plugin can handle the given file."""
        ...
    
    @abstractmethod
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List available variables in the file.

        Returns:
            List of dicts with at least: name, long_name, units, dimensions, shape.
            Format-specific fields (e.g. band_index, key) may also be present.
        """
        ...
    
    @abstractmethod
    def get_timestamps(self, file_path: PathLike, variable_name: str, **kwargs) -> list[datetime]:
        """
        Get available timestamps for a specific variable.

        Args:
            file_path: Path to the source file.
            variable_name: Variable to query timestamps for.
            **kwargs: Format-specific options (e.g. key for GRIB).

        Returns:
            Sorted list of datetime objects.
        """
        ...
    
    @abstractmethod
    @contextmanager
    def open_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            **kwargs,
    ) -> Generator[VariableInfo, None, None]:
        """
        Primary interface. Opens a variable lazily as a context manager.

        Yields a VariableInfo with a dask-backed DataArray — no data is read
        until you call .compute() or access .data.values.

        Usage:
            with plugin.open_variable("file.nc", "temperature") as var:
                # Lazy stats — dask computes in chunks
                min_val = float(var.data.min())
                max_val = float(var.data.max())

                # Or materialize when you need pixels
                array = var.compute()

        Args:
            file_path: Path to the source file.
            variable_name: Variable to open.
            timestamp: Specific timestamp to select (nearest match).
            window: Spatial subset as (x_offset, y_offset, width, height).
            **kwargs: Format-specific options (e.g. key for GRIB).
        """
        ...
    
    def extract_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            **kwargs,
    ) -> ExtractedVariable:
        """
        Convenience method: opens variable and materializes to numpy.

        For lazy access, use open_variable() instead.
        """
        with self.open_variable(
                file_path,
                variable_name,
                timestamp=timestamp,
                window=window,
                **kwargs,
        ) as var_info:
            data = var_info.compute()
            
            height = int(data.shape[0]) if data.ndim > 1 else 1
            width = int(data.shape[1]) if data.ndim > 1 else int(data.shape[0])
            
            return ExtractedVariable(
                data=data,
                bounds=var_info.bounds,
                crs=var_info.crs,
                width=width,
                height=height,
                resolution=var_info.resolution,
                timestamp=var_info.timestamp,
                variable_name=var_info.variable_name,
                units=var_info.units,
                metadata=var_info.metadata,
            )
    
    def get_metadata_for_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            **kwargs,
    ) -> dict:
        """
        Lightweight scan for dimensions and bounds without reading pixel data.

        Default implementation opens the variable lazily and reads only metadata.
        Subclasses may override for efficiency.

        Returns:
            Dict with: width, height, bounds, crs.
        """
        with self.open_variable(
                file_path,
                variable_name,
                timestamp=timestamp,
                **kwargs,
        ) as var_info:
            return {
                "width": var_info.width,
                "height": var_info.height,
                "bounds": var_info.bounds,
                "crs": var_info.crs,
            }
