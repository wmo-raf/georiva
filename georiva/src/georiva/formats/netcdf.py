"""
NetCDF Format Plugin (Lazy-first)

Two jobs:
1. List variables (with timestamps) in a NetCDF file.
2. Open a variable lazily for streaming stats or materialized extraction.

Design:
- NetCDF variables are uniquely identified by name — no key indirection needed.
- open_variable() is the primary interface: context manager, lazy DataArray.
- extract_variable() and get_metadata_for_variable() are inherited from BaseFormatPlugin.
- Supports rectilinear and curvilinear grids, CRS detection, and fill-value handling.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Generator

import numpy as np
import pandas as pd
import xarray as xr

from georiva.utils.path import PathLike
from .base import BaseFormatPlugin, VariableInfo

logger = logging.getLogger(__name__)


class NetCDFFormatPlugin(BaseFormatPlugin):
    name = "netcdf"
    display_name = "NetCDF"
    extensions = [".nc", ".nc4", ".netcdf"]
    
    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        try:
            with open(file_path, "rb") as f:
                magic = f.read(4)
                return magic[:3] == b"CDF" or magic == b"\x89HDF"
        except Exception:
            return False
    
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List all data variables in the NetCDF file.

        Returns a list of dicts, each containing:
            - name: variable name (use in open_variable / get_timestamps)
            - long_name: human-readable name
            - units: variable units
            - standard_name: CF standard name
            - dimensions: list of dimension names
            - shape: tuple of dimension sizes
        """
        file_path = Path(file_path)
        results: list[dict] = []
        
        try:
            with self._open(file_path) as ds:
                for var_name, var in ds.data_vars.items():
                    results.append(
                        {
                            "name": var_name,
                            "long_name": var.attrs.get("long_name", var_name),
                            "units": var.attrs.get("units", ""),
                            "standard_name": var.attrs.get("standard_name", ""),
                            "dimensions": list(var.dims),
                            "shape": tuple(var.shape),
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return results
    
    def get_timestamps(
            self, file_path: PathLike, variable_name: str
    ) -> list[datetime]:
        """Get timestamps for a specific variable."""
        file_path = Path(file_path)
        
        try:
            with self._open(file_path) as ds:
                if variable_name not in ds.data_vars:
                    return []
                var = ds[variable_name]
                time_dim = self._time_dim(var)
                if not time_dim:
                    return []
                return self._collect_timestamps(var.coords[time_dim])
        except Exception as e:
            self.logger.error(f"Failed to get timestamps from {file_path}: {e}")
            return []
    
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
        """Open a NetCDF variable lazily."""
        file_path = Path(file_path)
        
        ds = self._open(file_path)
        try:
            if variable_name not in ds.data_vars:
                raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
            
            var = ds[variable_name]
            
            # Time selection (lazy)
            time_dim = self._time_dim(var)
            if timestamp is not None and time_dim:
                var = var.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var[time_dim].size > 0:
                var = var.isel({time_dim: 0})
            
            valid_time = self._resolve_valid_time(var, ds, timestamp)
            
            # Orientation check
            y_dim, x_dim = self._spatial_dims(var)
            needs_flip = False
            if y_dim and y_dim in var.coords:
                y_vals = var.coords[y_dim].values
                if len(y_vals) > 1 and y_vals[0] < y_vals[-1]:
                    needs_flip = True
            
            full_height = var.sizes.get(y_dim, var.shape[-2])
            full_width = var.sizes.get(x_dim, var.shape[-1])
            
            # Window slicing (lazy — just adjusts dask graph)
            if window and y_dim and x_dim:
                x_off, y_off, w, h = window
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                var = var.isel(
                    {x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)}
                )
            
            bounds, resolution, crs = self._spatial_info(var, ds)
            
            yield VariableInfo(
                data=var,
                bounds=bounds,
                crs=crs,
                width=var.sizes.get(x_dim, var.shape[-1]),
                height=var.sizes.get(y_dim, var.shape[-2]),
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=var.attrs.get("units", ""),
                needs_flip=needs_flip,
                metadata={
                    "source_file": str(file_path),
                    "long_name": var.attrs.get("long_name", ""),
                    "standard_name": var.attrs.get("standard_name", ""),
                    "full_width": int(full_width),
                    "full_height": int(full_height),
                },
            )
        finally:
            ds.close()
    
    def extract_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            **kwargs,
    ) -> "ExtractedVariable":
        """Override to apply fill-value replacement after materialization."""
        from .base import ExtractedVariable
        
        with self.open_variable(
                file_path,
                variable_name,
                timestamp=timestamp,
                window=window,
                **kwargs,
        ) as var_info:
            data = var_info.compute()
            
            # NetCDF-specific: replace fill values with NaN
            data = self._apply_fill_value(data, var_info.data)
            
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
    
    # ------------------------------------------------------------------
    # Internal: opening files
    # ------------------------------------------------------------------
    
    def _open(self, file_path: Path) -> xr.Dataset:
        """Open a NetCDF file with lazy loading."""
        return xr.open_dataset(file_path, chunks={})
    
    # ------------------------------------------------------------------
    # Internal: time handling
    # ------------------------------------------------------------------
    
    _TIME_NAMES = {"time", "valid_time", "t", "datetime", "xtime"}
    
    def _time_dim(self, var) -> Optional[str]:
        for d in var.dims:
            if d.lower() in self._TIME_NAMES:
                return d
        return None
    
    def _collect_timestamps(self, time_coord) -> list[datetime]:
        timestamps: list[datetime] = []
        values = np.atleast_1d(time_coord.values)
        for t in values:
            if isinstance(t, np.datetime64):
                timestamps.append(pd.Timestamp(t).to_pydatetime())
        return sorted(timestamps)
    
    def _resolve_valid_time(
            self, var, ds: xr.Dataset, requested_time: Optional[datetime]
    ) -> datetime:
        if requested_time is not None:
            return requested_time
        
        for coord_name in ("valid_time", "time", "t"):
            if coord_name in var.coords:
                t = var.coords[coord_name].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
        
        for attr in ("time_coverage_start", "date_created"):
            if attr in ds.attrs:
                try:
                    return pd.Timestamp(ds.attrs[attr]).to_pydatetime()
                except Exception:
                    pass
        
        return datetime.now(timezone.utc)
    
    # ------------------------------------------------------------------
    # Internal: spatial helpers
    # ------------------------------------------------------------------
    
    _Y_NAMES = {"latitude", "lat", "y"}
    _X_NAMES = {"longitude", "lon", "x"}
    
    def _spatial_dims(self, var) -> tuple[Optional[str], Optional[str]]:
        y_dim = x_dim = None
        for d in var.dims:
            dl = d.lower()
            if dl in self._Y_NAMES:
                y_dim = d
            elif dl in self._X_NAMES:
                x_dim = d
        return y_dim, x_dim
    
    def _spatial_info(
            self, var, ds: xr.Dataset
    ) -> tuple[tuple[float, ...], tuple[float, float], str]:
        """Returns (bounds, resolution, crs). Supports rectilinear and curvilinear grids."""
        lat_name, lon_name = self._find_lat_lon_coords(var)
        
        if lat_name is None or lon_name is None:
            return (0.0, 0.0, 1.0, 1.0), (1.0, 1.0), "EPSG:4326"
        
        lats = np.array(var.coords[lat_name].values)
        lons = np.array(var.coords[lon_name].values)
        
        if np.nanmax(lons) > 180:
            lons = np.where(lons > 180, lons - 360, lons)
        
        lat_res = self._compute_resolution(lats, axis=0)
        lon_res = self._compute_resolution(lons, axis=-1)
        
        bounds = (
            float(np.nanmin(lons) - lon_res / 2),
            float(np.nanmin(lats) - lat_res / 2),
            float(np.nanmax(lons) + lon_res / 2),
            float(np.nanmax(lats) + lat_res / 2),
        )
        crs = self._detect_crs(ds)
        return bounds, (lon_res, lat_res), crs
    
    def _find_lat_lon_coords(self, var) -> tuple[Optional[str], Optional[str]]:
        lat_name = lon_name = None
        for name in var.coords:
            nl = name.lower()
            if nl in self._Y_NAMES and lat_name is None:
                lat_name = name
            elif nl in self._X_NAMES and lon_name is None:
                lon_name = name
        if lat_name is None or lon_name is None:
            for name in var.dims:
                nl = name.lower()
                if nl in self._Y_NAMES and lat_name is None:
                    lat_name = name
                elif nl in self._X_NAMES and lon_name is None:
                    lon_name = name
        return lat_name, lon_name
    
    @staticmethod
    def _compute_resolution(coords: np.ndarray, axis: int) -> float:
        try:
            if coords.ndim == 1 and coords.size > 1:
                return float(np.nanmedian(np.abs(np.diff(coords))))
            elif coords.ndim == 2 and coords.shape[abs(axis)] > 1:
                return float(np.nanmedian(np.abs(np.diff(coords, axis=axis))))
        except Exception:
            pass
        return 1.0
    
    @staticmethod
    def _detect_crs(ds: xr.Dataset) -> str:
        if "crs" in ds.attrs:
            return ds.attrs["crs"]
        if "spatial_ref" in ds.data_vars:
            return ds["spatial_ref"].attrs.get("crs_wkt", "EPSG:4326")
        return "EPSG:4326"
    
    # ------------------------------------------------------------------
    # Internal: data helpers
    # ------------------------------------------------------------------
    
    @staticmethod
    def _apply_fill_value(data: np.ndarray, var) -> np.ndarray:
        """Replace fill values with NaN."""
        fill_value = None
        if hasattr(var, "encoding") and "_FillValue" in var.encoding:
            fill_value = var.encoding["_FillValue"]
        if fill_value is None:
            fill_value = var.attrs.get("_FillValue")
        if fill_value is not None:
            data = np.where(data == fill_value, np.nan, data)
        return data
