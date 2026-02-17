"""
GRIB Format Plugin (Lazy-first)

Two jobs:
1. List variables (with timestamps) in a GRIB file.
2. Open a variable lazily for streaming stats or materialized extraction.

Design:
- A GRIB "variable" is uniquely identified by (short_name, type_of_level, level).
  We call this a VariableKey.
- list_variables() returns VariableKey entries. Pass one back to open_variable().
- cfgrib internals (filter_by_keys) stay internal — callers never see them.
- open_variable() is the primary interface: context manager, lazy DataArray.
- extract_variable() and get_metadata_for_variable() are inherited from BaseFormatPlugin.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Generator

import cfgrib
import numpy as np
import pandas as pd
import xarray as xr

from georiva.utils.path import PathLike
from .base import BaseFormatPlugin, VariableInfo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VariableKey:
    """Uniquely identifies a variable within a GRIB file."""
    
    short_name: str
    type_of_level: str
    level: Optional[int] = None
    
    def to_filter(self) -> dict:
        """Convert to cfgrib filter_by_keys."""
        fk: dict = {
            "shortName": self.short_name,
            "typeOfLevel": self.type_of_level,
        }
        if self.level is not None:
            fk["level"] = self.level
        return fk
    
    def __str__(self) -> str:
        if self.level is not None:
            return f"{self.short_name} ({self.type_of_level}={self.level})"
        return f"{self.short_name} ({self.type_of_level})"


class GRIBFormatPlugin(BaseFormatPlugin):
    name = "grib2"
    display_name = "GRIB"
    extensions = [".grib", ".grib2", ".grb", ".grb2"]
    
    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        try:
            with open(file_path, "rb") as f:
                return f.read(4) == b"GRIB"
        except Exception:
            return False
    
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List all variables in the GRIB file.

        Returns a list of dicts, each containing:
            - key: VariableKey (pass to open_variable / get_timestamps)
            - name: xarray variable name
            - short_name: GRIB shortName
            - long_name: human-readable name
            - units: variable units
            - dimensions: list of dimension names
            - shape: tuple of dimension sizes
        """
        results: list[dict] = []
        seen: set[VariableKey] = set()
        
        for ds, var_name, var in self._iter_variables(file_path):
            with ds:
                attrs = var.attrs
                key = VariableKey(
                    short_name=attrs.get("GRIB_shortName", var_name),
                    type_of_level=attrs.get("GRIB_typeOfLevel", "unknown"),
                    level=self._extract_level(var, attrs),
                )
                if key in seen:
                    continue
                seen.add(key)
                
                results.append(
                    {
                        "key": key,
                        "name": var_name,
                        "short_name": key.short_name,
                        "long_name": attrs.get("long_name", var_name),
                        "units": attrs.get("units", ""),
                        "dimensions": list(var.dims),
                        "shape": tuple(var.shape),
                    }
                )
        
        return results
    
    def get_timestamps(
            self, file_path: PathLike, variable_name: str, *, key: Optional[VariableKey] = None
    ) -> list[datetime]:
        """
        Get timestamps for a specific variable.

        Preferred: pass key from list_variables() for deterministic behavior.
        Fallback: pass variable_name as shortName or xarray name.
        """
        file_path = Path(file_path)
        
        if key is not None:
            ds = self._open(file_path, key.to_filter())
        else:
            ds, _ = self._find_by_name(file_path, variable_name)
        
        if ds is None:
            return []
        with ds:
            return sorted(self._collect_timestamps(ds))
    
    @contextmanager
    def open_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            key: Optional[VariableKey] = None,
    ) -> Generator[VariableInfo, None, None]:
        """
        Open a GRIB variable lazily.

        Preferred: pass a VariableKey from list_variables().
        Fallback: pass variable_name as shortName or xarray name (slower).
        """
        file_path = Path(file_path)
        
        if key is not None:
            ds = self._open(file_path, key.to_filter())
            if ds is None:
                raise ValueError(f"Variable not found for key: {key}")
            xr_name = self._find_xr_name(ds, key.short_name)
        else:
            ds, xr_name = self._find_by_name(file_path, variable_name)
        
        if ds is None or xr_name is None:
            raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
        
        try:
            var = ds[xr_name]
            
            # Time selection
            time_dim = self._time_dim(var)
            if timestamp is not None and time_dim:
                var = var.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var[time_dim].size > 0:
                var = var.isel({time_dim: 0})
            
            valid_time = self._resolve_valid_time(var, ds)
            
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
            
            bounds, resolution = self._spatial_info(var)
            
            yield VariableInfo(
                data=var,
                bounds=bounds,
                crs="EPSG:4326",
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
                    "full_width": int(full_width),
                    "full_height": int(full_height),
                },
            )
        finally:
            ds.close()
    
    # ------------------------------------------------------------------
    # Internal: opening GRIB files
    # ------------------------------------------------------------------
    
    def _open(self, file_path: Path, filter_by_keys: dict) -> Optional[xr.Dataset]:
        """Open a single GRIB view. Returns None if no data matches."""
        try:
            ds = xr.open_dataset(
                file_path,
                engine="cfgrib",
                chunks={},
                backend_kwargs={"filter_by_keys": filter_by_keys},
            )
            if ds.data_vars:
                return ds
            ds.close()
        except Exception as e:
            logger.debug(f"Failed to open {file_path} with {filter_by_keys}: {e}")
        return None
    
    def _open_all(self, file_path: Path) -> list[xr.Dataset]:
        """Discovery mode: let cfgrib split the file into datasets."""
        try:
            return cfgrib.open_datasets(str(file_path))
        except Exception as e:
            logger.error(f"cfgrib.open_datasets failed for {file_path}: {e}")
            return []
    
    def _iter_variables(self, file_path: PathLike):
        """Yield (dataset, var_name, var_dataarray) for every variable in the file."""
        file_path = Path(file_path)
        for ds in self._open_all(file_path):
            for var_name, var in ds.data_vars.items():
                yield ds, var_name, var
    
    # ------------------------------------------------------------------
    # Internal: variable lookup
    # ------------------------------------------------------------------
    
    def _find_xr_name(self, ds: xr.Dataset, short_name: str) -> Optional[str]:
        """Find the xarray variable name matching a GRIB shortName."""
        for var_name in ds.data_vars:
            if var_name == short_name:
                return var_name
            if ds[var_name].attrs.get("GRIB_shortName") == short_name:
                return var_name
        return None
    
    def _find_by_name(
            self, file_path: Path, variable_name: str
    ) -> tuple[Optional[xr.Dataset], Optional[str]]:
        """Fallback: search all datasets for a variable by name or shortName."""
        for ds in self._open_all(file_path):
            if variable_name in ds.data_vars:
                return ds, variable_name
            xr_name = self._find_xr_name(ds, variable_name)
            if xr_name:
                return ds, xr_name
            ds.close()
        return None, None
    
    def _extract_level(self, var, attrs: dict) -> Optional[int]:
        """Extract the level value from a variable's coordinates."""
        type_of_level = attrs.get("GRIB_typeOfLevel", "")
        if type_of_level and type_of_level in var.coords:
            val = var.coords[type_of_level].values
            if np.isscalar(val) or val.ndim == 0:
                return int(val)
        return None
    
    # ------------------------------------------------------------------
    # Internal: time handling
    # ------------------------------------------------------------------
    
    _TIME_DIMS = ("time", "valid_time", "forecast_time")
    
    def _time_dim(self, var) -> Optional[str]:
        for d in var.dims:
            if d in self._TIME_DIMS:
                return d
        return None
    
    def _collect_timestamps(self, ds: xr.Dataset) -> set[datetime]:
        """Collect timestamps from a dataset, preferring valid_time."""
        timestamps: set[datetime] = set()
        for coord_name in ("valid_time", "time"):
            if coord_name not in ds.coords:
                continue
            values = np.atleast_1d(ds.coords[coord_name].values)
            for t in values:
                if isinstance(t, np.datetime64):
                    timestamps.add(pd.Timestamp(t).to_pydatetime())
            if timestamps:
                break  # Don't mix valid_time and time
        return timestamps
    
    def _resolve_valid_time(self, var, ds: xr.Dataset) -> datetime:
        for coord_name in ("valid_time", "time"):
            if coord_name in var.coords:
                t = var.coords[coord_name].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
            if coord_name in ds.coords:
                t = ds.coords[coord_name].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
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
    
    def _spatial_info(self, var) -> tuple[tuple[float, ...], tuple[float, float]]:
        """Returns (bounds, resolution). Bounds = (west, south, east, north)."""
        y_dim, x_dim = self._spatial_dims(var)
        
        if y_dim is None or x_dim is None:
            for c in var.coords:
                cl = c.lower()
                if cl in self._Y_NAMES and y_dim is None:
                    y_dim = c
                elif cl in self._X_NAMES and x_dim is None:
                    x_dim = c
        
        if y_dim is None or x_dim is None:
            return (0.0, 0.0, 1.0, 1.0), (1.0, 1.0)
        
        lats = var.coords[y_dim].values
        lons = var.coords[x_dim].values
        
        lat_res = abs(float(lats[1] - lats[0])) if len(lats) > 1 else 1.0
        lon_res = abs(float(lons[1] - lons[0])) if len(lons) > 1 else 1.0
        
        if np.nanmax(lons) > 180:
            lons = np.where(lons > 180, lons - 360, lons)
        
        bounds = (
            float(np.nanmin(lons) - lon_res / 2),
            float(np.nanmin(lats) - lat_res / 2),
            float(np.nanmax(lons) + lon_res / 2),
            float(np.nanmax(lats) + lat_res / 2),
        )
        return bounds, (lon_res, lat_res)
