"""
GeoTIFF Format Plugin (Lazy-first)

Two jobs:
1. List variables (bands) in a GeoTIFF file.
2. Open a band lazily for streaming stats or materialized extraction.

Design:
- Variables are exposed as "band_1", "band_2", ..., "band_N".
- Band index is derived from the variable name.
- open_variable() uses rioxarray for dask-backed lazy access.
- extract_variable() overrides the base to use rasterio windowed reading
  (more efficient for GeoTIFF than materializing a dask graph).
- Timestamps are parsed from the filename.
"""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Generator

import numpy as np
import rasterio
import xarray as xr
from rasterio.windows import Window

from georiva.utils.path import PathLike
from .base import BaseFormatPlugin, ExtractedVariable, VariableInfo

logger = logging.getLogger(__name__)


class GeoTIFFFormatPlugin(BaseFormatPlugin):
    name = "geotiff"
    display_name = "GeoTIFF"
    extensions = [".tif", ".tiff", ".geotiff"]
    
    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        try:
            with open(file_path, "rb") as f:
                magic = f.read(4)
                return magic[:2] in (b"II", b"MM")
        except Exception:
            return False
    
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List all bands in the GeoTIFF file.

        Returns a list of dicts, each containing:
            - name: "band_1", "band_2", etc. (use in open_variable)
            - long_name: band description or "Band N"
            - units: band units
            - dimensions: always ["y", "x"]
            - shape: (height, width)
            - dtype: data type string
            - band_index: 1-based band number
        """
        file_path = Path(file_path)
        results: list[dict] = []
        
        try:
            with rasterio.open(file_path) as src:
                descriptions = list(getattr(src, "descriptions", []) or [])
                units = list(getattr(src, "units", []) or [])
                
                for i in range(1, src.count + 1):
                    desc = descriptions[i - 1] if i - 1 < len(descriptions) else None
                    unit = units[i - 1] if i - 1 < len(units) else ""
                    
                    results.append(
                        {
                            "name": f"band_{i}",
                            "long_name": desc or f"Band {i}",
                            "units": unit or "",
                            "dimensions": ["y", "x"],
                            "shape": (src.height, src.width),
                            "dtype": str(src.dtypes[i - 1]),
                            "band_index": i,
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return results
    
    def get_timestamps(self, file_path: PathLike, variable_name: str = "") -> list[datetime]:
        """Extract timestamps from the filename. Same for all bands."""
        file_path = Path(file_path)
        dt = self._parse_timestamp_from_filename(file_path.name)
        return [dt] if dt else []
    
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
        Open a GeoTIFF band lazily via rioxarray.

        The returned DataArray is dask-backed — no pixel data is read until
        you call .compute() or access .values.
        """
        file_path = Path(file_path)
        band = self._parse_band_index(variable_name)
        
        # Open lazily with rioxarray
        ds = xr.open_dataset(file_path, engine="rasterio", chunks={})
        try:
            # rioxarray puts bands in a "band" dimension
            var_names = list(ds.data_vars)
            if not var_names:
                raise ValueError(f"No data variables found in {file_path}")
            
            var = ds[var_names[0]]
            
            # Select band
            if "band" in var.dims:
                if band < 1 or band > var.sizes["band"]:
                    raise ValueError(
                        f"Band {band} not found (file has {var.sizes['band']} bands)"
                    )
                var = var.sel(band=band)
            
            # Window slicing (lazy)
            y_dim, x_dim = self._spatial_dims(var)
            if window and y_dim and x_dim:
                x_off, y_off, w, h = window
                full_w = var.sizes.get(x_dim, var.shape[-1])
                full_h = var.sizes.get(y_dim, var.shape[-2])
                w = min(w, full_w - x_off)
                h = min(h, full_h - y_off)
                var = var.isel(
                    {x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)}
                )
            
            # Spatial info from rasterio (more reliable than xarray coords for GeoTIFF)
            with rasterio.open(file_path) as src:
                bounds, resolution, crs, needs_flip = self._spatial_from_rasterio(
                    src, window
                )
                full_width = src.width
                full_height = src.height
                unit = ""
                long_name = ""
                src_units = list(getattr(src, "units", []) or [])
                src_descs = list(getattr(src, "descriptions", []) or [])
                if band - 1 < len(src_units):
                    unit = src_units[band - 1] or ""
                if band - 1 < len(src_descs):
                    long_name = src_descs[band - 1] or ""
            
            valid_time = timestamp
            if valid_time is None:
                ts = self.get_timestamps(file_path)
                valid_time = ts[0] if ts else datetime.now(timezone.utc)
            
            yield VariableInfo(
                data=var,
                bounds=bounds,
                crs=crs,
                width=var.sizes.get(x_dim, var.shape[-1]),
                height=var.sizes.get(y_dim, var.shape[-2]),
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=unit,
                needs_flip=needs_flip,
                metadata={
                    "source_file": str(file_path),
                    "long_name": long_name,
                    "band_index": band,
                    "full_width": full_width,
                    "full_height": full_height,
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
    ) -> ExtractedVariable:
        """
        Override: use rasterio windowed reading for efficient materialization.

        Rasterio's windowed read is more efficient than dask for single-band
        GeoTIFF extraction since it reads exactly the bytes needed.
        """
        file_path = Path(file_path)
        band = self._parse_band_index(variable_name)
        
        with rasterio.open(file_path) as src:
            if band < 1 or band > src.count:
                raise ValueError(f"Band {band} not found (file has {src.count} bands)")
            
            rio_window = None
            if window:
                x_off, y_off, w, h = window
                rio_window = Window(col_off=x_off, row_off=y_off, width=w, height=h)
            
            data = src.read(band, window=rio_window)
            
            # Replace nodata with NaN
            if src.nodata is not None:
                data = data.astype(float, copy=False)
                data = np.where(data == src.nodata, np.nan, data)
            
            bounds, resolution, crs, needs_flip = self._spatial_from_rasterio(
                src, window
            )
            if needs_flip:
                data = np.flipud(data)
            
            valid_time = timestamp
            if valid_time is None:
                ts = self.get_timestamps(file_path)
                valid_time = ts[0] if ts else datetime.now(timezone.utc)
            
            descriptions = list(getattr(src, "descriptions", []) or [])
            units = list(getattr(src, "units", []) or [])
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs=crs,
                width=int(data.shape[1]),
                height=int(data.shape[0]),
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=units[band - 1] if band - 1 < len(units) else "",
                metadata={
                    "source_file": str(file_path),
                    "long_name": descriptions[band - 1]
                    if band - 1 < len(descriptions)
                    else "",
                    "band_index": band,
                    "driver": src.driver,
                    "dtype": str(src.dtypes[band - 1]),
                    "full_width": int(src.width),
                    "full_height": int(src.height),
                },
            )
    
    def get_metadata_for_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            **kwargs,
    ) -> dict:
        """Override: use rasterio directly — faster than opening xarray."""
        file_path = Path(file_path)
        band = self._parse_band_index(variable_name)
        
        with rasterio.open(file_path) as src:
            if band < 1 or band > src.count:
                raise ValueError(f"Band {band} not found (file has {src.count} bands)")
            
            b = src.bounds
            return {
                "width": int(src.width),
                "height": int(src.height),
                "bounds": (float(b.left), float(b.bottom), float(b.right), float(b.top)),
                "crs": str(src.crs) if src.crs else "EPSG:4326",
            }
    
    # ------------------------------------------------------------------
    # Internal: band resolution
    # ------------------------------------------------------------------
    
    @staticmethod
    def _parse_band_index(variable_name: str) -> int:
        """Extract band index from variable name like 'band_3'. Defaults to 1."""
        if variable_name.startswith("band_"):
            try:
                return int(variable_name.split("_", 1)[1])
            except (ValueError, IndexError):
                pass
        return 1
    
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
    
    @staticmethod
    def _spatial_from_rasterio(
            src, window: Optional[tuple[int, int, int, int]]
    ) -> tuple[tuple[float, ...], tuple[float, float], str, bool]:
        """
        Extract bounds, resolution, CRS, and flip flag from a rasterio source.

        Returns: (bounds, resolution, crs, needs_flip)
        """
        if window:
            x_off, y_off, w, h = window
            rio_window = Window(col_off=x_off, row_off=y_off, width=w, height=h)
            wb = src.window_bounds(rio_window)
            bounds = (float(wb.left), float(wb.bottom), float(wb.right), float(wb.top))
        else:
            b = src.bounds
            bounds = (float(b.left), float(b.bottom), float(b.right), float(b.top))
        
        transform = src.transform
        resolution = (float(abs(transform.a)), float(abs(transform.e)))
        crs = str(src.crs) if src.crs else "EPSG:4326"
        needs_flip = transform.e > 0
        
        return bounds, resolution, crs, needs_flip
    
    # ------------------------------------------------------------------
    # Internal: time handling
    # ------------------------------------------------------------------
    
    _TIMESTAMP_PATTERNS = [
        (r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", "%Y-%m-%dT%H:%M:%S"),
        (r"(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
        (r"(\d{8})_(\d{4})", None),  # YYYYMMDD_HHMM
        (r"(\d{14})", "%Y%m%d%H%M%S"),
        (r"(\d{8})", "%Y%m%d"),
    ]
    
    @classmethod
    def _parse_timestamp_from_filename(cls, filename: str) -> Optional[datetime]:
        for pattern, fmt in cls._TIMESTAMP_PATTERNS:
            match = re.search(pattern, filename)
            if not match:
                continue
            try:
                if fmt is None:
                    date_str = match.group(1) + match.group(2)
                    return datetime.strptime(date_str, "%Y%m%d%H%M")
                else:
                    return datetime.strptime(match.group(1), fmt)
            except (ValueError, IndexError):
                continue
        return None
