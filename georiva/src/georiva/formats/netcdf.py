"""
NetCDF Format Plugin

Handles NetCDF (.nc) files using xarray with memory-optimized lazy loading.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

import numpy as np
import pandas as pd
import xarray as xr

from georiva.utils.path import PathLike
from .base import BaseFormatPlugin, ExtractedVariable


class NetCDFFormatPlugin(BaseFormatPlugin):
    name = "netcdf"
    display_name = "NetCDF"
    extensions = [".nc", ".nc4", ".netcdf"]
    
    _SPATIAL_DIM_NAMES = {"latitude", "lat", "y", "longitude", "lon", "x"}
    _TIME_NAMES = {"time", "valid_time", "t", "datetime", "xtime"}
    
    def can_handle(self, file_path: PathLike) -> bool:
        
        file_path = Path(file_path)
        
        if file_path.suffix.lower() in self.extensions:
            return True
        
        # Check magic bytes (CDF or HDF5)
        try:
            with open(file_path, "rb") as f:
                magic = f.read(4)
                # Classic NetCDF
                if magic[:3] == b"CDF":
                    return True
                # HDF5-based NetCDF4
                if magic == b"\x89HDF":
                    return True
        except Exception:
            pass
        
        return False
    
    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        
        file_path = Path(file_path)
        
        variables: list[dict] = []
        
        try:
            with xr.open_dataset(file_path, chunks={}) as ds:
                for var_name, var_data in ds.data_vars.items():
                    selector_names = self._list_dim_selectors_for_var(var_data)
                    
                    variables.append(
                        {
                            "name": var_name,
                            "long_name": var_data.attrs.get("long_name", var_name),
                            "units": var_data.attrs.get("units", ""),
                            "dimensions": list(var_data.dims),
                            "available_dim_selectors": sorted(selector_names),
                            "shape": tuple(var_data.shape),
                            "standard_name": var_data.attrs.get("standard_name", ""),
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return variables
    
    def get_timestamps(self, file_path: PathLike) -> list[datetime]:
        
        file_path = Path(file_path)
        
        timestamps: list[datetime] = []
        
        try:
            with xr.open_dataset(file_path, chunks={}) as ds:
                time_name = self._find_time_coord_name(ds)
                if not time_name:
                    return []
                
                times = ds.coords[time_name].values
                
                if isinstance(times, np.ndarray):
                    for t in times.flat:
                        if isinstance(t, np.datetime64):
                            timestamps.append(pd.Timestamp(t).to_pydatetime())
                elif isinstance(times, np.datetime64):
                    timestamps.append(pd.Timestamp(times).to_pydatetime())
        
        except Exception as e:
            self.logger.error(f"Failed to get timestamps from {file_path}: {e}")
        
        return sorted(timestamps)
    
    def get_metadata_for_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> dict:
        """
        Lightweight scan to get dimensions and bounds without reading full data.
        """
        
        file_path = Path(file_path)
        
        with xr.open_dataset(file_path, chunks={}) as ds:
            if variable_name not in ds.data_vars:
                raise ValueError(f"Variable '{variable_name}' not found")
            
            var = ds[variable_name]
            
            # time selection (cheap)
            time_dim = self._find_time_dim(var)
            if timestamp is not None and time_dim:
                var = var.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var[time_dim].size > 0:
                var = var.isel({time_dim: 0})
            
            # generic dim selection
            var = self._apply_dim_selectors(var, dim_selectors)
            
            bounds, _, crs = self._get_spatial_info(var, ds)
            
            y_dim, x_dim = self._find_spatial_dims(var)
            height = var.sizes[y_dim] if y_dim else var.shape[-2]
            width = var.sizes[x_dim] if x_dim else var.shape[-1]
            
            return {"width": int(width), "height": int(height), "bounds": bounds, "crs": crs}
    
    def get_lazy_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> tuple[Any, Any]:
        """
        Returns (lazy_dataarray, close_callable).
        """
        
        file_path = Path(file_path)
        
        ds = xr.open_dataset(file_path, chunks={})
        
        if variable_name not in ds.data_vars:
            ds.close()
            raise ValueError(f"Variable '{variable_name}' not found")
        
        var = ds[variable_name]
        
        time_dim = self._find_time_dim(var)
        if timestamp is not None and time_dim:
            var = var.sel({time_dim: timestamp}, method="nearest")
        elif time_dim and var[time_dim].size > 0:
            var = var.isel({time_dim: 0})
        
        var = self._apply_dim_selectors(var, dim_selectors)
        
        return var, ds.close
    
    def extract_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> ExtractedVariable:
        """
        Extract a variable from NetCDF.

        Args:
            dim_selectors:
                Generic selection dict applied via xarray (.sel/.isel).
                Examples:
                  {"level": 850}
                  {"pressure": 500}
                  {"member": 0}
                  {"depth": slice(0, 10)}
        """
        self.logger.info(
            f"Extracting {variable_name} from {file_path} (window={window}, dim_selectors={dim_selectors})"
        )
        
        file_path = Path(file_path)
        
        with xr.open_dataset(file_path, chunks={}) as ds:
            if variable_name not in ds.data_vars:
                raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
            
            var_data = ds[variable_name]
            
            # 1) time selection
            time_dim = self._find_time_dim(var_data)
            if timestamp is not None and time_dim:
                var_data = var_data.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var_data[time_dim].size > 0:
                var_data = var_data.isel({time_dim: 0})
            
            valid_time = self._get_valid_time(ds, var_data, timestamp)
            
            # 2) generic dim selectors
            var_data = self._apply_dim_selectors(var_data, dim_selectors)
            
            # 3) full dimensions (post selection -> likely 2D)
            y_dim, x_dim = self._find_spatial_dims(var_data)
            full_height = var_data.sizes[y_dim] if y_dim else var_data.shape[-2]
            full_width = var_data.sizes[x_dim] if x_dim else var_data.shape[-1]
            
            # 4) Check if latitude is ascending (south-to-north) before windowing
            # We need to know this for the flip later
            needs_flip = False
            if y_dim and y_dim in var_data.coords:
                y_coords = var_data.coords[y_dim].values
                if len(y_coords) > 1 and y_coords[0] < y_coords[-1]:
                    needs_flip = True
            
            # 5) window slicing (works well for rectilinear grids / dim-based lat/lon)
            if window and x_dim and y_dim:
                x_off, y_off, w, h = window
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                var_data = var_data.isel({x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)})
            elif window:
                x_off, y_off, w, h = window
                var_data = var_data[..., y_off: y_off + h, x_off: x_off + w]
            
            # 6) spatial info
            bounds, resolution, crs = self._get_spatial_info(var_data, ds)
            
            # 7) load data
            data = np.array(var_data.values)
            
            if data.ndim > 2:
                data = data.squeeze()
            
            # fill value handling
            fill_value = None
            if hasattr(var_data, "encoding") and "_FillValue" in var_data.encoding:
                fill_value = var_data.encoding.get("_FillValue")
            if fill_value is None:
                fill_value = var_data.attrs.get("_FillValue")
            
            if fill_value is not None:
                data = np.where(data == fill_value, np.nan, data)
            
            # 8) Ensure image orientation (row 0 = north)
            # If latitude was ascending (south-to-north), flip to image order
            if needs_flip:
                self.logger.debug("Flipping data array vertically to ensure north-up orientation")
                data = np.flipud(data)
            
            # Final sizing
            width = int(data.shape[-1]) if data.ndim >= 2 else int(data.shape[0])
            height = int(data.shape[-2]) if data.ndim >= 2 else 1
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs=crs,
                width=width,
                height=height,
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=var_data.attrs.get("units", ""),
                metadata={
                    "source_file": str(file_path),
                    "long_name": var_data.attrs.get("long_name", ""),
                    "standard_name": var_data.attrs.get("standard_name", ""),
                    "full_width": int(full_width),
                    "full_height": int(full_height),
                    "dim_selectors": dim_selectors or {},
                },
            )
    
    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    
    def _list_dim_selectors_for_var(self, var_data) -> set[str]:
        """
        Advertise only *real* xarray selectors:
        - dims excluding spatial/time
        - coords that have dimensions (non-scalar), excluding spatial/time
        """
        ignored = self._SPATIAL_DIM_NAMES | self._TIME_NAMES | {"step"}
        
        selector_names: set[str] = set()
        
        for d in var_data.dims:
            if d.lower() not in ignored:
                selector_names.add(d)
        
        for c, coord in var_data.coords.items():
            if c.lower() in ignored:
                continue
            # only non-scalar coords
            if getattr(coord, "dims", ()) and len(coord.dims) > 0:
                selector_names.add(c)
        
        return selector_names
    
    def _apply_dim_selectors(self, var_data, dim_selectors: Optional[dict[str, object]]):
        if not dim_selectors:
            return var_data
        
        for dim, val in dim_selectors.items():
            dl = dim.lower()
            if dl in self._SPATIAL_DIM_NAMES:
                raise ValueError("Spatial dimensions must be subset using `window`, not dim_selectors.")
            
            if dim not in var_data.coords and dim not in var_data.dims:
                continue
            
            if isinstance(val, slice):
                try:
                    var_data = var_data.sel({dim: val})
                except Exception:
                    pass
                continue
            
            try:
                var_data = var_data.sel({dim: val}, method="nearest")
            except Exception:
                try:
                    var_data = var_data.sel({dim: val})
                except Exception:
                    pass
        
        return var_data
    
    def _find_spatial_dims(self, var_data) -> Tuple[Optional[str], Optional[str]]:
        y_dim, x_dim = None, None
        
        for name in list(var_data.dims):
            nl = name.lower()
            if nl in {"latitude", "lat", "y"}:
                y_dim = name
            elif nl in {"longitude", "lon", "x"}:
                x_dim = name
        
        return y_dim, x_dim
    
    def _find_time_dim(self, var_data) -> Optional[str]:
        for dim in var_data.dims:
            if dim.lower() in self._TIME_NAMES:
                return dim
        return None
    
    def _find_time_coord_name(self, ds) -> Optional[str]:
        for name in ["time", "valid_time", "t", "datetime", "XTIME", "xtime"]:
            if name in ds.coords:
                return name
        return None
    
    def _get_valid_time(self, ds, var_data, requested_time: Optional[datetime]) -> datetime:
        if requested_time is not None:
            return requested_time
        
        # Try from variable coords
        for time_coord in ["time", "valid_time", "t", "datetime", "XTIME", "xtime"]:
            if time_coord in var_data.coords:
                t = var_data.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
        
        # Try dataset attrs
        for attr in ["time_coverage_start", "date_created"]:
            if attr in ds.attrs:
                try:
                    return pd.Timestamp(ds.attrs[attr]).to_pydatetime()
                except Exception:
                    pass
        
        return datetime.utcnow()
    
    def _get_spatial_info(self, var_data, ds) -> tuple[tuple, tuple, str]:
        """
        Supports:
        - Rectilinear grids (1D lat/lon coords)
        - Curvilinear grids (2D lat/lon coords): bounds from min/max, resolution best-effort
        """
        lat_name = None
        lon_name = None
        
        # Prefer coords
        for name in list(var_data.coords):
            nl = name.lower()
            if nl in {"latitude", "lat", "y"}:
                lat_name = name
            elif nl in {"longitude", "lon", "x"}:
                lon_name = name
        
        # Fallback to dims
        if lat_name is None or lon_name is None:
            for name in list(var_data.dims):
                nl = name.lower()
                if nl in {"latitude", "lat", "y"}:
                    lat_name = name
                elif nl in {"longitude", "lon", "x"}:
                    lon_name = name
        
        if lat_name is None or lon_name is None:
            # Non-geo array fallback
            lat_res, lon_res = 1.0, 1.0
            height, width = var_data.shape[-2:]
            return (0.0, 0.0, float(width), float(height)), (lon_res, lat_res), "EPSG:4326"
        
        lats = np.array(var_data.coords[lat_name].values)
        lons = np.array(var_data.coords[lon_name].values)
        
        # Bounds (works for 1D or 2D)
        south = float(np.nanmin(lats))
        north = float(np.nanmax(lats))
        west = float(np.nanmin(lons))
        east = float(np.nanmax(lons))
        
        # Resolution best-effort:
        # - If 1D, use neighbor diffs
        # - If 2D, take median diff along last axis for lon and along first axis for lat (rough)
        lat_res = 1.0
        lon_res = 1.0
        
        try:
            if lats.ndim == 1 and lats.size > 1:
                lat_res = float(np.nanmedian(np.abs(np.diff(lats))))
            elif lats.ndim == 2 and lats.shape[0] > 1:
                lat_res = float(np.nanmedian(np.abs(np.diff(lats, axis=0))))
        except Exception:
            pass
        
        try:
            if lons.ndim == 1 and lons.size > 1:
                lon_res = float(np.nanmedian(np.abs(np.diff(lons))))
            elif lons.ndim == 2 and lons.shape[1] > 1:
                lon_res = float(np.nanmedian(np.abs(np.diff(lons, axis=1))))
        except Exception:
            pass
        
        # Expand bounds by half-res where sensible
        west = west - lon_res / 2
        east = east + lon_res / 2
        south = south - lat_res / 2
        north = north + lat_res / 2
        
        bounds = (west, south, east, north)
        resolution = (lon_res, lat_res)
        
        # CRS discovery (best-effort)
        crs = "EPSG:4326"
        if "crs" in ds.attrs:
            crs = ds.attrs["crs"]
        elif "spatial_ref" in ds.data_vars:
            crs = ds["spatial_ref"].attrs.get("crs_wkt", crs)
        
        return bounds, resolution, crs
