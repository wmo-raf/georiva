"""
NetCDF Format Plugin

Handles NetCDF (.nc) files using xarray with memory-optimized lazy loading.
Updated to support native vertical level selection.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr

from .registry import BaseFormatPlugin, ExtractedVariable, FormatRegistry


@FormatRegistry.register
class NetCDFFormatPlugin(BaseFormatPlugin):
    """
    Format plugin for NetCDF files.
    Uses xarray with netcdf4 backend.
    
    Optimized for large files using lazy loading and windowed reads.
    """
    
    name = "netcdf"
    display_name = "NetCDF"
    extensions = ['.nc', '.nc4', '.netcdf']
    
    def can_handle(self, file_path: Path) -> bool:
        """Check if file is a NetCDF file."""
        if file_path.suffix.lower() in self.extensions:
            return True
        
        # Check magic bytes (CDF or HDF5)
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(4)
                # Classic NetCDF
                if magic[:3] == b'CDF':
                    return True
                # HDF5-based NetCDF4
                if magic == b'\x89HDF':
                    return True
        except Exception:
            pass
        
        return False
    
    def list_variables(self, file_path: Path) -> list[dict]:
        """List available variables in the NetCDF file."""
        
        variables = []
        
        try:
            # Use chunks={} to avoid loading data just to list variables
            with xr.open_dataset(file_path, chunks={}) as ds:
                for var_name, var_data in ds.data_vars.items():
                    variables.append({
                        'name': var_name,
                        'long_name': var_data.attrs.get('long_name', var_name),
                        'units': var_data.attrs.get('units', ''),
                        'dimensions': list(var_data.dims),
                        'shape': var_data.shape,
                        'standard_name': var_data.attrs.get('standard_name', ''),
                    })
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return variables
    
    def get_timestamps(self, file_path: Path) -> list[datetime]:
        """Get available timestamps in the NetCDF file."""
        
        timestamps = []
        
        try:
            with xr.open_dataset(file_path, chunks={}) as ds:
                # Look for time coordinate
                time_coord = None
                for name in ['time', 'valid_time', 't', 'datetime']:
                    if name in ds.coords:
                        time_coord = ds.coords[name]
                        break
                
                if time_coord is not None:
                    # Loading just the time coordinate is lightweight
                    times = time_coord.values
                    if isinstance(times, np.ndarray):
                        for t in times.flat:
                            if isinstance(t, np.datetime64):
                                timestamps.append(pd.Timestamp(t).to_pydatetime())
                    elif isinstance(times, np.datetime64):
                        timestamps.append(pd.Timestamp(times).to_pydatetime())
        
        except Exception as e:
            self.logger.error(f"Failed to get timestamps from {file_path}: {e}")
        
        return sorted(timestamps)
    
    def get_metadata(self, file_path: Path, dataset) -> dict:
        """
        Lightweight scan to get dimensions and bounds without reading data.
        """
        # Unpack vertical settings if available
        required_dim = getattr(dataset, 'vertical_dimension', None)
        vertical_value = getattr(dataset, 'vertical_value', None)
        
        # Open with chunks={} to ensure no data is loaded
        with xr.open_dataset(file_path, chunks={}) as ds:
            if dataset.primary_variable not in ds.data_vars:
                raise ValueError(f"Variable '{dataset.primary_variable}' not found")
            
            var = ds[dataset.primary_variable]
            
            # Apply vertical selection for metadata calculation (to get 2D shape)
            if required_dim and vertical_value is not None:
                if required_dim in var.coords or required_dim in var.dims:
                    var = var.sel({required_dim: vertical_value}, method='nearest')
            
            # Extract spatial info (bounds, res, crs)
            bounds, _, crs = self._get_spatial_info(var, ds)
            
            # Find spatial dimensions to report correct width/height
            y_dim, x_dim = self._find_spatial_dims(var)
            
            height = var.sizes[y_dim] if y_dim else var.shape[-2]
            width = var.sizes[x_dim] if x_dim else var.shape[-1]
            
            return {
                'width': int(width),
                'height': int(height),
                'bounds': bounds,
                'crs': crs
            }
    
    def get_lazy_dataset(self, file_path: Path, dataset, timestamp=None):
        """
        Return a lazy-loaded object (xarray DataArray) for global stats computation.
        The caller is responsible for managing the resource/memory.
        """
        # Unpack vertical settings
        required_dim = getattr(dataset, 'vertical_dimension', None)
        vertical_value = getattr(dataset, 'vertical_value', None)
        
        # Open dataset without context manager so it stays open for the caller
        ds = xr.open_dataset(file_path, chunks={})
        
        if dataset.primary_variable not in ds:
            ds.close()
            raise ValueError(f"Variable '{dataset.primary_variable}' not found")
        
        var_data = ds[dataset.primary_variable]
        
        # 1. Apply time selection
        time_dim = self._find_time_dim(var_data)
        if timestamp is not None and time_dim:
            var_data = var_data.sel({time_dim: timestamp}, method='nearest')
        elif time_dim and var_data[time_dim].size > 0:
            var_data = var_data.isel({time_dim: 0})
        
        # 2. Apply Vertical Selection
        if required_dim and vertical_value is not None:
            if required_dim in var_data.coords or required_dim in var_data.dims:
                var_data = var_data.sel({required_dim: vertical_value}, method='nearest')
        
        return var_data
    
    def extract_variable(
            self,
            file_path: Path,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            secondary_variable: Optional[str] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            vertical_selection: Optional[dict] = None,  # <--- New Argument
    ) -> ExtractedVariable:
        """
        Extract a variable from the NetCDF file.
        Supports 'window' argument for memory-efficient chunked reading.
        Supports 'vertical_selection' dict: {'dim': 'isobaricInhPa', 'value': 850}
        """
        
        self.logger.info(f"Extracting {variable_name} from {file_path} (Window: {window}, Level: {vertical_selection})")
        
        # Open with chunks={} to ensure lazy loading
        with xr.open_dataset(file_path, chunks={}) as ds:
            if variable_name not in ds.data_vars:
                raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
            
            var_data = ds[variable_name]
            
            # 1. Select timestamp if specified
            time_dim = self._find_time_dim(var_data)
            if timestamp is not None and time_dim:
                var_data = var_data.sel({time_dim: timestamp}, method='nearest')
            elif time_dim and var_data[time_dim].size > 0:
                var_data = var_data.isel({time_dim: 0})
            
            # Get valid time before we slice spatial dims
            valid_time = self._get_valid_time(var_data, ds, timestamp)
            
            # 2. Apply Vertical Selection (NEW)
            if vertical_selection:
                dim = vertical_selection['dim']
                val = vertical_selection['value']
                
                if dim in var_data.coords or dim in var_data.dims:
                    var_data = var_data.sel({dim: val}, method='nearest')
                else:
                    self.logger.warning(f"Requested vertical dim '{dim}' not found in {variable_name}")
            
            # 3. Capture Full Dimensions (now that we are down to 2D spatial)
            y_dim, x_dim = self._find_spatial_dims(var_data)
            full_height = var_data.sizes[y_dim] if y_dim else var_data.shape[-2]
            full_width = var_data.sizes[x_dim] if x_dim else var_data.shape[-1]
            
            # 4. Apply Window Slicing (Lazy)
            # This creates a "view" of the data, still no memory used
            if window and x_dim and y_dim:
                x_off, y_off, w, h = window
                # Ensure we don't go out of bounds
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                
                var_data = var_data.isel({
                    x_dim: slice(x_off, x_off + w),
                    y_dim: slice(y_off, y_off + h)
                })
            elif window:
                # Fallback if we couldn't identify named dims but have window
                x_off, y_off, w, h = window
                var_data = var_data[..., y_off:y_off + h, x_off:x_off + w]
            
            # 5. Extract spatial info (Bounds will be for the chunk if windowed)
            bounds, resolution, crs = self._get_spatial_info(var_data, ds)
            
            # 6. Extract Data (Trigger Memory Load)
            # Only NOW do we load bytes into RAM.
            data = var_data.values.copy()
            if data.ndim > 2:
                data = data.squeeze()
            
            # Handle missing values
            if hasattr(var_data, 'encoding') and '_FillValue' in var_data.encoding:
                fill_value = var_data.encoding['_FillValue']
                data = np.where(data == fill_value, np.nan, data)
            
            # 7. Extract Secondary Variable (Vector)
            secondary_data = None
            if secondary_variable:
                if secondary_variable in ds.data_vars:
                    sec_var_data = ds[secondary_variable]
                    
                    # Apply time selection
                    if timestamp is not None and time_dim:
                        sec_var_data = sec_var_data.sel({time_dim: timestamp}, method='nearest')
                    elif time_dim and sec_var_data[time_dim].size > 0:
                        sec_var_data = sec_var_data.isel({time_dim: 0})
                    
                    # Apply Vertical Selection (Secondary)
                    if vertical_selection:
                        dim = vertical_selection['dim']
                        val = vertical_selection['value']
                        if dim in sec_var_data.coords or dim in sec_var_data.dims:
                            sec_var_data = sec_var_data.sel({dim: val}, method='nearest')
                    
                    # Apply spatial window
                    if window and x_dim and y_dim:
                        x_off, y_off, w, h = window
                        sec_var_data = sec_var_data.isel({
                            x_dim: slice(x_off, x_off + w),
                            y_dim: slice(y_off, y_off + h)
                        })
                    elif window:
                        x_off, y_off, w, h = window
                        sec_var_data = sec_var_data[..., y_off:y_off + h, x_off:x_off + w]
                    
                    # Load secondary data
                    secondary_data = sec_var_data.values.copy()
                    if secondary_data.ndim > 2:
                        secondary_data = secondary_data.squeeze()
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs=crs,
                width=data.shape[-1],
                height=data.shape[-2],
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=var_data.attrs.get('units', ''),
                secondary_data=secondary_data,
                metadata={
                    'source_file': str(file_path),
                    'long_name': var_data.attrs.get('long_name', ''),
                    'standard_name': var_data.attrs.get('standard_name', ''),
                    'full_width': full_width,
                    'full_height': full_height,
                },
            )
    
    # --- Helper Methods ---
    
    def _find_spatial_dims(self, var_data) -> Tuple[Optional[str], Optional[str]]:
        """Identify (y_dim, x_dim) names from a variable."""
        y_dim, x_dim = None, None
        
        # Check coordinates and dimensions
        for name in list(var_data.dims):
            name_lower = name.lower()
            if name_lower in ['latitude', 'lat', 'y']:
                y_dim = name
            elif name_lower in ['longitude', 'lon', 'x']:
                x_dim = name
        
        return y_dim, x_dim
    
    def _find_time_dim(self, var_data) -> Optional[str]:
        """Find the time dimension in a variable."""
        for dim in var_data.dims:
            if dim in ['time', 'valid_time', 't', 'datetime']:
                return dim
        return None
    
    def _get_valid_time(self, var_data, ds, requested_time: Optional[datetime]) -> datetime:
        """Extract the valid time."""
        # If we selected a specific time, use that
        if requested_time is not None:
            return requested_time
        
        # Try to get from coordinates
        for time_coord in ['time', 'valid_time', 't', 'datetime']:
            if time_coord in var_data.coords:
                t = var_data.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
        
        # Try dataset attributes
        for attr in ['time_coverage_start', 'date_created']:
            if attr in ds.attrs:
                try:
                    return pd.Timestamp(ds.attrs[attr]).to_pydatetime()
                except Exception:
                    pass
        
        # Default
        from django.utils import timezone
        return timezone.now()
    
    def _get_spatial_info(self, var_data, ds) -> tuple[tuple, tuple, str]:
        """Extract spatial bounds, resolution, and CRS."""
        # Find lat/lon coordinates
        lat_name = None
        lon_name = None
        
        # Look in coords (dims often match, but sometimes coords are separate)
        for name in list(var_data.coords):
            name_lower = name.lower()
            if name_lower in ['latitude', 'lat', 'y']:
                lat_name = name
            elif name_lower in ['longitude', 'lon', 'x']:
                lon_name = name
        
        if lat_name is None or lon_name is None:
            # Fallback to dims if not in coords (less common for NetCDF)
            for name in list(var_data.dims):
                name_lower = name.lower()
                if name_lower in ['latitude', 'lat', 'y']:
                    lat_name = name
                elif name_lower in ['longitude', 'lon', 'x']:
                    lon_name = name
        
        if lat_name is None or lon_name is None:
            # If we really can't find them, assume last 2 dims and unit grid
            # This allows reading non-geo arrays without crashing, though bounds will be dummy
            lat_res, lon_res = 1.0, 1.0
            west, south = 0.0, 0.0
            height, width = var_data.shape[-2:]
            east, north = float(width), float(height)
            return (west, south, east, north), (lon_res, lat_res), "EPSG:4326"
        
        lats = var_data.coords[lat_name].values
        lons = var_data.coords[lon_name].values
        
        # Calculate resolution
        lat_res = abs(lats[1] - lats[0]) if len(lats) > 1 else 1.0
        lon_res = abs(lons[1] - lons[0]) if len(lons) > 1 else 1.0
        
        # Calculate bounds
        west = float(lons.min() - lon_res / 2)
        east = float(lons.max() + lon_res / 2)
        south = float(lats.min() - lat_res / 2)
        north = float(lats.max() + lat_res / 2)
        
        bounds = (west, south, east, north)
        resolution = (lon_res, lat_res)
        
        # Try to get CRS
        crs = "EPSG:4326"  # Default
        if 'crs' in ds.attrs:
            crs = ds.attrs['crs']
        elif 'spatial_ref' in ds.data_vars:
            crs = ds['spatial_ref'].attrs.get('crs_wkt', crs)
        
        return bounds, resolution, crs
