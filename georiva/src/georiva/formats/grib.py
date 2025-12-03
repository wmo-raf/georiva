"""
GRIB Format Plugin

Handles GRIB1 and GRIB2 files using cfgrib/xarray with memory optimization.

"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

from .registry import (
    BaseFormatPlugin,
    ExtractedVariable,
    FormatRegistry
)


@FormatRegistry.register
class GRIBFormatPlugin(BaseFormatPlugin):
    """
    Format plugin for GRIB (GRIdded Binary) files.
    Uses cfgrib backend for xarray to read GRIB files.
    Supports both GRIB1 and GRIB2 formats with chunked/windowed reading.
    """
    
    name = "grib2"
    display_name = "GRIB2"
    extensions = ['.grib', '.grib2', '.grb', '.grb2']
    
    def can_handle(self, file_path: Path) -> bool:
        """Check if file is a GRIB file."""
        if file_path.suffix.lower() in self.extensions:
            return True
        
        # Try to read magic bytes
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(4)
                return magic == b'GRIB'
        except Exception:
            return False
    
    def list_variables(self, file_path: Path) -> list[dict]:
        """List available variables in the GRIB file."""
        variables = []
        
        try:
            # [cite_start]Open with chunks={} to avoid loading data [cite: 339]
            datasets = self._open_grib_multi(file_path, chunks={})
            
            for ds in datasets:
                for var_name, var_data in ds.data_vars.items():
                    # Identify available vertical coordinates for this variable
                    vertical_dims = []
                    for coord_name in var_data.coords:
                        if coord_name not in ['time', 'valid_time', 'latitude', 'longitude', 'lat', 'lon', 'step']:
                            vertical_dims.append(coord_name)
                    
                    variables.append({
                        'name': var_name,
                        'long_name': var_data.attrs.get('long_name', var_name),
                        'units': var_data.attrs.get('units', ''),
                        'dimensions': list(var_data.dims),
                        'available_vertical_dims': vertical_dims,  # Helpful for UI debugging
                        'shape': var_data.shape,
                    })
                ds.close()
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return variables
    
    def get_timestamps(self, file_path: Path) -> list[datetime]:
        """Get available timestamps in the GRIB file."""
        import pandas as pd
        
        timestamps = set()
        
        try:
            datasets = self._open_grib_multi(file_path, chunks={})
            
            for ds in datasets:
                # [cite_start]Check for time dimensions [cite: 344]
                for dim in ['time', 'valid_time', 'forecast_time']:
                    if dim in ds.coords:
                        times = ds.coords[dim].values
                        if hasattr(times, '__iter__'):
                            for t in times:
                                if isinstance(t, np.datetime64):
                                    timestamps.add(pd.Timestamp(t).to_pydatetime())
                        else:
                            if isinstance(times, np.datetime64):
                                timestamps.add(pd.Timestamp(times).to_pydatetime())
                ds.close()
        
        except Exception as e:
            self.logger.error(f"Failed to get timestamps from {file_path}: {e}")
        
        return sorted(list(timestamps))
    
    def get_metadata(self, file_path: Path, dataset) -> dict:
        """
        Lightweight scan to get dimensions and bounds without reading data.
        Respects vertical selection if configured in the dataset.
        """
        # Determine if we need a specific vertical dimension
        required_dim = getattr(dataset, 'vertical_dimension', None)
        
        # Find the specific sub-dataset containing our variable
        target_ds = self._find_variable_dataset(
            file_path,
            dataset.primary_variable,
            chunks={},
            required_dim=required_dim
        )
        
        if target_ds is None:
            raise ValueError(f"Variable '{dataset.primary_variable}' not found")
        
        try:
            var = target_ds[dataset.primary_variable]
            
            # Apply vertical selection for metadata purposes if needed
            # (To ensure we get the shape of the 2D slice, not the 3D volume)
            if required_dim and getattr(dataset, 'vertical_value', None) is not None:
                if required_dim in var.coords:
                    var = var.sel({required_dim: dataset.vertical_value}, method='nearest')
            
            bounds, _, _ = self._get_spatial_info(var)
            
            # Find spatial dimensions
            y_dim, x_dim = self._find_spatial_dims(var)
            
            height = var.sizes[y_dim] if y_dim else var.shape[-2]
            width = var.sizes[x_dim] if x_dim else var.shape[-1]
            
            return {
                'width': int(width),
                'height': int(height),
                'bounds': bounds,
                'crs': "EPSG:4326"  # GRIB is usually WGS84
            }
        finally:
            target_ds.close()
    
    def get_lazy_dataset(self, file_path: Path, dataset, timestamp=None):
        """
        Return a lazy-loaded object (xarray DataArray) for global stats computation.
        """
        # Determine if we need a specific vertical dimension
        required_dim = getattr(dataset, 'vertical_dimension', None)
        vertical_value = getattr(dataset, 'vertical_value', None)
        
        # Find the right dataset
        ds = self._find_variable_dataset(
            file_path,
            dataset.primary_variable,
            chunks={},
            required_dim=required_dim
        )
        
        if ds is None:
            raise ValueError(f"Variable '{dataset.primary_variable}' not found")
        
        var_data = ds[dataset.primary_variable]
        
        # [cite_start]1. Apply time selection [cite: 353]
        time_dim = self._find_time_dim(var_data)
        if timestamp is not None and time_dim:
            var_data = var_data.sel({time_dim: timestamp}, method='nearest')
        elif time_dim and var_data[time_dim].size > 0:
            var_data = var_data.isel({time_dim: 0})
        
        # 2. Apply Vertical Selection
        if required_dim and vertical_value is not None:
            if required_dim in var_data.coords:
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
        Extract a variable from the GRIB file.
        Supports 'window' for memory-efficient chunked reading and vertical level slicing.
        
        vertical_selection: dict with {'dim': str, 'value': float}
        """
        
        self.logger.info(f"Extracting {variable_name} from {file_path} (Window: {window}, Level: {vertical_selection})")
        
        # Unpack vertical settings
        required_dim = vertical_selection['dim'] if vertical_selection else None
        
        # Find the dataset containing our variable (Lazy open)
        # We pass required_dim so we get the correct "typeOfLevel" dataset
        ds = self._find_variable_dataset(
            file_path,
            variable_name,
            chunks={},
            required_dim=required_dim
        )
        
        if ds is None:
            raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
        
        try:
            var_data = ds[variable_name]
            
            # [cite_start]1. Select timestamp [cite: 358]
            time_dim = self._find_time_dim(var_data)
            if timestamp is not None and time_dim:
                var_data = var_data.sel({time_dim: timestamp}, method='nearest')
            elif time_dim and var_data[time_dim].size > 0:
                var_data = var_data.isel({time_dim: 0})
            
            # Get valid time
            valid_time = self._get_valid_time(var_data, ds)
            
            # 2. Apply Vertical Selection (NEW)
            # Must be done BEFORE spatial dimensions are calculated if dimensions are collapsed
            if vertical_selection:
                dim = vertical_selection['dim']
                val = vertical_selection['value']
                
                if dim in var_data.coords:
                    var_data = var_data.sel({dim: val}, method='nearest')
                else:
                    self.logger.warning(f"Requested vertical dim '{dim}' not found. Available: {list(var_data.coords)}")
            
            # 3. Capture Full Dimensions (now that we are down to 2D spatial)
            y_dim, x_dim = self._find_spatial_dims(var_data)
            full_height = var_data.sizes[y_dim] if y_dim else var_data.shape[-2]
            full_width = var_data.sizes[x_dim] if x_dim else var_data.shape[-1]
            
            # [cite_start]4. Apply Window Slicing (Lazy) [cite: 360]
            if window and x_dim and y_dim:
                x_off, y_off, w, h = window
                # Ensure bounds
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                
                var_data = var_data.isel({
                    x_dim: slice(x_off, x_off + w),
                    y_dim: slice(y_off, y_off + h)
                })
            elif window:
                # Fallback positional slicing
                x_off, y_off, w, h = window
                var_data = var_data[..., y_off:y_off + h, x_off:x_off + w]
            
            # [cite_start]5. Extract Spatial Info (for the chunk) [cite: 363]
            bounds, resolution, _ = self._get_spatial_info(var_data)
            
            # [cite_start]6. Load Data (Trigger Memory Load) [cite: 363]
            data = var_data.values
            if data.ndim > 2:
                data = data.squeeze()
            
            # [cite_start]7. Handle Secondary Variable (Vector) [cite: 364]
            secondary_data = None
            if secondary_variable:
                # We assume secondary variable shares the same vertical structure
                secondary_ds = self._find_variable_dataset(
                    file_path,
                    secondary_variable,
                    chunks={},
                    required_dim=required_dim
                )
                
                if secondary_ds is not None:
                    try:
                        sec_var_data = secondary_ds[secondary_variable]
                        
                        # [cite_start]Apply time selection [cite: 366]
                        if timestamp is not None and time_dim:
                            sec_var_data = sec_var_data.sel({time_dim: timestamp}, method='nearest')
                        elif time_dim and sec_var_data[time_dim].size > 0:
                            sec_var_data = sec_var_data.isel({time_dim: 0})
                        
                        # Apply Vertical Selection (Secondary)
                        if vertical_selection:
                            dim = vertical_selection['dim']
                            val = vertical_selection['value']
                            if dim in sec_var_data.coords:
                                sec_var_data = sec_var_data.sel({dim: val}, method='nearest')
                        
                        # [cite_start]Apply spatial window [cite: 367]
                        if window and x_dim and y_dim:
                            x_off, y_off, w, h = window
                            sec_var_data = sec_var_data.isel({
                                x_dim: slice(x_off, x_off + w),
                                y_dim: slice(y_off, y_off + h)
                            })
                        elif window:
                            x_off, y_off, w, h = window
                            sec_var_data = sec_var_data[..., y_off:y_off + h, x_off:x_off + w]
                        
                        secondary_data = sec_var_data.values
                        if secondary_data.ndim > 2:
                            secondary_data = secondary_data.squeeze()
                    finally:
                        secondary_ds.close()
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs="EPSG:4326",  # GRIB typically uses WGS84
                width=data.shape[1] if data.ndim > 1 else data.shape[0],
                height=data.shape[0] if data.ndim > 1 else 1,
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
        
        finally:
            ds.close()
    
    def _open_grib_multi(self, file_path: Path, chunks=None, target_level_type=None) -> list:
        """
        Open a GRIB file.
        If target_level_type is provided, only attempts to open that specific view.
        Otherwise, attempts to discover all available views.
        """
        import xarray as xr
        
        datasets = []
        
        # 1. Determine which filters to apply
        if target_level_type:
            # Optimization: User knows exactly what they want (e.g., 'isobaricInhPa')
            # We only try this specific filter.
            filter_keys_list = [{'typeOfLevel': target_level_type}]
        else:
            # Fallback: Try all common level types
            filter_keys_list = [
                {'typeOfLevel': 'surface'},
                {'typeOfLevel': 'heightAboveGround'},
                {'typeOfLevel': 'meanSea'},
                {'typeOfLevel': 'isobaricInhPa'},
                {'typeOfLevel': 'atmosphere'},
                {'typeOfLevel': 'nominalTop'},
                {'typeOfLevel': 'cloudBase'},
                {'typeOfLevel': 'cloudTop'},
                {'typeOfLevel': 'isothermZero'},
                {},  # Catch-all (no filter)
            ]
        
        # 2. Iterate and Open
        for filter_keys in filter_keys_list:
            try:
                ds = xr.open_dataset(
                    file_path,
                    engine='cfgrib',
                    chunks=chunks,
                    backend_kwargs={'filter_by_keys': filter_keys} if filter_keys else {},
                )
                
                # Check if the dataset is valid/non-empty before adding
                if len(ds.data_vars) > 0:
                    datasets.append(ds)
                else:
                    ds.close()
            
            except Exception:
                # Common causes:
                # - Filter doesn't match any messages in the file
                # - Index file generation race conditions
                continue
        
        # 3. Last Resort Fallback
        # If we targeted a specific level and failed, or if we tried everything and failed,
        # try opening without filters (unless we already tried the empty filter above).
        if not datasets and not target_level_type:
            try:
                ds = xr.open_dataset(file_path, engine='cfgrib', chunks=chunks)
                datasets.append(ds)
            except Exception as e:
                self.logger.error(f"Failed to open GRIB file: {e}")
        
        return datasets
    
    def _find_variable_dataset(self, file_path: Path, variable_name: str, chunks=None, required_dim=None):
        """
        Find the dataset containing a specific variable.
        If required_dim is provided, ensures the dataset has that dimension/coordinate.
        """
        
        datasets = self._open_grib_multi(file_path, chunks=chunks)
        
        found_ds = None
        
        for ds in datasets:
            if variable_name in ds.data_vars:
                # Check for required vertical dimension if specified
                if required_dim:
                    if required_dim in ds.coords or required_dim in ds.dims:
                        found_ds = ds
                        break
                    else:
                        # Variable found, but wrong vertical level type (e.g., surface vs isobaric)
                        # Keep searching
                        continue
                
                # If no specific dimension required, take the first match
                found_ds = ds
                break
        
        # Cleanup: Close datasets we aren't returning
        for other_ds in datasets:
            if other_ds is not found_ds:
                other_ds.close()
        
        return found_ds
    
    # --- Helper Methods ---
    
    def _find_spatial_dims(self, var_data) -> Tuple[Optional[str], Optional[str]]:
        """Identify (y_dim, x_dim) names."""
        y_dim, x_dim = None, None
        
        # [cite_start]Check coordinates and dimensions [cite: 385]
        for name in list(var_data.dims):
            name_lower = name.lower()
            if name_lower in ['latitude', 'lat', 'y']:
                y_dim = name
            elif name_lower in ['longitude', 'lon', 'x']:
                x_dim = name
        
        return y_dim, x_dim
    
    def _find_time_dim(self, var_data) -> Optional[str]:
        """Find the time dimension."""
        for dim in var_data.dims:
            if dim in ['time', 'valid_time', 'forecast_time']:
                return dim
        return None
    
    def _get_valid_time(self, var_data, ds) -> datetime:
        """Extract the valid time from variable or dataset."""
        import pandas as pd
        
        # [cite_start]Try different time coordinate names [cite: 387]
        for time_coord in ['valid_time', 'time', 'forecast_time']:
            if time_coord in var_data.coords:
                t = var_data.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
            if time_coord in ds.coords:
                t = ds.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
        
        # [cite_start]Fall back to attributes [cite: 389]
        if 'valid_time' in var_data.attrs:
            return pd.Timestamp(var_data.attrs['valid_time']).to_pydatetime()
        
        from django.utils import timezone
        return timezone.now()
    
    def _get_spatial_info(self, var_data) -> tuple[tuple, tuple, str]:
        """Extract spatial bounds and resolution from variable."""
        # Get coordinate names
        y_dim, x_dim = self._find_spatial_dims(var_data)
        lat_name, lon_name = y_dim, x_dim
        
        if lat_name is None or lon_name is None:
            # [cite_start]Try coords directly if dims didn't match [cite: 390]
            for name in var_data.coords:
                if name in ['latitude', 'lat', 'y']:
                    lat_name = name
                elif name in ['longitude', 'lon', 'x']:
                    lon_name = name
        
        if lat_name is None or lon_name is None:
            # Fallback for non-geo arrays
            return (0.0, 0.0, 1.0, 1.0), (1.0, 1.0), "EPSG:4326"
        
        lats = var_data.coords[lat_name].values
        lons = var_data.coords[lon_name].values
        
        # [cite_start]Calculate bounds [cite: 393]
        lat_res = abs(lats[1] - lats[0]) if len(lats) > 1 else 1.0
        lon_res = abs(lons[1] - lons[0]) if len(lons) > 1 else 1.0
        
        # Handle longitude wrapping (0-360 vs -180-180)
        if lons.max() > 180:
            lons = np.where(lons > 180, lons - 360, lons)
        
        west = float(lons.min() - lon_res / 2)
        east = float(lons.max() + lon_res / 2)
        south = float(lats.min() - lat_res / 2)
        north = float(lats.max() + lat_res / 2)
        
        bounds = (west, south, east, north)
        resolution = (lon_res, lat_res)
        
        return bounds, resolution, "EPSG:4326"
