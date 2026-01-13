"""
GeoRiva Zarr Manager using ndpyramid (FIXED VERSION)

This module provides Zarr pyramid management for datasets using CarbonPlan's
ndpyramid library, ensuring full compatibility with @carbonplan/maps.

FIXES:
- Properly handles appending timesteps by resampling to existing grid
- Avoids coordinate alignment issues with xr.concat
- Uses zarr append mode for efficient updates

Installation requirements:
    pip install "ndpyramid[complete]"
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
import xarray as xr
import zarr

logger = logging.getLogger(__name__)


class ZarrPyramidManager:
    """
    Manages Zarr pyramids for datasets using ndpyramid.
    
    Creates multi-resolution pyramids compatible with @carbonplan/maps.
    """
    
    DEFAULT_LEVELS = 6
    DEFAULT_PIXELS_PER_TILE = 128
    DEFAULT_CRS = "EPSG:4326"
    DEFAULT_TARGET_CRS = "EPSG:3857"
    
    def __init__(self, storage_manager=None):
        """Initialize ZarrPyramidManager."""
        if storage_manager is None:
            try:
                from georiva.core.storage import storage_manager
                self.storage = storage_manager
            except ImportError:
                self.storage = None
        else:
            self.storage = storage_manager
        
        self._check_dependencies()
    
    def _check_dependencies(self):
        """Check that required dependencies are available."""
        try:
            import ndpyramid
            import rioxarray
            self._ndpyramid_available = True
        except ImportError as e:
            logger.warning(f"ndpyramid or rioxarray not available: {e}")
            self._ndpyramid_available = False
    
    def get_zarr_path(self, dataset) -> str:
        """Get the Zarr store path for a dataset."""
        if hasattr(dataset, 'collection'):
            return f"zarr/{dataset.collection.slug}/{dataset.slug}.zarr"
        return f"zarr/{dataset.slug}.zarr"
    
    def get_store_path(self, dataset) -> str:
        """Get the full filesystem path to the Zarr store."""
        zarr_path = self.get_zarr_path(dataset)
        
        if self.storage and hasattr(self.storage, 'storage'):
            base_path = Path(self.storage.storage.location)
        else:
            base_path = Path("./data")
        
        return str(base_path / zarr_path)
    
    def create_pyramid(
            self,
            dataset,
            data: Union[np.ndarray, xr.DataArray, xr.Dataset],
            timestamp: datetime,
            bounds: Tuple[float, float, float, float],
            crs: str = None,
            levels: int = None,
            variable_name: str = "value",
            reproject_to_mercator: bool = True,
    ) -> bool:
        """
        Create a new Zarr pyramid for a dataset.
        """
        if not self._ndpyramid_available:
            logger.error("ndpyramid not available")
            return False
        
        from ndpyramid import pyramid_reproject, pyramid_coarsen
        
        crs = crs or self.DEFAULT_CRS
        levels = levels or self.DEFAULT_LEVELS
        
        # Prepare dataset
        ds = self._prepare_dataset(
            data=data,
            timestamp=timestamp,
            bounds=bounds,
            crs=crs,
            variable_name=variable_name,
        )
        
        try:
            if reproject_to_mercator and crs != self.DEFAULT_TARGET_CRS:
                pyramid = pyramid_reproject(
                    ds,
                    levels=levels,
                    pixels_per_tile=self.DEFAULT_PIXELS_PER_TILE,
                    resampling='bilinear',
                    clear_attrs=True,
                )
            else:
                pyramid = pyramid_coarsen(
                    ds,
                    factors=[2 ** i for i in range(levels)],
                    dims=['y', 'x'],
                    boundary='trim',
                )
            
            # Add metadata
            pyramid.attrs.update({
                'georiva_dataset': dataset.slug if hasattr(dataset, 'slug') else str(dataset),
                'variable': variable_name,
                'units': getattr(dataset, 'units', '') or '',
                'clim': [
                    float(getattr(dataset, 'value_min', None) or np.nanmin(data)),
                    float(getattr(dataset, 'value_max', None) or np.nanmax(data)),
                ],
            })
            
            # Write to Zarr
            store_path = self.get_store_path(dataset)
            Path(store_path).parent.mkdir(parents=True, exist_ok=True)
            
            pyramid.to_zarr(store_path, zarr_format=2, consolidated=True, mode='w')
            
            logger.info(f"Created Zarr pyramid at {store_path}")
            return True
        
        except Exception as e:
            logger.exception(f"Failed to create pyramid: {e}")
            return False
    
    def append_timestep(
            self,
            dataset,
            timestamp: datetime,
            data: Union[np.ndarray, xr.DataArray],
            bounds: Tuple[float, float, float, float],
            crs: str = None,
            variable_name: str = "value",
    ) -> bool:
        """
        Append a new timestep to an existing Zarr pyramid.
        """
        crs = crs or self.DEFAULT_CRS
        store_path = self.get_store_path(dataset)
        
        # Check if pyramid exists
        if not Path(store_path).exists():
            logger.info(f"Creating new pyramid for {store_path}")
            return self.create_pyramid(
                dataset=dataset,
                data=data,
                timestamp=timestamp,
                bounds=bounds,
                crs=crs,
                variable_name=variable_name,
            )
        
        # Append to existing
        return self._append_to_existing(
            dataset=dataset,
            timestamp=timestamp,
            data=data,
            bounds=bounds,
            crs=crs,
            variable_name=variable_name,
        )
    
    def _append_to_existing(
            self,
            dataset,
            timestamp: datetime,
            data: np.ndarray,
            bounds: Tuple[float, float, float, float],
            crs: str,
            variable_name: str,
    ) -> bool:
        """
        Append a timestep to an existing pyramid.
        
        KEY FIX: Instead of regenerating the pyramid (which creates different
        coordinates each time), we:
        1. Read the existing coordinates from each level
        2. Resample new data to match those exact coordinates
        3. Append using zarr's native append functionality
        """
        import rioxarray  # noqa
        from odc.geo.xr import assign_crs
        
        store_path = self.get_store_path(dataset)
        
        try:
            # Convert timestamp to numpy datetime64
            ts_np = np.datetime64(timestamp, 'ns')
            
            # Prepare source data as xarray for reprojection
            height, width = data.shape
            minx, miny, maxx, maxy = bounds
            
            # Create source coordinates
            src_x = np.linspace(minx, maxx, width, dtype=np.float64)
            src_y = np.linspace(maxy, miny, height, dtype=np.float64)
            
            src_da = xr.DataArray(
                data=data.astype(np.float32),
                dims=['y', 'x'],
                coords={'y': src_y, 'x': src_x},
                name=variable_name,
            )
            src_da = assign_crs(src_da, crs)
            
            # Open existing pyramid to get level info
            # Use decode_times=False to avoid CF convention time decoding issues
            existing_tree = xr.open_datatree(
                store_path,
                engine='zarr',
                chunks={},
                decode_times=False,
            )
            level_names = sorted([k for k in existing_tree.children.keys() if k.isdigit()], key=int)
            
            if not level_names:
                logger.error("No valid levels found in existing pyramid")
                existing_tree.close()
                return False
            
            # Process each level
            for level_name in level_names:
                level_ds = existing_tree[level_name].to_dataset()
                
                # Get target coordinates from existing level
                target_x = level_ds['x'].values
                target_y = level_ds['y'].values
                target_crs = level_ds.rio.crs if hasattr(level_ds, 'rio') else None
                
                # Reproject/resample source data to target grid
                # Use rioxarray's reproject_match for proper CRS handling
                if target_crs and str(target_crs) != crs:
                    # Need to reproject
                    resampled = self._reproject_to_target(
                        src_da, target_x, target_y, target_crs
                    )
                else:
                    # Same CRS, just resample
                    resampled = self._resample_to_target(
                        data, target_y, target_x
                    )
                
                # Append to zarr store using zarr API directly
                level_path = f"{store_path}/{level_name}"
                self._zarr_append_timestep(
                    level_path,
                    variable_name,
                    resampled,
                    ts_np
                )
            
            existing_tree.close()
            
            # Reconsolidate metadata
            zarr.consolidate_metadata(store_path)
            
            logger.info(f"Appended timestep {timestamp} to {store_path}")
            return True
        
        except Exception as e:
            logger.exception(f"Failed to append: {e}")
            return False
    
    def _reproject_to_target(
            self,
            src_da: xr.DataArray,
            target_x: np.ndarray,
            target_y: np.ndarray,
            target_crs,
    ) -> np.ndarray:
        """
        Reproject source data to target CRS and grid.
        """
        import rioxarray  # noqa
        
        # Create a template dataset with target coordinates
        target_shape = (len(target_y), len(target_x))
        
        # Use rioxarray reproject
        try:
            reprojected = src_da.rio.reproject(
                target_crs,
                shape=target_shape,
                resampling=1,  # Bilinear
            )
            return reprojected.values.astype(np.float32)
        except Exception as e:
            logger.warning(f"Reprojection failed, using interpolation: {e}")
            return self._resample_to_target(src_da.values, target_y, target_x)
    
    def _resample_to_target(
            self,
            data: np.ndarray,
            target_y: np.ndarray,
            target_x: np.ndarray,
    ) -> np.ndarray:
        """
        Resample data to target grid dimensions using scipy.
        """
        from scipy.ndimage import zoom as scipy_zoom
        
        target_shape = (len(target_y), len(target_x))
        src_shape = data.shape
        
        if src_shape == target_shape:
            return data.astype(np.float32)
        
        # Calculate zoom factors
        zoom_y = target_shape[0] / src_shape[0]
        zoom_x = target_shape[1] / src_shape[1]
        
        # Use bilinear interpolation (order=1)
        resampled = scipy_zoom(
            data.astype(np.float32),
            (zoom_y, zoom_x),
            order=1,
            mode='nearest',
        )
        
        # Ensure exact target shape (zoom can be off by 1 pixel)
        if resampled.shape != target_shape:
            result = np.full(target_shape, np.nan, dtype=np.float32)
            min_y = min(resampled.shape[0], target_shape[0])
            min_x = min(resampled.shape[1], target_shape[1])
            result[:min_y, :min_x] = resampled[:min_y, :min_x]
            return result
        
        return resampled
    
    def _zarr_append_timestep(
            self,
            level_path: str,
            variable_name: str,
            data: np.ndarray,
            timestamp: np.datetime64,
    ):
        """
        Append a timestep to a zarr level using zarr's native API.
        
        This is more reliable than xarray concat for appending.
        Compatible with both Zarr 2.x and Zarr 3.x.
        """
        store = zarr.open_group(level_path, mode='r+')
        
        # Get or create time array
        if 'time' not in store:
            # Zarr 3.x requires shape as keyword argument
            time_data = np.array([timestamp], dtype='datetime64[ns]')
            store.create_dataset(
                name='time',
                shape=time_data.shape,
                chunks=(100,),
                dtype=time_data.dtype,
                data=time_data,
            )
        else:
            time_arr = store['time']
            current_len = time_arr.shape[0]
            time_arr.resize((current_len + 1,))
            time_arr[current_len] = timestamp
        
        # Get or create data array
        if variable_name not in store:
            height, width = data.shape
            initial_data = data[np.newaxis, :, :].astype(np.float32)  # Add time dimension
            store.create_dataset(
                name=variable_name,
                shape=initial_data.shape,
                chunks=(1, 128, 128),
                dtype=np.float32,
                data=initial_data,
            )
            # Set dimension metadata
            store[variable_name].attrs['_ARRAY_DIMENSIONS'] = ['time', 'y', 'x']
        else:
            var_arr = store[variable_name]
            current_time_len = var_arr.shape[0]
            
            # Resize and append - use tuple for new shape
            new_shape = (current_time_len + 1, var_arr.shape[1], var_arr.shape[2])
            var_arr.resize(new_shape)
            var_arr[current_time_len, :, :] = data
    
    def _prepare_dataset(
            self,
            data: Union[np.ndarray, xr.DataArray, xr.Dataset],
            timestamp: datetime,
            bounds: Tuple[float, float, float, float],
            crs: str,
            variable_name: str,
    ) -> xr.Dataset:
        """Prepare data as an xarray Dataset with proper CRS and coordinates."""
        from odc.geo.xr import assign_crs
        
        if isinstance(data, np.ndarray):
            if data.ndim != 2:
                raise ValueError(f"Expected 2D array, got {data.ndim}D")
            
            height, width = data.shape
            minx, miny, maxx, maxy = bounds
            
            x = np.linspace(minx, maxx, width, dtype=np.float64)
            y = np.linspace(maxy, miny, height, dtype=np.float64)
            
            da = xr.DataArray(
                data=data.astype(np.float32),
                dims=['y', 'x'],
                coords={'y': y, 'x': x, 'time': timestamp},
                name=variable_name,
            )
            ds = da.to_dataset()
        
        elif isinstance(data, xr.DataArray):
            ds = data.to_dataset(name=variable_name)
            if 'time' not in ds.coords:
                ds = ds.assign_coords(time=timestamp)
        
        elif isinstance(data, xr.Dataset):
            ds = data
            if 'time' not in ds.coords:
                ds = ds.assign_coords(time=timestamp)
        
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")
        
        if 'time' not in ds.dims:
            ds = ds.expand_dims('time')
        
        ds = assign_crs(ds, crs)
        
        return ds
    
    def get_pyramid_info(self, dataset) -> Optional[dict]:
        """Get information about an existing pyramid."""
        store_path = self.get_store_path(dataset)
        
        try:
            tree = xr.open_datatree(store_path, engine='zarr')
            
            levels = sorted([k for k in tree.children.keys() if k.isdigit()], key=int)
            
            info = {
                'path': store_path,
                'levels': len(levels),
                'attrs': dict(tree.attrs),
                'level_info': {},
            }
            
            for level_name in levels:
                ds = tree[level_name].to_dataset()
                info['level_info'][level_name] = {
                    'shape': dict(ds.sizes),
                    'variables': list(ds.data_vars.keys()),
                    'time_steps': ds.sizes.get('time', 0),
                }
            
            tree.close()
            return info
        
        except Exception as e:
            logger.debug(f"Could not get pyramid info: {e}")
            return None
    
    def delete_pyramid(self, dataset) -> bool:
        """Delete a dataset's Zarr pyramid."""
        import shutil
        
        store_path = self.get_store_path(dataset)
        
        try:
            path = Path(store_path)
            if path.exists():
                shutil.rmtree(path)
            logger.info(f"Deleted pyramid at {store_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete pyramid: {e}")
            return False


# =============================================================================
# FALLBACK FOR WHEN NDPYRAMID IS NOT AVAILABLE
# =============================================================================

class SimpleZarrManager:
    """
    Simple Zarr manager without pyramids.
    
    Use as fallback when ndpyramid is not installed.
    """
    
    def __init__(self, storage_manager=None):
        self.storage = storage_manager
    
    def get_zarr_path(self, dataset) -> str:
        if hasattr(dataset, 'collection'):
            return f"zarr/{dataset.collection.slug}/{dataset.slug}.zarr"
        return f"zarr/{dataset.slug}.zarr"
    
    def get_store_path(self, dataset) -> str:
        zarr_path = self.get_zarr_path(dataset)
        if self.storage and hasattr(self.storage, 'storage'):
            base_path = Path(self.storage.storage.location)
        else:
            base_path = Path("./data")
        return str(base_path / zarr_path)
    
    def append_timestep(
            self,
            dataset,
            timestamp: datetime,
            data: np.ndarray,
            bounds: Tuple[float, float, float, float],
            crs: str = "EPSG:4326",
            variable_name: str = "value",
    ) -> bool:
        """Append a timestep to a simple Zarr store."""
        from numcodecs import Blosc
        
        store_path = self.get_store_path(dataset)
        Path(store_path).parent.mkdir(parents=True, exist_ok=True)
        
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        
        try:
            store = zarr.open_group(store_path, mode='a')
            
            height, width = data.shape
            minx, miny, maxx, maxy = bounds
            ts_ns = np.datetime64(timestamp, 'ns')
            
            if 'x' not in store:
                x_coords = np.linspace(minx, maxx, width, dtype=np.float64)
                store.create_dataset(name='x', shape=x_coords.shape, dtype=x_coords.dtype, data=x_coords)
            
            if 'y' not in store:
                y_coords = np.linspace(maxy, miny, height, dtype=np.float64)
                store.create_dataset(name='y', shape=y_coords.shape, dtype=y_coords.dtype, data=y_coords)
            
            if 'time' not in store:
                store.create_dataset(name='time', shape=(0,), chunks=(100,), dtype='datetime64[ns]')
            
            if variable_name not in store:
                store.create_dataset(
                    name=variable_name,
                    shape=(0, height, width),
                    chunks=(1, 128, 128),
                    dtype=np.float32,
                    compressor=compressor,
                    fill_value=np.nan,
                )
            
            # Append time
            time_arr = store['time']
            current_len = time_arr.shape[0]
            time_arr.resize((current_len + 1,))
            time_arr[current_len] = ts_ns
            
            # Append data
            value_arr = store[variable_name]
            new_shape = (current_len + 1, value_arr.shape[1], value_arr.shape[2])
            value_arr.resize(new_shape)
            value_arr[current_len, :, :] = data.astype(np.float32)
            
            store.attrs.update({
                '_ARRAY_DIMENSIONS': ['time', 'y', 'x'],
                'crs': crs,
                'bounds': list(bounds),
            })
            
            logger.info(f"Appended to simple Zarr store: {store_path}")
            return True
        
        except Exception as e:
            logger.exception(f"Failed to append: {e}")
            return False
    
    def get_pyramid_info(self, dataset):
        return None
    
    def delete_pyramid(self, dataset):
        import shutil
        store_path = self.get_store_path(dataset)
        try:
            if Path(store_path).exists():
                shutil.rmtree(store_path)
            return True
        except:
            return False


# =============================================================================
# FACTORY AND CONVENIENCE FUNCTIONS
# =============================================================================

def get_zarr_manager(storage_manager=None):
    """Get the appropriate Zarr manager based on available dependencies."""
    try:
        import ndpyramid
        import rioxarray
        return ZarrPyramidManager(storage_manager)
    except ImportError:
        logger.warning("ndpyramid not available, using SimpleZarrManager")
        return SimpleZarrManager(storage_manager)


# Global instance
_zarr_manager = None


def get_default_zarr_manager():
    """Get the default Zarr manager instance."""
    global _zarr_manager
    if _zarr_manager is None:
        _zarr_manager = get_zarr_manager()
    return _zarr_manager


def update_zarr_for_item(item, raw_data: np.ndarray, companion_data: np.ndarray = None):
    """
    Update the Zarr store when a new Item is created.
    
    Usage in IngestionService:
        item.save()
        update_zarr_for_item(item, raw_data)
    """
    manager = get_default_zarr_manager()
    
    dataset = item.dataset
    timestamp = item.time
    bounds = tuple(item.bounds)
    crs = getattr(item, 'crs', None) or "EPSG:4326"
    
    if dataset.is_vector and companion_data is not None:
        magnitude = np.hypot(raw_data, companion_data)
        manager.append_timestep(
            dataset=dataset,
            timestamp=timestamp,
            data=magnitude,
            bounds=bounds,
            crs=crs,
            variable_name='speed',
        )
    else:
        var_name = getattr(dataset, 'primary_variable', None) or 'value'
        manager.append_timestep(
            dataset=dataset,
            timestamp=timestamp,
            data=raw_data,
            bounds=bounds,
            crs=crs,
            variable_name=var_name,
        )
