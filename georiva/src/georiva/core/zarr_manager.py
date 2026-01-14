"""
GeoRiva Zarr Manager using ndpyramid

Creates multi-resolution Zarr pyramids for Variables using ndpyramid,

Store structure:
    zarr/{catalog_slug}/{collection_slug}/{variable_slug}.zarr
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import ndpyramid  # noqa
import numpy as np
import rioxarray  # noqa
import xarray as xr
import zarr
from odc.geo.xr import assign_crs

logger = logging.getLogger(__name__)


class ZarrPyramidManager:
    """
    Manages Zarr pyramids for Variables using ndpyramid.
    
    Creates multi-resolution pyramids and allows appending new timesteps.
    """
    
    DEFAULT_LEVELS = 6
    DEFAULT_PIXELS_PER_TILE = 128
    DEFAULT_CRS = "EPSG:4326"
    DEFAULT_TARGET_CRS = "EPSG:3857"
    
    def __init__(self, storage_manager=None):
        """Initialize ZarrPyramidManager."""
        if storage_manager is None:
            from georiva.core.storage import storage_manager
            self.storage = storage_manager
        else:
            self.storage = storage_manager
    
    def get_zarr_path(
            self,
            collection: 'Collection',
            variable: 'Variable',
    ) -> str:
        """
        Get the Zarr store path for a Variable.
        
        Returns:
            Path like: zarr/{catalog_slug}/{collection_slug}/{variable_slug}.zarr
        """
        catalog_slug = collection.catalog.slug
        collection_slug = collection.slug
        variable_slug = variable.slug
        return f"zarr/{catalog_slug}/{collection_slug}/{variable_slug}.zarr"
    
    def get_store_path(
            self,
            collection: 'Collection',
            variable: 'Variable',
    ) -> str:
        """Get the full filesystem path to the Zarr store."""
        zarr_path = self.get_zarr_path(collection, variable)
        
        if self.storage and hasattr(self.storage, 'storage'):
            base_path = Path(self.storage.storage.location)
        else:
            base_path = Path("./data")
        
        return str(base_path / zarr_path)
    
    def create_pyramid(
            self,
            collection: 'Collection',
            variable: 'Variable',
            data: Union[np.ndarray, xr.DataArray, xr.Dataset],
            timestamp: datetime,
            bounds: Tuple[float, float, float, float],
            crs: str = None,
            levels: int = None,
            reproject_to_mercator: bool = True,
    ) -> bool:
        """Create a new Zarr pyramid for a Variable."""
        from ndpyramid import pyramid_reproject, pyramid_coarsen
        
        crs = crs or self.DEFAULT_CRS
        levels = levels or self.DEFAULT_LEVELS
        variable_name = variable.slug
        
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
            
            pyramid.attrs.update({
                'georiva_catalog': collection.catalog.slug,
                'georiva_collection': collection.slug,
                'georiva_variable': variable.slug,
                'variable': variable_name,
                'units': variable.units or '',
                'clim': [
                    float(variable.value_min) if variable.value_min is not None else float(np.nanmin(data)),
                    float(variable.value_max) if variable.value_max is not None else float(np.nanmax(data)),
                ],
            })
            
            store_path = self.get_store_path(collection, variable)
            Path(store_path).parent.mkdir(parents=True, exist_ok=True)
            
            pyramid.to_zarr(store_path, zarr_format=2, consolidated=True, mode='w')
            
            logger.info(f"Created Zarr pyramid at {store_path}")
            return True
        
        except Exception as e:
            logger.exception(f"Failed to create pyramid: {e}")
            return False
    
    def append_timestep(
            self,
            collection: 'Collection',
            variable: 'Variable',
            timestamp: datetime,
            data: Union[np.ndarray, xr.DataArray],
            bounds: Tuple[float, float, float, float],
            crs: str = None,
    ) -> bool:
        """Append a new timestep to an existing Zarr pyramid."""
        crs = crs or self.DEFAULT_CRS
        store_path = self.get_store_path(collection, variable)
        
        if not Path(store_path).exists():
            logger.info(f"Creating new pyramid for {store_path}")
            return self.create_pyramid(
                collection=collection,
                variable=variable,
                data=data,
                timestamp=timestamp,
                bounds=bounds,
                crs=crs,
            )
        
        return self._append_to_existing(
            collection=collection,
            variable=variable,
            timestamp=timestamp,
            data=data,
            bounds=bounds,
            crs=crs,
        )
    
    def _append_to_existing(
            self,
            collection: 'Collection',
            variable: 'Variable',
            timestamp: datetime,
            data: np.ndarray,
            bounds: Tuple[float, float, float, float],
            crs: str,
    ) -> bool:
        """Append a timestep to an existing pyramid."""
        store_path = self.get_store_path(collection, variable)
        variable_name = variable.slug
        
        try:
            ts_np = np.datetime64(timestamp, 'ns')
            
            height, width = data.shape
            minx, miny, maxx, maxy = bounds
            
            src_x = np.linspace(minx, maxx, width, dtype=np.float64)
            src_y = np.linspace(maxy, miny, height, dtype=np.float64)
            
            src_da = xr.DataArray(
                data=data.astype(np.float32),
                dims=['y', 'x'],
                coords={'y': src_y, 'x': src_x},
                name=variable_name,
            )
            src_da = assign_crs(src_da, crs)
            
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
            
            for level_name in level_names:
                level_ds = existing_tree[level_name].to_dataset()
                
                target_x = level_ds['x'].values
                target_y = level_ds['y'].values
                target_crs = level_ds.rio.crs if hasattr(level_ds, 'rio') else None
                
                if target_crs and str(target_crs) != crs:
                    resampled = self._reproject_to_target(
                        src_da, target_x, target_y, target_crs
                    )
                else:
                    resampled = self._resample_to_target(
                        data, target_y, target_x
                    )
                
                level_path = f"{store_path}/{level_name}"
                self._zarr_append_timestep(
                    level_path,
                    variable_name,
                    resampled,
                    ts_np
                )
            
            existing_tree.close()
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
        """Reproject source data to target CRS and grid."""
        target_shape = (len(target_y), len(target_x))
        
        try:
            reprojected = src_da.rio.reproject(
                target_crs,
                shape=target_shape,
                resampling=1,
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
        """Resample data to target grid dimensions."""
        from scipy.ndimage import zoom as scipy_zoom
        
        target_shape = (len(target_y), len(target_x))
        src_shape = data.shape
        
        if src_shape == target_shape:
            return data.astype(np.float32)
        
        zoom_y = target_shape[0] / src_shape[0]
        zoom_x = target_shape[1] / src_shape[1]
        
        resampled = scipy_zoom(
            data.astype(np.float32),
            (zoom_y, zoom_x),
            order=1,
            mode='nearest',
        )
        
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
        """Append a timestep to a zarr level."""
        store = zarr.open_group(level_path, mode='r+')
        
        if 'time' not in store:
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
        
        if variable_name not in store:
            height, width = data.shape
            initial_data = data[np.newaxis, :, :].astype(np.float32)
            store.create_dataset(
                name=variable_name,
                shape=initial_data.shape,
                chunks=(1, 128, 128),
                dtype=np.float32,
                data=initial_data,
            )
            store[variable_name].attrs['_ARRAY_DIMENSIONS'] = ['time', 'y', 'x']
        else:
            var_arr = store[variable_name]
            current_time_len = var_arr.shape[0]
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
    
    def get_pyramid_info(
            self,
            collection: 'Collection',
            variable: 'Variable',
    ) -> Optional[dict]:
        """Get information about an existing pyramid."""
        store_path = self.get_store_path(collection, variable)
        
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
    
    def delete_pyramid(
            self,
            collection: 'Collection',
            variable: 'Variable',
    ) -> bool:
        """Delete a Variable's Zarr pyramid."""
        import shutil
        
        store_path = self.get_store_path(collection, variable)
        
        try:
            path = Path(store_path)
            if path.exists():
                shutil.rmtree(path)
            logger.info(f"Deleted pyramid at {store_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete pyramid: {e}")
            return False
