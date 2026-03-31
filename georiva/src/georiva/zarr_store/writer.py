"""
ZarrWriter — writes analysis-ready Zarr v3 stores to MinIO / local storage.

Each variable in a collection has one Zarr store:
    georiva-zarr/{catalog}/{collection}/{variable}.zarr

The store is a derived cache of COG assets. GeoTIFF is canonical.

Write model:
  - First call for a store → initialises with mode='w'
  - Subsequent calls → appends along the 'time' dimension (mode='a')
  - consolidated=False during write; zarr.consolidate_metadata() called after
    so a mid-write crash leaves the store valid (missing chunk) not corrupt.
  - Duplicate timestamps are silently skipped by default.
  - If overwrite=True, duplicate timestamps are replaced in-place using a
    region write (mode='r+') — no surrounding timesteps are touched.

Local dev fallback:
  - When GEORIVA_STORAGE_BACKEND='local' the writer uses zarr.storage.LocalStore
    rooted at {GEORIVA_STORAGE_ROOT}/georiva-zarr/
"""

import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr
from zarr.storage import FsspecStore, LocalStore

logger = logging.getLogger(__name__)


class ZarrWriter:
    """Writes and appends xarray datasets to Zarr v3 stores on S3 or local disk."""
    
    def __init__(
            self,
            bucket_name: str,
            endpoint_url: str,
            aws_key: str,
            aws_secret: str,
            time_chunk: int = 1,
            storage_backend: str = 's3',
            local_root: str = None,
            use_ssl: bool = False,
    ):
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.time_chunk = time_chunk
        self.storage_backend = storage_backend
        self.local_root = local_root
        self.use_ssl = use_ssl
    
    # =========================================================================
    # Store helpers
    # =========================================================================
    
    def _get_store(self, store_path: str):
        """Return a zarr store object for the given relative store path."""
        if self.storage_backend == 'local':
            full_path = os.path.join(self.local_root or '', self.bucket_name, store_path)
            os.makedirs(full_path, exist_ok=True)
            return LocalStore(full_path)
        else:
            fs = s3fs.S3FileSystem(
                key=self.aws_key,
                secret=self.aws_secret,
                endpoint_url=self.endpoint_url,
                use_ssl=self.use_ssl,
                client_kwargs={'endpoint_url': self.endpoint_url},
            )
            full_path = f"{self.bucket_name}/{store_path}"
            return FsspecStore(fs, path=full_path)
    
    def store_exists(self, store: object) -> bool:
        """Return True if the Zarr store is initialised. Accepts a store object."""
        try:
            zarr.open(store, mode='r', zarr_format=3)
            return True
        except Exception:
            return False
    
    # =========================================================================
    # Chunking (mirrors AssetWriter._blocksize)
    # =========================================================================
    
    def _blocksize(self, width: int, height: int) -> int:
        """
        Derive spatial chunk size from raster dimensions.

        Country-level  (<512px)  → 128
        Regional       (<2048px) → 256
        Continental/Global       → 512
        """
        min_dim = min(width, height)
        if min_dim < 512:
            return 128
        elif min_dim < 2048:
            return 256
        else:
            return 512
    
    # =========================================================================
    # Dataset construction
    # =========================================================================
    
    def _make_dataset(
            self,
            data: np.ndarray,
            transform,
            crs: str,
            timestamp: datetime,
            variable_slug: str,
            units: str = '',
    ):
        """
        Build a one-timestep xarray Dataset from a 2-D numpy array.

        Dimensions: time (1), lat (H), lon (W)
        Coordinates are derived from the rasterio affine transform (pixel centres).
        The dataset is CF-convention compliant and pre-chunked for Zarr writing.
        """
        height, width = data.shape
        blocksize = self._blocksize(width, height)
        
        # Pixel-centre coordinates from affine transform.
        # transform.c = x origin (left edge), transform.a = pixel width
        # transform.f = y origin (top edge),  transform.e = pixel height (negative)
        lons = np.array([transform.c + (i + 0.5) * transform.a for i in range(width)])
        lats = np.array([transform.f + (j + 0.5) * transform.e for j in range(height)])
        
        # Mask nodata → NaN before storing (storage as float32)
        arr = data.astype(np.float32)
        arr[~np.isfinite(arr)] = np.nan
        
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is not None:
            ts = ts.tz_convert('UTC').tz_localize(None)
        
        da = xr.DataArray(
            arr[np.newaxis, :, :],  # (1, H, W)
            dims=['time', 'lat', 'lon'],
            coords={
                'time': [ts],
                'lat': ('lat', lats, {
                    'units': 'degrees_north',
                    'long_name': 'latitude',
                    'standard_name': 'latitude',
                    'axis': 'Y',
                }),
                'lon': ('lon', lons, {
                    'units': 'degrees_east',
                    'long_name': 'longitude',
                    'standard_name': 'longitude',
                    'axis': 'X',
                }),
            },
            name=variable_slug,
            attrs={
                'units': units,
                'long_name': variable_slug,
                'grid_mapping': 'crs',
            },
        )
        
        da.encoding["_FillValue"] = np.nan
        
        ds = da.to_dataset()
        
        # CF-compliant CRS scalar variable
        ds['crs'] = xr.DataArray(
            np.int32(0),
            attrs={
                'grid_mapping_name': 'latitude_longitude',
                'semi_major_axis': 6378137.0,
                'inverse_flattening': 298.257223563,
                'crs_wkt': crs,
            },
        )
        
        # Apply Zarr-friendly chunking
        ds = ds.chunk({'time': self.time_chunk, 'lat': blocksize, 'lon': blocksize})
        return ds
    
    # =========================================================================
    # Write (init or append)
    # =========================================================================
    
    def write(
            self,
            store_path: str,
            data: np.ndarray,
            transform,
            crs: str,
            timestamp: datetime,
            variable_slug: str,
            units: str = '',
            overwrite: bool = False,
    ) -> None:
        """
        Write one timestep to the Zarr store.

        If the store does not exist it is initialised (mode='w').
        If it exists the timestep is appended (mode='a', append_dim='time').

        Duplicate timestamps are detected and silently skipped by default.
        If overwrite=True, the existing timestep is replaced in-place using a
        region write (mode='r+') — no surrounding timesteps are touched.

        A spatial grid mismatch between the replacement data and the existing
        store raises ValueError so the caller can mark the record as permanently
        failed rather than writing misaligned data silently.

        Raises ValueError if xarray detects an out-of-order time append
        (v1 known limitation — caller should mark the record as permanently failed).
        """
        # Resolve the store once and reuse
        store = self._get_store(store_path)
        ds = self._make_dataset(data, transform, crs, timestamp, variable_slug, units)
        
        # ensure _FillValue is handled correctly
        if "_FillValue" in ds[variable_slug].attrs:
            ds[variable_slug].attrs.pop("_FillValue", None)
        ds[variable_slug].encoding["_FillValue"] = np.nan
        
        if self.store_exists(store):
            # Duplicate guard: check if this timestamp is already in the store
            try:
                existing = zarr.open(store, mode='r', zarr_format=3)
                existing_times = pd.to_datetime(existing[variable_slug]['time'][:])
                ts = pd.Timestamp(timestamp)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert('UTC').tz_localize(None)
                
                if ts in existing_times:
                    if not overwrite:
                        logger.warning(
                            "Duplicate timestamp %s already in %s — skipping",
                            timestamp, store_path,
                        )
                        return
                    
                    # Overwrite path: replace the existing timestep in-place.
                    # Region write targets only the slice at time_index — surrounding
                    # timesteps are not read or rewritten.
                    time_index = existing_times.get_loc(ts)
                    logger.info(
                        "Overwriting timestamp %s at index %d in %s",
                        timestamp, time_index, store_path,
                    )
                    
                    # Guard: verify spatial grid matches before overwriting.
                    # _make_dataset derives lat/lon from the affine transform, so a
                    # reprocessed COG with a different grid would silently write
                    # misaligned data without this check.
                    existing_lats = existing[variable_slug]['lat'][:]
                    existing_lons = existing[variable_slug]['lon'][:]
                    if not (np.allclose(existing_lats, ds.lat.values) and
                            np.allclose(existing_lons, ds.lon.values)):
                        raise ValueError(
                            f"Spatial grid mismatch for {store_path} at {timestamp} — "
                            f"cannot overwrite with different coordinates. "
                            f"Run a full store rebuild instead."
                        )
                    
                    # mode='r+' opens the store for in-place modification without
                    # truncating it. region dict must align with the existing array's
                    # coordinates — guaranteed by the grid check above.
                    ds.to_zarr(
                        store,
                        mode='r+',
                        region={'time': slice(time_index, time_index + 1)},
                        zarr_format=3,
                    )
                    zarr.consolidate_metadata(store)
                    return
            
            except ValueError:
                # Re-raise ValueError so the task's out-of-order / grid-mismatch
                # handler can mark the record as permanently failed.
                raise
            except Exception as exc:
                # If we can't read existing times, proceed with the append.
                # Worst case: xarray will raise ValueError on conflict.
                logger.debug("Could not read existing times from %s: %s", store_path, exc)
            
            ds.to_zarr(store, mode='a', append_dim='time', zarr_format=3, consolidated=False)
            logger.debug("Appended %s → %s", timestamp, store_path)
        else:
            ds.to_zarr(store, mode='w', zarr_format=3, consolidated=False)
            logger.info("Initialised Zarr store: %s", store_path)
        
        # Consolidate metadata only after a successful write so a mid-write
        # crash does not corrupt the store's consolidated metadata file.
        zarr.consolidate_metadata(store)
