"""
GeoTIFF Format Plugin

Handles GeoTIFF files using rasterio with memory-optimized windowed reading.
For vector data (U/V), expects multi-band TIFFs with band 1 = U, band 2 = V.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

from .registry import BaseFormatPlugin, ExtractedVariable


class GeoTIFFFormatPlugin(BaseFormatPlugin):
    """
    Format plugin for GeoTIFF files.
    Uses rasterio for reading with support for windowed reads.
    
    For vector datasets, expects:
    - Band 1: Primary variable (e.g., U component)
    - Band 2: Secondary variable (e.g., V component)
    
    Note: GeoTIFFs are typically 2D (or 2.5D with bands). Vertical level selection
    is not natively supported unless encoded as separate bands/variables.
    """
    
    name = "geotiff"
    display_name = "GeoTIFF"
    extensions = ['.tif', '.tiff', '.geotiff']
    
    def can_handle(self, file_path: Path) -> bool:
        """Check if file is a GeoTIFF."""
        if file_path.suffix.lower() in self.extensions:
            return True
        
        # Check magic bytes
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(4)
                # TIFF magic: II (little-endian) or MM (big-endian)
                if magic[:2] in [b'II', b'MM']:
                    return True
        except Exception:
            pass
        
        return False
    
    def list_variables(self, file_path: Path) -> list[dict]:
        """List available bands in the GeoTIFF."""
        import rasterio
        
        variables = []
        
        try:
            with rasterio.open(file_path) as src:
                for i in range(1, src.count + 1):
                    desc = src.descriptions[i - 1] if src.descriptions else None
                    variables.append({
                        'name': f'band_{i}',
                        'long_name': desc or f'Band {i}',
                        'units': src.units[i - 1] if src.units else '',
                        'dimensions': ['y', 'x'],
                        'shape': (src.height, src.width),
                        'dtype': str(src.dtypes[i - 1]),
                    })
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return variables
    
    def get_timestamps(self, file_path: Path) -> list[datetime]:
        """Get timestamp strictly from the filename."""
        timestamps = []
        
        try:
            dt = self._parse_timestamp_from_filename(file_path.name)
            if dt:
                timestamps.append(dt)
        except Exception as e:
            self.logger.error(f"Failed to parse timestamp from filename {file_path}: {e}")
        
        return timestamps
    
    def get_metadata(self, file_path: Path, dataset) -> dict:
        """
        Lightweight scan to get dimensions and bounds without reading data.
        """
        import rasterio
        
        # Note: GeoTIFF ignores vertical_dimension/vertical_value settings
        # as it doesn't support labeled dimensions like NetCDF.
        
        with rasterio.open(file_path) as src:
            bounds = src.bounds
            return {
                'width': src.width,
                'height': src.height,
                'bounds': (bounds.left, bounds.bottom, bounds.right, bounds.top),
                'crs': str(src.crs) if src.crs else "EPSG:4326"
            }
    
    def get_lazy_dataset(self, file_path: Path, dataset, timestamp=None):
        """
        Return a lazy-loaded object for global stats computation.
        Uses xarray with rasterio engine (rioxarray) if available to support lazy execution.
        """
        import xarray as xr
        
        # This relies on the 'rasterio' engine being available to xarray.
        # It opens the file lazily (dask-backed)
        try:
            # We open as a DataArray because GeoTIFFs don't have standard dataset vars
            da = xr.open_dataarray(file_path, engine='rasterio', chunks={})
            
            # Select the band corresponding to the dataset variable
            band_idx = 1
            if dataset.primary_variable.startswith('band_'):
                try:
                    band_idx = int(dataset.primary_variable.split('_')[1])
                except ValueError:
                    pass
            
            # Xarray/rasterio usually puts bands in the 'band' coordinate
            if 'band' in da.coords and da.sizes['band'] >= band_idx:
                return da.sel(band=band_idx)
            return da
        
        except Exception as e:
            self.logger.warning(f"Lazy load failed, falling back to eager load for stats: {e}")
            raise NotImplementedError("Lazy loading requires rioxarray or compatible engine")
    
    def extract_variable(
            self,
            file_path: Path,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            secondary_variable: Optional[str] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            vertical_selection: Optional[dict] = None,  # Argument added for interface compatibility
    ) -> ExtractedVariable:
        """
        Extract data from GeoTIFF.
        Supports 'window' argument for memory-efficient chunked reading.
        window = (x, y, width, height)
        """
        import rasterio
        from rasterio.windows import Window
        
        self.logger.info(f"Extracting {variable_name} from {file_path} (Window: {window})")
        
        if vertical_selection:
            self.logger.warning(
                f"Vertical selection {vertical_selection} requested but ignored by GeoTIFF plugin."
            )
        
        with rasterio.open(file_path) as src:
            # Determine which band to read
            primary_band = 1
            if variable_name.startswith('band_'):
                try:
                    primary_band = int(variable_name.split('_')[1])
                except ValueError:
                    pass
            
            if primary_band > src.count:
                raise ValueError(f"Band {primary_band} not found (file has {src.count} bands)")
            
            # Prepare Window object if requested
            rio_window = None
            if window:
                x_off, y_off, w, h = window
                rio_window = Window(col_off=x_off, row_off=y_off, width=w, height=h)
            
            # Read primary data (Chunked or Full)
            data = src.read(primary_band, window=rio_window)
            
            # Handle nodata
            nodata = src.nodata
            if nodata is not None:
                data = np.where(data == nodata, np.nan, data.astype(float))
            
            # Read secondary band for vector data
            secondary_data = None
            if secondary_variable:
                secondary_band = 2  # Default to band 2
                if secondary_variable.startswith('band_'):
                    try:
                        secondary_band = int(secondary_variable.split('_')[1])
                    except ValueError:
                        pass
                
                if secondary_band <= src.count:
                    secondary_data = src.read(secondary_band, window=rio_window)
                    if nodata is not None:
                        secondary_data = np.where(
                            secondary_data == nodata,
                            np.nan,
                            secondary_data.astype(float)
                        )
                else:
                    self.logger.warning(
                        f"Secondary band {secondary_band} not found for vector data"
                    )
            
            # Get spatial info
            if rio_window:
                # Get bounds specific to this window
                window_bounds = src.window_bounds(rio_window)
                bounds = window_bounds
            else:
                # Full bounds
                b = src.bounds
                bounds = (b.left, b.bottom, b.right, b.top)
            
            # Resolution and CRS
            transform = src.transform
            crs = str(src.crs) if src.crs else "EPSG:4326"
            res_x = abs(transform.a)
            res_y = abs(transform.e)
            
            # Get timestamp
            valid_time = timestamp
            if valid_time is None:
                timestamps = self.get_timestamps(file_path)
                valid_time = timestamps[0] if timestamps else datetime.now()
            
            # Get metadata
            tags = src.tags()
            descriptions = src.descriptions
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs=crs,
                width=data.shape[1],  # Shape is (height, width) for numpy
                height=data.shape[0],
                resolution=(res_x, res_y),
                timestamp=valid_time,
                variable_name=variable_name,
                units=tags.get('units', ''),
                secondary_data=secondary_data,
                metadata={
                    'source_file': str(file_path),
                    'description': descriptions[primary_band - 1] if descriptions else '',
                    'driver': src.driver,
                    'dtype': str(src.dtypes[primary_band - 1]),
                    'full_width': src.width,
                    'full_height': src.height,
                    **tags,
                },
            )
    
    def _parse_timestamp_from_filename(self, filename: str) -> Optional[datetime]:
        """Try to extract a timestamp from the filename."""
        import re
        from dateutil.parser import parse
        
        patterns = [
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',  # ISO format
            r'(\d{8})_(\d{4})',  # YYYYMMDD_HHMM
            r'(\d{14})',  # YYYYMMDDHHMMSS
            r'(\d{8})',  # YYYYMMDD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_str = ''.join(match.groups())
                    return parse(date_str)
                except Exception:
                    continue
        
        return None
