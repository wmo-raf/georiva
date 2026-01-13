"""
GeoTIFF Format Plugin

Handles GeoTIFF files using rasterio with memory-optimized windowed reading.

Conventions:
- Variables are exposed as "band_1", "band_2", ..., "band_N".
- Generic dimension selection is supported ONLY for {"band": <int>} via dim_selectors.
  Other selectors are ignored (GeoTIFF has no labeled non-spatial dims).
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import numpy as np
import rasterio
from rasterio.windows import Window

from georiva.utils.path import PathLike
from .registry import BaseFormatPlugin, ExtractedVariable


class GeoTIFFFormatPlugin(BaseFormatPlugin):
    name = "geotiff"
    display_name = "GeoTIFF"
    extensions = [".tif", ".tiff", ".geotiff"]
    
    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        
        # TIFF magic bytes
        try:
            with open(file_path, "rb") as f:
                magic = f.read(4)
                return magic[:2] in [b"II", b"MM"]
        except Exception:
            return False
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        file_path = Path(file_path)
        
        variables: list[dict] = []
        try:
            with rasterio.open(file_path) as src:
                units = list(getattr(src, "units", []) or [])
                descriptions = list(getattr(src, "descriptions", []) or [])
                
                for i in range(1, src.count + 1):
                    desc = descriptions[i - 1] if i - 1 < len(descriptions) else None
                    unit = units[i - 1] if i - 1 < len(units) else ""
                    
                    variables.append(
                        {
                            "name": f"band_{i}",
                            "long_name": desc or f"Band {i}",
                            "units": unit or "",
                            "dimensions": ["y", "x"],
                            "available_dim_selectors": ["band"],  # meaningful selector for GeoTIFF
                            "shape": (src.height, src.width),
                            "dtype": str(src.dtypes[i - 1]),
                            "band_index": i,
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        return variables
    
    def get_timestamps(self, file_path: PathLike) -> list[datetime]:
        """
        GeoTIFF typically doesn't carry time in a standard place.
        We try filename parsing
        """
        file_path = Path(file_path)
        timestamps: list[datetime] = []
        
        # 1) filename
        dt = self._parse_timestamp_from_filename(file_path.name)
        if dt:
            timestamps.append(dt)
        else:
            self.logger.debug(f"No timestamp found in filename {file_path.name}")
        
        # de-dup + sort
        out = sorted({t.replace(tzinfo=None) for t in timestamps})
        return out
    
    def get_metadata_for_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> dict:
        """
        Lightweight scan: width/height/bounds/crs.
        GeoTIFF ignores timestamp. `dim_selectors` can include {'band': int} to map variable_name.
        """
        
        file_path = Path(file_path)
        
        band = self._resolve_band(variable_name, dim_selectors)
        
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
    
    def get_lazy_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> tuple[Any, Any]:
        """
        Optional lazy support via rioxarray/xarray (dask-backed).
        Returns (lazy_dataarray, close_callable).
        """
        
        file_path = Path(file_path)
        band = self._resolve_band(variable_name, dim_selectors)
        
        try:
            import xarray as xr
            
            # Note: xr.open_dataarray(..., engine="rasterio") is supported in many setups,
            # but chunking requires dask installed.
            da = xr.open_dataarray(file_path, engine="rasterio", chunks={})
            
            # da dims commonly: ("band","y","x") or ("band","latitude","longitude") depending on engine/version
            if "band" in da.coords:
                da = da.sel(band=band)
            
            # xr objects have .close only for datasets; dataarray shares the underlying file manager
            # but doesn't always expose .close. We'll return a no-op close_fn.
            def close_fn():
                try:
                    da.close()  # may exist depending on version
                except Exception:
                    pass
            
            return da, close_fn
        
        except Exception as e:
            raise NotImplementedError(
                f"Lazy loading requires xarray + rasterio engine (and usually dask). Error: {e}"
            )
    
    def extract_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            timestamp: Optional[datetime] = None,
            window: Optional[tuple[int, int, int, int]] = None,
            dim_selectors: Optional[dict[str, object]] = None,
    ) -> ExtractedVariable:
        """
        Extract data from GeoTIFF.

        Args:
            variable_name: usually "band_1" etc.
            dim_selectors: supports {"band": <int>} as an alternative way to pick band.
            window: (x_offset, y_offset, width, height)
        """
        
        file_path = Path(file_path)
        
        band = self._resolve_band(variable_name, dim_selectors)
        
        self.logger.info(
            f"Extracting {variable_name} from {file_path} (band={band}, window={window}, dim_selectors={dim_selectors})"
        )
        
        with rasterio.open(file_path) as src:
            if band < 1 or band > src.count:
                raise ValueError(f"Band {band} not found (file has {src.count} bands)")
            
            rio_window = None
            if window:
                x_off, y_off, w, h = window
                rio_window = Window(col_off=x_off, row_off=y_off, width=w, height=h)
            
            # read primary
            data = src.read(band, window=rio_window)
            
            # nodata -> nan
            nodata = src.nodata
            if nodata is not None:
                data = data.astype(float, copy=False)
                data = np.where(data == nodata, np.nan, data)
            
            # bounds
            if rio_window:
                wb = src.window_bounds(rio_window)
                bounds = (float(wb.left), float(wb.bottom), float(wb.right), float(wb.top))
            else:
                b = src.bounds
                bounds = (float(b.left), float(b.bottom), float(b.right), float(b.top))
            
            # resolution & crs
            transform = src.transform
            if transform.e > 0:
                data = np.flipud(data)
            
            res_x = float(abs(transform.a))
            res_y = float(abs(transform.e))
            crs = str(src.crs) if src.crs else "EPSG:4326"
            
            # time
            valid_time = timestamp
            if valid_time is None:
                ts = self.get_timestamps(file_path)
                valid_time = ts[0] if ts else datetime.utcnow()
            
            # metadata
            tags = src.tags() or {}
            descriptions = list(getattr(src, "descriptions", []) or [])
            units = list(getattr(src, "units", []) or [])
            
            desc = descriptions[band - 1] if band - 1 < len(descriptions) else ""
            unit = units[band - 1] if band - 1 < len(units) else tags.get("units", "")
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs=crs,
                width=int(data.shape[1]),
                height=int(data.shape[0]),
                resolution=(res_x, res_y),
                timestamp=valid_time,
                variable_name=variable_name,
                units=unit or "",
                metadata={
                    "source_file": str(file_path),
                    "driver": src.driver,
                    "dtype": str(src.dtypes[band - 1]),
                    "description": desc,
                    "band_index": band,
                    "full_width": int(src.width),
                    "full_height": int(src.height),
                    "dim_selectors": dim_selectors or {},
                    **tags,
                },
            )
    
    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    
    def _resolve_band(self, variable_name: str, dim_selectors: Optional[dict[str, object]]) -> int:
        """
        Determine which band to read.
        Priority:
          1) dim_selectors["band"] if present
          2) variable_name "band_<n>"
          3) default 1
        """
        if dim_selectors and "band" in dim_selectors:
            try:
                return int(dim_selectors["band"])
            except Exception:
                pass
        
        if variable_name.startswith("band_"):
            try:
                return int(variable_name.split("_", 1)[1])
            except Exception:
                return 1
        
        return 1
    
    def _parse_timestamp_from_filename(self, filename: str) -> Optional[datetime]:
        import re
        from dateutil.parser import parse
        
        patterns = [
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})",  # ISO
            r"(\d{8})_(\d{4})",  # YYYYMMDD_HHMM
            r"(\d{14})",  # YYYYMMDDHHMMSS
            r"(\d{8})",  # YYYYMMDD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_str = "".join(match.groups())
                    return parse(date_str)
                except Exception:
                    continue
        
        return None
