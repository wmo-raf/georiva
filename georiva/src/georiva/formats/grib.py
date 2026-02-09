"""
GRIB Format Plugin

Handles GRIB1 and GRIB2 files using cfgrib/xarray with memory optimization.

Key design points:
- Uses `dim_selectors` for generic non-spatial selections (xarray .sel/.isel).
- Uses `grib_view` (cfgrib filter_by_keys) to deterministically open the correct GRIB "view"
  and avoid merge conflicts (e.g., heightAboveGround=2 vs 10).
- If `grib_view` is provided, it takes precedence. Otherwise we derive a best-effort view
  from `dim_selectors`.

Return shape:
- `list_variables()` returns per-variable entries WITH a `grib_view` field.
  Callers should pass that exact `grib_view` back to `extract_variable()` for deterministic behavior.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

import numpy as np
import pandas as pd

from georiva.utils.path import PathLike
from .base import BaseFormatPlugin, ExtractedVariable


class GRIBFormatPlugin(BaseFormatPlugin):
    name = "grib2"
    display_name = "GRIB2"
    extensions = [".grib", ".grib2", ".grb", ".grb2"]
    
    _SPATIAL_NAMES = {"latitude", "lat", "y", "longitude", "lon", "x"}
    _TIME_NAMES = {"time", "valid_time", "forecast_time"}
    _IGNORED_COORDS = _SPATIAL_NAMES | _TIME_NAMES | {"step"}
    
    _LEVEL_TYPE_KEYS = {
        "surface",
        "meanSea",
        "isobaricInhPa",
        "heightAboveGround",
        "atmosphere",
        "nominalTop",
        "cloudBase",
        "cloudTop",
        "isothermZero",
    }
    
    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        try:
            with open(file_path, "rb") as f:
                return f.read(4) == b"GRIB"
        except Exception:
            return False
    
    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List available variables in the GRIB file.

        IMPORTANT:
        - `available_dim_selectors` reports ONLY real xarray dims/coords you can select with `.sel()`.
        - `grib_view` reports the cfgrib `filter_by_keys` used to open the GRIB messages for this entry.
          Pass `grib_view` back into extract_variable() for deterministic reading.
        """
        
        file_path = Path(file_path)
        variables: list[dict] = []
        
        try:
            ds_pairs = self._open_grib_multi(file_path, chunks={})
            
            for ds, fk in ds_pairs:
                try:
                    for var_name, var_data in ds.data_vars.items():
                        selector_names = set()
                        
                        # dims (excluding spatial/time)
                        for d in var_data.dims:
                            dl = d.lower()
                            if dl not in self._IGNORED_COORDS:
                                selector_names.add(d)
                        
                        # coords (exclude spatial/time AND exclude scalar coords)
                        for c, coord in var_data.coords.items():
                            cl = c.lower()
                            if cl in self._IGNORED_COORDS:
                                continue
                            if getattr(coord, "dims", ()) and len(coord.dims) > 0:
                                selector_names.add(c)
                        
                        variables.append(
                            {
                                "name": var_name,
                                "short_name": var_data.attrs.get("GRIB_shortName", ""),
                                "long_name": var_data.attrs.get("long_name", var_name),
                                "units": var_data.attrs.get("units", ""),
                                "dimensions": list(var_data.dims),
                                "available_dim_selectors": sorted(selector_names),
                                "shape": tuple(var_data.shape),
                                "grib_view": fk,
                            }
                        )
                finally:
                    ds.close()
        
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")
        
        # De-duplicate by (name, grib_view)
        seen = set()
        out: list[dict] = []
        for v in variables:
            fv = v.get("grib_view") or {}
            key = (v["name"], tuple(sorted(fv.items())))
            if key in seen:
                continue
            seen.add(key)
            out.append(v)
        
        return out
    
    def get_timestamps(self, file_path: PathLike) -> list[datetime]:
        file_path = Path(file_path)
        timestamps: set[datetime] = set()
        
        try:
            ds_pairs = self._open_grib_multi(file_path, chunks={})
            
            for ds, _fk in ds_pairs:
                try:
                    for dim in ["time", "valid_time", "forecast_time"]:
                        if dim in ds.coords:
                            times = ds.coords[dim].values
                            if hasattr(times, "__iter__"):
                                for t in times:
                                    if isinstance(t, np.datetime64):
                                        timestamps.add(pd.Timestamp(t).to_pydatetime())
                            else:
                                if isinstance(times, np.datetime64):
                                    timestamps.add(pd.Timestamp(times).to_pydatetime())
                finally:
                    ds.close()
        
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
            grib_view: Optional[dict] = None,
    ) -> dict:
        
        file_path = Path(file_path)
        
        ds = self._find_variable_dataset(
            file_path=file_path,
            variable_name=variable_name,
            chunks={},
            dim_selectors=dim_selectors,
            grib_view=grib_view,
        )
        if ds is None:
            raise ValueError(f"Variable '{variable_name}' not found")
        
        try:
            resolved_name = ds.attrs.get("_resolved_variable_name", variable_name)
            var = ds[resolved_name]
            
            # time selection
            time_dim = self._find_time_dim(var)
            if timestamp is not None and time_dim:
                var = var.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var[time_dim].size > 0:
                var = var.isel({time_dim: 0})
            
            # dim selection
            var = self._apply_dim_selectors(var, dim_selectors)
            
            bounds, _, _ = self._get_spatial_info(var)
            y_dim, x_dim = self._find_spatial_dims(var)
            height = var.sizes[y_dim] if y_dim else var.shape[-2]
            width = var.sizes[x_dim] if x_dim else var.shape[-1]
            
            return {"width": int(width), "height": int(height), "bounds": bounds, "crs": "EPSG:4326"}
        finally:
            ds.close()
    
    def get_lazy_variable(
            self,
            file_path: PathLike,
            variable_name: str,
            *,
            timestamp: Optional[datetime] = None,
            dim_selectors: Optional[dict[str, object]] = None,
            grib_view: Optional[dict] = None,
    ) -> Any:
        """
        Returns (lazy_dataarray, closer_callable).

        This avoids leaking datasets and makes lifecycle explicit to the caller.
        """
        
        file_path = Path(file_path)
        
        ds = self._find_variable_dataset(
            file_path=file_path,
            variable_name=variable_name,
            chunks={},
            dim_selectors=dim_selectors,
            grib_view=grib_view,
        )
        if ds is None:
            raise ValueError(f"Variable '{variable_name}' not found")
        
        resolved_name = ds.attrs.get("_resolved_variable_name", variable_name)
        
        var = ds[resolved_name]
        
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
            grib_view: Optional[dict] = None,
    ) -> ExtractedVariable:
        """
        Extract a variable from the GRIB file.

        Args:
            dim_selectors: Generic selection dict applied via xarray .sel() / .isel()
            grib_view: cfgrib filter_by_keys dict for deterministic opening (preferred).
                      Example:
                        {"typeOfLevel":"heightAboveGround","level":2}
                        {"typeOfLevel":"surface"}

        Returns:
            ExtractedVariable with data in image orientation (row 0 = north)
        """
        self.logger.info(
            f"Extracting {variable_name} from {file_path} (window={window}, dim_selectors={dim_selectors}, grib_view={grib_view})"
        )
        
        file_path = Path(file_path)
        
        ds = self._find_variable_dataset(
            file_path=file_path,
            variable_name=variable_name,
            chunks={},
            dim_selectors=dim_selectors,
            grib_view=grib_view,
        )
        
        if ds is None:
            raise ValueError(f"Variable '{variable_name}' not found in {file_path}")
        
        try:
            resolved_name = ds.attrs.get("_resolved_variable_name", variable_name)
            var_data = ds[resolved_name]
            
            # 1) time selection
            time_dim = self._find_time_dim(var_data)
            if timestamp is not None and time_dim:
                var_data = var_data.sel({time_dim: timestamp}, method="nearest")
            elif time_dim and var_data[time_dim].size > 0:
                var_data = var_data.isel({time_dim: 0})
            
            valid_time = self._get_valid_time(var_data, ds)
            
            # 2) generic dim selection
            var_data = self._apply_dim_selectors(var_data, dim_selectors)
            
            # 3) spatial dims
            y_dim, x_dim = self._find_spatial_dims(var_data)
            full_height = var_data.sizes[y_dim] if y_dim else var_data.shape[-2]
            full_width = var_data.sizes[x_dim] if x_dim else var_data.shape[-1]
            
            # 4) Check if latitude is ascending (south-to-north) before windowing
            needs_flip = False
            if y_dim and y_dim in var_data.coords:
                y_coords = var_data.coords[y_dim].values
                if len(y_coords) > 1 and y_coords[0] < y_coords[-1]:
                    needs_flip = True
            
            # 5) window slicing
            if window and x_dim and y_dim:
                x_off, y_off, w, h = window
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                var_data = var_data.isel({x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)})
            elif window:
                x_off, y_off, w, h = window
                var_data = var_data[..., y_off: y_off + h, x_off: x_off + w]
            
            # 6) spatial info
            bounds, resolution, _ = self._get_spatial_info(var_data)
            
            # 7) load primary
            data = var_data.values
            if data.ndim > 2:
                data = data.squeeze()
            
            # 8) Ensure image orientation (row 0 = north)
            if needs_flip:
                data = np.flipud(data)
            
            width = int(data.shape[1]) if data.ndim > 1 else int(data.shape[0])
            height = int(data.shape[0]) if data.ndim > 1 else 1
            
            return ExtractedVariable(
                data=data,
                bounds=bounds,
                crs="EPSG:4326",
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
                    "grib_view": grib_view or {},
                },
            )
        finally:
            ds.close()
    
    # -------------------------------------------------------------------------
    # GRIB opening helpers
    # -------------------------------------------------------------------------
    
    def _open_grib_multi(
            self, file_path: Path, chunks=None, filter_by_keys: Optional[dict] = None
    ) -> list[tuple[Any, dict]]:
        """
        Returns a list of (dataset, filter_by_keys_used) pairs.
        
        If filter_by_keys is provided, opens only that specific view.
        Otherwise uses cfgrib.open_datasets for automatic discovery.
        """
        import xarray as xr
        import cfgrib
        
        datasets: list[tuple[Any, dict]] = []
        
        # Explicit filter provided
        if filter_by_keys is not None:
            try:
                ds = xr.open_dataset(
                    file_path,
                    engine="cfgrib",
                    chunks=chunks,
                    backend_kwargs={"filter_by_keys": filter_by_keys},
                )
                if ds.data_vars:
                    datasets.append((ds, filter_by_keys))
                else:
                    ds.close()
            except Exception:
                pass
            return datasets
        
        # Discovery mode: let cfgrib split automatically
        try:
            ds_list = cfgrib.open_datasets(str(file_path))
            for ds in ds_list:
                fk = self._infer_grib_view_from_dataset(ds)
                datasets.append((ds, fk))
        except Exception as e:
            self.logger.warning(f"cfgrib.open_datasets failed: {e}")
        
        return datasets
    
    def _infer_grib_view_from_dataset(self, ds) -> dict:
        """Infer the grib_view filter from a dataset's GRIB attributes."""
        fk = {}
        
        for var_name in ds.data_vars:
            var = ds[var_name]
            attrs = var.attrs
            
            # Extract GRIB metadata
            if "GRIB_typeOfLevel" in attrs:
                fk["typeOfLevel"] = attrs["GRIB_typeOfLevel"]
            
            if "GRIB_shortName" in attrs:
                fk["shortName"] = attrs["GRIB_shortName"]
            
            # Get level value from coordinate matching typeOfLevel
            type_of_level = attrs.get("GRIB_typeOfLevel", "")
            if type_of_level in ds.coords:
                level_val = ds.coords[type_of_level].values
                if np.isscalar(level_val) or level_val.ndim == 0:
                    fk["level"] = int(level_val)
            
            break  # Only need first var's attrs
        
        return fk
    
    def _find_variable_dataset(
            self,
            file_path: Path,
            variable_name: str,
            chunks=None,
            dim_selectors: Optional[dict[str, object]] = None,
            grib_view: Optional[dict] = None,
    ):
        """
        Find the dataset containing a specific variable.
    
        Supports both xarray names (t2m) and GRIB shortNames (2t).
        """
        # 1) explicit view wins
        if grib_view:
            ds_pairs = self._open_grib_multi(file_path, chunks=chunks, filter_by_keys=grib_view)
        else:
            # 2) derive from selectors
            derived = self._derive_cfgrib_filter(dim_selectors)
            if derived:
                ds_pairs = self._open_grib_multi(file_path, chunks=chunks, filter_by_keys=derived)
            else:
                # 3) discovery
                ds_pairs = self._open_grib_multi(file_path, chunks=chunks, filter_by_keys=None)
        
        found_ds = None
        resolved_name = None
        
        for ds, _fk in ds_pairs:
            # Direct match
            if variable_name in ds.data_vars:
                found_ds = ds
                resolved_name = variable_name
                break
            
            # Try shortName -> xarray name lookup
            resolved = self._resolve_variable_name(ds, variable_name)
            if resolved:
                found_ds = ds
                resolved_name = resolved
                break
        
        # close everything else
        for ds, _fk in ds_pairs:
            if ds is not found_ds:
                ds.close()
        
        # Store resolved name for caller to use
        if found_ds is not None:
            found_ds.attrs["_resolved_variable_name"] = resolved_name
        
        return found_ds
    
    def _resolve_variable_name(self, ds, variable_name: str) -> Optional[str]:
        """
        Resolve a variable name, supporting both xarray names and GRIB shortNames.
        
        Returns the actual xarray variable name, or None if not found.
        """
        # Direct match
        if variable_name in ds.data_vars:
            return variable_name
        
        # Search by GRIB_shortName attribute
        for var_name in ds.data_vars:
            attrs = ds[var_name].attrs
            if attrs.get("GRIB_shortName") == variable_name:
                return var_name
            # Also check GRIB_cfName and GRIB_cfVarName
            if attrs.get("GRIB_cfName") == variable_name:
                return var_name
            if attrs.get("GRIB_cfVarName") == variable_name:
                return var_name
        
        return None
    
    def _derive_cfgrib_filter(self, dim_selectors: Optional[dict[str, object]]) -> Optional[dict]:
        """
        Best-effort derivation of cfgrib filter_by_keys from dim_selectors.

        Handles shortName and level type filters.
        """
        if not dim_selectors:
            return None
        
        # Check for shortName first
        if "shortName" in dim_selectors:
            return {"shortName": dim_selectors["shortName"]}
        
        # Check for level type keys
        for k, v in dim_selectors.items():
            if k in self._LEVEL_TYPE_KEYS:
                fbk = {"typeOfLevel": k}
                if v is not None:
                    try:
                        fbk["level"] = int(v)
                    except Exception:
                        pass
                return fbk
        
        return None
    
    # -------------------------------------------------------------------------
    # Selection helpers
    # -------------------------------------------------------------------------
    
    def _apply_dim_selectors(self, var_data, dim_selectors: Optional[dict[str, object]]):
        """
        Apply xarray selection across non-spatial dimensions.

        - For slice values: try .sel({dim: slice})
        - For scalars: try .sel(..., method="nearest") then exact .sel(...)
        - Ignores keys not present in coords/dims
        """
        if not dim_selectors:
            return var_data
        
        for dim, val in dim_selectors.items():
            dl = dim.lower()
            if dl in self._SPATIAL_NAMES:
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
    
    # -------------------------------------------------------------------------
    # Generic helpers
    # -------------------------------------------------------------------------
    
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
            if dim in ["time", "valid_time", "forecast_time"]:
                return dim
        return None
    
    def _get_valid_time(self, var_data, ds) -> datetime:
        for time_coord in ["valid_time", "time", "forecast_time"]:
            if time_coord in var_data.coords:
                t = var_data.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
            if time_coord in ds.coords:
                t = ds.coords[time_coord].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()
        
        if "valid_time" in var_data.attrs:
            return pd.Timestamp(var_data.attrs["valid_time"]).to_pydatetime()
        
        return datetime.utcnow()
    
    def _get_spatial_info(self, var_data) -> tuple[tuple, tuple, str]:
        y_dim, x_dim = self._find_spatial_dims(var_data)
        lat_name, lon_name = y_dim, x_dim
        
        if lat_name is None or lon_name is None:
            for name in var_data.coords:
                nl = name.lower()
                if nl in {"latitude", "lat", "y"}:
                    lat_name = name
                elif nl in {"longitude", "lon", "x"}:
                    lon_name = name
        
        if lat_name is None or lon_name is None:
            return (0.0, 0.0, 1.0, 1.0), (1.0, 1.0), "EPSG:4326"
        
        lats = var_data.coords[lat_name].values
        lons = var_data.coords[lon_name].values
        
        lat_res = abs(lats[1] - lats[0]) if len(lats) > 1 else 1.0
        lon_res = abs(lons[1] - lons[0]) if len(lons) > 1 else 1.0
        
        # Handle longitude wrapping (0-360 vs -180-180)
        try:
            if np.nanmax(lons) > 180:
                lons = np.where(lons > 180, lons - 360, lons)
        except Exception:
            pass
        
        west = float(np.nanmin(lons) - lon_res / 2)
        east = float(np.nanmax(lons) + lon_res / 2)
        south = float(np.nanmin(lats) - lat_res / 2)
        north = float(np.nanmax(lats) + lat_res / 2)
        
        bounds = (west, south, east, north)
        resolution = (float(lon_res), float(lat_res))
        
        return bounds, resolution, "EPSG:4326"
