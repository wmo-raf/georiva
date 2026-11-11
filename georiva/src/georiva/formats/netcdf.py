"""
NetCDF Format Plugin (Lazy-first)

Two jobs:
1. List variables (with timestamps) in a NetCDF file.
2. Open a variable lazily for streaming stats or materialized extraction.

Design:
- NetCDF variables are uniquely identified by name — no key indirection needed.
- open_variable() is the primary interface: context manager, lazy DataArray.
- extract_variable() and get_metadata_for_variable() are inherited from BaseFormatPlugin.
- Places regular grids only: 1D coordinate axes with even spacing. A curvilinear
  grid (2D lat/lon, e.g. WRF on Lambert) or an unevenly spaced axis (e.g. WRF on
  Mercator) cannot be described by bounds plus one resolution, so it is refused
  with UnsupportedGridError rather than stretched into a box and misplaced.
- CRS detection and fill-value handling.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from rasterio.crs import CRS
from rasterio.errors import CRSError

from georiva.utils.path import PathLike

from .base import BaseFormatPlugin, ExtractedVariable, UnsupportedGridError, VariableInfo

logger = logging.getLogger(__name__)


class NetCDFFormatPlugin(BaseFormatPlugin):
    name = "netcdf"
    display_name = "NetCDF"
    extensions = [".nc", ".nc4", ".netcdf"]

    def can_handle(self, file_path: PathLike) -> bool:
        file_path = Path(file_path)
        if file_path.suffix.lower() in self.extensions:
            return True
        try:
            with open(file_path, "rb") as f:
                magic = f.read(4)
                return magic[:3] == b"CDF" or magic == b"\x89HDF"
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_variables(self, file_path: PathLike) -> list[dict]:
        """
        List all data variables in the NetCDF file.

        Returns a list of dicts, each containing:
            - name: variable name (use in open_variable / get_timestamps)
            - long_name: human-readable name
            - units: variable units
            - standard_name: CF standard name
            - dimensions: list of dimension names
            - shape: tuple of dimension sizes
        """
        file_path = Path(file_path)
        results: list[dict] = []

        try:
            with self._open(file_path) as ds:
                for var_name, var in ds.data_vars.items():
                    results.append(
                        {
                            "name": var_name,
                            "long_name": var.attrs.get("long_name", var_name),
                            "units": var.attrs.get("units", ""),
                            "standard_name": var.attrs.get("standard_name", ""),
                            "dimensions": list(var.dims),
                            "shape": tuple(var.shape),
                        }
                    )
        except Exception as e:
            self.logger.error(f"Failed to list variables in {file_path}: {e}")

        return results

    def get_timestamps(self, file_path: PathLike, variable_name: str) -> list[datetime]:
        """Get timestamps for a specific variable."""
        file_path = Path(file_path)

        try:
            with self._open(file_path) as ds:
                if variable_name not in ds.data_vars:
                    return []
                var = ds[variable_name]
                time_dim = self._time_dim(var)
                if not time_dim:
                    return []
                return self._collect_timestamps(var.coords[time_dim])
        except Exception as e:
            self.logger.error(f"Failed to get timestamps from {file_path}: {e}")
            return []

    @contextmanager
    def open_variable(
        self,
        file_path: PathLike,
        variable_name: str,
        *,
        timestamp: datetime | None = None,
        window: tuple[int, int, int, int] | None = None,
        **kwargs,
    ) -> Generator[VariableInfo, None, None]:
        """Open a NetCDF variable lazily."""
        file_path = Path(file_path)

        ds = self._open(file_path)
        try:
            if variable_name not in ds.data_vars:
                raise ValueError(f"Variable '{variable_name}' not found in {file_path}")

            var = ds[variable_name]

            # Time selection (lazy)
            time_dim = self._time_dim(var)
            if timestamp is not None and time_dim:
                sel_ts = timestamp
                # If the file's time axis is tz-naive but our timestamp is tz-aware,
                # strip tzinfo before selection — pandas refuses to compare the two.
                coord_tz = getattr(var.coords[time_dim].dtype, "tz", None)
                if coord_tz is None and getattr(timestamp, "tzinfo", None) is not None:
                    sel_ts = timestamp.replace(tzinfo=None)
                var = var.sel({time_dim: sel_ts}, method="nearest")
            elif time_dim and var[time_dim].size > 0:
                var = var.isel({time_dim: 0})

            valid_time = self._resolve_valid_time(var, ds, timestamp)

            # Refuse a grid that cannot be placed, and lay 0..360 longitudes out
            # west to east — on the whole grid, before any window is cut from it.
            lat_name, lon_name = self._find_lat_lon_coords(var)
            crs = self._detect_crs(ds)
            var = self._regular_grid(var, lat_name, lon_name, crs)

            # Orientation check
            y_dim, x_dim = self._spatial_dims(var)
            needs_flip = False
            if y_dim and y_dim in var.coords:
                y_vals = var.coords[y_dim].values
                if len(y_vals) > 1 and y_vals[0] < y_vals[-1]:
                    needs_flip = True

            full_height = var.sizes.get(y_dim, var.shape[-2])
            full_width = var.sizes.get(x_dim, var.shape[-1])

            # Window slicing (lazy — just adjusts dask graph)
            if window and y_dim and x_dim:
                x_off, y_off, w, h = window
                w = min(w, full_width - x_off)
                h = min(h, full_height - y_off)
                var = var.isel({x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)})

            bounds, resolution = self._spatial_info(var, lat_name, lon_name)

            yield VariableInfo(
                data=var,
                bounds=bounds,
                crs=crs,
                width=var.sizes.get(x_dim, var.shape[-1]),
                height=var.sizes.get(y_dim, var.shape[-2]),
                resolution=resolution,
                timestamp=valid_time,
                variable_name=variable_name,
                units=var.attrs.get("units", ""),
                needs_flip=needs_flip,
                metadata={
                    "source_file": str(file_path),
                    "long_name": var.attrs.get("long_name", ""),
                    "standard_name": var.attrs.get("standard_name", ""),
                    "full_width": int(full_width),
                    "full_height": int(full_height),
                },
            )
        finally:
            ds.close()

    def extract_variable(
        self,
        file_path: PathLike,
        variable_name: str,
        timestamp: datetime | None = None,
        window: tuple[int, int, int, int] | None = None,
        **kwargs,
    ) -> ExtractedVariable:
        """Override to apply fill-value replacement after materialization."""
        with self.open_variable(
            file_path,
            variable_name,
            timestamp=timestamp,
            window=window,
            **kwargs,
        ) as var_info:
            data = var_info.compute()

            # NetCDF-specific: replace fill values with NaN
            data = self._apply_fill_value(data, var_info.data)

            height = int(data.shape[0]) if data.ndim > 1 else 1
            width = int(data.shape[1]) if data.ndim > 1 else int(data.shape[0])

            return ExtractedVariable(
                data=data,
                bounds=var_info.bounds,
                crs=var_info.crs,
                width=width,
                height=height,
                resolution=var_info.resolution,
                timestamp=var_info.timestamp,
                variable_name=var_info.variable_name,
                units=var_info.units,
                metadata=var_info.metadata,
            )

    # ------------------------------------------------------------------
    # Internal: opening files
    # ------------------------------------------------------------------

    def _open(self, file_path: Path) -> xr.Dataset:
        """Open a NetCDF file with lazy loading."""
        return xr.open_dataset(file_path, chunks={}, engine="netcdf4")

    # ------------------------------------------------------------------
    # Internal: time handling
    # ------------------------------------------------------------------

    _TIME_NAMES = {"time", "valid_time", "t", "datetime", "xtime"}

    def _time_dim(self, var) -> str | None:
        for d in var.dims:
            if d.lower() in self._TIME_NAMES:
                return d
        return None

    def _collect_timestamps(self, time_coord) -> list[datetime]:
        timestamps: list[datetime] = []
        values = np.atleast_1d(time_coord.values)
        for t in values:
            if isinstance(t, np.datetime64):
                timestamps.append(pd.Timestamp(t).to_pydatetime())
        return sorted(timestamps)

    def _resolve_valid_time(self, var, ds: xr.Dataset, requested_time: datetime | None) -> datetime:
        if requested_time is not None:
            return requested_time

        for coord_name in ("valid_time", "time", "t"):
            if coord_name in var.coords:
                t = var.coords[coord_name].values
                if isinstance(t, np.datetime64):
                    return pd.Timestamp(t).to_pydatetime()

        for attr in ("time_coverage_start", "date_created"):
            if attr in ds.attrs:
                try:
                    return pd.Timestamp(ds.attrs[attr]).to_pydatetime()
                except Exception:
                    pass

        return datetime.now(UTC)

    # ------------------------------------------------------------------
    # Internal: spatial helpers
    # ------------------------------------------------------------------

    _Y_NAMES = {"latitude", "lat", "y"}
    _X_NAMES = {"longitude", "lon", "x"}

    def _spatial_dims(self, var) -> tuple[str | None, str | None]:
        y_dim = x_dim = None
        for d in var.dims:
            dl = d.lower()
            if dl in self._Y_NAMES:
                y_dim = d
            elif dl in self._X_NAMES:
                x_dim = d
        return y_dim, x_dim

    def _spatial_info(self, var, lat_name: str, lon_name: str) -> tuple[tuple[float, ...], tuple[float, float]]:
        """Returns (bounds, resolution) of a grid _regular_grid has accepted."""
        lats = np.asarray(var.coords[lat_name].values, dtype=np.float64)
        lons = np.asarray(var.coords[lon_name].values, dtype=np.float64)

        lat_res = self._compute_resolution(lats)
        lon_res = self._compute_resolution(lons)

        bounds = (
            float(lons.min() - lon_res / 2),
            float(lats.min() - lat_res / 2),
            float(lons.max() + lon_res / 2),
            float(lats.max() + lat_res / 2),
        )
        return bounds, (lon_res, lat_res)

    _LAT_UNITS = {"degrees_north", "degree_north", "degrees_n", "degree_n", "degreesn", "degreen"}
    _LON_UNITS = {"degrees_east", "degree_east", "degrees_e", "degree_e", "degreese", "degreee"}

    def _find_lat_lon_coords(self, var) -> tuple[str, str]:
        """Name the coordinates that place *var*'s rows and columns.

        A 1D dimension coordinate wins over an auxiliary one, so a projected
        file's x/y axes are used even when it also carries 2D lat/lon. Failing
        a known name, CF attributes identify the coordinate (raw WRF's XLAT and
        XLONG carry only units). A grid with neither cannot be placed.
        """
        lat_name = self._find_axis_coord(var, self._Y_NAMES, "latitude", self._LAT_UNITS)
        lon_name = self._find_axis_coord(var, self._X_NAMES, "longitude", self._LON_UNITS)
        if lat_name is None or lon_name is None:
            raise UnsupportedGridError(
                f"The grid has no latitude/longitude coordinates the NetCDF handler recognises "
                f"(found coordinates: {', '.join(map(str, var.coords)) or 'none'}), so its cells "
                f"cannot be placed. {self._REGULAR_GRIDS_ONLY}"
            )
        return lat_name, lon_name

    @staticmethod
    def _find_axis_coord(var, names: set[str], standard_name: str, units: set[str]) -> str | None:
        coords = [(name, var.coords[name]) for name in var.coords]
        by_name = [name for name, c in coords if str(name).lower() in names]
        for name in by_name:
            if name in var.dims:
                return name
        if by_name:
            return by_name[0]
        for name, c in coords:
            if c.attrs.get("standard_name") == standard_name:
                return name
            if str(c.attrs.get("units", "")).lower() in units:
                return name
        return None

    # How far, in cells, any coordinate may sit from the evenly spaced axis
    # running between the axis's two ends. Float32 jitter and decimal-rounded
    # coordinates stay under 0.01 of a cell. A 9 km Mercator grid drifts ~1 cell
    # over -12..15 deg and ~3.6 over -35..-20; a +-5 deg equatorial strip drifts
    # 0.03 (~270 m) and passes.
    _SPACING_TOLERANCE_CELLS = 0.1

    _REGULAR_GRIDS_ONLY = (
        "Only regular grids can be placed — reproject the file onto a regular "
        "latitude-longitude grid before ingesting it."
    )

    def _regular_grid(self, var, lat_name: str, lon_name: str, crs: str):
        """Return *var* on a placeable grid, or refuse it with UnsupportedGridError."""
        lats = np.asarray(var.coords[lat_name].values)
        lons = np.asarray(var.coords[lon_name].values)
        # Before the 0..360 recentring, which reorders a global axis.
        self._require_regular_grid(lats, lat_name, lons, lon_name)
        if not self._is_geographic(crs):
            return var
        if np.abs(lats).max() > 90 or lons.min() < -180 or lons.max() > 360:
            raise UnsupportedGridError(
                f"The grid's '{lat_name}'/'{lon_name}' axes are not in degrees (they span "
                f"{lats.min():g}..{lats.max():g} and {lons.min():g}..{lons.max():g}) and the file "
                "declares no projected CRS, so its cells cannot be placed. Declare the CRS "
                "(a 'crs' attribute or a 'spatial_ref' variable) or reproject the file onto a "
                "regular latitude-longitude grid before ingesting it."
            )
        return self._recentre_longitudes(var, lon_name)

    def _recentre_longitudes(self, var, lon_name: str):
        """Lay a 0..360 longitude axis out from -180 to 180, west to east.

        Wrapping the coordinate values alone would leave the columns in their
        stored order, so a global grid starting at Greenwich would be placed
        180 degrees off; the columns are reordered with it. A regional grid
        that straddles 180 splits in two under -180..180 and is refused.
        """
        lons = np.asarray(var.coords[lon_name].values, dtype=np.float64)
        if lons.max() <= 180:
            return var
        wrapped = np.where(lons > 180, lons - 360, lons)
        order = np.argsort(wrapped, kind="stable")
        try:
            self._require_even_spacing(wrapped[order], lon_name)
        except UnsupportedGridError:
            raise UnsupportedGridError(
                f"The grid's '{lon_name}' axis crosses the antimeridian ({lons.min():g}..{lons.max():g} "
                "degrees east) without covering the globe, so it splits in two under -180..180. "
                "Split the file at 180 degrees, or reproject it onto -180..180, before ingesting it."
            ) from None
        dim = var.coords[lon_name].dims[0]
        var = var.assign_coords({lon_name: (dim, wrapped, var.coords[lon_name].attrs)})
        if (order != np.arange(order.size)).any():
            var = var.isel({dim: order})
        return var

    @staticmethod
    def _is_geographic(crs: str) -> bool:
        try:
            return CRS.from_user_input(crs).is_geographic
        except CRSError:
            return crs == "EPSG:4326"

    @classmethod
    def _require_regular_grid(cls, lats: np.ndarray, lat_name: str, lons: np.ndarray, lon_name: str) -> None:
        if lats.ndim != 1 or lons.ndim != 1:
            raise UnsupportedGridError(
                f"The grid is curvilinear: '{lat_name}' and '{lon_name}' are {lats.ndim}D/{lons.ndim}D "
                f"coordinates, not 1D axes. {cls._REGULAR_GRIDS_ONLY}"
            )
        cls._require_even_spacing(lats, lat_name)
        cls._require_even_spacing(lons, lon_name)

    @classmethod
    def _require_even_spacing(cls, coords: np.ndarray, name: str) -> None:
        coords = coords.astype(np.float64)
        if not np.isfinite(coords).all():
            raise UnsupportedGridError(
                f"The grid's '{name}' axis has missing (NaN or infinite) coordinates, so its "
                f"cells cannot be placed. {cls._REGULAR_GRIDS_ONLY}"
            )
        if coords.size < 3:
            return
        cell = (coords[-1] - coords[0]) / (coords.size - 1)
        even = np.linspace(coords[0], coords[-1], coords.size)
        drift = float(np.max(np.abs(coords - even))) / abs(cell) if cell else np.inf
        if drift > cls._SPACING_TOLERANCE_CELLS:
            raise UnsupportedGridError(
                f"The grid's '{name}' axis is not evenly spaced: its coordinates drift up to "
                f"{drift:.2f} cells from an even spacing (tolerance "
                f"{cls._SPACING_TOLERANCE_CELLS} cells), so placing it by one resolution would "
                f"misplace the data. {cls._REGULAR_GRIDS_ONLY}"
            )

    @staticmethod
    def _compute_resolution(coords: np.ndarray) -> float:
        """Spacing of a 1D axis already checked by _require_even_spacing."""
        if coords.size > 1:
            return float(np.median(np.abs(np.diff(coords))))
        return 1.0

    @staticmethod
    def _detect_crs(ds: xr.Dataset) -> str:
        if "crs" in ds.attrs:
            return ds.attrs["crs"]
        if "spatial_ref" in ds.data_vars:
            return ds["spatial_ref"].attrs.get("crs_wkt", "EPSG:4326")
        return "EPSG:4326"

    # ------------------------------------------------------------------
    # Internal: data helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_fill_value(data: np.ndarray, var) -> np.ndarray:
        """Replace fill values with NaN."""
        fill_value = None
        if hasattr(var, "encoding") and "_FillValue" in var.encoding:
            fill_value = var.encoding["_FillValue"]
        if fill_value is None:
            fill_value = var.attrs.get("_FillValue")
        if fill_value is not None:
            data = np.where(data == fill_value, np.nan, data)
        return data
