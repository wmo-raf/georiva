# GeoRiva Format Plugin System

## Overview

The format plugin system handles reading geospatial data files (GRIB, NetCDF, GeoTIFF) in GeoRiva. Each plugin knows how to open a specific file format, list its variables, and provide access to the data — either lazily for streaming computation or materialized as numpy arrays.

The system follows a **lazy-first** design. The primary interface, `open_variable()`, returns a dask-backed xarray DataArray inside a context manager. No pixel data is loaded into RAM until you explicitly ask for it. This matters when working with large files — a 0.25° global GRIB field is ~1M grid points per variable per timestep, and a high-resolution GeoTIFF can be gigabytes.

## Architecture

```
BaseFormatPlugin (ABC)
├── GRIBFormatPlugin      — GRIB1/GRIB2 via cfgrib + xarray
├── NetCDFFormatPlugin    — NetCDF via xarray
└── GeoTIFFFormatPlugin   — GeoTIFF via rasterio + rioxarray
```

### Data flow

```
list_variables()  →  get_timestamps()  →  open_variable()  →  compute / stats
                                               │
                                               ├── extract_variable()    (numpy array)
                                               └── get_metadata_for_variable()  (bounds/size only)
```

The typical workflow is: discover what's in a file, then open a specific variable for processing.

### Key data structures

**`VariableInfo`** — returned by `open_variable()`. Wraps a lazy DataArray with spatial metadata:

| Field | Type | Description |
|-------|------|-------------|
| `data` | `xr.DataArray` | Dask-backed, lazy. No RAM used until computed. |
| `bounds` | `(west, south, east, north)` | Geographic bounding box. |
| `crs` | `str` | Coordinate reference system (e.g. `"EPSG:4326"`). |
| `width`, `height` | `int` | Grid dimensions. |
| `resolution` | `(x_res, y_res)` | Pixel size in CRS units. |
| `timestamp` | `datetime` | Valid time for this data. |
| `variable_name` | `str` | The variable identifier. |
| `units` | `str` | Physical units (e.g. `"K"`, `"m/s"`). |
| `needs_flip` | `bool` | Whether data needs vertical flip on materialize. |
| `metadata` | `dict` | Format-specific extras. |

Call `var_info.compute()` to materialize to a numpy array with correct image orientation (row 0 = north).

**`ExtractedVariable`** — returned by `extract_variable()`. Same fields as `VariableInfo` but with `data` as a materialized `np.ndarray` instead of a lazy DataArray.

## Plugin contract

Every plugin must implement four abstract methods and may optionally override two concrete methods:

### Required (abstract)

**`can_handle(file_path) → bool`**

Detect whether this plugin can read the file. Checks file extension first, then falls back to magic bytes for ambiguous cases.

**`list_variables(file_path) → list[dict]`**

Return metadata for every variable in the file. Each dict must include at minimum: `name`, `long_name`, `units`, `dimensions`, `shape`. Format-specific fields are allowed (e.g. `band_index` for GeoTIFF, `key` for GRIB).

**`get_timestamps(file_path, variable_name, **kwargs) → list[datetime]`**

Return sorted timestamps available for a variable. `variable_name` is always required — different variables in the same file can have different time steps, so unscoped queries are a footgun. GRIB callers should additionally pass `key=VariableKey(...)` for deterministic behavior; `variable_name` serves as the fallback (resolved by shortName). GeoTIFF accepts `variable_name` for signature consistency but ignores it (timestamps come from the filename).

**`open_variable(file_path, variable_name, *, timestamp, window, **kwargs) → ContextManager[VariableInfo]`**

The core method. Opens a variable lazily and yields a `VariableInfo`. Must be a context manager (`@contextmanager`) to ensure file handles are cleaned up. All operations up to the `yield` should be lazy — no `.values` or `.compute()` calls.

### Optional (concrete, overridable)

**`extract_variable(file_path, variable_name, timestamp, window, **kwargs) → ExtractedVariable`**

Materializes data to numpy. The default implementation calls `open_variable()` then `var_info.compute()`. Override when the format has a more efficient materialization path (e.g. GeoTIFF uses rasterio windowed reads instead of going through dask).

**`get_metadata_for_variable(file_path, variable_name, *, timestamp, **kwargs) → dict`**

Returns `{width, height, bounds, crs}` without reading pixel data. The default implementation opens the variable lazily and reads only the metadata fields. Override when you can get this information cheaper (e.g. GeoTIFF reads it directly from the rasterio file header).


## Existing plugins

### GRIB (`GRIBFormatPlugin`)

Handles GRIB1 and GRIB2 files using cfgrib and xarray.

**Variable identity:** GRIB is the most complex format because a single file contains interleaved messages for different variables, level types, and levels. A variable name alone isn't unique — "temperature" could mean 2m temperature (heightAboveGround=2) or 850hPa temperature (isobaricInhPa=850). The plugin uses a `VariableKey` dataclass to uniquely identify variables:

```python
@dataclass(frozen=True)
class VariableKey:
    short_name: str        # e.g. "2t", "tp"
    type_of_level: str     # e.g. "heightAboveGround", "isobaricInhPa"
    level: Optional[int]   # e.g. 2, 850
```

`list_variables()` returns a `key` field in each dict. Pass it back to `open_variable()` and `get_timestamps()` for deterministic behavior. The key converts itself to a cfgrib `filter_by_keys` dict internally — callers never deal with cfgrib directly.

**Fallback:** If no key is provided, the plugin searches all GRIB datasets by variable name or GRIB shortName. This is slower and potentially ambiguous.

**Usage:**

```python
plugin = GRIBFormatPlugin()
variables = plugin.list_variables("forecast.grib2")
# [{"key": VariableKey("2t", "heightAboveGround", 2), "name": "t2m", ...}, ...]

key = variables[0]["key"]
timestamps = plugin.get_timestamps("forecast.grib2", "2t", key=key)

# Lazy: compute stats without loading full array
with plugin.open_variable("forecast.grib2", "2t", key=key, timestamp=timestamps[0]) as var:
    min_temp = float(var.data.min())
    max_temp = float(var.data.max())

# Materialize: get numpy array for a tile
result = plugin.extract_variable("forecast.grib2", "2t", key=key, timestamp=timestamps[0], window=(0, 0, 256, 256))
```

**Inherits from base:** `extract_variable`, `get_metadata_for_variable`.

**Dependencies:** `cfgrib`, `xarray`, `numpy`, `pandas`.


### NetCDF (`NetCDFFormatPlugin`)

Handles NetCDF files (.nc, .nc4) using xarray.

**Variable identity:** NetCDF variables are uniquely identified by name — no special key needed. Just pass the variable name string.

**Format-specific behavior:**
- Supports both rectilinear grids (1D lat/lon) and curvilinear grids (2D lat/lon arrays).
- Detects CRS from dataset attributes or `spatial_ref` variable (not hardcoded to EPSG:4326 like GRIB).
- Handles `_FillValue` replacement: both from `encoding["_FillValue"]` and `attrs["_FillValue"]`.

**Overrides `extract_variable`** to apply fill-value replacement after materialization. The lazy DataArray doesn't have fill values replaced — this only happens when you call `extract_variable()` or manually compute and apply the fill value yourself.

**Usage:**

```python
plugin = NetCDFFormatPlugin()
variables = plugin.list_variables("era5.nc")
timestamps = plugin.get_timestamps("era5.nc", "temperature")

with plugin.open_variable("era5.nc", "temperature", timestamp=timestamps[0]) as var:
    mean = float(var.data.mean())
```

**Inherits from base:** `get_metadata_for_variable`.

**Dependencies:** `xarray`, `numpy`, `pandas`.


### GeoTIFF (`GeoTIFFFormatPlugin`)

Handles GeoTIFF files using rasterio and rioxarray.

**Variable identity:** Variables are bands, named `"band_1"`, `"band_2"`, etc. The band index is parsed from the name.

**Format-specific behavior:**
- Timestamps come from filename parsing (GeoTIFF has no standard time metadata). Supports patterns: ISO datetime, `YYYYMMDD_HHMM`, `YYYYMMDDHHMMSS`, `YYYYMMDD`, `YYYY-MM-DD`.
- `open_variable()` uses rioxarray for dask-backed lazy access.
- `extract_variable()` overrides the base to use rasterio windowed reads — more efficient than materializing a dask graph for single-band extraction, since rasterio reads exactly the bytes needed from disk.
- `get_metadata_for_variable()` overrides the base to read directly from the rasterio file header — avoids opening an xarray dataset entirely.

**Usage:**

```python
plugin = GeoTIFFFormatPlugin()
variables = plugin.list_variables("dem.tif")
# [{"name": "band_1", "long_name": "Elevation", "band_index": 1, ...}]

# Lazy: compute stats over a large raster
with plugin.open_variable("dem.tif", "band_1") as var:
    min_elev = float(var.data.min())
    max_elev = float(var.data.max())

# Materialize a tile (uses rasterio windowed read, not dask)
tile = plugin.extract_variable("dem.tif", "band_1", window=(1024, 1024, 256, 256))
```

**Inherits from base:** nothing — overrides both `extract_variable` and `get_metadata_for_variable`.

**Dependencies:** `rasterio`, `xarray` (with rasterio engine), `numpy`.


## Developing a new plugin

### Step 1: Subclass `BaseFormatPlugin`

```python
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

from .base import BaseFormatPlugin, VariableInfo


class HDF5FormatPlugin(BaseFormatPlugin):
    name = "hdf5"
    display_name = "HDF5"
    extensions = [".h5", ".hdf5", ".he5"]
```

### Step 2: Implement `can_handle`

Check extension first, then magic bytes:

```python
def can_handle(self, file_path):
    file_path = Path(file_path)
    if file_path.suffix.lower() in self.extensions:
        return True
    try:
        with open(file_path, "rb") as f:
            return f.read(4) == b"\x89HDF"
    except Exception:
        return False
```

### Step 3: Implement `list_variables`

Return a list of dicts. Must include `name`, `long_name`, `units`, `dimensions`, `shape`. Add format-specific fields as needed.

```python
def list_variables(self, file_path):
    file_path = Path(file_path)
    results = []
    with h5py.File(file_path, "r") as f:
        for name, dataset in f.items():
            if isinstance(dataset, h5py.Dataset):
                results.append({
                    "name": name,
                    "long_name": dataset.attrs.get("long_name", name),
                    "units": dataset.attrs.get("units", ""),
                    "dimensions": list(dataset.dims) or ["y", "x"],
                    "shape": dataset.shape,
                })
    return results
```

### Step 4: Implement `get_timestamps`

Always scope to a specific variable — `variable_name` is required. Return a sorted list of datetimes.

```python
def get_timestamps(self, file_path, variable_name):
    # ... read time coordinate for the given variable
    return sorted(timestamps)
```

### Step 5: Implement `open_variable`

This is the core method. Must be a `@contextmanager`. Key rules:

1. Open the file/dataset in the try block.
2. Do all selection (time, window) lazily — no `.values` or `.compute()`.
3. Yield a `VariableInfo` with the lazy DataArray.
4. Close the file in the finally block.

```python
@contextmanager
def open_variable(self, file_path, variable_name, *, timestamp=None, window=None, **kwargs):
    file_path = Path(file_path)

    ds = xr.open_dataset(file_path, engine="h5netcdf", chunks={})
    try:
        var = ds[variable_name]

        # Time selection (lazy — just slices the dask graph)
        time_dim = ...  # find it
        if timestamp and time_dim:
            var = var.sel({time_dim: timestamp}, method="nearest")

        # Orientation check
        needs_flip = ...  # check if lat is ascending

        # Window slicing (lazy)
        if window:
            y_dim, x_dim = ...
            x_off, y_off, w, h = window
            var = var.isel({x_dim: slice(x_off, x_off + w), y_dim: slice(y_off, y_off + h)})

        bounds, resolution = ...  # compute from coordinates

        yield VariableInfo(
            data=var,
            bounds=bounds,
            crs="EPSG:4326",
            width=...,
            height=...,
            resolution=resolution,
            timestamp=valid_time,
            variable_name=variable_name,
            units=var.attrs.get("units", ""),
            needs_flip=needs_flip,
            metadata={
                "source_file": str(file_path),
                "full_width": ...,
                "full_height": ...,
            },
        )
    finally:
        ds.close()
```

### Step 6: Optionally override `extract_variable` and `get_metadata_for_variable`

Override `extract_variable` if your format has a more efficient materialization path than dask (like GeoTIFF's rasterio windowed reads). Override `get_metadata_for_variable` if you can read bounds/size from a file header without opening the full dataset (like GeoTIFF's rasterio header).

If the defaults work well enough for your format, don't override — less code to maintain.

### Checklist

- [ ] `name` and `display_name` class attributes set
- [ ] `extensions` list covers all common extensions for the format
- [ ] `can_handle` checks extension then magic bytes
- [ ] `list_variables` returns dicts with `name`, `long_name`, `units`, `dimensions`, `shape`
- [ ] `get_timestamps` scoped to a specific variable
- [ ] `open_variable` is a `@contextmanager` with `try/finally` for cleanup
- [ ] `open_variable` keeps everything lazy until `yield`
- [ ] `VariableInfo.needs_flip` set correctly for south-to-north data
- [ ] `metadata` dict includes `source_file`, `full_width`, `full_height`
- [ ] No `.values` or `.compute()` calls inside `open_variable`


## Common patterns

### Spatial orientation

Geospatial data can store latitude either north-to-south (image order) or south-to-north (math order). The plugin system normalizes to image order (row 0 = north) on materialization:

1. In `open_variable`, check if latitude is ascending: `y_vals[0] < y_vals[-1]`
2. Set `needs_flip=True` on `VariableInfo` if so
3. `VariableInfo.compute()` calls `np.flipud()` automatically

This happens only on materialize — lazy operations work in the file's native orientation.

### Window slicing

The `window` parameter is `(x_offset, y_offset, width, height)` in pixel coordinates. Slicing is applied lazily via `xr.DataArray.isel()` — it adjusts the dask task graph without reading data. For GeoTIFF, the `extract_variable` override uses rasterio's `Window` object for byte-level efficiency.

### Longitude normalization

GRIB and NetCDF files may use 0–360° longitude. The spatial helpers normalize to -180–180° when computing bounds:

```python
if np.nanmax(lons) > 180:
    lons = np.where(lons > 180, lons - 360, lons)
```

### Bounds computation

Bounds are computed as the outer edges of the grid, expanded by half a pixel from the coordinate centers:

```
west  = min(lon) - lon_res / 2
east  = max(lon) + lon_res / 2
south = min(lat) - lat_res / 2
north = max(lat) + lat_res / 2
```
