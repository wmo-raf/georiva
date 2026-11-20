"""The reading session and the one-call helpers built on it (ADR 0046 §10, §12)."""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

import numpy as np
from affine import Affine

from georiva.core.storage import BucketType, storage
from georiva.core.storage.asset_location import AssetLocation

#: Upstream reads in flight at once for one series. Each is a few small range
#: requests; the wait is the upstream's latency, not our CPU.
SERIES_READ_WORKERS = 8

#: How the cells inside an area are aggregated.
AGGREGATIONS = ("mean", "sum", "min", "max", "std")

WGS84 = "EPSG:4326"

#: Below this, a bounding-box edge is on a cell edge rather than inside a cell.
_EDGE = 1e-9


class SessionClosed(RuntimeError):
    """A session was read from after it ended."""


@dataclass(frozen=True)
class Stored:
    """An object in one of our buckets, named by its key: what a staging asset, or
    an asset bucket key without a row, is read as."""

    bucket_type: BucketType
    key: str

    def open_raster(self):
        return storage.bucket(self.bucket_type).open_raster(self.key)


@dataclass(frozen=True)
class Field:
    """Values over a grid: 2-D float32, row 0 north, NaN where there is no value."""

    data: np.ndarray
    #: The north-up transform of ``data``: ``e`` is negative.
    transform: Affine
    crs: str

    @property
    def height(self) -> int:
        return self.data.shape[0]

    @property
    def width(self) -> int:
        return self.data.shape[1]

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """``(west, south, east, north)`` in :attr:`crs`."""
        t = self.transform
        west, north = t.c, t.f
        east, south = t * (self.width, self.height)
        return (west, south, east, north)


class Session:
    """One batch of reads. Use :func:`session`, which ends it."""

    def __init__(self):
        self._open = True

    def close(self) -> None:
        self._open = False

    # ---- reads ---------------------------------------------------------------

    def field(self, asset) -> Field:
        """The asset's whole field."""
        with self._raster(asset) as src:
            return _read(src)

    def window(self, asset, bbox: Sequence[float]) -> Field:
        """The cells under ``bbox`` — ``(west, south, east, north)`` in WGS84 —
        read by range: only the blocks under it are fetched."""
        with self._raster(asset) as src:
            return _read(src, _window(src, _in_crs_of(src, bbox)))

    def point(self, asset, lon: float, lat: float) -> float:
        """The value of the cell containing ``(lon, lat)``; NaN outside the field or on nodata."""
        with self._raster(asset) as src:
            return _point(src, lon, lat)

    def area(self, asset, geometry: dict, aggregation: str) -> float:
        """``aggregation`` over the cells whose centres fall inside a WGS84 GeoJSON
        ``geometry``; NaN where there are none with a value."""
        if aggregation not in AGGREGATIONS:
            raise ValueError(f"Unsupported aggregation {aggregation!r}. Choose from: {sorted(AGGREGATIONS)}")
        with self._raster(asset) as src:
            return _area(src, geometry, aggregation)

    def point_series(self, assets: Sequence, lon: float, lat: float) -> list[float]:
        """:meth:`point` of each asset, in the order given. A read that fails raises."""
        return self._each(assets, lambda asset: self.point(asset, lon, lat))

    def area_series(self, assets: Sequence, geometry: dict, aggregation: str) -> list[float]:
        """:meth:`area` of each asset, in the order given. A read that fails raises."""
        return self._each(assets, lambda asset: self.area(asset, geometry, aggregation))

    def virtual_dataset(self, manifest, *, internal: bool = True):
        """A Variable's virtual Zarr store as a lazy xarray Dataset — its local
        assets' values over time, with no COG opened until a chunk is read."""
        self._check_open()
        return manifest.open_dataset(internal=internal, chunks={})

    # ---- helpers -------------------------------------------------------------

    def _each(self, assets: Sequence, read) -> list[float]:
        from concurrent.futures import ThreadPoolExecutor

        assets = list(assets)
        if not assets:
            return []
        with ThreadPoolExecutor(max_workers=min(SERIES_READ_WORKERS, len(assets))) as pool:
            return list(pool.map(read, assets))

    def _raster(self, asset):
        self._check_open()
        return _location(asset).open_raster()

    def _check_open(self) -> None:
        if not self._open:
            raise SessionClosed("This reading session has ended; open another to read.")


@contextmanager
def session() -> Iterator[Session]:
    """A reading session, ended when the block is left."""
    s = Session()
    try:
        yield s
    finally:
        s.close()


def field(asset) -> Field:
    with session() as s:
        return s.field(asset)


def window(asset, bbox: Sequence[float]) -> Field:
    with session() as s:
        return s.window(asset, bbox)


def point(asset, lon: float, lat: float) -> float:
    with session() as s:
        return s.point(asset, lon, lat)


def area(asset, geometry: dict, aggregation: str) -> float:
    with session() as s:
        return s.area(asset, geometry, aggregation)


# ---- reading one open raster ------------------------------------------------------


def _location(asset):
    """Where the bytes of whatever was asked for are."""
    if isinstance(asset, AssetLocation | Stored):
        return asset
    location = getattr(type(asset), "location", None)
    if location is not None:
        return asset.location
    href = getattr(asset, "href", None)
    if href:
        # A StagingAsset: its href is always a key on the staging bucket.
        return Stored(BucketType.STAGING, href)
    raise TypeError(f"Cannot read values from {asset!r}")


def _values(src, raw: np.ndarray) -> np.ndarray:
    """Band 1's cells as float32 values: nodata and non-finite as NaN, the band's
    own scale and offset applied."""
    data = raw.astype(np.float32)
    if src.nodata is not None and not math.isnan(src.nodata):
        data[raw == src.nodata] = np.nan
    data[~np.isfinite(data)] = np.nan
    scale, offset = src.scales[0], src.offsets[0]
    if scale != 1.0 or offset != 0.0:
        data = (data * scale + offset).astype(np.float32)
    return data


def _read(src, window=None) -> Field:
    if window is None:
        return _north_up(_values(src, src.read(1)), src.transform, src)
    transform = src.window_transform(window)
    if window.width == 0 or window.height == 0:
        # Nothing under the box: an empty field, read from nowhere.
        return _north_up(np.empty((0, 0), dtype=np.float32), transform, src)
    return _north_up(_values(src, src.read(1, window=window)), transform, src)


def _north_up(data: np.ndarray, transform: Affine, src) -> Field:
    """Row 0 north: a south-up grid (``e > 0``) is flipped, and its transform with it."""
    if transform.e > 0:
        data = np.flipud(data)
        transform = Affine(
            transform.a, transform.b, transform.c, transform.d, -transform.e, transform.f + transform.e * data.shape[0]
        )
    crs = src.crs.to_string() if src.crs else WGS84
    return Field(data=np.ascontiguousarray(data), transform=transform, crs=crs)


def _in_wgs84(src) -> bool:
    return src.crs is None or src.crs.to_epsg() == 4326


def _in_crs_of(src, bbox: Sequence[float]) -> tuple[float, float, float, float]:
    """A WGS84 ``(west, south, east, north)`` in the raster's CRS."""
    if _in_wgs84(src):
        return tuple(bbox)
    from rasterio.warp import transform_bounds

    return transform_bounds(WGS84, src.crs, *bbox)


def _window(src, bounds: Sequence[float]):
    """The window of the cells under ``bounds`` (in the raster's CRS), clipped to the raster."""
    from rasterio.windows import Window

    west, south, east, north = bounds
    inverse = ~src.transform
    corners = [inverse * corner for corner in ((west, south), (west, north), (east, south), (east, north))]
    cols, rows = [c for c, _ in corners], [r for _, r in corners]
    col0 = max(0, math.floor(min(cols) + _EDGE))
    col1 = min(src.width, math.ceil(max(cols) - _EDGE))
    row0 = max(0, math.floor(min(rows) + _EDGE))
    row1 = min(src.height, math.ceil(max(rows) - _EDGE))
    return Window(col0, row0, max(0, col1 - col0), max(0, row1 - row0))


def _point(src, lon: float, lat: float) -> float:
    from rasterio.windows import Window

    x, y = lon, lat
    if not _in_wgs84(src):
        from rasterio.warp import transform

        xs, ys = transform(WGS84, src.crs, [lon], [lat])
        x, y = xs[0], ys[0]
    row, col = src.index(x, y)
    if not (0 <= row < src.height and 0 <= col < src.width):
        return math.nan
    return float(_values(src, src.read(1, window=Window(col, row, 1, 1)))[0, 0])


def _area(src, geometry: dict, aggregation: str) -> float:
    from rasterio.features import bounds as geometry_bounds
    from rasterio.features import geometry_mask

    from georiva.geoprocessing.zonal import reproject_geometry

    shape = geometry if src.crs is None else reproject_geometry(geometry, src.crs)
    field = _read(src, _window(src, geometry_bounds(shape)))
    if field.data.size == 0:
        return math.nan
    inside = geometry_mask([shape], out_shape=field.data.shape, transform=field.transform, invert=True)
    cells = field.data[inside]
    cells = cells[np.isfinite(cells)]
    if cells.size == 0:
        return math.nan
    return float(getattr(np, aggregation)(cells.astype(np.float64)))
