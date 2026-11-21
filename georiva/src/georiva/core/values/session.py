"""The reading session and the one-call helpers built on it (ADR 0046 §10, §12)."""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass

import numpy as np
from affine import Affine
from django.db.models import Q, QuerySet

from georiva.core.storage import BucketType, storage
from georiva.core.storage.asset_location import AssetLocation, store_q

from .store import OpenStores, StoreSlice, store_slice
from .units import Conversion, Conversions

#: A read in the unit its session reads in.
_SESSION = object()

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
    """One batch of reads. Use :func:`session`, which ends it.

    It holds the stores it opened, each at the snapshot it was opened on, and
    the fields it decoded from them, until it ends (ADR 0046 §12).
    """

    def __init__(self, *, in_unit: str | None = None):
        self._open = True
        self._stores = OpenStores()
        #: The unit every value is returned in; ``None`` for each Variable's served unit.
        self.in_unit = in_unit
        self._conversions = Conversions(in_unit)
        #: Conversions for reads that name a unit of their own, by that unit.
        self._other_units: dict[str, Conversions] = {}

    @property
    def open_stores(self) -> int:
        """How many Upstream stores this session holds open."""
        return len(self._stores)

    @property
    def held_fields(self) -> int:
        """How many decoded Store fields this session holds — never more than
        :data:`~.store.HELD_FIELDS`, however many it has read."""
        return self._stores.held_fields

    @property
    def held_field_bytes(self) -> int:
        """The memory this session's held Store fields take."""
        return self._stores.held_field_bytes

    def close(self) -> None:
        self._open = False
        self._stores.close()

    # ---- reads ---------------------------------------------------------------

    def field(self, asset, *, in_unit=_SESSION) -> Field:
        """The asset's whole field. ``in_unit``, on this or any read, names a unit
        for this read alone; by default it is the session's."""
        to = self._conversion(asset, in_unit)
        with self._raster(asset) as src:
            return _read(src, to=to)

    def window(self, asset, bbox: Sequence[float], *, in_unit=_SESSION) -> Field:
        """The cells under ``bbox`` — ``(west, south, east, north)`` in WGS84 —
        read by range: only the blocks under it are fetched."""
        to = self._conversion(asset, in_unit)
        with self._raster(asset) as src:
            return _read(src, _window(src, _in_crs_of(src, bbox)), to=to)

    def point(self, asset, lon: float, lat: float, *, in_unit=_SESSION) -> float:
        """The value of the cell containing ``(lon, lat)``; NaN outside the field or on nodata."""
        to = self._conversion(asset, in_unit)
        with self._raster(asset) as src:
            return _point(src, lon, lat, to)

    def area(self, asset, geometry: dict, aggregation: str, *, in_unit=_SESSION) -> float:
        """``aggregation`` over the cells whose centres fall inside a WGS84 GeoJSON
        ``geometry``; NaN where there are none with a value."""
        if aggregation not in AGGREGATIONS:
            raise ValueError(f"Unsupported aggregation {aggregation!r}. Choose from: {sorted(AGGREGATIONS)}")
        to = self._conversion(asset, in_unit)
        with self._raster(asset) as src:
            return _area(src, geometry, aggregation, to)

    def point_series(self, assets: Sequence, lon: float, lat: float) -> list[float]:
        """:meth:`point` of each asset, in the order given. A read that fails raises."""
        return self._each(assets, lambda asset: self.point(asset, lon, lat))

    def area_series(self, assets: Sequence, geometry: dict, aggregation: str) -> list[float]:
        """:meth:`area` of each asset, in the order given. A read that fails raises."""
        return self._each(assets, lambda asset: self.area(asset, geometry, aggregation))

    def series(self, asset, *, in_unit=_SESSION):
        """A multi-temporal file's values over its own time axis — a staged
        NetCDF series — as a loaded xarray DataArray. The asset's Variable is the
        file's data variable named by its slug, or the file's only one."""
        import tempfile

        import xarray as xr

        to = self._conversion(asset, in_unit)
        location = _location(asset)
        if not isinstance(location, Stored):
            raise TypeError(f"A series is read from a file in one of our buckets, not {asset!r}")
        data = storage.bucket(location.bucket_type).read_bytes(location.key)
        with tempfile.NamedTemporaryFile(suffix=".nc") as fh:
            fh.write(data)
            fh.flush()
            with xr.open_dataset(fh.name) as dataset:
                array = _data_variable(dataset, asset).load()
        return to.apply(array)

    def virtual_dataset(self, manifest, *, internal: bool = True):
        """A Variable's virtual Zarr store as a lazy xarray Dataset — its local
        assets' values over time, with no COG opened until a chunk is read.
        The Variable's array (named by its slug) is converted lazily, like
        every other read.

        The manifest is opened undescribed — its raw values — and converted
        here, so values are converted once, into the unit this session reads in."""
        to = self._conversions.of(manifest.variable_id)
        self._check_open()
        dataset = manifest.open_dataset(internal=internal, chunks={}, described=False)
        name = manifest.variable.slug
        if name in dataset and not to.is_identity:
            dataset = dataset.assign({name: to.apply(dataset[name])})
        return dataset

    # ---- helpers -------------------------------------------------------------

    def _each(self, assets: Sequence, read) -> list[float]:
        from concurrent.futures import ThreadPoolExecutor

        assets = list(assets)
        if not assets:
            return []
        for asset in assets:
            # Every conversion first: one that is refused is refused before any read.
            self._conversion(asset)
        with ThreadPoolExecutor(max_workers=min(SERIES_READ_WORKERS, len(assets))) as pool:
            return list(pool.map(read, assets))

    def _conversion(self, asset, in_unit=_SESSION) -> Conversion:
        """How ``asset``'s raw values become values in the unit this read is in."""
        self._check_open()
        conversions = self._conversions
        if in_unit is not _SESSION and in_unit != self.in_unit:
            conversions = self._other_units.setdefault(in_unit, Conversions(in_unit))
        return conversions.of(getattr(asset, "variable_id", None))

    def _raster(self, asset):
        self._check_open()
        location = _location(asset)
        if isinstance(location, StoreSlice):
            return nullcontext(self._stores.source(location))
        return location.open_raster()

    def _check_open(self) -> None:
        if not self._open:
            raise SessionClosed("This reading session has ended; open another to read.")


@contextmanager
def session(*, in_unit: str | None = None) -> Iterator[Session]:
    """A reading session, ended when the block is left.

    Every value is returned in its Variable's served unit, or in ``in_unit``
    when one is named (ADR 0046 §11). A unit the source unit cannot reach is
    refused — :class:`ConversionRefused` — before anything is read.
    """
    s = Session(in_unit=in_unit)
    try:
        yield s
    finally:
        s.close()


def field(asset, *, in_unit: str | None = None) -> Field:
    with session(in_unit=in_unit) as s:
        return s.field(asset)


def window(asset, bbox: Sequence[float], *, in_unit: str | None = None) -> Field:
    with session(in_unit=in_unit) as s:
        return s.window(asset, bbox)


def point(asset, lon: float, lat: float, *, in_unit: str | None = None) -> float:
    with session(in_unit=in_unit) as s:
        return s.point(asset, lon, lat)


def area(asset, geometry: dict, aggregation: str, *, in_unit: str | None = None) -> float:
    with session(in_unit=in_unit) as s:
        return s.area(asset, geometry, aggregation)


def readable(assets: QuerySet) -> QuerySet:
    """The assets of ``assets`` this module gives values for: COGs, local or
    Remote, and Store assets. Not a texture (PNG, WebP, JPEG), nor any format
    a session cannot read.

    Callers narrow to readable assets through this, not by filtering on
    ``format`` themselves, so what a session can read is said in one place.
    """
    from georiva.core.models import Asset  # late: the models import the storage seam

    return assets.filter(Q(format=Asset.Format.COG) | store_q())


# ---- reading one open raster ------------------------------------------------------


def _location(asset):
    """Where the bytes of whatever was asked for are: a raster to open, or a
    Store asset's field (:class:`StoreSlice`)."""
    if isinstance(asset, AssetLocation | Stored | StoreSlice):
        return asset
    location = getattr(type(asset), "location", None)
    if location is not None:
        return store_slice(asset) if asset.location.is_store else asset.location
    href = getattr(asset, "href", None)
    if href:
        # A StagingAsset: its href is always a key on the staging bucket.
        return Stored(BucketType.STAGING, href)
    raise TypeError(f"Cannot read values from {asset!r}")


def _data_variable(dataset, asset):
    """The data variable of ``dataset`` that ``asset``'s Variable names, or its only one."""
    variable = getattr(asset, "variable", None)
    if variable is not None and variable.slug in dataset.data_vars:
        return dataset[variable.slug]
    names = list(dataset.data_vars)
    if len(names) != 1:
        raise ValueError(f"Cannot tell which of {names} holds the values of {asset!r}")
    return dataset[names[0]]


def _values(src, raw: np.ndarray, to: Conversion) -> np.ndarray:
    """Band 1's cells as float32 values: nodata and non-finite as NaN, the band's
    own scale and offset applied, then converted ``to`` the unit read in."""
    data = raw.astype(np.float32)
    if src.nodata is not None and not math.isnan(src.nodata):
        data[raw == src.nodata] = np.nan
    data[~np.isfinite(data)] = np.nan
    scale, offset = src.scales[0], src.offsets[0]
    if scale != 1.0 or offset != 0.0:
        data = (data * scale + offset).astype(np.float32)
    return to.array(data)


def _read(src, window=None, *, to: Conversion) -> Field:
    if window is None:
        return _north_up(_values(src, src.read(1), to), src.transform, src)
    transform = src.window_transform(window)
    if window.width == 0 or window.height == 0:
        # Nothing under the box: an empty field, read from nowhere.
        return _north_up(np.empty((0, 0), dtype=np.float32), transform, src)
    return _north_up(_values(src, src.read(1, window=window), to), transform, src)


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


def _point(src, lon: float, lat: float, to: Conversion) -> float:
    from rasterio.windows import Window

    x, y = lon, lat
    if not _in_wgs84(src):
        from rasterio.warp import transform

        xs, ys = transform(WGS84, src.crs, [lon], [lat])
        x, y = xs[0], ys[0]
    row, col = src.index(x, y)
    if not (0 <= row < src.height and 0 <= col < src.width):
        return math.nan
    return float(_values(src, src.read(1, window=Window(col, row, 1, 1)), to)[0, 0])


def _area(src, geometry: dict, aggregation: str, to: Conversion) -> float:
    from rasterio.features import bounds as geometry_bounds
    from rasterio.features import geometry_mask

    from georiva.geoprocessing.zonal import reproject_geometry

    shape = geometry if src.crs is None else reproject_geometry(geometry, src.crs)
    # Each cell is converted before it is aggregated: a sum gains the offset
    # once per cell, a spread none (ADR 0046 §7).
    field = _read(src, _window(src, geometry_bounds(shape)), to=to)
    if field.data.size == 0:
        return math.nan
    inside = geometry_mask([shape], out_shape=field.data.shape, transform=field.transform, invert=True)
    cells = field.data[inside]
    cells = cells[np.isfinite(cells)]
    if cells.size == 0:
        return math.nan
    return float(getattr(np, aggregation)(cells.astype(np.float64)))
