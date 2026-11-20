"""Reading a Store asset's values (ADR 0046 §10, §12; ADR 0044 §9, §11).

A **Store asset** is not a file. Its href is a whole Icechunk repository, and
which field it means is its item's reference time and valid time and an
array's path in the store (:class:`StoreSlice`). This module opens the store,
decodes that field and hands it to the session as a raster held in memory, so
a window, a point or an area of it is cut exactly as a COG's is.

**It follows only what an operator pinned.** The store is opened by core's one
store-opening function, ``harvest.upstream.open_repository``: the feed's Pinned
locations replace the repository's own word on where its data lives, so a
chunk anywhere else fails to read — :class:`StoreDataNotPinned`, naming the
place.

**A session holds what it opened** (:class:`OpenStores`): each store opened
once, read-only at the snapshot it was opened on, for the session's life — a
batch reads one moment of a store that commits every few seconds — and each
field decoded once. Closing the session lets go of all of it. Nothing is kept
between sessions.
"""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np
from affine import Affine

#: The branch a store is read at: its tip, as the session opens it (ADR 0044 §10).
BRANCH = "main"

#: How long one request to an upstream store may take, and to connect.
READ_TIMEOUT = "30s"
CONNECT_TIMEOUT = "5s"

#: An address in what the store's library says of a read it would not make.
_ADDRESS = re.compile(r"[a-z][a-z0-9+.-]*://[^\s()'\"]+", re.IGNORECASE)

_TIMEDELTA_UNITS = frozenset({"days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"})


class StoreUnreadable(ValueError):
    """A Store asset's field could not be read: the store is not laid out as a
    map-shaped store is, or does not hold the step."""


class StepNotHeld(StoreUnreadable):
    """The store, at the snapshot this session reads, does not hold the field's step."""


class StoreDataNotPinned(StoreUnreadable):
    """The field's data is in a place the feed's Pinned locations do not include (ADR 0044 §11)."""

    def __init__(self, place: str):
        super().__init__(
            f"This field's data is kept at {place or 'a place'}, which the harvest feed's pinned locations do "
            "not include. Re-check the upstream to confirm where its data lives."
        )
        self.place = place


@dataclass(frozen=True)
class StoreSlice:
    """One field of an Upstream store: what a Store asset names.

    ``chunk_locations`` are the harvest feed's Pinned locations — each place as
    the store declares it, with its region (empty where it has none) — the only
    places its data is followed into.
    """

    store: str
    array: str
    time: datetime
    reference_time: datetime | None = None
    chunk_locations: Mapping[str, str] = field(default_factory=dict, hash=False, compare=False)

    @property
    def key(self) -> tuple:
        """What tells two slices apart: the same field read with the same pins."""
        return (self.store, self.array, self.time, self.reference_time, self.pins)

    @property
    def pins(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((str(prefix), str(region or "")) for prefix, region in self.chunk_locations.items()))


def store_slice(asset) -> StoreSlice:
    """The field a Store asset row names, with its harvest feed's Pinned locations.
    A row whose feed is gone pins nothing, so its data is followed nowhere."""
    from georiva.core.machine_plane.remote_assets import feed_chunk_locations

    item = asset.item
    pins = feed_chunk_locations([asset.harvested_by_id]).get(asset.harvested_by_id, {})
    return StoreSlice(
        store=asset.href,
        array=asset.store_array,
        time=item.time,
        reference_time=item.reference_time,
        chunk_locations=pins,
    )


def repository_storage(href: str):
    """Where the repository at ``href`` is read from: over HTTP, every request bounded."""
    import icechunk

    options = {"timeout": READ_TIMEOUT, "connect_timeout": CONNECT_TIMEOUT}
    if href.startswith("http://"):
        options["allow_http"] = "true"
    return icechunk.http_storage(href, opts=options)


# ---- what a session holds open ------------------------------------------------------


class OpenStores:
    """The stores one session opened, and the fields it decoded from them."""

    def __init__(self):
        self._lock = threading.Lock()
        self._stores: dict[tuple, _OpenStore] = {}
        self._store_locks: dict[tuple, threading.Lock] = {}
        self._fields: dict[tuple, _ArraySource] = {}
        self._field_locks: dict[tuple, threading.Lock] = {}

    def __len__(self) -> int:
        return len(self._stores)

    def source(self, wanted: StoreSlice) -> _ArraySource:
        """The decoded field ``wanted`` names, as a raster held in memory."""
        key = wanted.key
        with self._lock_for(self._field_locks, key):
            if key not in self._fields:
                self._fields[key] = self._store(wanted).field(wanted)
            return self._fields[key]

    def close(self) -> None:
        with self._lock:
            self._stores.clear()
            self._store_locks.clear()
            self._fields.clear()
            self._field_locks.clear()

    def _store(self, wanted: StoreSlice) -> _OpenStore:
        key = (wanted.store, wanted.pins)
        with self._lock_for(self._store_locks, key):
            if key not in self._stores:
                self._stores[key] = _open(wanted.store, dict(wanted.pins))
            return self._stores[key]

    def _lock_for(self, locks: dict, key) -> threading.Lock:
        with self._lock:
            return locks.setdefault(key, threading.Lock())


def _open(href: str, chunk_locations: dict[str, str]) -> _OpenStore:
    # Late: harvest is built on core, and its upstream client imports Django models.
    from georiva.harvest import upstream

    repository = upstream.open_repository(repository_storage(href), chunk_locations)
    return _OpenStore(repository.readonly_session(BRANCH).store)


class _OpenStore:
    """A store opened read-only at one snapshot, with its arrays and coordinates as they are read."""

    def __init__(self, store):
        self.store = store
        self._arrays: dict = {}
        self._coordinates: dict = {}
        self._lock = threading.Lock()

    def field(self, wanted: StoreSlice) -> _ArraySource:
        array, axes, measured = self._measured(wanted.array)
        at = _where(dict(zip(axes, measured, strict=True)), wanted.time, wanted.reference_time)
        for part in at:
            if array.chunks[axes.index(part)] != 1:
                raise StoreUnreadable("This upstream store does not keep one map per time step.")
        cell = tuple(at.get(part, 0) for part in axes)
        if not asyncio.run(self.store.exists(f"{wanted.array}/{array.metadata.encode_chunk_key(cell)}")):
            raise StepNotHeld(f"The upstream store does not hold {wanted.array} at {_when(wanted)}.")
        try:
            data = np.asarray(array[tuple(at.get(part, slice(None)) for part in axes)])
        except Exception as exc:
            said = str(exc)
            if "virtual chunk" in said:
                address = _ADDRESS.search(said)
                raise StoreDataNotPinned(address.group(0) if address else "") from exc
            raise
        if axes.index("x") < axes.index("y"):
            data = data.T
        return _ArraySource(data, _grid(measured[axes.index("y")], measured[axes.index("x")], data.shape))

    def _array(self, path: str):
        import zarr

        with self._lock:
            if path not in self._arrays:
                self._arrays[path] = zarr.open_array(self.store, path=path, mode="r")
            return self._arrays[path]

    def _coordinate(self, path: str, name: str) -> np.ndarray:
        """The values a dimension of the array at ``path`` is measured on: beside
        the array or at the store's root, under the dimension's own name."""
        import zarr

        group = path.rpartition("/")[0]
        for at in dict.fromkeys([f"{group}/{name}" if group else name, name]):
            with self._lock:
                if at in self._coordinates:
                    return self._coordinates[at]
            try:
                coordinate = zarr.open_array(self.store, path=at, mode="r")
            except (FileNotFoundError, KeyError, zarr.errors.ArrayNotFoundError):
                continue
            decoded = _decoded(np.asarray(coordinate[:]), coordinate.attrs)
            if decoded.ndim != 1:
                raise StoreUnreadable(f"The upstream store's “{name}” is not a list of values.")
            with self._lock:
                self._coordinates[at] = decoded
            return decoded
        raise StoreUnreadable(f"The upstream store has no “{name}” to place its maps by.")

    def _measured(self, path: str):
        array = self._array(path)
        names = getattr(array.metadata, "dimension_names", None)
        if array.ndim not in (3, 4) or not names or len(names) != array.ndim:
            raise StoreUnreadable(f"The upstream array “{path}” is not one map per time step.")
        measured = [self._coordinate(path, str(name)) for name in names]
        return array, _axes([str(name) for name in names], measured), measured


# ---- a decoded field as a raster -------------------------------------------------------


class _ArraySource:
    """A decoded field, offering the part of a rasterio dataset the session reads through."""

    nodata = None
    scales = (1.0,)
    offsets = (0.0,)

    def __init__(self, data: np.ndarray, transform: Affine):
        from rasterio.crs import CRS

        self._data = data
        self.transform = transform
        self.crs = CRS.from_epsg(4326)
        self.height, self.width = data.shape

    def read(self, band: int, window=None) -> np.ndarray:
        if window is None:
            return self._data
        row, col = int(window.row_off), int(window.col_off)
        return self._data[row : row + int(window.height), col : col + int(window.width)]

    def window_transform(self, window) -> Affine:
        from rasterio.windows import transform

        return transform(window, self.transform)

    def index(self, x: float, y: float) -> tuple[int, int]:
        from rasterio.transform import rowcol

        row, col = rowcol(self.transform, x, y)
        return int(row), int(col)


def _grid(latitudes: np.ndarray, longitudes: np.ndarray, shape: tuple[int, int]) -> Affine:
    """The transform placing a field on its regular latitude/longitude cell centres, as stored:
    rows south first where the latitudes rise (the session turns such a field north-up)."""
    latitudes, longitudes = latitudes.astype("f8"), longitudes.astype("f8")
    if shape != (len(latitudes), len(longitudes)) or min(shape) < 2:
        raise StoreUnreadable("This upstream map does not match its own latitudes and longitudes.")
    x_step = (longitudes[-1] - longitudes[0]) / (len(longitudes) - 1)
    y_step = (latitudes[-1] - latitudes[0]) / (len(latitudes) - 1)
    regular = np.allclose(np.diff(longitudes), x_step, rtol=1e-4) and np.allclose(np.diff(latitudes), y_step, rtol=1e-4)
    if x_step <= 0 or y_step == 0 or not regular:
        raise StoreUnreadable("This upstream map is not on a regular latitude/longitude grid.")
    return Affine(x_step, 0.0, longitudes[0] - x_step / 2, 0.0, y_step, latitudes[0] - y_step / 2)


def _axes(names: list[str], measured: list[np.ndarray]) -> list[str]:
    """Which part each dimension is — "run", "step", "time", "y", "x"."""
    axes = []
    for name, coordinate in zip(names, measured, strict=True):
        lowered = name.lower()
        if coordinate.dtype.kind == "M":
            axes.append("time")
        elif coordinate.dtype.kind == "m":
            axes.append("step")
        elif lowered in ("lat", "latitude", "y"):
            axes.append("y")
        elif lowered in ("lon", "longitude", "x"):
            axes.append("x")
        else:
            raise StoreUnreadable(f"This upstream array is divided up in a way maps are not read from (“{name}”).")
    if "step" in axes and axes.count("time") == 1:
        axes[axes.index("time")] = "run"
    if sorted(axes) not in (sorted(["run", "step", "y", "x"]), sorted(["time", "y", "x"])):
        raise StoreUnreadable("This upstream array is not one map per time step on a latitude/longitude grid.")
    return axes


def _where(coordinates: dict[str, np.ndarray], time: datetime, reference_time: datetime | None) -> dict[str, int]:
    """The index of the field's run and step, or time, found by value among the store's own coordinates."""
    if "time" in coordinates:
        return {"time": _index(coordinates["time"], np.datetime64(_naive(time), "us"), time)}
    if reference_time is None:
        raise StoreUnreadable("A forecast field needs its reference time.")
    return {
        "run": _index(coordinates["run"], np.datetime64(_naive(reference_time), "us"), reference_time),
        "step": _index(coordinates["step"], np.timedelta64(time - reference_time, "us"), time),
    }


def _index(coordinate: np.ndarray, wanted, said) -> int:
    found = np.flatnonzero(coordinate == wanted)
    if not len(found):
        raise StepNotHeld(f"The upstream store has no step at {said.isoformat()}.")
    return int(found[0])


def _naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _when(wanted: StoreSlice) -> str:
    if wanted.reference_time is None:
        return wanted.time.isoformat()
    return f"{wanted.time.isoformat()} (run {wanted.reference_time.isoformat()})"


def _decoded(raw: np.ndarray, attrs) -> np.ndarray:
    """A coordinate's values, with times as numpy dates or lengths of time, as xarray writes them:
    whole numbers counting what their attributes say, the missing ones as the smallest int64 or a
    declared fill."""
    if raw.dtype.kind in "Mm" or raw.dtype.kind not in "iuf":
        return raw
    units = str(attrs.get("units") or "")
    if " since " not in units and units not in _TIMEDELTA_UNITS:
        return raw
    from xarray.coding.times import decode_cf_datetime, decode_cf_timedelta

    numbers = raw
    if raw.dtype.kind in "iu":
        missing = raw == np.iinfo(np.int64).min if raw.dtype.kind == "i" else np.zeros(raw.shape, bool)
        fill = attrs.get("_FillValue")
        if fill is not None:
            missing |= raw == fill
        numbers = np.where(missing, np.nan, raw.astype("f8")) if missing.any() else raw
    if " since " in units:
        return np.asarray(decode_cf_datetime(numbers, units, attrs.get("calendar")))
    return np.asarray(decode_cf_timedelta(numbers, units))
