"""How this service reads a map from an Upstream store (ADR 0044 §9, §11).

A **Store asset** is not a file. Its href is a whole Icechunk repository — every
run, every step, every array — and which field it means is its Item's reference
time and valid time plus an array's path in the store. Django publishes those,
with the feed's Pinned locations for the store's data, beside a Remote COG's
href (``dependencies.remote_source``); this module is what turns them into
something the COG reader can tile.

**It follows only what an operator pinned.** A virtual store's chunks are
references into its producer's buckets, and the repository carries its own word
on where those are and how to reach them — any endpoint, any credentials. That
word is discarded on opening: the reader's container settings are built from the
published pins alone, anonymously, at the provider's public endpoint, no further
than each prefix. A chunk anywhere else fails to read, and the response says the
map's data is kept somewhere this feed was not allowed to read.

**It keeps a store open.** Opening one costs seconds, reading a field does not
(#656), so one open store is kept per process for each (address, pins), and its
coordinates and arrays with it. The same function in core opens a store the same
way (``harvest.upstream.open_repository``); tests on each side keep the two in
agreement.

**It reads one whole field, and writes nothing.** The field is decoded by
whatever chain the store names — for a virtual store, ``gribberish`` — and the
values and units are whatever that chain yields. It is georeferenced from the
store's own latitude and longitude, handed to the COG reader as an in-memory
raster, and from there renders exactly as a COG does: the same rescale,
palette and crop. Every upstream read is bounded in time, and a failure is an
error for the request in hand and nothing else.

A step the open store does not hold is an error, never a blank map. Reopening
the store to look again is not done here yet (#662).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as TimedOut
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np

from app.config import STORE_CONNECT_TIMEOUT, STORE_READ_TIMEOUT

logger = logging.getLogger(__name__)

#: The branch a store is read at: its tip, not a snapshot a harvest saw (ADR 0044 §10).
BRANCH = "main"

# ---------------------------------------------------------------------------
# What a request asks for
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoreSlice:
    """One field of an Upstream store: what a Store asset at one timestep names.

    Handed to the reader in place of a COG's URL. ``chunk_locations`` are the
    feed's Pinned locations for the store's data, each prefix as the store
    declares it with its region (empty where it has none), sorted, so that two
    requests naming the same pins share one open store.
    """

    store: str
    array: str
    chunk_locations: tuple[tuple[str, str], ...]
    time: datetime
    reftime: datetime | None = None

    @classmethod
    def published(cls, entry: dict, time: datetime, reftime: datetime | None) -> StoreSlice:
        """The slice a published entry names at this timestep. Raises
        :class:`StoreMisdescribed` for an entry that is not Django's shape."""
        store, array, places = entry.get("store"), entry.get("array"), entry.get("chunk_locations") or {}
        if not isinstance(store, str) or not isinstance(array, str) or not array or not isinstance(places, dict):
            raise StoreMisdescribed("The published description of this upstream store is incomplete.")
        if not store.startswith(("https://", "http://")):
            raise StoreMisdescribed("The upstream store's address is not an http(s) URL.")
        return cls(
            store=store,
            array=array,
            chunk_locations=tuple(sorted((str(prefix), str(region or "")) for prefix, region in places.items())),
            time=time,
            reftime=reftime,
        )

    def __str__(self) -> str:
        at = self.time.isoformat() if self.reftime is None else f"{self.reftime.isoformat()}+{self.time.isoformat()}"
        return f"{self.store}#{self.array}@{at}"


# ---------------------------------------------------------------------------
# What goes wrong, in words a map user can be shown
# ---------------------------------------------------------------------------


class StoreError(Exception):
    """A store read that failed. ``status_code`` and ``detail`` are the response."""

    status_code = 502

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class StoreUnreachable(StoreError):
    """The store, or the place its data lives, did not answer in time or at all."""

    status_code = 504


class StoreMisdescribed(StoreError):
    """The store is not laid out as a map-shaped store is, or was published wrong."""

    status_code = 502


class StoreStepMissing(StoreError):
    """The store does not hold this time step. "time" is in every detail, so the
    WMTS shim blames TIME as it does for a COG that is not there."""

    status_code = 404


class StoreDataNotPinned(StoreError):
    """The field's data is in a place the feed's pins do not include.

    An operator may have declined a place the sampled run did not need and
    another run does (ADR 0044 §11). The cure is to re-check the Upstream, so
    that is what the response says.
    """

    status_code = 502


NOT_PINNED = (
    "This map's data is kept somewhere this feed was not allowed to read. "
    "Re-check the upstream to confirm where its data lives."
)


# ---------------------------------------------------------------------------
# Where a store's data may be read
# ---------------------------------------------------------------------------

#: An S3 bucket's name, and a Google bucket's (dots and underscores allowed).
#: Spelled to match ``georiva/harvest/asset_hosts.py``.
_BUCKET_NAMES = {
    "s3": re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]"),
    "gs": re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]"),
}

#: How a Google bucket is read: its public endpoint, anonymously. Options only
#: bound how long a read may take; nothing here names an address. A test points
#: this at a stand-in for the provider, which is the one reason it is a name.
_TIMEOUTS = {"timeout": f"{STORE_READ_TIMEOUT}s", "connect_timeout": f"{STORE_CONNECT_TIMEOUT}s"}
GCS_OPTIONS = dict(_TIMEOUTS)


def place_kind(prefix: str) -> str | None:
    """``"s3"`` or ``"gs"`` for a prefix naming a bucket of that kind, or a
    folder of one, ending in a slash; ``None`` for anything else (ADR 0044 §11)."""
    scheme, separator, rest = prefix.partition("://")
    name = _BUCKET_NAMES.get(scheme)
    if not separator or name is None or not name.fullmatch(rest.partition("/")[0]) or not prefix.endswith("/"):
        return None
    if any(char.isspace() or not char.isprintable() or char == "\\" for char in prefix):
        return None
    return scheme


def _container(prefix: str, region: str):
    """The container settings and the credentials one pinned place is read with."""
    import icechunk

    kind = place_kind(prefix)
    if kind == "s3":
        reader = icechunk.s3_store(
            region=region or None, anonymous=True, network_stream_timeout_seconds=int(STORE_READ_TIMEOUT)
        )
        return icechunk.VirtualChunkContainer(prefix, reader), icechunk.s3_anonymous_credentials()
    if kind == "gs":
        reader = icechunk.gcs_store(dict(GCS_OPTIONS))
        return icechunk.VirtualChunkContainer(prefix, reader), icechunk.gcs_credentials(anonymous=True)
    # Django publishes no other kind. Meeting one means something else wrote
    # the entry, and a reader that guessed would be following it.
    raise StoreMisdescribed("This feed names a place for its data that maps are never read from.")


def storage(href: str):
    """The repository at ``href``, read over HTTP with every request bounded."""
    import icechunk

    options = dict(_TIMEOUTS)
    if href.startswith("http://"):
        options["allow_http"] = "true"
    return icechunk.http_storage(href, opts=options)


def open_repository(repository_storage, chunk_locations: tuple[tuple[str, str], ...]):
    """Open the repository in ``repository_storage``, following its data only
    into ``chunk_locations``.

    The container configuration inside the repository is discarded and one is
    built from the pins alone — anonymous, the provider's public endpoint, those
    prefixes only — whatever the repository says of where its chunks live or
    how to reach them. Everything else it says of itself is kept: it names no
    address, and a large store's own caching is what makes it quick to open.
    """
    import icechunk

    # Opened first with nowhere authorised: that reads its settings and none of its data.
    repository = icechunk.Repository.open(repository_storage)
    config = repository.config
    config.clear_virtual_chunk_containers()
    credentials = {}
    for prefix, region in chunk_locations:
        container, credential = _container(prefix, region)
        config.set_virtual_chunk_container(container)
        credentials[prefix] = credential
    return repository.reopen(
        config=config,
        authorize_virtual_chunk_access=icechunk.containers_credentials(credentials) if credentials else None,
    )


# ---------------------------------------------------------------------------
# A store kept open
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Field:
    """One decoded field, rows north to south, on a regular latitude/longitude grid."""

    values: np.ndarray
    west: float
    north: float
    x_resolution: float
    y_resolution: float


class OpenStore:
    """A store opened read-only at the tip of its branch, with what has been
    learnt of it: arrays opened, coordinates read. Both are kept because
    opening an array was measured at seconds and coordinates are small.

    Coordinates are read once per open store. A store's coordinates only grow,
    and at the end, so a run or step found here stays where it was found.
    """

    def __init__(self, store):
        self.store = store
        self._arrays: dict = {}
        self._coordinates: dict = {}
        self._lock = threading.Lock()

    def _array(self, path: str):
        import zarr

        with self._lock:
            if path not in self._arrays:
                self._arrays[path] = zarr.open_array(self.store, path=path, mode="r")
            return self._arrays[path]

    def _coordinate(self, path: str, name: str) -> np.ndarray:
        """The values a dimension of the array at ``path`` is measured on. It sits
        beside the array or at the store's root, under the dimension's own name."""
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
            values = _decoded(np.asarray(coordinate[:]), coordinate.attrs)
            if values.ndim != 1:
                raise StoreMisdescribed(f"The upstream store's “{name}” is not a list of values.")
            with self._lock:
                self._coordinates[at] = values
            return values
        raise StoreMisdescribed(f"The upstream store has no “{name}” to place its maps by.")

    def field(self, path: str, time: datetime, reftime: datetime | None) -> Field:
        array = self._array(path)
        names = getattr(array.metadata, "dimension_names", None)
        if array.ndim not in (3, 4) or not names or len(names) != array.ndim:
            raise StoreMisdescribed("This upstream array is not one map per time step.")

        measured = [self._coordinate(path, str(name)) for name in names]
        axes = _axes([str(name) for name in names], measured)
        at = _where(dict(zip(axes, measured, strict=True)), time, reftime)
        for part in at:
            if array.chunks[axes.index(part)] != 1:
                raise StoreMisdescribed("This upstream store does not keep one map per time step.")

        cell = tuple(at.get(part, 0) for part in axes)
        if not self._held(f"{path}/{array.metadata.encode_chunk_key(cell)}"):
            raise StoreStepMissing("This time step is not in the upstream store yet.")

        values = np.asarray(array[tuple(at.get(part, slice(None)) for part in axes)])
        if axes.index("x") < axes.index("y"):
            values = values.T
        y, x = measured[axes.index("y")], measured[axes.index("x")]
        return _georeferenced(values, y.astype("f8"), x.astype("f8"))

    def _held(self, key: str) -> bool:
        """Whether the store holds data at a chunk key: asked by key, nothing decoded."""
        return bool(asyncio.run(self.store.exists(key)))


def _where(coordinates: dict[str, np.ndarray], time: datetime, reftime: datetime | None) -> dict[str, int]:
    """The index of the requested run and step, or time, on each time axis.

    Selected by value: the run is the request's reftime and the step its time
    less its reftime, found among the store's own coordinates.
    """
    if "time" in coordinates:
        return {"time": _index(coordinates["time"], np.datetime64(_naive(time), "us"))}
    if reftime is None:
        raise StoreStepMissing("A forecast time step needs its reftime.")
    run = _index(coordinates["run"], np.datetime64(_naive(reftime), "us"))
    step = _index(coordinates["step"], np.timedelta64(time - reftime, "us"))
    return {"run": run, "step": step}


def _axes(names: list[str], measured: list[np.ndarray]) -> list[str]:
    """Which part each dimension is — "run", "step", "time", "y", "x" — from
    the coordinates it is measured on and, for the grid, its name."""
    axes = []
    for name, values in zip(names, measured, strict=True):
        lowered = name.lower()
        if values.dtype.kind == "M":
            axes.append("time")
        elif values.dtype.kind == "m":
            axes.append("step")
        elif lowered in ("lat", "latitude", "y"):
            axes.append("y")
        elif lowered in ("lon", "longitude", "x"):
            axes.append("x")
        else:
            raise StoreMisdescribed(f"This upstream array is divided up in a way maps are not drawn from (“{name}”).")
    if "step" in axes and axes.count("time") == 1:
        axes[axes.index("time")] = "run"
    if sorted(axes) not in (sorted(["run", "step", "y", "x"]), sorted(["time", "y", "x"])):
        raise StoreMisdescribed("This upstream array is not one map per time step on a latitude/longitude grid.")
    return axes


def _index(values: np.ndarray, wanted) -> int:
    found = np.flatnonzero(values == wanted)
    if not len(found):
        raise StoreStepMissing("This time step is not in the upstream store yet.")
    return int(found[0])


def _naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _decoded(values: np.ndarray, attrs) -> np.ndarray:
    """A coordinate's values, with times as numpy dates or lengths of time.

    As xarray writes times: whole numbers, with what they count in their
    attributes — the missing ones as the smallest int64 or a declared fill.
    """
    if values.dtype.kind in "Mm" or values.dtype.kind not in "iuf":
        return values
    units = str(attrs.get("units") or "")
    if " since " not in units and units not in _TIMEDELTA_UNITS:
        return values
    from xarray.coding.times import decode_cf_datetime, decode_cf_timedelta

    numbers = values
    if values.dtype.kind in "iu":
        missing = values == np.iinfo(np.int64).min if values.dtype.kind == "i" else np.zeros(values.shape, bool)
        fill = attrs.get("_FillValue")
        if fill is not None:
            missing |= values == fill
        numbers = np.where(missing, np.nan, values.astype("f8")) if missing.any() else values
    if " since " in units:
        return np.asarray(decode_cf_datetime(numbers, units, attrs.get("calendar")))
    return np.asarray(decode_cf_timedelta(numbers, units))


_TIMEDELTA_UNITS = frozenset({"days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"})


def _georeferenced(values: np.ndarray, latitudes: np.ndarray, longitudes: np.ndarray) -> Field:
    """``values`` placed on the grid its coordinates describe, north-up.

    Coordinates are cell centres on a regular grid. A store decoded north-up
    (``gribberish``'s ``north_up``) needs no flip; one written south-up is
    flipped here, so the raster is north-up whichever it is.
    """
    if values.shape != (len(latitudes), len(longitudes)) or min(values.shape) < 2:
        raise StoreMisdescribed("This upstream map does not match its own latitudes and longitudes.")
    if latitudes[0] < latitudes[-1]:
        values, latitudes = values[::-1], latitudes[::-1]
    x_resolution = (longitudes[-1] - longitudes[0]) / (len(longitudes) - 1)
    y_resolution = (latitudes[0] - latitudes[-1]) / (len(latitudes) - 1)
    regular = np.allclose(np.diff(longitudes), x_resolution, rtol=1e-4) and np.allclose(
        -np.diff(latitudes), y_resolution, rtol=1e-4
    )
    if x_resolution <= 0 or y_resolution <= 0 or not regular:
        raise StoreMisdescribed("This upstream map is not on a regular latitude/longitude grid.")
    return Field(
        values=values,
        west=float(longitudes[0] - x_resolution / 2),
        north=float(latitudes[0] + y_resolution / 2),
        x_resolution=float(x_resolution),
        y_resolution=float(y_resolution),
    )


# ---------------------------------------------------------------------------
# One open store per process, per (address, pins)
# ---------------------------------------------------------------------------


@dataclass
class _Opened:
    stores: dict = field(default_factory=dict)
    locks: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


_opened = _Opened()


def opened_store(href: str, chunk_locations: tuple[tuple[str, str], ...]) -> OpenStore:
    """The store at ``href`` read with these pins, opened once per process.

    Concurrent first requests wait on one open rather than each paying for
    their own. An open that fails leaves nothing behind, so the next request
    tries again. Unbounded itself: :func:`read_field` bounds it with the read.
    """
    key = (href, chunk_locations)
    with _opened.lock:
        if key in _opened.stores:
            return _opened.stores[key]
        lock = _opened.locks.setdefault(key, threading.Lock())
    with lock:
        with _opened.lock:
            if key in _opened.stores:
                return _opened.stores[key]
        logger.info("Opening upstream store %s", href)
        opened = OpenStore(open_repository(storage(href), chunk_locations).readonly_session(BRANCH).store)
        with _opened.lock:
            _opened.stores[key] = opened
        return opened


def forget_all() -> None:
    """Drop every open store. For tests, which open a new local store each."""
    with _opened.lock:
        _opened.stores.clear()
        _opened.locks.clear()


def read_field(target: StoreSlice) -> Field:
    """Decode the field ``target`` names: opening the store if this process
    has not yet, and reading the field, within one ``STORE_READ_TIMEOUT``."""
    return _bounded(
        lambda: opened_store(target.store, target.chunk_locations).field(target.array, target.time, target.reftime)
    )


#: How icechunk says a chunk lies outside every container the reader was given,
#: or in one it was not authorised for. Other failures that mention virtual
#: chunks — a truncated or rewritten object in a pinned place — are not this.
_NOT_FOLLOWED = (
    "resolves to the url prefix",
    "no virtual chunk container can handle",
    "no matching virtual chunk container",
    "authorize the virtual chunk container",
)


def _bounded(call: Callable):
    """``call()``, in a thread of its own, given ``STORE_READ_TIMEOUT`` seconds.

    The library's own timeouts bound each request it makes; this bounds them all
    together, so a store that answers slowly but steadily cannot hold the tile
    longer either. Whatever the library raises becomes a :class:`StoreError`.
    A call that overruns is abandoned, not stopped: its thread ends when the
    library's own timeouts end it.
    """
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="store-read")
    try:
        return pool.submit(call).result(timeout=STORE_READ_TIMEOUT)
    except TimedOut as late:
        raise StoreUnreachable(f"The upstream store did not answer within {int(STORE_READ_TIMEOUT)} seconds.") from late
    except StoreError:
        raise
    except Exception as exc:
        said = str(exc)
        if any(phrase in said for phrase in _NOT_FOLLOWED):
            logger.warning("Upstream store data outside the pinned places: %s", said.splitlines()[0])
            raise StoreDataNotPinned(NOT_PINNED) from exc
        logger.warning("Upstream store read failed: %s", said.splitlines()[0] if said else type(exc).__name__)
        raise StoreUnreachable("The upstream store could not be opened or read.") from exc
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------------------
# What the COG reader is handed
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def open_field(target: StoreSlice) -> Iterator:
    """The field ``target`` names, as a rasterio dataset in memory: one band,
    EPSG:4326, NaN as nodata, the values exactly as decoded."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    found = read_field(target)
    height, width = found.values.shape
    dtype = found.values.dtype if found.values.dtype.kind == "f" else np.dtype("f8")
    with MemoryFile() as memory:
        with memory.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype=dtype,
            crs="EPSG:4326",
            nodata=np.nan,
            transform=from_origin(found.west, found.north, found.x_resolution, found.y_resolution),
        ) as sink:
            sink.write(found.values.astype(dtype, copy=False), 1)
        with memory.open() as dataset:
            yield dataset
