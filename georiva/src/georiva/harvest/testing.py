"""An Upstream that lives in memory: the one fake a harvest test needs (ADR 0042 §13)."""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .upstream import (
    STORE_TIMEOUT,
    AssetProbe,
    CubeArray,
    CubeDimension,
    FieldSample,
    HeldStep,
    StoreChunking,
    StoreField,
    StoreFormat,
    StoreProbe,
    UpstreamAsset,
    UpstreamCollection,
    UpstreamCube,
    UpstreamItem,
    UpstreamRoot,
    UpstreamTimedOut,
    UpstreamUnreachable,
    Window,
    in_window,
)

#: What every host resolves to unless a test says otherwise: a public address.
PUBLIC_ADDRESS = "93.184.216.34"


#: The codecs a store's arrays name unless a test says otherwise: ones every install decodes.
PLAIN_CODECS = ("bytes", "zstd")


def _as_tuple(chunks) -> tuple[int, ...] | None:
    return None if chunks is None else tuple(chunks)


def cube_dimension(name: str, size: int | None) -> CubeDimension:
    """A cube dimension as a publisher would describe one of that name."""
    if name in ("latitude", "y"):
        return CubeDimension(name, kind="spatial", axis="y", size=size)
    if name in ("longitude", "x"):
        return CubeDimension(name, kind="spatial", axis="x", size=size)
    return CubeDimension(name, kind="temporal" if name.endswith("time") else "other", size=size)


class FakeUpstream:
    """An Upstream catalog client over canned collections and items.

    It can also stand for an Upstream store (`become_store`): a collection with
    no items, whose one asset is a store with arrays, dimensions, chunking,
    codecs and declared chunk locations.

    Honest by default: it answers `list_items` with the window it was asked
    for. Set `ignores_window` for an Upstream that returns everything it has,
    `unreachable` to a message for one that cannot be read at all,
    `times_out` to a message for one that does not answer in time, and
    `collections_unreachable` for one that lists its items and not its collections.
    """

    def __init__(self, collection_id: str = "chirps-daily", *, title: str = "", description: str = ""):
        self.collection_id = collection_id
        self.collections = [UpstreamCollection(id=collection_id, title=title, description=description)]
        self.items: list[UpstreamItem] = []
        self.addresses: dict[str, list[str]] = {}
        self.unreachable: str | None = None
        #: A message for an Upstream that does not answer in the time it is given.
        self.times_out: str | None = None
        #: How many times its root was read: every survey starts there.
        self.roots_read = 0
        #: A message for an Upstream whose collections cannot be read while its items can.
        self.collections_unreachable: str | None = None
        self.ignores_window = False
        #: How many items a listing yields before the Upstream stops answering.
        self.fails_after: int | None = None
        #: The windows `list_items` was asked for, in order.
        self.windows_asked: list[Window] = []
        #: The hosts `resolve_host` was asked about, in order.
        self.hosts_resolved: list[str] = []

        # What only the Upstream survey asks.
        #: False for a catalog of plain files, which cannot be asked for items by time.
        self.is_api = True
        #: True when the address given is one collection's own document, not a catalog's root.
        self.is_collection_document = False
        #: The region of each public bucket; one not here has none that can be found.
        self.regions: dict[str, str] = {}
        self.regions_asked: list[str] = []
        #: What opening a file shows, by href; one not here is a sound COG ranging 0–100.
        self.probes: dict[str, AssetProbe] = {}
        #: A message for an Upstream none of whose files can be read.
        self.unreadable: str | None = None
        #: The files opened, in order, each with whether a value range was asked for.
        self.probed: list[tuple[str, bool]] = []

        # What only a materialization asks: a file's whole bytes.
        #: The pixels of each file, by href — a grid, or bands of grids; one not here is an 8×8 grid of
        #: 7.0 over the items' usual box.
        self.rasters: dict = {}
        #: The nodata value each file declares, by href — None for none; one not here declares -9999.
        self.nodata: dict = {}
        #: Where each href redirects to, when it does.
        self.redirects: dict[str, str] = {}
        #: Why a file cannot be had, by href.
        self.unfetchable: dict[str, str] = {}
        #: The hrefs whose bytes are not a raster at all.
        self.garbage: set[str] = set()
        #: The addresses whose bytes were copied, in order: an href, or where it redirected to.
        self.fetched: list[str] = []
        #: The exact bytes of each file, by href, where a test gives them rather than pixels.
        self.files: dict[str, bytes] = {}

        # What only an Upstream store answers.
        #: The codecs each store's arrays name, by the store's address and the array's path.
        self.stores: dict[str, dict[str, tuple[str, ...]]] = {}
        #: How each store's arrays are divided up, as the store itself says, by its address and the array's path.
        self.store_chunking: dict[str, dict[str, StoreChunking]] = {}
        #: The Store format of each store, by its address: an Icechunk repository unless a test says otherwise.
        self.store_formats: dict[str, StoreFormat] = {}
        #: A message for a store that cannot be opened.
        self.store_unreadable: str | None = None
        #: What the store's library says, word for word, when it does not open or answer.
        self.store_detail: str = ""
        #: True for a store that does not answer in the time it is given.
        self.store_times_out = False
        #: The stores opened, in order.
        self.stores_opened: list[str] = []
        #: The range a sample of each array shows, by path; one not here ranges 0–100.
        self.field_ranges: dict[str, tuple[float, float]] = {}
        #: The declared place each array's data is in, by path; one not here needs none.
        self.field_places: dict[str, str] = {}
        #: The steps, by index, a sample of each forecast array finds its run holding; one not here says none.
        self.field_steps: dict[str, tuple[int, ...]] = {}
        #: What each field's sample says it declares of its time statistic (ADR 0051), by path.
        self.field_declared: dict = {}
        #: Why a sample of an array cannot be read, by path.
        self.field_problems: dict[str, str] = {}
        #: The samples asked for, in order: the store, the arrays, and the places the read may follow.
        self.samples_asked: list[tuple[str, list[StoreField], dict[str, str]]] = []
        #: What the store holds: by array path, each `(run, valid time)` — `run` is `None` where there are no runs.
        self.held: dict[str, set[tuple[datetime | None, datetime]]] = {}
        #: What each array declares of its time statistic (ADR 0051), by path.
        self.declared: dict = {}
        #: A message for a store that describes itself and cannot be listed.
        self.store_unreachable: str | None = None
        #: The listings of Held steps asked for, in order: the store, the arrays, and the window.
        self.steps_asked: list[tuple[str, list[str], Window]] = []
        #: The runs each listing looked into, in order: those in its window it was not told to skip.
        self.runs_asked: list[set[datetime]] = []
        #: How many steps the store expects of a run, by run; one not here expects `steps_per_run`.
        self.expected_steps: dict[datetime, int] = {}
        #: How far apart a run's steps are: its lead times are 0, this, twice this, and so on.
        self.lead_interval = timedelta(hours=3)
        #: How many steps the store expects of a run it says nothing more of: its step
        #: dimension's size, when `become_store` is given one named `lead_time`.
        self.steps_per_run: int | None = None

    def become_store(
        self,
        href: str,
        dimensions: dict[str, int | None | CubeDimension],
        arrays: dict[str, tuple],
        *,
        codecs: dict[str, tuple[str, ...]] | None = None,
        chunk_locations: tuple[str, ...] = (),
        chunk_endpoints: tuple[str, ...] = (),
        s3_href: str = "",
        media_type: str = "application/x-icechunk",
        described: bool = True,
        published_chunks: bool = True,
        published: dict[str, tuple[int, ...]] | None = None,
        store_format: StoreFormat = StoreFormat.ICECHUNK,
    ) -> None:
        """Make the collection an Upstream store at `href`.

        `dimensions` gives each dimension's size (or a whole `CubeDimension`).
        `arrays` gives each array, by path, as `(dimensions, chunks)`; `chunks`
        may be `None` for one the collection does not describe. A third member,
        a dict, is what the collection says of it: `title`, `unit`,
        `description`. `codecs` names
        the codecs of any array that does not use `PLAIN_CODECS`. `s3_href` is
        the same store's `s3://` address, when the collection publishes both;
        `chunk_endpoints` are the declared places said to be served at an
        address of their own;
        `described=False` leaves the cube out of the collection.

        The store itself says how each array is divided up: `chunks`, over
        `dimensions`' sizes. The collection says the same unless told otherwise:
        `published_chunks=False` leaves the chunking out of its cube, as most
        publishers do, and `published` gives, by path, chunking it says instead
        of the store's. `store_format=StoreFormat.ZARR` makes it a plain Zarr store.
        """
        asset = UpstreamAsset(
            href=href,
            media_type=media_type,
            chunk_locations=tuple(chunk_locations),
            chunk_endpoints=tuple(chunk_endpoints),
        )
        assets = {"icechunk-https": asset}
        if s3_href:
            # First, as dynamical.org lists it: the survey must still choose the `https` one.
            assets = {"icechunk": replace(asset, href=s3_href), **assets}
        cube = UpstreamCube(
            dimensions={
                name: size if isinstance(size, CubeDimension) else cube_dimension(name, size)
                for name, size in dimensions.items()
            },
            arrays={
                path: CubeArray(
                    path,
                    tuple(named),
                    _as_tuple((published or {}).get(path, chunks) if published_chunks else None),
                    **dict(*said),
                )
                for path, (named, chunks, *said) in arrays.items()
            },
        )
        self.collections = [
            replace(c, assets=assets, cube=cube if described else None) if c.id == self.collection_id else c
            for c in self.collections
        ]
        self.items = []
        self.stores[href] = {path: (codecs or {}).get(path, PLAIN_CODECS) for path in arrays}
        self.store_formats[href] = store_format
        sizes = {name: size.size if isinstance(size, CubeDimension) else size for name, size in dimensions.items()}
        self.store_chunking[href] = {
            path: StoreChunking(
                tuple(named),
                tuple(sizes.get(name) or span for name, span in zip(named, chunks, strict=True)),
                tuple(chunks),
            )
            for path, (named, chunks, *_said) in arrays.items()
            if chunks is not None and len(chunks) == len(named)
        }
        lead = dimensions.get("lead_time")
        self.steps_per_run = lead.size if isinstance(lead, CubeDimension) else lead

    def add_item(self, when: datetime, assets: dict[str, UpstreamAsset | str], **fields) -> UpstreamItem:
        """Add an item at `when`. An asset given as a string is its href."""
        item = UpstreamItem(
            id=fields.pop("id", None) or f"item-{when:%Y%m%dT%H%M%S}",
            datetime=when,
            bbox=fields.pop("bbox", [33.0, -5.0, 42.0, 5.0]),
            assets={
                key: asset if isinstance(asset, UpstreamAsset) else UpstreamAsset(href=asset)
                for key, asset in assets.items()
            },
            **fields,
        )
        self.items.append(item)
        return item

    def hold(self, path: str, when: datetime, *, run: datetime | None = None) -> None:
        """The store holds data for the array at `path` at valid time `when`, of `run` where it has runs."""
        self.held.setdefault(path, set()).add((run, when))

    def declare(self, path: str, declared) -> None:
        """The array at `path` declares this time statistic, as its `step_type` would (ADR 0051)."""
        self.declared[path] = declared

    def release(self, path: str, when: datetime, *, run: datetime | None = None) -> None:
        """The store no longer holds that step of that array."""
        self.held.get(path, set()).discard((run, when))

    def drop_item(self, when: datetime) -> None:
        """The Upstream no longer has an item at `when`."""
        self.items = [item for item in self.items if item.datetime != when]

    def _answer(self):
        if self.times_out:
            raise UpstreamTimedOut(self.times_out)
        if self.unreachable:
            raise UpstreamUnreachable(self.unreachable)

    def list_collections(self):
        self._answer()
        if self.collections_unreachable:
            raise UpstreamUnreachable(self.collections_unreachable)
        return list(self.collections)

    def list_items(self, collection_id: str, window: Window):
        self._answer()
        self.windows_asked.append(window)
        if collection_id != self.collection_id:
            return []
        if self.ignores_window:
            found = list(self.items)
        else:
            # Asked for no window, it gives all it has, an item with no usable time included.
            found = [
                item
                for item in self.items
                if window == (None, None) or (item.datetime is not None and in_window(item.datetime, window))
            ]
        return found if self.fails_after is None else self._break_after(found, self.fails_after)

    def list_held_steps(
        self,
        href: str,
        fields: list[StoreField],
        window: Window,
        *,
        skip_runs: frozenset = frozenset(),
        allow_http: bool = False,
    ) -> list[HeldStep]:
        self._answer()
        if href.lower().startswith("http://") and not allow_http:
            # As the real one: a plain http store is opened only where the feed allows it.
            raise UpstreamUnreachable("upstream store is at a plain http address its feed does not allow")
        self.steps_asked.append((href, [field.path for field in fields], window))
        if self.store_unreachable:
            raise UpstreamUnreachable(self.store_unreachable)
        if href not in self.stores:
            raise UpstreamUnreachable("there is no store at that address")
        steps: dict[tuple[datetime | None, datetime], set[str]] = {}
        looked_into = set()
        for field in fields:
            for run, when in self.held.get(field.path, ()):
                if run in skip_runs:
                    continue
                # On run time where there are runs, as the real one: a run inside keeps every step.
                if self.ignores_window or in_window(run or when, window):
                    steps.setdefault((run, when), set()).add(field.path)
                    if run is not None:
                        looked_into.add(run)
        self.runs_asked.append(looked_into)
        return [
            HeldStep(
                time=when,
                reference_time=run,
                arrays=frozenset(paths),
                run_steps=self._run_steps(run),
                array_steps=self._array_steps(run, paths),
                declared={path: self.declared[path] for path in paths if path in self.declared},
            )
            for (run, when), paths in sorted(steps.items(), key=lambda step: (step[0][0] or step[0][1], step[0][1]))
        ]

    def _run_steps(self, run: datetime | None) -> int | None:
        if run is None:
            return None
        return self.expected_steps.get(run, self.steps_per_run)

    def _array_steps(self, run: datetime | None, paths) -> dict[str, frozenset[timedelta]]:
        """As the real store answers: an array is published at its run's
        expected lead times, whichever it holds past them (#672); which of them
        its Variable is expected at is the Variable's to say (ADR 0051)."""
        steps = self._run_steps(run)
        if steps is None:
            return {}
        leads = frozenset(n * self.lead_interval for n in range(steps))
        return dict.fromkeys(paths, leads)

    @staticmethod
    def _break_after(items, count):
        yield from items[:count]
        raise UpstreamUnreachable("upstream catalog unreachable: connection reset mid-listing")

    def resolve_host(self, host: str) -> list[str]:
        self.hosts_resolved.append(host)
        return self.addresses.get(host, [PUBLIC_ADDRESS])

    def fetch_asset(self, href: str, dest, *, addresses) -> int:
        url = href
        if not addresses(url):
            raise UpstreamUnreachable(f"upstream file is at an address not allowed: {url}")
        while url in self.redirects:
            url = self.redirects[url]
            if not addresses(url):
                raise UpstreamUnreachable(f"upstream file is at an address not allowed: {url}")
        if href in self.unfetchable:
            raise UpstreamUnreachable(self.unfetchable[href])
        self.fetched.append(url)
        if href in self.garbage:
            with open(dest, "wb") as out:
                out.write(b"<html>not found</html>")
            return 22
        return self._write_raster(href, dest)

    def _write_raster(self, href: str, dest) -> int:
        """Write the file at `href` to `dest`: its exact bytes when a test gave them, else its pixels as a GTiff."""

        import numpy
        import rasterio
        from rasterio.transform import from_bounds

        if href in self.files:
            Path(dest).write_bytes(self.files[href])
            return len(self.files[href])
        data = self.rasters.get(href)
        if data is None:
            data = numpy.full((8, 8), 7.0, dtype="float32")
        bands = data if data.ndim == 3 else data[numpy.newaxis]
        count, height, width = bands.shape
        with rasterio.open(
            dest,
            "w",
            driver="GTiff",
            width=width,
            height=height,
            count=count,
            dtype=str(data.dtype),
            crs="EPSG:4326",
            nodata=self.nodata.get(href, -9999.0),
            transform=from_bounds(33.0, -5.0, 42.0, 5.0, width, height),
        ) as out:
            out.write(bands)
        return Path(dest).stat().st_size

    @contextmanager
    def serving(self):
        """Serve files over HTTP with range requests, as an Upstream's host would.

        Yields a :class:`ServedFiles`: `put` a file's bytes at a path and it
        answers the file's href. The server runs in its own process: GDAL holds
        the GIL while it reads, so a server thread in the reader's process would
        never answer.
        """
        import subprocess
        import sys
        import tempfile

        with tempfile.TemporaryDirectory(prefix="georiva-upstream-") as root:
            process = subprocess.Popen(
                [sys.executable, "-c", _RANGE_SERVER, root],
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                port = int(process.stdout.readline())
                yield ServedFiles(self, Path(root), f"http://127.0.0.1:{port}")
            finally:
                process.terminate()
                process.wait(timeout=10)
                process.stdout.close()

    def describe_root(self) -> UpstreamRoot:
        self.roots_read += 1
        self._answer()
        return UpstreamRoot(
            is_api=self.is_api,
            children=[UpstreamCollection(id=c.id) for c in self.collections],
            own=self.collections[0] if self.is_collection_document else None,
        )

    def describe_collection(self, collection_id: str) -> UpstreamCollection | None:
        self._answer()
        if self.collections_unreachable:
            return None
        return next((c for c in self.collections if c.id == collection_id), None)

    def bucket_region(self, bucket: str) -> str | None:
        self.regions_asked.append(bucket)
        return self.regions.get(bucket)

    def probe_asset(self, href: str, *, estimate_range: bool) -> AssetProbe:
        self.probed.append((href, estimate_range))
        if self.unreadable:
            return AssetProbe(readable=False, problem=self.unreadable)
        probe = self.probes.get(href) or AssetProbe(
            readable=True, tiled=True, overviews=True, crs="EPSG:4326", value_min=0.0, value_max=100.0
        )
        return probe if estimate_range else replace(probe, value_min=None, value_max=None)

    def describe_store(self, href: str) -> StoreProbe:
        self.stores_opened.append(href)
        if self.store_times_out:
            problem = f"it did not answer within {STORE_TIMEOUT} seconds"
            return StoreProbe(readable=False, problem=problem, timed_out=True, detail=self.store_detail)
        if self.store_unreadable:
            return StoreProbe(readable=False, problem=self.store_unreadable, detail=self.store_detail)
        if href not in self.stores:
            return StoreProbe(readable=False, problem="there is no store at that address")
        return StoreProbe(
            readable=True,
            codecs=dict(self.stores[href]),
            chunking=dict(self.store_chunking.get(href, {})),
            store_format=self.store_formats.get(href, StoreFormat.ICECHUNK),
        )

    def sample_store(
        self, href: str, fields: list[StoreField], chunk_locations: dict[str, str]
    ) -> dict[str, FieldSample]:
        self.samples_asked.append((href, list(fields), dict(chunk_locations)))
        samples = {}
        for field in fields:
            place = self.field_places.get(field.path)
            if field.path in self.field_problems:
                samples[field.path] = FieldSample(readable=False, problem=self.field_problems[field.path])
            elif place and place not in chunk_locations:
                # As the real reader finds it: by being refused the address of one file there.
                problem = "it could not be read by a reader with no account"
                samples[field.path] = FieldSample(readable=False, problem=problem, elsewhere=f"{place}a/file.grib2")
            else:
                low, high = self.field_ranges.get(field.path, (0.0, 100.0))
                steps = self.field_steps.get(field.path, ())
                samples[field.path] = FieldSample(
                    readable=True,
                    value_min=low,
                    value_max=high,
                    held_steps=steps,
                    declared=self.field_declared.get(field.path),
                )
        return samples


class ServedFiles:
    """The files `FakeUpstream.serving` serves, and the ranges it was asked for."""

    def __init__(self, upstream: FakeUpstream, root: Path, base: str):
        self.upstream, self.root, self.base = upstream, root, base

    def put(self, path: str, body: bytes) -> str:
        """Serve `body` at `path`; answers its href, at which the Upstream holds the same bytes."""
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        href = f"{self.base}/{path}"
        self.upstream.files[href] = body
        return href

    def ranges(self, href: str) -> list[tuple[int, int]]:
        """The byte ranges asked of `href`, in order, each as `(first, last)`."""
        log = self.root / ".ranges"
        if not log.exists():
            return []
        path = "/" + href.removeprefix(self.base).lstrip("/")
        found = []
        for line in log.read_text().splitlines():
            asked, first, last = line.split(" ")
            if asked == path:
                found.append((int(first), int(last)))
        return found


#: A file server answering HEAD and ranged GETs from the folder it is given; it
#: prints its port, and logs each range asked to `.ranges` in that folder.
_RANGE_SERVER = """
import os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

root = sys.argv[1]

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def answer(self, with_body):
        path = os.path.join(root, self.path.lstrip("/"))
        if self.path.startswith("/.") or not os.path.isfile(path):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with open(path, "rb") as f:
            body = f.read()
        first, last, status = 0, len(body) - 1, 200
        asked = self.headers.get("Range")
        if asked and asked.startswith("bytes="):
            a, _, b = asked[len("bytes="):].partition("-")
            first, last, status = int(a), min(int(b), len(body) - 1) if b else len(body) - 1, 206
            with open(os.path.join(root, ".ranges"), "a") as log:
                log.write(f"{self.path} {first} {last}\\n")
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(last - first + 1))
        if status == 206:
            self.send_header("Content-Range", f"bytes {first}-{last}/{len(body)}")
        self.end_headers()
        if with_body:
            self.wfile.write(body[first:last + 1])

    def do_HEAD(self):
        self.answer(False)

    def do_GET(self):
        self.answer(True)

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
"""


# ---- a real Upstream store, on local disk ---------------------------------------------

#: Where a :class:`LocalStore` says it lives unless a test says otherwise.
LOCAL_STORE_HREF = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"


def ramp(run: int, step: int, shape: tuple[int, int] = (4, 8)):
    """A field of run `run` and step `step`, row 0 north: every cell a different value."""
    import numpy as np

    return (100.0 * run + 10.0 * step + np.arange(float(shape[0] * shape[1])).reshape(shape)).astype(np.float32)


@dataclass(frozen=True)
class StoreArray:
    """One array of a :class:`LocalStore`, by its path in the store.

    `values(run, step)` is its field at that run and step (indexes into the
    store's runs and leads), north-up. `held(run, step)` says whether the store
    holds that step at all: a step not held has no chunk written, as an
    Upstream store that has not received it yet, or never will (precipitation
    at step 0). Every step is held unless a test says otherwise.
    """

    path: str
    values: Callable | None = None
    held: Callable[[int, int], bool] | None = None

    def holds(self, run: int, step: int) -> bool:
        return self.held is None or bool(self.held(run, step))


@dataclass(frozen=True)
class StoreVariable:
    """The Variable an array of a :class:`LocalStore` is harvested into.

    Units are symbols; a unit not yet known is created.
    """

    slug: str
    name: str = ""
    source_unit: str = "K"
    unit: str = ""
    water: bool = False
    #: Its time statistic and window (ADR 0051): a non-instant one gets no row at a run's first step.
    time_statistic: str = "instant"
    statistic_window: str = ""


@dataclass(frozen=True)
class StoreVector:
    """A vector Variable over two arrays of a :class:`LocalStore` (ADR 0048):
    their magnitude or direction, by `transform` (a ``Variable.TransformType``)."""

    slug: str
    u: str
    v: str
    transform: str
    source_unit: str = "m/s"
    unit: str = ""


@dataclass
class HarvestedStore:
    """The rows a store harvest of a :class:`LocalStore` would have made."""

    feed: object
    collection: object
    #: The Variable each harvested array became, by its path in the store.
    variables: dict


class LocalStore:
    """A forecast-shaped Icechunk store on local disk, read wherever its address is opened.

    Its arrays are laid out as an Upstream forecast store's are: a run axis
    (``init_time`` × ``lead_time``) over a latitude–longitude grid, latitudes
    south first, one chunk per run and step. Built for `test` (a
    ``unittest.TestCase``): its folder is removed and the address patched back
    when the test ends. While the test runs, opening a store at `href` through
    ``core.values`` opens this one instead; nothing reaches a network.

    The store is written as it is made; with ``build=False`` it is left empty,
    for a test that changes :attr:`config` first and then calls :meth:`commit`.
    :meth:`harvest` makes the feed, collection, Variables, Items and Store
    assets a store harvest would have made of it.
    """

    def __init__(
        self,
        test,
        *,
        href: str = LOCAL_STORE_HREF,
        runs: list[datetime] | None = None,
        leads: list[timedelta] | None = None,
        latitudes: list[float] | None = None,
        longitudes: list[float] | None = None,
        arrays: list[StoreArray] | None = None,
        build: bool = True,
    ):
        import tempfile
        from unittest import mock

        import icechunk

        self.test = test
        self.href = href
        self.runs = list(runs or [datetime(2026, 3, 22, 12, tzinfo=UTC), datetime(2026, 3, 23, 0, tzinfo=UTC)])
        self.leads = list(leads or [timedelta(hours=0), timedelta(hours=6), timedelta(hours=12)])
        #: Cell centres, south first as many stores keep them; a read is north-up all the same.
        self.latitudes = list(latitudes or [-15.0, -5.0, 5.0, 15.0])
        self.longitudes = list(longitudes or [5.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0, 75.0])
        self.arrays = list(arrays or [StoreArray("temperature_2m")])
        #: Whether the last opening of the store was allowed plain HTTP.
        self.opened_allowing_http: bool | None = None

        folder = tempfile.TemporaryDirectory()
        test.addCleanup(folder.cleanup)
        self.folder = folder.name
        self.storage = icechunk.local_filesystem_storage(os.path.join(self.folder, "repo"))
        self.config = icechunk.RepositoryConfig.default()
        self.repository = None
        opening = mock.patch("georiva.core.values.store.repository_storage", side_effect=self._storage_at)
        opening.start()
        test.addCleanup(opening.stop)
        if build:
            self.commit(self.write_forecast)

    @property
    def shape(self) -> tuple[int, int]:
        return len(self.latitudes), len(self.longitudes)

    @property
    def bounds(self) -> list[float]:
        """The grid's extent, ``[west, south, east, north]``, from its cell centres."""
        dy = abs(self.latitudes[1] - self.latitudes[0]) if len(self.latitudes) > 1 else 1.0
        dx = abs(self.longitudes[1] - self.longitudes[0]) if len(self.longitudes) > 1 else 1.0
        return [
            min(self.longitudes) - dx / 2,
            min(self.latitudes) - dy / 2,
            max(self.longitudes) + dx / 2,
            max(self.latitudes) + dy / 2,
        ]

    def _storage_at(self, href, *, allow_http=False, **_options):
        self.test.assertEqual(href, self.href)
        self.opened_allowing_http = allow_http
        return self.storage

    def commit(self, build) -> None:
        """Write to the store: `build(store)` on a writable session, then committed."""
        import icechunk

        if self.repository is None:
            self.repository = icechunk.Repository.create(self.storage, config=self.config)
            self.repository.save_config()
        session = self.repository.writable_session("main")
        build(session.store)
        session.commit("build")

    def field(self, array: StoreArray, run: int, step: int):
        """The field `array` holds at `run` and `step`, north-up, as written."""
        return (array.values or (lambda r, s: ramp(r, s, self.shape)))(run, step)

    def write_forecast(self, store) -> None:
        """Write the run axis, the grid and every array's held steps into `store`."""
        import numpy as np
        import zarr

        runs, leads = len(self.runs), len(self.leads)
        init_time = zarr.create_array(store, name="init_time", shape=(runs,), dtype="i8", dimension_names=["init_time"])
        init_time[:] = [int(run.timestamp()) for run in self.runs]
        init_time.attrs.update({"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"})
        lead_time = zarr.create_array(
            store, name="lead_time", shape=(leads,), dtype="i8", dimension_names=["lead_time"]
        )
        lead_time[:] = [int(lead.total_seconds()) for lead in self.leads]
        lead_time.attrs.update({"units": "seconds"})
        ny, nx = self.shape
        zarr.create_array(store, name="latitude", shape=(ny,), dtype="f8", dimension_names=["latitude"])[:] = (
            self.latitudes
        )
        zarr.create_array(store, name="longitude", shape=(nx,), dtype="f8", dimension_names=["longitude"])[:] = (
            self.longitudes
        )
        for spec in self.arrays:
            array = zarr.create_array(
                store,
                name=spec.path,
                shape=(runs, leads, ny, nx),
                chunks=(1, 1, ny, nx),
                dtype="f8",
                fill_value=np.nan,
                dimension_names=["init_time", "lead_time", "latitude", "longitude"],
            )
            for run in range(runs):
                for step in range(leads):
                    if spec.holds(run, step):
                        array[run, step] = np.flipud(self.field(spec, run, step))  # south first, as written

    def _array(self, path: str) -> StoreArray:
        for spec in self.arrays:
            if spec.path == path:
                return spec
        raise KeyError(f"The store has no array {path!r}.")

    def at(self, array: str, run: int, step: int, *, places: dict[str, str] | None = None, allow_http=False):
        """The ``values.StoreSlice`` naming `array` at `run` and `step` (indexes), with pinned `places`."""
        from georiva.core.values import StoreSlice

        return StoreSlice(
            store=self.href,
            array=array,
            time=self.runs[run] + self.leads[step],
            reference_time=self.runs[run],
            chunk_locations=places or {},
            allow_http=allow_http,
        )

    def harvest(
        self,
        organisation,
        *,
        variables: dict[str, StoreVariable],
        pins: dict[str, str] | None = None,
        catalog_slug: str = "forecast",
        upstream_collection_id: str = "noaa-gfs-forecast-virtual",
        allow_http: bool = False,
        vectors: list[StoreVector] = (),
    ) -> HarvestedStore:
        """The rows a store harvest would have made of this store, in a new catalog of `organisation`.

        A HarvestFeed at :attr:`href` pinning `pins`; its collection; a Variable
        (and mapping) per array in `variables`; an Item per run and step that
        any of those arrays holds; and on it a Store asset per array holding that step.
        Each of `vectors` is a vector Variable over two of those arrays, with a
        Store asset on every item where both are held.
        """
        from urllib.parse import urlsplit

        from georiva.core.models import Asset, Catalog, Item, Unit, Variable

        from .models import HarvestFeed, UpstreamShape, VariableMapping

        def unit(symbol):
            found = Unit.objects.filter(symbol=symbol).first()
            return found or Unit.objects.create(name=symbol, symbol=symbol)

        catalog = Catalog.objects.create(
            organisation=organisation,
            name=catalog_slug.title(),
            slug=catalog_slug,
            file_format=Catalog.FileFormat.ZARR,
        )
        feed = HarvestFeed.objects.create(
            name=f"{upstream_collection_id} upstream",
            catalog=catalog,
            upstream_url="https://stac.upstream.example/catalog.json",
            upstream_collection_id=upstream_collection_id,
            upstream_shape=UpstreamShape.STORE,
            store_href=self.href,
            asset_hosts=[urlsplit(self.href).hostname],
            pinned_chunk_locations=dict(pins or {}),
            allow_http=allow_http,
        )
        collection = feed.ensure_collection()
        made = {}
        for path, wanted in variables.items():
            self._array(path)
            made[path] = Variable.objects.create(
                collection=collection,
                slug=wanted.slug,
                name=wanted.name or wanted.slug.replace("-", " ").title(),
                source_unit=unit(wanted.source_unit),
                unit=unit(wanted.unit or wanted.source_unit),
                water=wanted.water,
                time_statistic=wanted.time_statistic,
                statistic_window=wanted.statistic_window,
            )
            VariableMapping.objects.create(feed=feed, asset_key=path, variable=made[path])
        vector_variables = []
        for wanted in vectors:
            vector_variables.append(
                Variable.objects.create(
                    collection=collection,
                    slug=wanted.slug,
                    name=wanted.slug.replace("-", " ").title(),
                    source_unit=unit(wanted.source_unit),
                    unit=unit(wanted.unit or wanted.source_unit),
                    transform_type=wanted.transform,
                    sources=[
                        {"type": "u_component", "value": {"source_name": wanted.u}},
                        {"type": "v_component", "value": {"source_name": wanted.v}},
                    ],
                )
            )

        for run, reference_time in enumerate(self.runs):
            for step, lead in enumerate(self.leads):
                # As a harvest indexes them: a step its Variable has, that the store holds.
                holding = [
                    path
                    for path in made
                    if self._array(path).holds(run, step) and made[path].has_step(reference_time, reference_time + lead)
                ]
                if not holding:
                    continue
                item = Item.objects.create(
                    collection=collection,
                    time=reference_time + lead,
                    reference_time=reference_time,
                    bounds=self.bounds,
                    harvested_by=feed,
                )
                for path in holding:
                    Asset.objects.create(
                        item=item,
                        variable=made[path],
                        format=Asset.Format.ZARR,
                        href=self.href,
                        store_array=path,
                        roles=[Asset.Role.DATA],
                        harvested_by=feed,
                    )
                for wanted, variable in zip(vectors, vector_variables, strict=True):
                    if wanted.u in holding and wanted.v in holding:
                        Asset.objects.create(
                            item=item,
                            variable=variable,
                            format=Asset.Format.ZARR,
                            href=self.href,
                            roles=[Asset.Role.DATA],
                            harvested_by=feed,
                        )
        return HarvestedStore(feed=feed, collection=collection, variables=made)
