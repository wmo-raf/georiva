"""
The Upstream catalog client: the one seam through which a Harvest sees an Upstream.

Everything a harvest learns from outside our network comes through a client:
which collections the Upstream has, which items with which assets fall in a
window, and what address an asset host resolves to. All remoteness lives behind
it, so it is the only thing faked in tests (ADR 0042 §13), and the harvest itself
never opens a socket.

`StacApiClient` is the real one: an anonymous STAC API (ADR 0042 §2), or a
catalog of plain files whose collections are Upstream stores (ADR 0044 §1).
"""

from __future__ import annotations

import logging
import socket
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import quote, unquote, urljoin, urlsplit

logger = logging.getLogger(__name__)

#: A window: `(start, end)`, both inclusive, either `None` for unbounded.
Window = tuple[datetime | None, datetime | None]


class UpstreamUnreachable(Exception):
    """The Upstream could not be read: down, refusing us, or answering nonsense.

    A `source` failure (ADR 0031): the harvest that meets it stops and retries."""


@dataclass(frozen=True)
class CubeDimension:
    """One dimension of a collection's cube, as the collection describes it."""

    name: str
    #: The datacube extension's `type`: "temporal", "spatial", or whatever else the publisher wrote.
    kind: str = ""
    #: "x" or "y" for a horizontal dimension; empty for any other.
    axis: str = ""
    #: How many steps it has; `None` when the collection does not say.
    size: int | None = None


@dataclass(frozen=True)
class CubeArray:
    """One array of a collection's cube, by its path in the store."""

    path: str
    dimensions: tuple[str, ...] = ()
    #: How many steps of each dimension one chunk spans; `None` when the collection does not say.
    chunks: tuple[int, ...] | None = None
    title: str = ""
    unit: str = ""


@dataclass(frozen=True)
class UpstreamCube:
    """A collection's own description of the cube its store holds (`cube:dimensions`, `cube:variables`)."""

    dimensions: dict[str, CubeDimension] = field(default_factory=dict)
    arrays: dict[str, CubeArray] = field(default_factory=dict)


@dataclass(frozen=True)
class UpstreamCollection:
    id: str
    title: str = ""
    description: str = ""
    bbox: list[float] | None = None
    #: Assets the collection itself carries, beside its items': a whole-collection store, typically.
    assets: dict[str, UpstreamAsset] = field(default_factory=dict)
    #: The cube an Upstream store's collection describes; `None` when it describes none.
    cube: UpstreamCube | None = None


@dataclass(frozen=True)
class UpstreamRoot:
    """What an Upstream's root address says of it."""

    #: Whether it can be asked for a collection's items by time; a catalog of plain files cannot.
    is_api: bool
    #: The collections its root links to, by id (and title, where the link gives one).
    children: list[UpstreamCollection] = field(default_factory=list)
    #: Where each of those is described, by id.
    child_hrefs: dict[str, str] = field(default_factory=dict)
    #: The collection the address itself is, when it is one's own document and not a catalog's root.
    own: UpstreamCollection | None = None


@dataclass(frozen=True)
class AssetProbe:
    """What opening one asset's file showed. Nothing here is stored: a probe
    informs the Upstream survey and no one else."""

    readable: bool
    #: Why it could not be read; empty when it could.
    problem: str = ""
    #: Internally tiled, and with overviews: together, what makes range reads cheap.
    tiled: bool = False
    overviews: bool = False
    crs: str = ""
    #: The range most of its values lie in (`RANGE_PERCENTILES` of its coarsest
    #: overview), when one was asked for and could be read cheaply.
    value_min: float | None = None
    value_max: float | None = None


@dataclass(frozen=True)
class StoreProbe:
    """What opening an Upstream store showed of its arrays. Only their metadata
    is read: no data, and so nothing of where a virtual store's chunks live."""

    readable: bool
    #: Why it could not be opened; empty when it could.
    problem: str = ""
    #: The codecs each array names, by the array's path in the store, in the order it names them.
    codecs: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class UpstreamStatistics:
    """Display statistics an Upstream publishes for an asset's band. Any may be missing."""

    minimum: float | None = None
    maximum: float | None = None
    mean: float | None = None
    stddev: float | None = None


@dataclass(frozen=True)
class UpstreamAsset:
    href: str
    media_type: str = ""
    title: str = ""
    unit: str = ""
    file_size: int | None = None
    checksum: str = ""
    bands: int | None = None
    nodata: float | None = None
    #: From upstream metadata only (ADR 0042 §10); `None` when it publishes none.
    statistics: UpstreamStatistics | None = None
    #: For a virtual Upstream store: the places it declares its chunks live in, as it spells them.
    chunk_locations: tuple[str, ...] = ()


@dataclass(frozen=True)
class UpstreamItem:
    id: str
    #: The item's valid time; `None` when the Upstream gives it none we can use.
    datetime: datetime | None
    bbox: list[float] | None = None
    geometry: dict | None = None
    updated: datetime | None = None
    assets: dict[str, UpstreamAsset] = field(default_factory=dict)


def in_window(when: datetime, window: Window) -> bool:
    start, end = window
    return (start is None or when >= start) and (end is None or when <= end)


class UpstreamCatalogClient(Protocol):
    """What a harvest asks of an Upstream. Each method raises `UpstreamUnreachable`
    when the Upstream cannot answer."""

    def list_collections(self) -> Iterable[UpstreamCollection]:
        """Every collection the Upstream has, whether its root is an API or a catalog of plain files."""

    def list_items(self, collection_id: str, window: Window) -> Iterable[UpstreamItem]:
        """The collection's items, with their assets, whose time is in `window`."""

    def resolve_host(self, host: str) -> list[str]:
        """The IP addresses `host` resolves to; empty when it does not resolve."""

    # What only the Upstream survey asks, before a feed exists.

    def describe_root(self) -> UpstreamRoot:
        """What the Upstream's root address says of it."""

    def describe_collection(self, collection_id: str) -> UpstreamCollection | None:
        """The Upstream's own description of one collection; `None` when it has
        none to give. Raises only when the root itself cannot be read."""

    def bucket_region(self, bucket: str) -> str | None:
        """The region a public `s3://` bucket lives in; `None` when that cannot be found. Never raises."""

    def probe_asset(self, href: str, *, estimate_range: bool) -> AssetProbe:
        """Open the file at `href` by range and say what it is. Never raises."""

    def describe_store(self, href: str) -> StoreProbe:
        """Open the Upstream store at `href` and say which codecs its arrays name. Never raises."""


# ── STAC JSON → what a harvest reads ────────────────────────────────────────


def _parse_time(value) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value) if value == value and abs(value) != float("inf") else None


def _flat_bbox(bbox) -> list[float] | None:
    """`[west, south, east, north]` from a STAC bbox, dropping elevation from a 3D one."""
    if not isinstance(bbox, list | tuple) or len(bbox) not in (4, 6):
        return None
    if len(bbox) == 6:
        bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]
    numbers = [_number(v) for v in bbox]
    return None if None in numbers else numbers


def parse_collection(payload: dict, *, base_url: str = "") -> UpstreamCollection:
    bboxes = ((payload.get("extent") or {}).get("spatial") or {}).get("bbox") or []
    return UpstreamCollection(
        id=str(payload.get("id", "")),
        title=str(payload.get("title") or ""),
        description=str(payload.get("description") or ""),
        bbox=_flat_bbox(bboxes[0]) if bboxes else None,
        assets=_parse_assets(payload.get("assets"), base_url=base_url),
        cube=parse_cube(payload),
    )


def _whole(value) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def parse_cube(payload: dict) -> UpstreamCube | None:
    """The cube a collection describes with the datacube extension; `None` when
    it describes neither dimensions nor variables. What is described badly is
    kept as far as it goes: saying what is missing is the survey's to do."""
    dimensions, variables = payload.get("cube:dimensions"), payload.get("cube:variables")
    if not isinstance(dimensions, dict) and not isinstance(variables, dict):
        return None
    found = {}
    for name, dimension in (dimensions if isinstance(dimensions, dict) else {}).items():
        if not isinstance(dimension, dict):
            continue
        size = _whole(dimension.get("size"))
        values = dimension.get("values")
        if size is None and isinstance(values, list):
            size = len(values)
        found[str(name)] = CubeDimension(
            name=str(name),
            kind=str(dimension.get("type") or "").lower(),
            axis=str(dimension.get("axis") or "").lower(),
            size=size,
        )
    arrays = {}
    for path, variable in (variables if isinstance(variables, dict) else {}).items():
        if not isinstance(variable, dict):
            continue
        named, chunks = variable.get("dimensions"), variable.get("chunks")
        whole = [_whole(n) for n in chunks] if isinstance(chunks, list) else [None]
        arrays[str(path)] = CubeArray(
            path=str(path),
            dimensions=tuple(str(n) for n in named) if isinstance(named, list) else (),
            chunks=None if None in whole else tuple(whole),
            title=str(variable.get("long_name") or variable.get("description") or ""),
            unit=str(variable.get("unit") or ""),
        )
    return UpstreamCube(dimensions=found, arrays=arrays)


def _parse_assets(assets, *, base_url: str = "") -> dict[str, UpstreamAsset]:
    if not isinstance(assets, dict):
        return {}
    return {str(key): parse_asset(asset, base_url=base_url) for key, asset in assets.items() if isinstance(asset, dict)}


def parse_root(payload: dict, *, base_url: str = "") -> UpstreamRoot:
    """A catalog's landing page. One that declares what it conforms to, or links
    to its collections or its search, can be asked for items by time; a catalog
    of plain files only links to more files."""
    links = [link for link in payload.get("links") or [] if isinstance(link, dict) and link.get("href")]
    is_api = bool(payload.get("conformsTo")) or any(link.get("rel") in ("data", "search") for link in links)
    children, child_hrefs = [], {}
    for link in links:
        if link.get("rel") != "child":
            continue
        href = urljoin(base_url, str(link["href"]))
        child_id = _child_id(urlsplit(href).path, is_api=is_api)
        if child_id and child_id not in child_hrefs:
            children.append(UpstreamCollection(id=child_id, title=str(link.get("title") or "")))
            child_hrefs[child_id] = href
    return UpstreamRoot(
        is_api=is_api,
        children=children,
        child_hrefs=child_hrefs,
        own=parse_collection(payload, base_url=base_url) if payload.get("type") == "Collection" else None,
    )


def _child_id(path: str, *, is_api: bool) -> str | None:
    """The id of the collection a root's child link leads to, read from its
    address. An API keeps its collections under `/collections/`, and any other
    child there is a sub-catalog; a catalog of plain files names a collection
    by its folder (`…/chirps/collection.json`) or its file (`…/chirps.json`)."""
    parent, _, last = path.rstrip("/").rpartition("/")
    if parent.endswith("/collections"):
        return unquote(last) or None
    if is_api:
        return None
    if last.lower() in ("collection.json", "catalog.json"):
        last = parent.rpartition("/")[2]
    return unquote(last.removesuffix(".json")) or None


def parse_asset(payload: dict, *, base_url: str = "") -> UpstreamAsset:
    """One STAC asset. Band metadata is read from `raster:bands` (the raster
    extension) or `bands` (STAC 1.1's common metadata, its equivalent); the
    first band speaks for the asset, as one COG carries one Variable."""
    bands = payload.get("raster:bands") or payload.get("bands") or []
    band = bands[0] if bands and isinstance(bands[0], dict) else {}
    stats = band.get("statistics")
    size = payload.get("file:size")
    href = str(payload.get("href") or "")
    containers = payload.get("icechunk:virtual_chunk_containers")
    return UpstreamAsset(
        # An asset with no href stays without one: joined to the page it would name the page.
        href=urljoin(base_url, href) if href else "",
        media_type=str(payload.get("type") or ""),
        title=str(payload.get("title") or ""),
        unit=str(band.get("unit") or ""),
        file_size=size if isinstance(size, int) and not isinstance(size, bool) else None,
        checksum=str(payload.get("file:checksum") or ""),
        bands=len(bands) or None,
        nodata=_number(band.get("nodata")),
        statistics=(
            UpstreamStatistics(
                minimum=_number(stats.get("minimum")),
                maximum=_number(stats.get("maximum")),
                mean=_number(stats.get("mean")),
                stddev=_number(stats.get("stddev")),
            )
            if isinstance(stats, dict)
            else None
        ),
        chunk_locations=tuple(
            str(container["url_prefix"])
            for container in (containers if isinstance(containers, list) else [])
            if isinstance(container, dict) and container.get("url_prefix")
        ),
    )


def parse_item(payload: dict, *, base_url: str = "") -> UpstreamItem:
    """One STAC item. Its time is `datetime`, or `start_datetime` for an item
    that covers a period and so carries a null `datetime`."""
    properties = payload.get("properties") or {}
    assets = payload.get("assets") or {}
    return UpstreamItem(
        id=str(payload.get("id", "")),
        datetime=_parse_time(properties.get("datetime")) or _parse_time(properties.get("start_datetime")),
        bbox=_flat_bbox(payload.get("bbox")),
        geometry=payload.get("geometry") if isinstance(payload.get("geometry"), dict) else None,
        updated=_parse_time(properties.get("updated")),
        assets=_parse_assets(assets, base_url=base_url),
    )


# ── Opening one file ────────────────────────────────────────────────────────

#: Seconds one request of a probe may take. A probe runs inside a web request.
PROBE_TIMEOUT = 10

#: The most pixels a probe reads to estimate a value range.
PROBE_MAX_POINTS = 1_000_000

#: The share of a file's values an estimated range spans: its 2nd to its 98th percentile.
RANGE_PERCENTILES = (2, 98)


def probe_raster(path: str, *, estimate_range: bool, timeout: float = PROBE_TIMEOUT) -> AssetProbe:
    """Open the raster at `path` — a URL, read by range — and say what it is.

    Opening reads the file's header and nothing else. A value range is read
    only when asked for, from the coarsest overview, and only when that is
    small enough to be cheap: a file with no overviews is not read whole to
    learn its range. Whatever goes wrong is the probe's answer, not an error.
    """
    import numpy as np
    import rasterio
    from rasterio.errors import RasterioError

    options = {
        "GDAL_HTTP_TIMEOUT": timeout,
        "GDAL_HTTP_CONNECTTIMEOUT": min(timeout, 5),
        "GDAL_HTTP_MAX_RETRY": 0,
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    }
    try:
        with rasterio.Env(**options), rasterio.open(path) as src:
            factors = src.overviews(1) if src.count else []
            low = high = None
            if estimate_range and src.count:
                factor = max(factors, default=1)
                shape = (max(1, src.height // factor), max(1, src.width // factor))
                if shape[0] * shape[1] <= PROBE_MAX_POINTS:
                    values = np.ma.masked_invalid(src.read(1, out_shape=shape, masked=True)).compressed()
                    if values.size:
                        # A display range, not the extremes: one storm's maximum would wash out every map.
                        low, high = (float(v) for v in np.percentile(values, RANGE_PERCENTILES))
                        if low == high:
                            low, high = float(values.min()), float(values.max())
            return AssetProbe(
                readable=True,
                tiled=bool(src.profile.get("tiled")),
                overviews=bool(factors),
                crs=src.crs.to_string() if src.crs else "",
                value_min=low,
                value_max=high,
            )
    except (RasterioError, OSError, ValueError) as exc:
        return AssetProbe(readable=False, problem=str(exc)[:300] or type(exc).__name__)


# ── Opening one store ───────────────────────────────────────────────────────

#: Seconds opening a store and reading its arrays' metadata may take in all. A
#: cold open was measured at several seconds, and one probe in three lost its connection.
STORE_TIMEOUT = 45

#: The most arrays of a store that are asked about at once.
STORE_CONCURRENCY = 32


def codec_names(codecs) -> tuple[str, ...]:
    """Every codec an array's metadata names, in order and once each, those a
    codec wraps included: a sharded array's inner codecs decode its data too."""
    names: list[str] = []

    def walk(listed):
        for codec in listed if isinstance(listed, list) else []:
            if isinstance(codec, str):
                codec = {"name": codec}
            if not isinstance(codec, dict):
                continue
            name = str(codec.get("name") or "")
            if name and name not in names:
                names.append(name)
            configuration = codec.get("configuration")
            if isinstance(configuration, dict):
                walk(configuration.get("codecs"))
                walk(configuration.get("index_codecs"))

    walk(codecs)
    return tuple(names)


def can_decode(codec: str) -> bool:
    """Whether a codec of that name is registered here. Nothing about any one
    publisher: a codec nobody installed is refused by name, without new code."""
    from zarr.registry import get_codec_class

    try:
        get_codec_class(codec)
    except Exception:  # zarr raises KeyError today and names its own error for it elsewhere
        return False
    return True


async def _array_codecs(store) -> dict[str, tuple[str, ...]]:
    """The codecs of every array under a store's root, by path.

    Read from each node's own metadata document, never by opening the array:
    opening one whose codec is not installed raises, and what it names is the
    thing to be learnt. Groups are walked by listing; no chunk is touched.
    """
    import asyncio
    import json

    from zarr.core.buffer import default_buffer_prototype

    prototype = default_buffer_prototype()
    gate = asyncio.Semaphore(STORE_CONCURRENCY)
    found: dict[str, tuple[str, ...]] = {}

    async def visit(path: str):
        async with gate:
            document = await store.get(f"{path}/zarr.json" if path else "zarr.json", prototype)
            if document is None:
                return
            node = json.loads(document.to_bytes())
            if not isinstance(node, dict):
                return
            if node.get("node_type") == "array":
                found[path] = codec_names(node.get("codecs"))
                return
            children = [name async for name in store.list_dir(f"{path}/" if path else "")]
        await asyncio.gather(
            *(visit(f"{path}/{name}" if path else name) for name in children if name and name != "zarr.json")
        )

    await visit("")
    return found


def read_store_codecs(storage, *, branch: str = "main") -> dict[str, tuple[str, ...]]:
    """Open the repository in `storage` read-only and name its arrays' codecs.

    No virtual chunk access is authorised: metadata needs none, and where a
    store's chunks may be read from is an operator's to confirm (ADR 0044 §11).
    """
    import asyncio

    import icechunk

    repository = icechunk.Repository.open(storage)
    return asyncio.run(_array_codecs(repository.readonly_session(branch).store))


def probe_store(href: str, *, timeout: float = STORE_TIMEOUT) -> StoreProbe:
    """Open the store at `href` over `https` and say which codecs its arrays
    name. Whatever goes wrong is the probe's answer, not an error, and it is
    said in words an operator may be shown; the library's own are logged."""
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as TimedOut

    def read():
        import icechunk

        each = f"{max(1, int(min(timeout, 20)))}s"
        return read_store_codecs(icechunk.http_storage(href, opts={"timeout": each, "connect_timeout": "5s"}))

    # Its own thread, so a store that stops answering costs the wait and no more.
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="store-probe")
    try:
        return StoreProbe(readable=True, codecs=pool.submit(read).result(timeout=timeout))
    except TimedOut:
        return StoreProbe(readable=False, problem=f"it did not answer within {int(timeout)} seconds")
    except Exception as exc:  # whatever the store or its library raises is an answer, never a crash
        logger.warning("Upstream store at %s did not open: %s", href, exc)
        return StoreProbe(readable=False, problem="it did not open for a reader with no account")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


# ── The real client ─────────────────────────────────────────────────────────


def _rfc3339(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _scheme_host_port(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    return (parts.scheme, parts.hostname, parts.port)


#: The answers that send a GET elsewhere.
_REDIRECTS = frozenset({301, 302, 303, 307, 308})


class StacApiClient:
    """An anonymous STAC API, or a catalog of plain files, read over HTTP.

    The operator chose the catalog's host, and that is the only host this
    client talks to: a redirect or a `next` link to any other scheme, host or
    port is refused, so the Upstream's content never decides where our servers
    connect (ADR 0042 §9). A redirect that stays there is followed, a few hops
    at most: catalogs answer a listing with one to their own search.
    """

    #: How many redirects one request may take before the Upstream is going nowhere.
    MAX_REDIRECTS = 5

    def __init__(self, url: str, *, session=None, timeout: float = 30, page_size: int = 200):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.page_size = page_size
        self._session = session
        #: Whether the root, once read, turned out to be a catalog of plain files.
        self._plain_files: bool | None = None

    @property
    def session(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def list_collections(self) -> Iterator[UpstreamCollection]:
        if self._is_plain_files():
            yield from self._linked_collections(self.describe_root())
            return
        for page, page_url in self._pages(f"{self.url}/collections"):
            for payload in page.get("collections") or []:
                if isinstance(payload, dict):
                    yield parse_collection(payload, base_url=page_url)

    def _is_plain_files(self) -> bool:
        """Whether the Upstream is a catalog of plain files, with no listing to
        ask. Known once its root has been read; before that, only an address
        that names a file is worth reading the root for — an API is asked for
        its collections directly, with no request it was not sent before."""
        if self._plain_files is None and urlsplit(self.url).path.lower().endswith(".json"):
            self.describe_root()
        return bool(self._plain_files)

    def _linked_collections(self, root: UpstreamRoot) -> Iterator[UpstreamCollection]:
        """A catalog of plain files has no listing: its collections are what
        its root links to, each described by its own document. One that will
        not describe itself is still offered, by the id and title of its link."""
        if root.own is not None:
            yield root.own
            return
        for child in root.children:
            yield self._follow(root.child_hrefs.get(child.id), child.id) or child

    def _follow(self, href: str | None, collection_id: str) -> UpstreamCollection | None:
        """The collection document at `href`, when it is on the operator's own
        host, answers, and is a collection: a child link is the Upstream's content."""
        if href is None or not self._is_ours(href):
            return None
        try:
            page, url = self._get(href, None)
        except UpstreamUnreachable:
            return None
        if page.get("type") not in (None, "Collection"):
            return None
        described = parse_collection(page, base_url=url)
        # The id the root's link gave is the one the operator was offered and picks by.
        return replace(described, id=collection_id)

    def list_items(self, collection_id: str, window: Window) -> Iterator[UpstreamItem]:
        params = {"limit": self.page_size}
        start, end = window
        if start or end:
            params["datetime"] = f"{_rfc3339(start) if start else '..'}/{_rfc3339(end) if end else '..'}"
        for page, page_url in self._pages(f"{self.url}/collections/{collection_id}/items", params):
            for payload in page.get("features") or []:
                if isinstance(payload, dict):
                    yield parse_item(payload, base_url=page_url)

    def resolve_host(self, host: str) -> list[str]:
        try:
            found = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except OSError:
            return []
        return sorted({info[4][0] for info in found})

    def describe_root(self) -> UpstreamRoot:
        page, url = self._get(self.url, None)
        root = parse_root(page, base_url=url)
        self._plain_files = not root.is_api
        return root

    def describe_collection(self, collection_id: str) -> UpstreamCollection | None:
        root = self.describe_root()
        if root.own is not None and root.own.id == collection_id:
            return root.own
        href = root.child_hrefs.get(collection_id)
        if href is None and root.is_api:
            href = f"{self.url}/collections/{quote(collection_id, safe='')}"
        return self._follow(href, collection_id)

    def bucket_region(self, bucket: str) -> str | None:
        """Asked of S3 itself, which names a bucket's region on any answer. This
        is the one address here that is not the operator's: it is S3's, and
        only a bucket's bare name is ever put into it."""
        import requests

        from .asset_hosts import is_bucket_name

        if not is_bucket_name(bucket):
            return None
        try:
            response = self.session.get(
                f"https://{bucket}.s3.amazonaws.com/", timeout=self.timeout, allow_redirects=False
            )
        except requests.RequestException:
            return None
        return response.headers.get("x-amz-bucket-region") or None

    def probe_asset(self, href: str, *, estimate_range: bool) -> AssetProbe:
        return probe_raster(href, estimate_range=estimate_range)

    def describe_store(self, href: str) -> StoreProbe:
        return probe_store(href)

    def _pages(self, url: str, params: dict | None = None) -> Iterator[tuple[dict, str]]:
        """Each page of a paged listing with the URL it came from, following `next` links."""
        seen = set()
        while url:
            if url in seen:
                raise UpstreamUnreachable(f"upstream catalog pages in a loop at {url}")
            seen.add(url)
            # Where the page was found, which a redirect makes another place than
            # where it was asked for: its links and hrefs are relative to that.
            page, url = self._get(url, params)
            seen.add(url)
            yield page, url
            url, params = self._next(page, url), None

    def _next(self, page: dict, current: str) -> str | None:
        for link in page.get("links") or []:
            if isinstance(link, dict) and link.get("rel") == "next" and link.get("href"):
                target = urljoin(current, str(link["href"]))
                if not self._is_ours(target):
                    raise UpstreamUnreachable(f"upstream catalog's next page is on another host: {target}")
                return target
        return None

    def _is_ours(self, url: str) -> bool:
        """Whether `url` is on the scheme, host and port the operator pointed this client at."""
        return _scheme_host_port(url) == _scheme_host_port(self.url)

    def _get(self, url: str, params: dict | None) -> tuple[dict, str]:
        """The JSON object at `url`, and the URL it was found at once any redirects were followed."""
        import requests

        for _hop in range(self.MAX_REDIRECTS + 1):
            try:
                # Never the session's own redirect handling: each hop is ours to judge.
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    allow_redirects=False,
                    headers={"Accept": "application/geo+json, application/json"},
                )
            except requests.RequestException as exc:
                raise UpstreamUnreachable(f"upstream catalog unreachable: {exc}") from exc
            location = response.headers.get("Location") if response.status_code in _REDIRECTS else None
            if not location:
                break
            target = urljoin(url, str(location))
            if not self._is_ours(target):
                raise UpstreamUnreachable(f"upstream catalog redirects to another host: {target}")
            # A redirect's address is whole: it carries whatever of our query the Upstream kept.
            url, params = target, None
        else:
            raise UpstreamUnreachable(f"upstream catalog keeps redirecting, last to {url}")
        if response.status_code != 200:
            raise UpstreamUnreachable(f"upstream catalog answered {response.status_code} for {url}")
        try:
            page = response.json()
        except ValueError as exc:
            raise UpstreamUnreachable(f"upstream catalog did not answer JSON for {url}") from exc
        if not isinstance(page, dict):
            raise UpstreamUnreachable(f"upstream catalog did not answer a JSON object for {url}")
        return page, url
