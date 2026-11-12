"""
The Upstream catalog client: the one seam through which a Harvest sees an Upstream.

Everything a harvest learns from outside our network comes through a client:
which collections the Upstream has, which items with which assets fall in a
window, and what address an asset host resolves to. All remoteness lives behind
it, so it is the only thing faked in tests (ADR 0042 §13), and the harvest itself
never opens a socket.

`StacApiClient` is the real one: an anonymous STAC API (ADR 0042 §2).
"""

from __future__ import annotations

import socket
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import unquote, urljoin, urlsplit

#: A window: `(start, end)`, both inclusive, either `None` for unbounded.
Window = tuple[datetime | None, datetime | None]


class UpstreamUnreachable(Exception):
    """The Upstream could not be read: down, refusing us, or answering nonsense.

    A `source` failure (ADR 0031): the harvest that meets it stops and retries."""


@dataclass(frozen=True)
class UpstreamCollection:
    id: str
    title: str = ""
    description: str = ""
    bbox: list[float] | None = None
    #: Assets the collection itself carries, beside its items': a whole-collection store, typically.
    assets: dict[str, UpstreamAsset] = field(default_factory=dict)


@dataclass(frozen=True)
class UpstreamRoot:
    """What an Upstream's root address says of it."""

    #: Whether it can be asked for a collection's items by time; a catalog of plain files cannot.
    is_api: bool
    #: The collections its root links to, by id (and title, where the link gives one).
    children: list[UpstreamCollection] = field(default_factory=list)


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
    #: The value range of its coarsest overview, when one was asked for and could be read cheaply.
    value_min: float | None = None
    value_max: float | None = None


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
        """Every collection the Upstream has."""

    def list_items(self, collection_id: str, window: Window) -> Iterable[UpstreamItem]:
        """The collection's items, with their assets, whose time is in `window`."""

    def resolve_host(self, host: str) -> list[str]:
        """The IP addresses `host` resolves to; empty when it does not resolve."""

    # What only the Upstream survey asks, before a feed exists.

    def describe_root(self) -> UpstreamRoot:
        """What the Upstream's root address says of it."""

    def bucket_region(self, bucket: str) -> str | None:
        """The region a public `s3://` bucket lives in; `None` when that cannot be found. Never raises."""

    def probe_asset(self, href: str, *, estimate_range: bool) -> AssetProbe:
        """Open the file at `href` by range and say what it is. Never raises."""


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


def parse_collection(payload: dict) -> UpstreamCollection:
    bboxes = ((payload.get("extent") or {}).get("spatial") or {}).get("bbox") or []
    return UpstreamCollection(
        id=str(payload.get("id", "")),
        title=str(payload.get("title") or ""),
        description=str(payload.get("description") or ""),
        bbox=_flat_bbox(bboxes[0]) if bboxes else None,
        assets=_parse_assets(payload.get("assets")),
    )


def _parse_assets(assets, *, base_url: str = "") -> dict[str, UpstreamAsset]:
    if not isinstance(assets, dict):
        return {}
    return {str(key): parse_asset(asset, base_url=base_url) for key, asset in assets.items() if isinstance(asset, dict)}


def parse_root(payload: dict, *, base_url: str = "") -> UpstreamRoot:
    """A catalog's landing page. One that declares what it conforms to, or links
    to its collections or its search, can be asked for items by time; a catalog
    of plain files only links to more files."""
    links = [link for link in payload.get("links") or [] if isinstance(link, dict) and link.get("href")]
    children = []
    for link in links:
        path = urlsplit(urljoin(base_url, str(link["href"]))).path.rstrip("/")
        parent, _, last = path.rpartition("/")
        if link.get("rel") == "child" and last and parent.endswith("/collections"):
            children.append(UpstreamCollection(id=unquote(last), title=str(link.get("title") or "")))
    return UpstreamRoot(
        is_api=bool(payload.get("conformsTo")) or any(link.get("rel") in ("data", "search") for link in links),
        children=children,
    )


def parse_asset(payload: dict, *, base_url: str = "") -> UpstreamAsset:
    """One STAC asset. Band metadata is read from `raster:bands` (the raster
    extension) or `bands` (STAC 1.1's common metadata, its equivalent); the
    first band speaks for the asset, as one COG carries one Variable."""
    bands = payload.get("raster:bands") or payload.get("bands") or []
    band = bands[0] if bands and isinstance(bands[0], dict) else {}
    stats = band.get("statistics")
    size = payload.get("file:size")
    href = str(payload.get("href") or "")
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
                    values = np.ma.masked_invalid(src.read(1, out_shape=shape, masked=True))
                    if values.count():
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


# ── The real client ─────────────────────────────────────────────────────────


def _rfc3339(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _scheme_host_port(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    return (parts.scheme, parts.hostname, parts.port)


#: The answers that send a GET elsewhere.
_REDIRECTS = frozenset({301, 302, 303, 307, 308})


class StacApiClient:
    """An anonymous STAC API, read over HTTP.

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

    @property
    def session(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def list_collections(self) -> Iterator[UpstreamCollection]:
        for page, _ in self._pages(f"{self.url}/collections"):
            for payload in page.get("collections") or []:
                if isinstance(payload, dict):
                    yield parse_collection(payload)

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
        return parse_root(page, base_url=url)

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
