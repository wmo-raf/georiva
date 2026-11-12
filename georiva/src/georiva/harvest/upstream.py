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
from urllib.parse import urljoin, urlsplit

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
        assets={
            str(key): parse_asset(asset, base_url=base_url) for key, asset in assets.items() if isinstance(asset, dict)
        },
    )


# ── The real client ─────────────────────────────────────────────────────────


def _rfc3339(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _scheme_host_port(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    return (parts.scheme, parts.hostname, parts.port)


class StacApiClient:
    """An anonymous STAC API, read over HTTP.

    The operator chose the catalog's host, and that is the only host this
    client talks to: redirects are not followed and a `next` link to any other
    scheme, host or port is refused, so the Upstream's content never decides
    where our servers connect (ADR 0042 §9).
    """

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

    def _pages(self, url: str, params: dict | None = None) -> Iterator[tuple[dict, str]]:
        """Each page of a paged listing with the URL it came from, following `next` links."""
        seen = set()
        while url:
            if url in seen:
                raise UpstreamUnreachable(f"upstream catalog pages in a loop at {url}")
            seen.add(url)
            page = self._get(url, params)
            yield page, url
            url, params = self._next(page, url), None

    def _next(self, page: dict, current: str) -> str | None:
        for link in page.get("links") or []:
            if isinstance(link, dict) and link.get("rel") == "next" and link.get("href"):
                target = urljoin(current, str(link["href"]))
                if _scheme_host_port(target) != _scheme_host_port(self.url):
                    raise UpstreamUnreachable(f"upstream catalog's next page is on another host: {target}")
                return target
        return None

    def _get(self, url: str, params: dict | None) -> dict:
        import requests

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout,
                allow_redirects=False,
                headers={"Accept": "application/geo+json, application/json"},
            )
        except requests.RequestException as exc:
            raise UpstreamUnreachable(f"upstream catalog unreachable: {exc}") from exc
        if response.status_code != 200:
            raise UpstreamUnreachable(f"upstream catalog answered {response.status_code} for {url}")
        try:
            page = response.json()
        except ValueError as exc:
            raise UpstreamUnreachable(f"upstream catalog did not answer JSON for {url}") from exc
        if not isinstance(page, dict):
            raise UpstreamUnreachable(f"upstream catalog did not answer a JSON object for {url}")
        return page
