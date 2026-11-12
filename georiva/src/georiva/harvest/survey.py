"""
The Upstream survey: what is learnt about an Upstream before a feed exists (ADR 0042 §2, §9).

An operator gives the Harvest Feed Wizard a catalog's address and nothing they
must already know. The survey finds the rest: which collections the catalog
has, how the chosen one's items are organised (its **shape**), which raster
assets they carry, and where those files live. Only one shape can be harvested
— one COG per time step per asset key — and for any other the survey says why,
in words an operator can act on.

It reads through the Upstream catalog client and writes nothing. It also never
opens an address the Asset host allowlist would refuse: a file's place is
judged first, by the rule a harvest judges it by, and only then read.

Give it a client that pages small (`StacApiClient(url, page_size=SAMPLE_ITEMS)`):
it reads one page of a listing and stops.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import islice
from urllib.parse import urlsplit

from .asset_hosts import AssetHostAllowlist, PinnedBuckets, is_bucket_name, public_bucket_host, s3_bucket_of
from .upstream import (
    UpstreamAsset,
    UpstreamCatalogClient,
    UpstreamCollection,
    UpstreamUnreachable,
)

#: How many of a collection's items are looked at to learn its shape.
SAMPLE_ITEMS = 20

#: How many Sample items' files are opened at once: a survey runs inside a web request.
PROBE_WORKERS = 8

#: How far, in degrees, two items' extents may differ and still be the same place.
FOOTPRINT_TOLERANCE = 0.01

_RASTER_SUFFIXES = (".tif", ".tiff")
_STORE_SUFFIXES = (".zarr", ".icechunk", ".nc", ".nc4", ".h5", ".hdf5", ".grib", ".grib2")
_STORE_TYPES = ("zarr", "icechunk", "netcdf", "hdf", "grib")


class Shape(StrEnum):
    """How a collection's items are organised. Only the first can be harvested."""

    ONE_COG_PER_TIME = "one-cog-per-time"
    TILED = "tiled"
    STORE = "store"
    NEEDS_ACCOUNT = "needs-account"
    NO_API = "no-api"
    EMPTY = "empty"
    NO_RASTERS = "no-rasters"
    UNTIMED = "untimed"


#: Why each other shape cannot be harvested, for an operator. No word here is
#: one they would have to look up.
_REASONS = {
    Shape.TILED: (
        "Each time step of this collection is split across several items (tiles or scenes). "
        "GeoRiva can only show collections that publish one file per time step."
    ),
    Shape.STORE: (
        "This collection is published as one data store (Zarr, NetCDF or similar) rather than "
        "one image file per time step. GeoRiva can't show those yet."
    ),
    Shape.NEEDS_ACCOUNT: (
        "This collection's files could not be read. They may need an account, or sit somewhere "
        "GeoRiva is not allowed to read from."
    ),
    Shape.NO_API: (
        "This address is a catalog of plain files that cannot be searched by time. "
        "GeoRiva can only show catalogs that can."
    ),
    Shape.EMPTY: "This collection lists no items.",
    Shape.NO_RASTERS: "This collection's items carry no GeoTIFF image files.",
    Shape.UNTIMED: "This collection's items carry no time GeoRiva can use.",
}


@dataclass(frozen=True)
class CatalogSurvey:
    """What an Upstream's root offers to choose from."""

    has_api: bool
    collections: list[UpstreamCollection] = field(default_factory=list)
    #: False when the collections are known by id alone: the catalog links to
    #: them and would not describe them.
    described: bool = True


@dataclass(frozen=True)
class FileLocation:
    """One place a collection's raster files live."""

    #: The `https` host the files are read at; empty when none could be worked out.
    host: str
    #: The `s3://` bucket the catalog names them by, when it names one.
    bucket: str = ""
    #: The catalog's own host, which a feed allows without being asked.
    own: bool = False
    #: Whether a file here answered an anonymous read.
    readable: bool = False
    #: Why this place cannot be used; empty when it can.
    problem: str = ""

    @property
    def proposed(self) -> bool:
        """Whether this is a place to ask the operator to confirm."""
        return self.readable and not self.own and not self.problem


@dataclass(frozen=True)
class SurveyedAsset:
    """One raster asset key of the collection, as its Sample item shows it."""

    key: str
    title: str = ""
    unit: str = ""
    nodata: float | None = None
    #: The Sample item's file, at the address a harvest would store.
    href: str = ""
    readable: bool = False
    problem: str = ""
    value_min: float | None = None
    value_max: float | None = None
    #: Where the range came from: "upstream" (its published statistics),
    #: "estimated" (the file's coarsest overview), or "" when there is none.
    range_source: str = ""
    cloud_optimized: bool = False
    crs: str = ""
    #: Something the operator should know before choosing this asset; empty when there is nothing.
    warning: str = ""


@dataclass(frozen=True)
class CollectionSurvey:
    shape: Shape
    reason: str = ""
    items_sampled: int = 0
    assets: list[SurveyedAsset] = field(default_factory=list)
    locations: list[FileLocation] = field(default_factory=list)

    @property
    def harvestable(self) -> bool:
        return self.shape is Shape.ONE_COG_PER_TIME

    @property
    def proposed_locations(self) -> list[FileLocation]:
        return [location for location in self.locations if location.proposed]

    @staticmethod
    def pins(confirmed: list[FileLocation]) -> tuple[dict[str, str], list[str]]:
        """What the locations an operator confirmed pin on a feed: its
        `pinned_buckets` and its `asset_hosts`."""
        buckets = {location.bucket: location.host for location in confirmed if location.bucket}
        hosts = [location.host for location in confirmed if not location.bucket and not location.own]
        return buckets, hosts


# ── The catalog ─────────────────────────────────────────────────────────────


def survey_catalog(upstream: UpstreamCatalogClient) -> CatalogSurvey:
    """The collections an Upstream offers. Raises `UpstreamUnreachable` when its
    root cannot be read at all."""
    root = upstream.describe_root()
    if not root.is_api:
        # The address may be one collection's own document: that one is what it offers.
        return CatalogSurvey(has_api=False, collections=[root.own] if root.own else [])
    try:
        return CatalogSurvey(has_api=True, collections=list(upstream.list_collections()))
    except UpstreamUnreachable:
        # Its collections do not answer; its root still links to each of them.
        return CatalogSurvey(has_api=True, collections=list(root.children), described=False)


# ── One collection ──────────────────────────────────────────────────────────


def survey_collection(upstream: UpstreamCatalogClient, catalog_url: str, collection_id: str) -> CollectionSurvey:
    """How `collection_id` is organised and whether it can be harvested.

    Raises `UpstreamUnreachable` when the catalog's root or the collection's
    items cannot be listed; everything about its files is reported, not raised.
    """
    if not upstream.describe_root().is_api:
        # Two things are wrong at once with a store in a catalog of plain files,
        # and the store is the one an operator can do nothing about.
        stored = any(_is_store(asset) for asset in _described_assets(upstream, collection_id))
        return _verdict(Shape.STORE if stored else Shape.NO_API)

    items = list(islice(upstream.list_items(collection_id, (None, None)), SAMPLE_ITEMS))
    sampled = len(items)

    rasters = {key for item in items for key, asset in item.assets.items() if _is_raster(asset)}
    if not rasters:
        stores = [asset for item in items for asset in item.assets.values()]
        stores += list(_described_assets(upstream, collection_id))
        if any(_is_store(asset) for asset in stores):
            return _verdict(Shape.STORE, sampled)
        return _verdict(Shape.NO_RASTERS if items else Shape.EMPTY, sampled)

    timed = [item for item in items if item.datetime is not None and rasters & item.assets.keys()]
    if not timed:
        return _verdict(Shape.UNTIMED, sampled)
    if _is_tiled(timed):
        return _verdict(Shape.TILED, sampled)

    places = _Places(upstream, catalog_url)
    # Places are worked out one at a time, each once; the files are then opened together.
    located = [(key, asset, *places.of(asset.href)) for key, asset in _samples(items, rasters)]
    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
        assets = list(pool.map(lambda found: _survey_asset(*found, upstream), located))
    locations = places.locations(assets)
    if not any(asset.readable for asset in assets):
        problem = next((asset.problem for asset in assets if asset.problem), "")
        reason = f"{_REASONS[Shape.NEEDS_ACCOUNT]} ({problem})" if problem else _REASONS[Shape.NEEDS_ACCOUNT]
        return CollectionSurvey(Shape.NEEDS_ACCOUNT, reason, sampled, assets, locations)
    return CollectionSurvey(Shape.ONE_COG_PER_TIME, "", sampled, assets, locations)


def _is_tiled(items) -> bool:
    """Whether a time step is spread over several items: two share a time, or
    the items cover different places. A scene collection gives each scene its
    own instant, so the times alone would not show it; a collection of one
    grid per time step covers the same place every time."""
    times = [item.datetime for item in items]
    if len(set(times)) < len(times):
        return True
    footprints = [item.bbox for item in items if item.bbox]
    return any(
        abs(a - b) > FOOTPRINT_TOLERANCE for bbox in footprints[1:] for a, b in zip(bbox, footprints[0], strict=False)
    )


def _verdict(shape: Shape, sampled: int = 0) -> CollectionSurvey:
    return CollectionSurvey(shape, _REASONS[shape], sampled)


def _described_assets(upstream, collection_id):
    """The assets the collection itself carries, when the Upstream will describe it."""
    described = upstream.describe_collection(collection_id)
    return described.assets.values() if described else []


def _path(asset: UpstreamAsset) -> str:
    return urlsplit(asset.href).path.rstrip("/").lower()


def _is_raster(asset: UpstreamAsset) -> bool:
    media_type = asset.media_type.lower()
    if media_type:
        return media_type.startswith("image/tiff")
    return _path(asset).endswith(_RASTER_SUFFIXES)


def _is_store(asset: UpstreamAsset) -> bool:
    media_type = asset.media_type.lower()
    return any(kind in media_type for kind in _STORE_TYPES) or _path(asset).endswith(_STORE_SUFFIXES)


def _samples(items, rasters):
    """Each raster asset key with the asset of its Sample item: the first item
    that carries it. Keys come in the order the Upstream first shows them."""
    seen = set()
    for item in items:
        for key, asset in item.assets.items():
            if key in rasters and key not in seen and _is_raster(asset):
                seen.add(key)
                yield key, asset


# ── Where the files live ────────────────────────────────────────────────────


class _Places:
    """The places a collection's files live, each worked out and judged once."""

    def __init__(self, upstream, catalog_url: str):
        self.upstream = upstream
        self.own_host = (urlsplit(catalog_url).hostname or "").lower()
        self._found: dict[str, FileLocation] = {}

    def of(self, href: str) -> tuple[FileLocation, str]:
        """The place `href` is in, and `href` as a harvest would store it."""
        bucket = s3_bucket_of(href)
        name = f"s3://{bucket}" if bucket is not None else self._host(href)
        if name not in self._found:
            self._found[name] = self._locate(href, bucket)
        location = self._found[name]
        if location.bucket and location.host:
            href = PinnedBuckets({location.bucket: location.host}).locate(href)
        return location, href

    @staticmethod
    def _host(href: str) -> str:
        parts = urlsplit(href)
        return f"{parts.scheme}://{(parts.hostname or '').lower()}"

    def _locate(self, href: str, bucket: str | None) -> FileLocation:
        if bucket is not None:
            # Only a bare bucket name is ever asked about, and only a bare
            # region name is ever built into a host (ADR 0042 §9).
            region = self.upstream.bucket_region(bucket) if is_bucket_name(bucket) else None
            host = public_bucket_host(bucket, region or "")
            if not host:
                return FileLocation(
                    host="", bucket=bucket, problem="the bucket's public address could not be worked out"
                )
            located = PinnedBuckets({bucket: host}).locate(href)
        else:
            host, located = (urlsplit(href).hostname or "").lower(), href
        # Judged as a harvest will judge it, before anything is opened there.
        refusal = AssetHostAllowlist({host}, allow_http=False, resolve=self.upstream.resolve_host).refusal(located)
        return FileLocation(host=host, bucket=bucket or "", own=host == self.own_host, problem=refusal or "")

    def locations(self, assets: list[SurveyedAsset]) -> list[FileLocation]:
        """Every place found, each with whether a file there could be read."""
        answered: dict[int, list[SurveyedAsset]] = {}
        for asset in assets:
            answered.setdefault(id(asset.location), []).append(asset)
        found = []
        for location in self._found.values():
            here = answered.get(id(location), [])
            readable = any(asset.readable for asset in here)
            problem = location.problem or ("" if readable else next((a.problem for a in here if a.problem), ""))
            found.append(FileLocation(location.host, location.bucket, location.own, readable=readable, problem=problem))
        return found


@dataclass(frozen=True)
class _Located(SurveyedAsset):
    #: The place this asset's file is in, as `_Places` holds it.
    location: FileLocation | None = None


def _survey_asset(key: str, asset: UpstreamAsset, location: FileLocation, href: str, upstream) -> _Located:
    said = {"key": key, "title": asset.title, "unit": asset.unit, "nodata": asset.nodata, "location": location}
    if location.problem:
        return _Located(**said, problem=location.problem)

    stats = asset.statistics
    lifted = stats is not None and stats.minimum is not None and stats.maximum is not None
    probe = upstream.probe_asset(href, estimate_range=not lifted)
    if not probe.readable:
        return _Located(**said, href=href, problem=probe.problem or "the file could not be read")

    if lifted:
        low, high, source = stats.minimum, stats.maximum, "upstream"
    elif probe.value_min is not None and probe.value_max is not None:
        low, high, source = probe.value_min, probe.value_max, "estimated"
    else:
        low, high, source = None, None, ""
    cloud_optimized = probe.tiled and probe.overviews
    return _Located(
        **said,
        href=href,
        readable=True,
        value_min=low,
        value_max=high,
        range_source=source,
        cloud_optimized=cloud_optimized,
        crs=probe.crs,
        warning=(
            ""
            if cloud_optimized
            else "This file is not laid out for reading over the internet, so maps of it will be slow to draw."
        ),
    )
