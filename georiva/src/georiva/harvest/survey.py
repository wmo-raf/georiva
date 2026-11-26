"""
The Upstream survey: what is learnt about an Upstream before a feed exists (ADR 0042 §2, §9).

An operator gives the Harvest Feed Wizard a catalog's address and nothing they
must already know. The survey finds the rest: which collections the catalog
has, how the chosen one's items are organised (its **shape**), which raster
assets they carry, and where those files live. Two shapes are accepted — one
COG per time step per asset key, and a map-shaped Upstream store whose codecs
we can decode and whose data lives in places of a kind we can read (ADR 0044
§5, §11, §12) — and for any other the survey says why, in words an operator
can act on. Of an accepted store it also says which arrays can become
Variables, and why each of the others cannot (§8).

How a store is divided up is read from the store itself, wherever its place lets it be
opened; what the collection publishes is the fallback (ADR 0052). Stores kept
on the items, one per item or one per item per variable, and plain Zarr stores
are recognised and refused by name.

It reads through the Upstream catalog client and writes nothing. It also never
opens an address the Asset host allowlist would refuse: a file's place is
judged first, by the rule a harvest judges it by, and only then read.

Give it a client that pages small (`StacApiClient(url, page_size=SAMPLE_ITEMS)`):
it reads one page of a listing and stops.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from enum import StrEnum
from itertools import islice
from urllib.parse import urlsplit

from .asset_hosts import (
    AssetHostAllowlist,
    PinnedBuckets,
    chunk_place,
    is_bucket_name,
    is_region_name,
    public_bucket_host,
    s3_bucket_of,
)
from .upstream import (
    CubeArray,
    CubeDimension,
    StoreChunking,
    StoreField,
    StoreFormat,
    UpstreamAsset,
    UpstreamCatalogClient,
    UpstreamCollection,
    UpstreamCube,
    UpstreamTimedOut,
    UpstreamUnreachable,
    can_decode,
)

#: How many of a collection's items are looked at to learn its shape.
SAMPLE_ITEMS = 20

#: How many Sample items' files are opened at once: a survey runs inside a web request.
PROBE_WORKERS = 8

#: How far, in degrees, two items' extents may differ and still be the same place.
FOOTPRINT_TOLERANCE = 0.01

_RASTER_SUFFIXES = (".tif", ".tiff")
#: What makes an asset a store: a NetCDF, HDF or GRIB file is a file, however much it holds (ADR 0052 §3).
_STORE_SUFFIXES = (".zarr", ".icechunk")
_STORE_TYPES = ("zarr", "icechunk")


class Shape(StrEnum):
    """How a collection is organised. A feed can be set up from the first two."""

    ONE_COG_PER_TIME = "one-cog-per-time"
    #: An Upstream store whose chunks are each one whole field at one step.
    MAP_STORE = "map-shaped-store"
    #: An Upstream store whose chunks each span many steps over a small area.
    SERIES_STORE = "series-shaped-store"
    #: An Upstream store whose fields are on cells of another shape than a grid of rows and columns.
    UNGRIDDED_STORE = "ungridded-store"
    #: A plain Zarr store: how it is divided up is read, its data is not yet (Store format, ADR 0052).
    PLAIN_ZARR_STORE = "plain-zarr-store"
    #: An Upstream store naming a codec nobody installed here.
    UNDECODABLE_STORE = "undecodable-store"
    #: An Upstream store whose shape its collection does not say.
    UNREADABLE_STORE = "unreadable-store"
    #: An Upstream store declaring that its data lives in a kind of place we do not read from.
    STORE_KEPT_ELSEWHERE = "store-kept-elsewhere"
    #: A map-shaped Upstream store none of whose arrays is a plain field on a latitude/longitude grid.
    UNMAPPABLE_STORE = "unmappable-store"
    #: Each item is a whole dataset with a store of its own (Store placement per item, ADR 0052).
    STORE_PER_ITEM = "store-per-item"
    #: Each item keeps a store of its own for each variable (Store placement per item per variable).
    STORE_PER_ITEM_PER_VARIABLE = "store-per-item-per-variable"
    TILED = "tiled"
    #: A store whose collection describes no cube: nothing can be learnt of it.
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
        "This collection is published as one data store rather than one image file per time step, "
        "and it does not describe what the store holds. GeoRiva can't show it."
    ),
    Shape.SERIES_STORE: (
        "This collection is laid out for charting one place over time, not for drawing maps. GeoRiva cannot use it yet."
    ),
    Shape.STORE_PER_ITEM: (
        "Each item of this collection is a dataset with a store of its own. GeoRiva reads one store per collection."
    ),
    Shape.STORE_PER_ITEM_PER_VARIABLE: (
        "Each item of this collection keeps a store of its own for each variable. "
        "GeoRiva reads one store per collection."
    ),
    Shape.UNGRIDDED_STORE: (
        "This collection's fields are not on a grid of rows and columns (they are on cells of another shape), "
        "which GeoRiva cannot show yet."
    ),
    Shape.PLAIN_ZARR_STORE: "This collection's data is kept in a kind of data store GeoRiva cannot read yet.",
    Shape.UNDECODABLE_STORE: "This collection's data is packed in a way GeoRiva cannot unpack",
    Shape.UNREADABLE_STORE: "GeoRiva could not work out how this collection's data is laid out",
    Shape.STORE_KEPT_ELSEWHERE: "Some of this collection's data is kept somewhere GeoRiva cannot read from yet",
    Shape.UNMAPPABLE_STORE: (
        "This collection is laid out for drawing maps, but GeoRiva cannot show any of what it holds yet."
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


def reason_for(shape: Shape) -> str:
    """Why a collection of `shape` cannot be harvested, for an operator; empty for a shape that is accepted."""
    return _REASONS.get(shape, "")


# ── The four axes an Upstream store varies along (ADR 0052) ─────────────────


class Placement(StrEnum):
    """Store placement: where one readable store sits in the catalog."""

    PER_COLLECTION = "per-collection"
    PER_ITEM = "per-item"
    PER_ITEM_PER_VARIABLE = "per-item-per-variable"


class StoreShape(StrEnum):
    """Store shape: how its chunking divides its data (ADR 0044 §5)."""

    MAP = "map"
    SERIES = "series"
    #: Each field cut into tiles within a single step: neither shape.
    TILED = "tiled"
    #: Its fields are on cells that are no grid of rows and columns (an icosahedral mesh).
    NOT_A_GRID = "not-a-grid"
    #: The store was read, and nothing in it or its collection says.
    UNKNOWN = "unknown"


class Format(StrEnum):
    """Store format, with whether its codecs decode: a store of a format we
    read must also unpack."""

    ICECHUNK = "icechunk"
    PLAIN_ZARR = "plain-zarr"
    UNDECODABLE = "undecodable"


class Access(StrEnum):
    """Access: whether it, and every place its data is declared to live, can be read anonymously."""

    ANONYMOUS = "anonymous"
    #: The store itself was not read: its place is refused, or it would not open.
    NOT_READ = "not-read"
    #: It opened, but declares its data kept in a kind of place we do not read from.
    KEPT_ELSEWHERE = "kept-elsewhere"


#: The order a store's axes are judged in: what an operator can do least about first.
AXES = ("placement", "shape", "format", "access")

#: The one table of what is accepted. Accepting another value is adding it
#: here, with the reader or enumerator it needs (ADR 0052 §4).
ACCEPTED = {
    "placement": {Placement.PER_COLLECTION},
    "shape": {StoreShape.MAP},
    "format": {Format.ICECHUNK},
    "access": {Access.ANONYMOUS},
}

#: The verdict an operator is given for each refused value.
_REFUSED_AS = {
    Placement.PER_ITEM: Shape.STORE_PER_ITEM,
    Placement.PER_ITEM_PER_VARIABLE: Shape.STORE_PER_ITEM_PER_VARIABLE,
    StoreShape.SERIES: Shape.SERIES_STORE,
    StoreShape.TILED: Shape.UNREADABLE_STORE,
    StoreShape.NOT_A_GRID: Shape.UNGRIDDED_STORE,
    StoreShape.UNKNOWN: Shape.UNREADABLE_STORE,
    Format.PLAIN_ZARR: Shape.PLAIN_ZARR_STORE,
    Format.UNDECODABLE: Shape.UNDECODABLE_STORE,
    Access.NOT_READ: Shape.NEEDS_ACCOUNT,
    Access.KEPT_ELSEWHERE: Shape.STORE_KEPT_ELSEWHERE,
}


@dataclass(frozen=True)
class StoreAxes:
    """The value the survey found on each axis of an Upstream store; `None`
    where it could not be told — a store kept on the items is judged on its
    placement alone, and one that was not read has no format. Each value may
    carry what an operator is told of it beside its verdict."""

    placement: Placement
    shape: StoreShape | None = None
    format: Format | None = None
    access: Access | None = None
    #: What was missing (UNKNOWN), or why it is neither shape (TILED).
    shape_said: str = ""
    #: The codecs that cannot be decoded (UNDECODABLE).
    format_said: str = ""
    #: Why the store was not read (NOT_READ), or the kinds of place its data is kept in (KEPT_ELSEWHERE).
    access_said: str = ""

    @property
    def refused(self) -> tuple[str, StrEnum] | None:
        """The first axis, in `AXES` order, whose value is not accepted, with
        that value; `None` when every axis that could be told is."""
        for axis in AXES:
            value = getattr(self, axis)
            if value is not None and value not in ACCEPTED[axis]:
                return axis, value
        return None

    def summary(self) -> str:
        """Each axis that could be told with its value, in order: for the verdict's technical details."""
        return " · ".join(f"{axis}: {getattr(self, axis)}" for axis in AXES if getattr(self, axis) is not None)

    def verdict(self) -> tuple[Shape, str] | None:
        """The verdict and its reason, in an operator's words, for the axis that
        stops it; `None` when nothing does."""
        refused = self.refused
        if refused is None:
            return None
        axis, value = refused
        shape = _REFUSED_AS[value]
        reason = _REASONS[shape]
        if value in (StoreShape.TILED, StoreShape.UNKNOWN):
            reason = f"{reason}: {self.shape_said}."
        elif value is StoreShape.SERIES and self.access is Access.NOT_READ:
            said = "GeoRiva read this from the collection's description, as its data could not be opened"
            reason = f"{reason} {said} ({self.access_said})."
        elif value is Format.UNDECODABLE:
            reason = f"{reason}: {self.format_said}."
        elif value is Access.NOT_READ:
            reason = f"{reason} ({self.access_said})"
            if self.shape is StoreShape.MAP:
                # What the collection says of its shape is named too: it is not what stops it (ADR 0052 §2).
                reason += " The collection's description says it is laid out for drawing maps."
        elif value is Access.KEPT_ELSEWHERE:
            reason = f"{reason}: {self.access_said}."
        return shape, reason


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
class SurveyedArray:
    """One array of an Upstream store that is a field on its grid: offered to
    become a Variable, or listed with why it cannot (ADR 0044 §8)."""

    #: Its full path in the store: the Variable mapping's key.
    path: str
    title: str = ""
    unit: str = ""
    description: str = ""
    #: Each of its axes' part, in its own order — "run" and "step", or "time",
    #: then "y" and "x"; empty for an array that is not offered.
    axes: list[str] = field(default_factory=list)
    #: Why it cannot become a Variable, for an operator; empty when it can.
    reason: str = ""

    @property
    def offered(self) -> bool:
        return not self.reason


@dataclass(frozen=True)
class SurveyedStore:
    """An Upstream store, as far as the survey got with it."""

    #: The `https` address it was, or would have been, opened at.
    href: str
    #: How many of its arrays are fields on its grid: the ones its shape was read from.
    fields: int = 0
    #: Every codec its arrays name; empty when it was never opened.
    codecs: list[str] = field(default_factory=list)
    #: Where a virtual store declares its chunks live, as the collection spells
    #: them. Proposed for confirmation when a feed is set up, not before.
    chunk_locations: list[str] = field(default_factory=list)
    #: The region of each of those that needs one to be read, where it could be
    #: found. One that needs a region and has none here cannot be read from.
    chunk_regions: dict[str, str] = field(default_factory=dict)
    #: Its fields, offered or not; empty unless the store was accepted.
    arrays: list[SurveyedArray] = field(default_factory=list)

    @property
    def offered(self) -> list[SurveyedArray]:
        return [array for array in self.arrays if array.offered]

    @property
    def has_runs(self) -> bool:
        """Whether it is a forecast store: its window rolls on run time (ADR 0044 §7)."""
        return any("run" in array.axes for array in self.arrays)


@dataclass(frozen=True)
class CollectionSurvey:
    shape: Shape
    reason: str = ""
    items_sampled: int = 0
    assets: list[SurveyedAsset] = field(default_factory=list)
    locations: list[FileLocation] = field(default_factory=list)
    #: Set when the collection is an Upstream store, whatever the verdict on it.
    store: SurveyedStore | None = None
    #: The value found on each axis, when the collection is kept as stores (ADR 0052).
    axes: StoreAxes | None = None
    #: Technical facts for whoever supports the operator, by key: `address`
    #: opened, `error` as the library said it, what was `found` on each axis.
    #: Never the headline: that is `reason`, in an operator's words (#825).
    details: dict[str, str] = field(default_factory=dict)

    @property
    def harvestable(self) -> bool:
        """Whether a feed can be set up from it: the wizard goes on only for this."""
        return self.shape in (Shape.ONE_COG_PER_TIME, Shape.MAP_STORE)

    @property
    def accepted(self) -> bool:
        """Whether GeoRiva can show it."""
        return self.harvestable

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
    if not root.is_api and root.own:
        # The address is one collection's own document: that one is what it offers.
        return CatalogSurvey(has_api=False, collections=[root.own])
    try:
        return CatalogSurvey(has_api=root.is_api, collections=list(upstream.list_collections()))
    except UpstreamUnreachable:
        # Its collections do not answer; its root still links to each of them.
        return CatalogSurvey(has_api=root.is_api, collections=list(root.children), described=False)


#: How much of a collection's description its card keeps: the first paragraph
#: on its face, and the rest, behind "More", up to this many characters.
CARD_SUMMARY_CHARS = 300
CARD_DETAILS_CHARS = 1500
#: How many of the publisher's stated facts a card shows on its face, and how many keywords.
CARD_FACTS = 2
CARD_KEYWORDS = 4
#: How short of the whole world, in degrees, an extent may fall and still be called global.
GLOBAL_TOLERANCE = 1.0

_MD_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_TAG = re.compile(r"<[^>]+>")
_MD_MARKS = re.compile(r"(\*\*|__|\*|`)")
_MD_LINE_START = re.compile(r"^\s{0,3}(#{1,6}\s+|>\s?|[-*+]\s+|\d+\.\s+)")


def collection_card(collection: UpstreamCollection) -> dict:
    """What step 2 shows of one collection, in a form the session can keep: only
    what the collection itself says, trimmed. A collection known by id alone
    gives a card with its id and title and nothing else."""
    paragraphs = _plain_paragraphs(collection.description)
    summary = _clip(paragraphs[0], CARD_SUMMARY_CHARS) if paragraphs else ""
    details = _clip("\n\n".join(paragraphs[1:]), CARD_DETAILS_CHARS)
    if paragraphs and len(paragraphs[0]) > CARD_SUMMARY_CHARS:
        details = _clip("\n\n".join(paragraphs), CARD_DETAILS_CHARS)
    facts = [[_label(key), value] for key, value in collection.stated.items()]
    start, end = collection.interval or (None, None)
    producer = next((p.name for p in collection.providers if "producer" in p.roles), "")
    producer = producer or (collection.providers[0].name if collection.providers else "")
    keywords = list(dict.fromkeys(collection.keywords))[:CARD_KEYWORDS]
    stores = [asset for asset in collection.assets.values() if _is_store(asset)]
    bbox = collection.bbox
    title = collection.title or collection.id
    return {
        "id": collection.id,
        "title": title,
        "summary": summary,
        "details": details,
        "store": bool(stores),
        "variables": len(collection.cube.arrays) if stores and collection.cube and collection.cube.arrays else None,
        "license": collection.license,
        "start": start.date().isoformat() if start else "",
        "end": end.date().isoformat() if end else "",
        "ongoing": bool(start) and end is None,
        "global": _is_global(bbox),
        "bbox": [round(v, 1) for v in bbox] if bbox and not _is_global(bbox) else None,
        "producer": producer,
        "facts": facts[:CARD_FACTS],
        "more_facts": facts[CARD_FACTS:],
        "keywords": keywords,
        "search": " ".join([title, collection.id, summary, producer, *keywords]).lower(),
    }


def _plain_paragraphs(text: str) -> list[str]:
    """A description's paragraphs as plain text: its markdown and markup gone,
    and a paragraph that was only a heading or an image left out."""
    paragraphs = []
    for block in re.split(r"\n\s*\n", text.replace("\r\n", "\n")):
        lines = [line for line in block.strip().splitlines() if line.strip()]
        if not lines or all(line.lstrip().startswith("#") for line in lines):
            continue
        plain = " ".join(_MD_LINE_START.sub("", line).strip() for line in lines)
        plain = _MD_MARKS.sub("", _MD_TAG.sub("", _MD_LINK.sub(r"\1", _MD_IMAGE.sub("", plain))))
        plain = re.sub(r"\s+", " ", plain).strip()
        if plain:
            paragraphs.append(plain)
    return paragraphs


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].rstrip(",.;:") + "…"


def _label(key: str) -> str:
    """A summary's key as a label: `spatial_resolution` → "Spatial resolution"."""
    words = re.sub(r"[_\-]+", " ", key.rpartition(":")[2]).strip()
    return words[:1].upper() + words[1:]


def _is_global(bbox: list[float] | None) -> bool:
    if not bbox:
        return False
    west, south, east, north = bbox
    return east - west >= 360 - GLOBAL_TOLERANCE and north - south >= 180 - GLOBAL_TOLERANCE


# ── One collection ──────────────────────────────────────────────────────────


#: What the survey is waiting on, as an operator is told it while they wait.
#: No word here is one they would have to look up, and none ends in a full
#: stop: the waiting page follows each with "…".
WAITING_TO_START = "Waiting for GeoRiva to start the check: it is busy with other work"
READING_CATALOG = "Reading the catalog"
READING_COLLECTION = "Reading what the catalog says of this collection"
READING_ENTRIES = "Looking at a few of the collection's entries"
OPENING_FILES = "Opening a few of its image files to check they can be read"
OPENING_STORE = (
    "Opening the provider's data store to check GeoRiva can unpack it. A large store can take up to a minute"
)
FINDING_PLACES = "Finding where the store's data is kept"


def _silent(_text: str) -> None:
    pass


def survey_collection(
    upstream: UpstreamCatalogClient,
    catalog_url: str,
    collection_id: str,
    *,
    progress: Callable[[str], None] = _silent,
) -> CollectionSurvey:
    """How `collection_id` is organised and whether it can be harvested.

    Raises `UpstreamUnreachable` when the catalog's root or the collection's
    items cannot be listed, and `UpstreamTimedOut` when the Upstream did not
    answer in time — its store included: a wait is worth trying again, and is
    no verdict. Everything else about its files is reported, not raised.

    `progress` is told, in an operator's words, what it is about to wait on.
    """
    progress(READING_CATALOG)
    if not upstream.describe_root().is_api:
        # A catalog of plain files cannot list items by time, so only a store in it can be read at all.
        progress(READING_COLLECTION)
        described = upstream.describe_collection(collection_id)
        if described is not None and (found := _survey_store(upstream, catalog_url, described, progress=progress)):
            return found
        # Two things are wrong at once with a store that describes nothing, in a
        # catalog of plain files, and the store is the one an operator can do nothing about.
        stored = described is not None and any(_is_store(asset) for asset in described.assets.values())
        return _verdict(Shape.STORE if stored else Shape.NO_API)

    progress(READING_ENTRIES)
    items = list(islice(upstream.list_items(collection_id, (None, None)), SAMPLE_ITEMS))
    sampled = len(items)

    rasters = {key for item in items for key, asset in item.assets.items() if _is_raster(asset)}
    if not rasters:
        progress(READING_COLLECTION)
        described = upstream.describe_collection(collection_id)
        if described is not None and (
            found := _survey_store(upstream, catalog_url, described, sampled, progress=progress)
        ):
            return found
        if placed := _placed_in_items(items):
            axes = StoreAxes(placement=placed)
            shape, reason = axes.verdict()
            return CollectionSurvey(shape, reason, sampled, axes=axes, details={"found": axes.summary()})
        if described is not None and any(_is_store(asset) for asset in described.assets.values()):
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
    progress(OPENING_FILES)
    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as pool:
        assets = list(pool.map(lambda found: _survey_asset(*found, upstream), located))
    locations = places.locations(assets)
    if not any(asset.readable for asset in assets):
        problem = next((asset.problem for asset in assets if asset.problem), "")
        reason = f"{_REASONS[Shape.NEEDS_ACCOUNT]} ({problem})" if problem else _REASONS[Shape.NEEDS_ACCOUNT]
        return CollectionSurvey(Shape.NEEDS_ACCOUNT, reason, sampled, assets, locations)
    return CollectionSurvey(Shape.ONE_COG_PER_TIME, "", sampled, assets, locations)


def _placed_in_items(items) -> Placement | None:
    """The Store placement of a collection whose stores are on its items, not
    on itself; `None` when its items carry none (ADR 0052 §5). An item with more
    than one store keeps one per variable. Stores are told apart by name, so
    one store an item publishes twice — at an `https` and an `s3://` address —
    is one store."""
    most = max((len(_store_names(item)) for item in items), default=0)
    if most > 1:
        return Placement.PER_ITEM_PER_VARIABLE
    return Placement.PER_ITEM if most else None


def _store_names(item) -> set[str]:
    return {_path(asset).rpartition("/")[2] for asset in item.assets.values() if _is_store(asset)}


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


# ── An Upstream store ───────────────────────────────────────────────────────

#: Names a time-like dimension goes by where the collection does not call it temporal.
_STEP_NAMES = frozenset(
    {
        "time",
        "init_time",
        "lead_time",
        "valid_time",
        "step",
        "forecast_period",
        "reference_time",
        "forecast_reference_time",
    }
)
_HORIZONTAL_NAMES = {"x": "x", "lon": "x", "longitude": "x", "y": "y", "lat": "y", "latitude": "y"}


def _store_asset(described: UpstreamCollection) -> UpstreamAsset | None:
    """The asset an Upstream store is opened at: the `https` one where the
    collection publishes the same store at an `s3://` address too."""
    stores = [asset for asset in described.assets.values() if asset.href and _is_store(asset)]
    stores.sort(key=lambda asset: urlsplit(asset.href).scheme.lower() != "https")
    return stores[0] if stores else None


def _survey_store(
    upstream: UpstreamCatalogClient,
    catalog_url: str,
    described: UpstreamCollection,
    sampled: int = 0,
    *,
    progress: Callable[[str], None] = _silent,
) -> CollectionSurvey | None:
    """The verdict on `described` as an Upstream store, or `None` when it is
    not one: a store asset on the collection itself, and a cube described.

    The store is opened first, wherever its place lets a harvest read it, and
    only its metadata is read: how its arrays are divided up — the Store shape
    — and which codecs they name come from the store itself. What the collection
    publishes is the fallback, for a store that cannot be opened (ADR 0052 §1,
    §2). Where several things stop it, the shape is named before the access.
    """
    asset, cube = _store_asset(described), described.cube
    if asset is None or cube is None:
        return None
    # Every store asset's declaration counts: the `s3://` twin often carries what the `https` one leaves out.
    declared = sorted({_as_prefix(place) for other in described.assets.values() for place in other.chunk_locations})
    places = _Places(upstream, catalog_url)
    location, href = places.of(asset.href)

    def verdict(shape, reason, axes, *, readable=False, problem="", **store):
        here = FileLocation(location.host, location.bucket, location.own, readable, location.problem or problem)
        said = SurveyedStore(href=href, chunk_locations=declared, **store)
        error = location.problem or (probe.detail if probe else "")
        details = {"address": href, "error": error, "found": axes.summary()}
        details = {key: value for key, value in details.items() if value}
        return CollectionSurvey(shape, reason, sampled, locations=[here], store=said, axes=axes, details=details)

    # Judged as a harvest will judge it, and never opened anywhere that judgement refuses.
    probe, why_not_read = None, location.problem
    if not why_not_read:
        progress(OPENING_STORE)
        probe = upstream.describe_store(href)
        if probe.timed_out and _store_shape(cube)[0] is not StoreShape.SERIES:
            # A wait is worth trying again, unless what the collection says already refuses it.
            said = (probe.problem or "did not answer in time").removeprefix("it ")
            details = {key: value for key, value in {"address": href, "error": probe.detail}.items() if value}
            raise UpstreamTimedOut(f"the provider's data store {said}", details=details)
        if not probe.readable:
            why_not_read = probe.problem or "it could not be opened"
    read = not why_not_read
    if read:
        cube = _as_the_store_says(cube, probe.chunking, held=probe.codecs.keys())

    shape, fields, shape_said = _store_shape(cube)
    if shape is StoreShape.UNKNOWN and not read:
        shape, shape_said = None, ""  # nothing to blame the collection for: STAC does not ask it to say
    named = list(dict.fromkeys(codec for codecs in probe.codecs.values() for codec in codecs)) if read else []
    axes = StoreAxes(
        placement=Placement.PER_COLLECTION,
        shape=shape,
        shape_said=shape_said,
        **(_format_of(probe, named) if read else {}),
        **(_access_of(described, declared) if read else {"access": Access.NOT_READ, "access_said": why_not_read}),
    )
    store = {"readable": read, "fields": fields, "codecs": named}
    store["problem"] = why_not_read if probe and not read else ""
    if (refused := axes.verdict()) is not None:
        return verdict(*refused, axes, **store)

    # Every axis is accepted: what is left is which of its arrays can be shown (ADR 0044 §8).
    arrays = _store_arrays(cube)
    if not any(array.offered for array in arrays):
        why = "; ".join(dict.fromkeys(array.reason for array in arrays))
        return verdict(Shape.UNMAPPABLE_STORE, f"{_REASONS[Shape.UNMAPPABLE_STORE]} {why}", axes, **store)
    progress(FINDING_PLACES)
    regions = _chunk_regions(upstream, declared)
    return verdict(Shape.MAP_STORE, "", axes, **store, arrays=arrays, chunk_regions=regions)


def _format_of(probe, named: list[str]) -> dict:
    """The format axis of a store that was read: its kind, and whether every codec it names decodes."""
    if probe.store_format is not StoreFormat.ICECHUNK:
        return {"format": Format.PLAIN_ZARR}
    unknown = [codec for codec in named if not can_decode(codec)]
    if unknown:
        return {"format": Format.UNDECODABLE, "format_said": ", ".join(f"“{codec}”" for codec in unknown)}
    return {"format": Format.ICECHUNK}


def _access_of(described: UpstreamCollection, declared: list[str]) -> dict:
    """The access axis of a store that was read: whether every place its data
    is declared to live is a kind we read anonymously (ADR 0044 §11)."""
    # Every asset's word counts: one twin may say what the other leaves out.
    own_endpoint = {_as_prefix(place) for other in described.assets.values() for place in other.chunk_endpoints}
    elsewhere = [kind for kind in dict.fromkeys(_kind_of_place(p, p in own_endpoint) for p in declared) if kind]
    if elsewhere:
        return {"access": Access.KEPT_ELSEWHERE, "access_said": ", ".join(elsewhere)}
    return {"access": Access.ANONYMOUS}


#: What an operator is told a place GeoRiva does not read from is, by how its address begins.
_OTHER_PLACES = {
    "az": "Microsoft Azure storage",
    "azure": "Microsoft Azure storage",
    "abfs": "Microsoft Azure storage",
    "abfss": "Microsoft Azure storage",
    "http": "a plain web address",
    "https": "a plain web address",
    "file": "a folder on the provider's own computer",
}


def _as_prefix(declared: str) -> str:
    """A declared place as the prefix it is: a bucket or folder spelled without
    its closing slash names the same place, and only the slashed spelling is
    one a store's reader takes."""
    return f"{declared}/" if chunk_place(f"{declared}/") and not declared.endswith("/") else declared


def _kind_of_place(declared: str, own_endpoint: bool) -> str:
    """What kind of place `declared` is, in an operator's words, when it is
    one a store's data cannot be pinned in; empty when it can (ADR 0044 §11)."""
    if own_endpoint:
        return f"a storage service at an address of its own (“{declared}”)"
    if chunk_place(declared) is not None:
        return ""
    scheme = declared.partition("://")[0].lower() if "://" in declared else ""
    return f"{_OTHER_PLACES.get(scheme, 'a kind of storage GeoRiva does not know')} (“{declared}”)"


def _chunk_regions(upstream: UpstreamCatalogClient, declared: list[str]) -> dict[str, str]:
    """The region of each declared place that is read by region, where S3
    itself would say it. The repository's own word for it is never used."""
    regions = {}
    for place in declared:
        named = chunk_place(place)
        if named is None or named.kind != "s3":
            continue
        region = upstream.bucket_region(named.bucket)
        # Only a bare region name is ever handed to a reader, whatever answered for the bucket.
        if region and is_region_name(region):
            regions[place] = region
    return regions


# ── Which of a store's arrays can become Variables ──────────────────────────

_GEOGRAPHIC_NAMES = frozenset({"lat", "latitude", "lon", "longitude"})
_GEOGRAPHIC_SYSTEMS = ("4326", "crs84", "wgs84", "wgs 84")
#: Names the dimension that says which run a forecast field is of.
_RUN_NAMES = frozenset({"init_time", "reference_time", "forecast_reference_time", "run", "run_time"})
#: What a dimension that is neither time nor the grid is taken for, by a word in its name.
_OTHER_DIMENSIONS = (
    (("level", "height", "pressure", "depth", "altitude", "layer"), "It has levels."),
    (("ensemble", "member", "realization", "realisation"), "It has ensemble members."),
    (("statistic", "quantile", "percentile"), "It holds statistics, not one map per time step."),
)


def _store_arrays(cube: UpstreamCube) -> list[SurveyedArray]:
    """Every field of a map-shaped store: those whose dimensions are exactly
    run, step, latitude and longitude — or time, latitude and longitude — to
    be offered, and each other with its reason (ADR 0044 §8). A store's
    coordinates, which are on no grid, are not fields and are not listed."""
    horizontal = _horizontal(cube)
    found = []
    for array in cube.arrays.values():
        if len(horizontal.keys() & set(array.dimensions)) != 2:
            continue
        axes, reason = _axes(array, cube, horizontal)
        found.append(
            SurveyedArray(
                path=array.path,
                title=array.title,
                unit=array.unit,
                description=array.description,
                axes=[] if reason else axes,
                reason=reason,
            )
        )
    # One store, one window: where a forecast's fields and plain time steps sit together, the forecast's are offered.
    if any("run" in array.axes for array in found):
        found = [
            array
            if "time" not in array.axes
            else SurveyedArray(
                array.path, array.title, array.unit, array.description, reason="It is not part of the forecast."
            )
            for array in found
        ]
    return found


def store_fields(cube: UpstreamCube, paths: Iterable[str]) -> list[StoreField]:
    """Each of `paths` that is still a field a Variable can be mapped from, with
    which of its axes is which: what a harvest asks a store about. A path the
    cube no longer offers is left out."""
    offered = {array.path: array for array in _store_arrays(cube) if array.offered}
    return [StoreField(path, tuple(offered[path].axes)) for path in paths if path in offered]


def store_media_type(described: UpstreamCollection) -> str:
    """The media type the collection gives the store a feed reads; empty when it gives none."""
    asset = _store_asset(described)
    return asset.media_type if asset is not None else ""


def _horizontal(cube: UpstreamCube) -> dict[str, str]:
    """Each horizontal dimension of a cube with its axis, "x" or "y"."""
    found = {}
    for name, dimension in cube.dimensions.items():
        if dimension.kind not in ("spatial", ""):
            continue
        axis = dimension.axis if dimension.axis in ("x", "y") else _HORIZONTAL_NAMES.get(name.lower(), "")
        if axis:
            found[name] = axis
    return found


def _is_geographic(dimension: CubeDimension) -> bool:
    system = dimension.reference_system.lower()
    if system:
        return any(known in system for known in _GEOGRAPHIC_SYSTEMS)
    return dimension.name.lower() in _GEOGRAPHIC_NAMES


def _axes(array: CubeArray, cube: UpstreamCube, horizontal: dict[str, str]) -> tuple[list[str], str]:
    """`(each axis's part, "")` for an array that can become a Variable, or
    `([], why not)` in an operator's words."""
    axes, times = [], []
    for position, name in enumerate(array.dimensions):
        dimension = cube.dimensions.get(name)
        if name in horizontal:
            if not _is_geographic(dimension):
                return [], "It is on a projected grid."
            axes.append(horizontal[name])
        elif (dimension is not None and dimension.kind == "temporal") or name.lower() in _STEP_NAMES:
            times.append(position)
            axes.append("time")
        else:
            lowered = name.lower()
            known = next((why for words, why in _OTHER_DIMENSIONS if any(word in lowered for word in words)), None)
            return [], known or f"It is divided up in a way GeoRiva does not know (“{name}”)."
    if not times:
        return [], "It has no time steps."
    if len(times) > 2:
        return [], "It has more kinds of time step than GeoRiva can show."
    if len(times) == 2:
        run = _run_axis(array, cube, times)
        for position in times:
            axes[position] = "run" if position == run else "step"
    return axes, ""


def _run_axis(array: CubeArray, cube: UpstreamCube, times: list[int]) -> int:
    """Which of a forecast field's two time-like axes says the run: the one
    named for it, else the one that is a date where the other is a length of
    time, else the first."""
    named = [position for position in times if array.dimensions[position].lower() in _RUN_NAMES]
    if len(named) == 1:
        return named[0]
    dated = [
        position
        for position in times
        if (dimension := cube.dimensions.get(array.dimensions[position])) is not None and dimension.kind == "temporal"
    ]
    return dated[0] if len(dated) == 1 else times[0]


def _as_the_store_says(cube: UpstreamCube, chunking: dict[str, StoreChunking], *, held) -> UpstreamCube:
    """`cube` with what the store itself says in place of what the collection
    published: which of its arrays the store holds (`held`), each one's
    chunking, and each dimension's size (ADR 0052 §1). What kind each dimension
    is stays the collection's to say, and so do the names it is known by
    wherever the store names it otherwise; an array the store says nothing
    readable of keeps the collection's word."""
    arrays, sizes = {}, {}
    for path, array in cube.arrays.items():
        if path not in held:
            continue
        said = chunking.get(path)
        named = said.dimensions if said else ()
        unknown = not named or not set(named) <= cube.dimensions.keys()
        if said and unknown and len(array.dimensions) == len(said.shape):
            named = array.dimensions  # the store's own names, or none, in the collection's order
        if not said or not named:
            arrays[path] = array
            continue
        arrays[path] = replace(array, dimensions=named, chunks=said.chunks)
        for name, size in zip(named, said.shape, strict=True):
            sizes.setdefault(name, size)
    dimensions = {name: replace(d, size=sizes.get(name, d.size)) for name, d in cube.dimensions.items()}
    return UpstreamCube(dimensions=dimensions, arrays=arrays)


def _store_shape(cube: UpstreamCube) -> tuple[StoreShape, int, str]:
    """`(shape, how many fields it was read from, what was missing)` for a
    store, from the chunking `cube` gives: the store's own, where it was read
    (`_as_the_store_says`), else what its collection published.

    A field is an array on the horizontal grid. Map-shaped: every field's
    chunk covers the whole grid and one step of each time-like dimension.
    Series-shaped: some field's chunk spans more than one step. What is
    missing is said in an operator's words, and leaves the shape unknown.
    """
    horizontal = _horizontal(cube)
    across = set(horizontal)
    if set(horizontal.values()) != {"x", "y"}:
        return StoreShape.NOT_A_GRID, 0, ""
    fields = [array for array in cube.arrays.values() if len(across & set(array.dimensions)) == 2]
    if not fields:
        return StoreShape.UNKNOWN, 0, "the collection lists no fields on its grid"

    shapes = set()
    for array in fields:
        found, missing = _field_shape(array, cube, across)
        if missing:
            return StoreShape.UNKNOWN, len(fields), missing
        shapes.add(found)
    if StoreShape.SERIES in shapes:
        return StoreShape.SERIES, len(fields), ""
    if shapes != {StoreShape.MAP}:
        return StoreShape.TILED, len(fields), "its fields are stored as many small tiles, which GeoRiva cannot read yet"
    return StoreShape.MAP, len(fields), ""


def _field_shape(array: CubeArray, cube: UpstreamCube, across: set[str]) -> tuple[StoreShape | None, str]:
    """One field's shape, or what its description is missing. `TILED` is a
    field cut into tiles within a single step."""
    if array.chunks is None or len(array.chunks) != len(array.dimensions):
        return None, f"nothing says how “{array.path}” is divided up"
    whole = True
    for name, span in zip(array.dimensions, array.chunks, strict=True):
        dimension = cube.dimensions.get(name)
        if name in across:
            if dimension.size is None:
                return None, f"nothing gives the size of its grid along “{name}”"
            whole = whole and span >= dimension.size
        elif (dimension is not None and dimension.kind == "temporal") or name.lower() in _STEP_NAMES:
            if span > 1:
                return StoreShape.SERIES, ""
    return (StoreShape.MAP if whole else StoreShape.TILED), ""


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
