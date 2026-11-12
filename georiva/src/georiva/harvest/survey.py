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

It reads through the Upstream catalog client and writes nothing. It also never
opens an address the Asset host allowlist would refuse: a file's place is
judged first, by the rule a harvest judges it by, and only then read.

Give it a client that pages small (`StacApiClient(url, page_size=SAMPLE_ITEMS)`):
it reads one page of a listing and stops.
"""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
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
    StoreField,
    UpstreamAsset,
    UpstreamCatalogClient,
    UpstreamCollection,
    UpstreamCube,
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
_STORE_SUFFIXES = (".zarr", ".icechunk", ".nc", ".nc4", ".h5", ".hdf5", ".grib", ".grib2")
_STORE_TYPES = ("zarr", "icechunk", "netcdf", "hdf", "grib")


class Shape(StrEnum):
    """How a collection is organised. A feed can be set up from the first two."""

    ONE_COG_PER_TIME = "one-cog-per-time"
    #: An Upstream store whose chunks are each one whole field at one step.
    MAP_STORE = "map-shaped-store"
    #: An Upstream store whose chunks each span many steps over a small area.
    SERIES_STORE = "series-shaped-store"
    #: An Upstream store naming a codec nobody installed here.
    UNDECODABLE_STORE = "undecodable-store"
    #: An Upstream store whose shape its collection does not say.
    UNREADABLE_STORE = "unreadable-store"
    #: An Upstream store declaring that its data lives in a kind of place we do not read from.
    STORE_KEPT_ELSEWHERE = "store-kept-elsewhere"
    #: A map-shaped Upstream store none of whose arrays is a plain field on a latitude/longitude grid.
    UNMAPPABLE_STORE = "unmappable-store"
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


# ── One collection ──────────────────────────────────────────────────────────


def survey_collection(upstream: UpstreamCatalogClient, catalog_url: str, collection_id: str) -> CollectionSurvey:
    """How `collection_id` is organised and whether it can be harvested.

    Raises `UpstreamUnreachable` when the catalog's root or the collection's
    items cannot be listed; everything about its files is reported, not raised.
    """
    if not upstream.describe_root().is_api:
        # A catalog of plain files cannot list items by time, so only a store in it can be read at all.
        described = upstream.describe_collection(collection_id)
        if described is not None and (found := _survey_store(upstream, catalog_url, described)):
            return found
        # Two things are wrong at once with a store that describes nothing, in a
        # catalog of plain files, and the store is the one an operator can do nothing about.
        stored = described is not None and any(_is_store(asset) for asset in described.assets.values())
        return _verdict(Shape.STORE if stored else Shape.NO_API)

    items = list(islice(upstream.list_items(collection_id, (None, None)), SAMPLE_ITEMS))
    sampled = len(items)

    rasters = {key for item in items for key, asset in item.assets.items() if _is_raster(asset)}
    if not rasters:
        described = upstream.describe_collection(collection_id)
        if described is not None and (found := _survey_store(upstream, catalog_url, described, sampled)):
            return found
        stores = [asset for item in items for asset in item.assets.values()]
        stores += list(described.assets.values()) if described else []
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
    upstream: UpstreamCatalogClient, catalog_url: str, described: UpstreamCollection, sampled: int = 0
) -> CollectionSurvey | None:
    """The verdict on `described` as an Upstream store, or `None` when it is
    not one: a store asset on the collection itself, and a cube described.

    The Store shape is read from what the collection publishes, and costs
    nothing; the store is opened only once that says map-shaped, to learn
    whether every codec its arrays name is one we can decode.
    """
    asset, cube = _store_asset(described), described.cube
    if asset is None or cube is None:
        return None
    # Every store asset's declaration counts: the `s3://` twin often carries what the `https` one leaves out.
    declared = sorted({_as_prefix(place) for other in described.assets.values() for place in other.chunk_locations})
    places = _Places(upstream, catalog_url)
    location, href = places.of(asset.href)

    def verdict(shape, reason, *, readable=False, problem="", **store):
        here = FileLocation(location.host, location.bucket, location.own, readable, location.problem or problem)
        said = SurveyedStore(href=href, chunk_locations=declared, **store)
        return CollectionSurvey(shape, reason, sampled, locations=[here], store=said)

    layout, fields, missing = _store_shape(cube)
    if missing:
        return verdict(Shape.UNREADABLE_STORE, f"{_REASONS[Shape.UNREADABLE_STORE]}: {missing}.", fields=fields)
    if layout is Shape.SERIES_STORE:
        return verdict(Shape.SERIES_STORE, _REASONS[Shape.SERIES_STORE], fields=fields)
    if location.problem:
        # Judged as a harvest will judge it, and never opened anywhere that judgement refuses.
        return verdict(Shape.NEEDS_ACCOUNT, f"{_REASONS[Shape.NEEDS_ACCOUNT]} ({location.problem})", fields=fields)
    # Every asset's word counts here too: one twin may say what the other leaves out.
    own_endpoint = {_as_prefix(place) for other in described.assets.values() for place in other.chunk_endpoints}
    elsewhere = list(dict.fromkeys(_kind_of_place(place, place in own_endpoint) for place in declared))
    if any(elsewhere):
        kinds = ", ".join(kind for kind in elsewhere if kind)
        return verdict(Shape.STORE_KEPT_ELSEWHERE, f"{_REASONS[Shape.STORE_KEPT_ELSEWHERE]}: {kinds}.", fields=fields)

    probe = upstream.describe_store(href)
    if not probe.readable:
        problem = probe.problem or "it could not be opened"
        reason = f"{_REASONS[Shape.NEEDS_ACCOUNT]} ({problem})"
        return verdict(Shape.NEEDS_ACCOUNT, reason, problem=problem, fields=fields)
    named = list(dict.fromkeys(codec for codecs in probe.codecs.values() for codec in codecs))
    unknown = [codec for codec in named if not can_decode(codec)]
    if unknown:
        reason = f"{_REASONS[Shape.UNDECODABLE_STORE]}: {', '.join(f'“{codec}”' for codec in unknown)}."
        return verdict(Shape.UNDECODABLE_STORE, reason, readable=True, fields=fields, codecs=named)
    arrays = _store_arrays(cube)
    if not any(array.offered for array in arrays):
        why = "; ".join(dict.fromkeys(array.reason for array in arrays))
        reason = f"{_REASONS[Shape.UNMAPPABLE_STORE]} {why}"
        return verdict(Shape.UNMAPPABLE_STORE, reason, readable=True, fields=fields, codecs=named)
    regions = _chunk_regions(upstream, declared)
    return verdict(
        Shape.MAP_STORE, "", readable=True, fields=fields, codecs=named, arrays=arrays, chunk_regions=regions
    )


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


def _store_shape(cube: UpstreamCube) -> tuple[Shape | None, int, str]:
    """`(shape, how many fields it was read from, what was missing)` for a
    store, from its collection's published chunking alone.

    A field is an array on the horizontal grid. Map-shaped: every field's
    chunk covers the whole grid and one step of each time-like dimension.
    Series-shaped: some field's chunk spans more than one step. What is
    missing is said in an operator's words, and leaves the shape unknown.
    """
    horizontal = _horizontal(cube)
    across = set(horizontal)
    if set(horizontal.values()) != {"x", "y"}:
        return None, 0, "the collection does not describe a grid with a width and a height"
    fields = [array for array in cube.arrays.values() if len(across & set(array.dimensions)) == 2]
    if not fields:
        return None, 0, "the collection lists no fields on its grid"

    shapes = set()
    for array in fields:
        layout, missing = _field_shape(array, cube, across)
        if missing:
            return None, len(fields), missing
        shapes.add(layout)
    if Shape.SERIES_STORE in shapes:
        return Shape.SERIES_STORE, len(fields), ""
    if shapes != {Shape.MAP_STORE}:
        return None, len(fields), "its fields are stored as many small tiles, which GeoRiva cannot read yet"
    return Shape.MAP_STORE, len(fields), ""


def _field_shape(array: CubeArray, cube: UpstreamCube, across: set[str]) -> tuple[Shape | None, str]:
    """One field's shape, or what its description is missing. `None` with
    nothing missing is a field cut into tiles within a single step."""
    if array.chunks is None or len(array.chunks) != len(array.dimensions):
        return None, f"the collection does not say how “{array.path}” is divided up"
    whole = True
    for name, span in zip(array.dimensions, array.chunks, strict=True):
        dimension = cube.dimensions.get(name)
        if name in across:
            if dimension.size is None:
                return None, f"the collection does not give the size of its grid along “{name}”"
            whole = whole and span >= dimension.size
        elif (dimension is not None and dimension.kind == "temporal") or name.lower() in _STEP_NAMES:
            if span > 1:
                return Shape.SERIES_STORE, ""
    return (Shape.MAP_STORE if whole else None), ""


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
