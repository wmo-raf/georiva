"""
The harvest: one run of a Harvest feed, on the run-tracking spine (ADR 0042).

It lists the feed's window of an Upstream and indexes what it finds as ordinary
Published rows whose assets point upstream. No pixel is read and no file is
fetched: the run is a `FetchRun` with no `FetchedFile` rows, and its counts are
its own.

The Upstream is items of COGs, or an Upstream store (ADR 0044). Which, is asked
in one place — `_listed` — and answered as the same list either way: a store's
Held steps become items, each carrying a Store asset per array held there.
A store's runs are kept on the model-run record (ADR 0026) by the harvest
itself, which writes no file (`runs`): a run opens on its first Held step and
closes once the store holds every step it expects, for every mapped Variable.
A closed run is not asked about again (ADR 0044 §6) — except by a re-check of
the whole window, which an operator's own run is.

A completed harvest is also a Mirror of the feed's window (ADR 0042 §6): what
is gone upstream loses its rows here, and what changed upstream is re-synced.
Both happen only once the whole window has been listed, and only within the
Mirror's Jurisdiction (§7) — the assets that bear this feed's Provenance and
are still Remote. Every other row is left exactly as it is.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import UTC

from django.db import transaction
from django.db.models import Exists, F, OuterRef, Q
from django.utils import timezone

from georiva.core.machine_plane.remote_assets import publish_variable
from georiva.core.storage.asset_location import remote_q
from georiva.sources.fetch_run_spine import RunResult, RunStopped, run_tracked

from . import runs
from .asset_hosts import AssetHostAllowlist, PinnedBuckets
from .survey import store_fields, store_media_type
from .upstream import (
    UpstreamAsset,
    UpstreamCatalogClient,
    UpstreamItem,
    UpstreamUnreachable,
    Window,
    in_window,
)

logger = logging.getLogger(__name__)

#: What a harvested asset is taken to be when the Upstream does not say (wave 1 is COGs).
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

#: What a Store asset is taken to be when the collection gives its store no media type.
STORE_MEDIA_TYPE = "application/vnd+zarr"

#: `Asset.media_type`'s column width.
MAX_MEDIA_TYPE_LENGTH = 100

#: `Asset.checksum`'s column width. A longer upstream checksum is left out, not cut short.
MAX_CHECKSUM_LENGTH = 64

#: Where a harvested asset keeps its upstream item's `updated`, in `extra_fields`:
#: what the next harvest compares to tell whether the item changed (ADR 0042 §6).
UPSTREAM_UPDATED = "upstream_updated"

#: Rows removed per statement when a window loses many at once.
REMOVAL_BATCH = 500


@dataclass
class HarvestResult(RunResult):
    """What one harvest came to. Counts are in assets."""

    indexed: int = 0
    updated: int = 0
    removed: int = 0
    rejected: int = 0

    def counters(self) -> dict[str, int]:
        return {
            "harvest_indexed": self.indexed,
            "harvest_updated": self.updated,
            "harvest_removed": self.removed,
            "harvest_rejected": self.rejected,
        }

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "indexed": self.indexed,
            "updated": self.updated,
            "removed": self.removed,
            "rejected": self.rejected,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


def harvest(
    feed, *, upstream: UpstreamCatalogClient | None = None, resumed_from=None, recheck: bool = False
) -> HarvestResult:
    """
    Harvest `feed`'s window of its Upstream, as one `FetchRun`.

    `upstream` is the Upstream catalog client; the feed's own when not given.
    `resumed_from` is the run this one retries: a harvest's plan is its window,
    so a retry harvests the same window again (ADR 0042 §5). `recheck` asks a
    store about every run in the window, closed ones too: how a step gone from
    a closed run is found, and the run reopened.

    An Upstream that cannot be read ends the run FAILED and due its retry, with
    no item or asset written, rewritten or removed: the whole window is listed
    before any row is touched, so an outage never reads as "everything was
    deleted". The feed's own Collection is its setup, not the Upstream's
    content, and is there from the first run however that run ends.
    """
    upstream = upstream or feed.upstream_client()
    collection = feed.ensure_collection()
    result = HarvestResult()

    def walk(fetch_run):
        _index_window(feed, collection, upstream, result, recheck=recheck)

    return run_tracked(
        walk,
        result=result,
        data_feed=feed,
        collection=collection,
        resumed_from=resumed_from,
        logger=logger,
    )


def _index_window(feed, collection, upstream, result: HarvestResult, *, recheck: bool = False) -> None:
    reach = _Reach.of(feed)
    variables = feed.mapped_variables(collection)
    mapped = {variable.pk for variable in variables.values()}
    # The closed runs every mapped Variable holds whole are not asked about again.
    settled = set() if recheck or not reach.is_store else runs.settled_runs(collection, mapped, reach.runs_q())
    described, undescribed = _description(feed, upstream)
    try:
        listed, expected = _listed(feed, upstream, described, reach.window, list(variables), settled)
    except UpstreamUnreachable as unreachable:
        raise RunStopped(f"source unavailable — {unreachable}") from unreachable
    # An empty window is believed only of a collection something vouched for.
    # Undescribed and unlisted, it may as well be gone, and the Mirror would
    # take that for "everything was deleted" (ADR 0042 §6).
    if described is None and not listed:
        raise RunStopped(
            f"source unavailable — upstream neither describes collection {feed.upstream_collection_id!r} "
            f"nor lists an item of it ({undescribed})"
        )

    if described is not None:
        _describe(collection, described, feed)

    allowlist = AssetHostAllowlist(
        feed.allowed_asset_hosts,
        allow_http=feed.allow_http,
        resolve=upstream.resolve_host,
    )
    # The Upstream is asked for the window and is not trusted to have kept to it.
    # Each asset is located before anything looks at it: the href that is judged,
    # compared and stored is the one a reader will open (ADR 0042 §9).
    pins = PinnedBuckets(feed.pinned_buckets)
    items = [_located(item, pins) for item in listed if reach.covers(item)]
    indexed = _Index(collection, reach)
    jurisdiction = _Jurisdiction(feed, collection, reach)
    created_items = []
    # What the Upstream still has: the times of its items — each a run, where it
    # has runs, and a valid time — and the places, a time and a Variable, its
    # mapped assets fill.
    upstream_times, upstream_places = set(), set()
    # The places in the Jurisdiction this run has already compared with the Upstream.
    judged = set()

    def refused(key, asset, item) -> bool:
        refusal = allowlist.refusal(asset.href)
        if refusal:
            result.rejected += 1
            logger.warning("Harvest %s: rejected asset %r of item %s — %s", feed.name, key, item.id, refusal)
        return bool(refusal)

    for item in items:
        at = (item.reference_time, item.datetime)
        upstream_times.add(at)
        admitted = []
        for key, variable in variables.items():
            asset = item.assets.get(key)
            if asset is None:
                continue
            place = (*at, variable.pk)
            upstream_places.add(place)

            mine = jurisdiction.assets.get(place)
            if mine is not None:
                # In the Jurisdiction: re-synced when the item changed upstream, else not written.
                # Of two upstream items at one time, the first listed speaks for the place.
                if place in judged:
                    continue
                judged.add(place)
                if mine.is_current(item, asset) or refused(key, asset, item):
                    continue
                result.updated += jurisdiction.resync(mine, asset, item)
            elif not indexed.has_asset(at, variable) and not refused(key, asset, item):
                admitted.append((variable, asset))

        # An item none of whose assets we index is not indexed either.
        if not admitted:
            continue

        with transaction.atomic():
            row, created = indexed.item_for(item, feed)
            for variable, asset in admitted:
                result.indexed += _create_asset(row, variable, asset, item, feed, reach.format)
                indexed.note_asset(at, variable)
        if created:
            created_items.append(row)

    _extend_collection(collection, created_items)

    # Last, and only here: the window was listed whole and indexed (ADR 0042 §6).
    result.removed += jurisdiction.remove_gone(upstream_times, upstream_places, mapped, settled)

    if reach.is_store:
        expected = {run: steps for run, steps in expected.items() if reach.covers_run(run)}
        runs.keep_runs(collection, mapped, reach.runs_q(), expected=expected, settled=settled)
        if reach.rolls:
            runs.forget_aged_out(collection, reach.window[0])

    # A re-sync rewrites hrefs without saving a row, so the tile server is told
    # once, here, where the window's Remote assets now are (ADR 0043 §3).
    for variable in variables.values():
        publish_variable(variable)


def _description(feed, upstream):
    """The Upstream's own description of the feed's collection, and when it has
    none to give, why: `(described, None)` or `(None, why)`.

    Describing is best effort. A catalog may list a collection's items soundly
    while its collection pages do not answer, and its items are what a harvest
    is for. A catalog that does answer and has no such collection is believed.
    """
    try:
        described = next((c for c in upstream.list_collections() if c.id == feed.upstream_collection_id), None)
    except UpstreamUnreachable as unreachable:
        logger.warning("Harvest %s: the upstream collection could not be described — %s", feed.name, unreachable)
        return None, str(unreachable)
    if described is None:
        raise RunStopped(f"source unavailable — upstream has no collection {feed.upstream_collection_id!r}")
    return described, None


def _listed(
    feed, upstream, described, window: Window, mapped: list[str], settled: set = frozenset()
) -> tuple[list[UpstreamItem], dict]:
    """What the Upstream has in `window`, as items with their assets, and how
    many steps a store expects of each run it holds a step of: the one place a
    harvest asks which shape its Upstream is (ADR 0044 §1).

    A store has no items. It is asked which steps it holds for the mapped
    arrays, of every run but the `settled` ones, and each Held step is an item:
    at its valid time, of its run where the store has runs, with a Store asset
    — the store's address and the array's path — for each array held there. A
    step that is in the store's coordinates and not held is not in the answer,
    and gets no row (§6).
    """
    if not feed.is_store:
        return list(upstream.list_items(feed.upstream_collection_id, window)), {}

    # Which of an array's axes is its run, its step or its time is the
    # collection's to say, so a store whose collection is not described cannot be listed.
    if described is None or described.cube is None:
        raise RunStopped(
            f"source unavailable — upstream does not describe the data store of collection "
            f"{feed.upstream_collection_id!r}"
        )
    if not feed.store_href:
        raise RunStopped("source unavailable — the feed names no upstream store to read")
    fields = store_fields(described.cube, mapped)
    missing = sorted(set(mapped) - {field.path for field in fields})
    if missing:
        # Believed, this would read as "every step of it was deleted". An array
        # that is truly gone is the operator's to unmap.
        raise RunStopped(
            f"source unavailable — upstream no longer describes as a map: {', '.join(repr(path) for path in missing)}"
        )
    held = (
        list(upstream.list_held_steps(feed.store_href, fields, window, skip_runs=frozenset(settled))) if fields else []
    )
    media_type = store_media_type(described)
    if not media_type or len(media_type) > MAX_MEDIA_TYPE_LENGTH:
        # One too long for its column is left out, not cut short, like a checksum.
        media_type = STORE_MEDIA_TYPE
    expected = {step.reference_time: step.run_steps for step in held if step.reference_time and step.run_steps}
    items = [
        UpstreamItem(
            id="/".join(f"{when:%Y%m%dT%H%M%S}" for when in (step.reference_time, step.time) if when),
            datetime=step.time,
            reference_time=step.reference_time,
            bbox=described.bbox,
            assets={
                path: UpstreamAsset(href=feed.store_href, media_type=media_type, array=path)
                for path in sorted(step.arrays)
            },
        )
        for step in held
    ]
    return items, expected


@dataclass(frozen=True)
class _Reach:
    """What one harvest reads and judges: the kind of asset its feed writes,
    and the feed's window as it stood when the harvest began — a Rolling
    window moves, and one harvest must list and judge by the same one."""

    window: Window
    format: str
    #: Whether rows carry a run: a store's may, and a COG harvest's never do.
    is_store: bool = False
    #: Whether the window rolls: then what has aged out of it is the Mirror's too (ADR 0044 §7).
    rolls: bool = False

    @classmethod
    def of(cls, feed) -> _Reach:
        from georiva.core.models import Asset

        return cls(
            window=feed.window,
            format=Asset.Format.ZARR if feed.is_store else Asset.Format.COG,
            is_store=feed.is_store,
            rolls=bool(feed.is_store and feed.rolling_window_days),
        )

    def covers(self, item: UpstreamItem) -> bool:
        """Whether an upstream item is in the window: by its run's time where it
        has a run, so a run inside keeps every step however far ahead."""
        return item.datetime is not None and self.covers_step(item.reference_time, item.datetime)

    def covers_run(self, run) -> bool:
        return self.covers_step(run, run)

    def covers_step(self, run, when) -> bool:
        return in_window(run or when, self.window)

    def runs_q(self) -> Q:
        """The runs in the window, by `reference_time`."""
        return _window_q(self.window, "reference_time")

    def written_q(self, prefix: str = "") -> Q:
        """The item rows a harvest of this kind could have written, at any time."""
        return Q() if self.is_store else Q(**{f"{prefix}reference_time__isnull": True})

    def judged_q(self, prefix: str = "") -> Q:
        """The item rows the Mirror judges: those in the window and, under a
        Rolling window, those that were in it once and have aged out."""
        start, end = self.window
        window = (None if self.rolls else start, end)
        on_time = Q(**{f"{prefix}reference_time__isnull": True}) & _window_q(window, f"{prefix}time")
        if not self.is_store:
            return on_time
        on_run = Q(**{f"{prefix}reference_time__isnull": False}) & _window_q(window, f"{prefix}reference_time")
        return on_time | on_run


def _located(item: UpstreamItem, pins: PinnedBuckets) -> UpstreamItem:
    """`item` with every asset's href as it may be stored here."""
    return replace(
        item,
        assets={key: replace(asset, href=pins.locate(asset.href)) for key, asset in item.assets.items()},
    )


class _Index:
    """What the collection already holds, read once: a second harvest of an
    unchanged window asks the database nothing per item."""

    def __init__(self, collection, reach: _Reach):
        from georiva.core.models import Asset

        self.collection = collection
        # An item of COGs is an observation at a valid time: no reference time.
        self._assets = set(
            Asset.objects.filter(
                reach.written_q("item__"), item__collection=collection, format=reach.format
            ).values_list("item__reference_time", "item__time", "variable_id")
        )

    def has_asset(self, at, variable) -> bool:
        """Whether an asset for `variable` is already there at `at` — a run, or
        none, and a valid time. A harvested one, a Materialized one, or one that
        was never ours: none is created over; only one in the Mirror's
        Jurisdiction is ever written again."""
        return (*at, variable.pk) in self._assets

    def note_asset(self, at, variable) -> None:
        """Two upstream items at one time share an item row here; the first to
        bring a Variable's asset has it."""
        self._assets.add((*at, variable.pk))

    def item_for(self, item: UpstreamItem, feed):
        """The item row at this upstream item's time, created with the feed's
        Provenance if the collection has none there yet. Returns `(row, created)`."""
        from georiva.core.models import Item

        row, created = Item.objects.get_or_create(
            collection=self.collection,
            time=item.datetime,
            reference_time=item.reference_time,
            defaults={
                "bounds": item.bbox,
                "geometry": item.geometry,
                "harvested_by": feed,
            },
        )
        return row, created


@dataclass
class _Mine:
    """One asset in the Jurisdiction, as it stood when the harvest began."""

    pk: int
    extra_fields: dict
    href: str = ""
    store_array: str = ""

    def is_current(self, item: UpstreamItem, asset: UpstreamAsset) -> bool:
        """Whether the upstream item's `updated` is the one this asset was last
        written from. An Upstream that changes an item and not its `updated`
        is not re-synced (ADR 0042, consequences).

        A step of a store has no `updated`. Its Store asset says where the
        store is and which array it means, and is current while both still hold:
        a store the feed now names at another address is followed there."""
        if asset.array:
            return (self.href, self.store_array) == (asset.href, asset.array)
        return self.extra_fields.get(UPSTREAM_UPDATED) == _updated_stamp(item)


class _Jurisdiction:
    """The feed's Jurisdiction within its window: the assets its Mirror may
    rewrite or remove (ADR 0042 §7).

    Mine, and still Remote — both, always. The two conditions pick the rows
    read here, once, and are applied again when a row is written: a rewrite is
    one guarded statement, and a removal locks the rows that still pass before
    deleting them. A row Materialized since the read is no longer the Mirror's.
    """

    def __init__(self, feed, collection, reach: _Reach):
        self.feed = feed
        self.collection = collection
        self.reach = reach
        #: By place: the asset's run, or none, its valid time and its Variable's pk.
        self.assets = {
            (run, when, variable_pk): _Mine(pk, extra_fields, href, store_array)
            for pk, run, when, variable_pk, extra_fields, href, store_array in self._rows()
            .filter(reach.judged_q("item__"), item__collection=collection, format=reach.format)
            .values_list(
                "pk", "item__reference_time", "item__time", "variable_id", "extra_fields", "href", "store_array"
            )
        }

    def _rows(self):
        """Mine, and still Remote: the test every read, rewrite and delete goes through."""
        from georiva.core.models import Asset

        return Asset.objects.filter(remote_q(), harvested_by_id=self.feed.pk)

    def resync(self, mine: _Mine, asset, item: UpstreamItem) -> int:
        """Rewrite one asset from what the Upstream now says of it. Returns how
        many rows that rewrote: 1, or 0 when it left the Jurisdiction meanwhile."""
        return (
            self._rows()
            .filter(pk=mine.pk)
            .update(**_lifted(asset, item, kept=mine.extra_fields), modified=timezone.now())
        )

    def remove_gone(self, upstream_times: set, upstream_places: set, mapped: set, settled: set = frozenset()) -> int:
        """Remove the assets that are gone upstream, and then every item of this
        feed's in the window that is left with no asset of any kind. Returns how
        many assets went.

        An asset is gone when the Upstream has no item at its time, or has one
        that no longer carries the asset its (still `mapped`) Variable is
        indexed from. For a store that is a step no longer held, and — nothing
        being listed of a run outside the window — every step of a run that has
        aged out of a Rolling one. A `settled` run was not asked about, and
        what it holds stands. Call it only with a window that was listed whole.
        """
        from georiva.core.models import Asset

        def gone(at, variable_pk) -> bool:
            if at[0] is not None and at[0] in settled:
                return False
            if variable_pk in mapped:
                return at not in upstream_times or (*at, variable_pk) not in upstream_places
            # A Variable no longer mapped is not asked about. Of items, the
            # Upstream still says whether its time is there; a store is asked
            # only about mapped arrays, so its row stays until its run ages out.
            return not self.reach.covers_step(*at) if self.reach.is_store else at not in upstream_times

        pks = [mine.pk for (*at, variable_pk), mine in self.assets.items() if gone(tuple(at), variable_pk)]
        removed = 0
        for start in range(0, len(pks), REMOVAL_BATCH):
            with transaction.atomic():
                # Locked as they are judged: a Materialize under way either lands
                # first and the row is no longer here, or waits and finds it gone.
                still_mine = list(
                    self._rows()
                    .filter(pk__in=pks[start : start + REMOVAL_BATCH])
                    .select_for_update(of=("self",))
                    .values_list("pk", flat=True)
                )
                _, by_model = Asset.objects.filter(pk__in=still_mine).delete()
            removed += by_model.get(Asset._meta.label, 0)

        self._remove_empty_items()
        return removed

    def _remove_empty_items(self) -> None:
        """An Item goes when it bears the feed's Provenance and no asset of any
        kind is left on it (ADR 0042 §7)."""
        from georiva.core.models import Asset, Item

        empty = Item.objects.filter(
            self.reach.judged_q(),
            ~Exists(Asset.objects.filter(item=OuterRef("pk"))),
            collection=self.collection,
            harvested_by_id=self.feed.pk,
        )
        with transaction.atomic():
            locked = list(empty.select_for_update(of=("self",)).values_list("pk", flat=True))
            # Emptiness is asked again by the delete itself, so an asset that
            # arrived while the items were being locked keeps its item.
            empty.filter(pk__in=locked).delete()


def _window_q(window, field: str) -> Q:
    start, end = window
    q = Q()
    if start is not None:
        q &= Q(**{f"{field}__gte": start})
    if end is not None:
        q &= Q(**{f"{field}__lte": end})
    return q


def _updated_stamp(item: UpstreamItem) -> str | None:
    return item.updated.astimezone(UTC).isoformat() if item.updated else None


def _lifted(asset, item: UpstreamItem, *, kept: dict | None = None) -> dict:
    """The fields of an `Asset` a harvest takes from the Upstream: written at
    creation, and written again whole on a re-sync. `kept` is the `extra_fields`
    the row already has; what the harvest did not put there stays."""
    stats = asset.statistics
    extra = {key: value for key, value in (kept or {}).items() if key not in ("nodata", UPSTREAM_UPDATED)}
    if asset.nodata is not None:
        extra["nodata"] = asset.nodata
    if item.updated is not None:
        extra[UPSTREAM_UPDATED] = _updated_stamp(item)
    return {
        "href": asset.href,
        "media_type": asset.media_type or COG_MEDIA_TYPE,
        # What a Store asset's slice is of; empty for any other (ADR 0044 §3).
        "store_array": asset.array,
        "file_size": asset.file_size,
        "checksum": asset.checksum if len(asset.checksum) <= MAX_CHECKSUM_LENGTH else "",
        "bands": asset.bands or 1,
        # Lifted from upstream metadata or left null; nothing reads pixels (ADR 0042 §10).
        "stats_min": stats.minimum if stats else None,
        "stats_max": stats.maximum if stats else None,
        "stats_mean": stats.mean if stats else None,
        "stats_std": stats.stddev if stats else None,
        "extra_fields": extra,
    }


def _create_asset(row, variable, asset, item: UpstreamItem, feed, format: str) -> int:
    """Index one Remote asset — of a store, a Store asset — in `format`. Returns
    how many rows that created: 1, or 0 when another asset took its place since
    the collection was read."""
    from georiva.core.models import Asset

    _, created = Asset.objects.get_or_create(
        item=row,
        variable=variable,
        format=format,
        defaults={**_lifted(asset, item), "roles": [Asset.Role.DATA], "harvested_by": feed},
    )
    return int(created)


def _describe(collection, described, feed) -> None:
    """Give a collection this feed created the Upstream's own title, description
    and extent, wherever it still holds the placeholder it was created with.
    Anything an operator has since written is theirs."""
    if collection.harvested_by_id != feed.pk:
        return
    changes = {}
    if described.title and collection.name == feed.upstream_collection_id[:200]:
        changes["name"] = described.title[:200]
    if described.description and not collection.description:
        changes["description"] = described.description
    if described.bbox and not collection.bounds:
        changes["bounds"] = described.bbox
    if changes:
        for name, value in changes.items():
            setattr(collection, name, value)
        collection.save(update_fields=list(changes))


def _extend_collection(collection, created_items) -> None:
    """Count the new items into the collection and widen its extent to hold them."""
    from georiva.core.models import Collection

    if not created_items:
        return
    times = [row.time for row in created_items]
    changes = {"item_count": F("item_count") + len(created_items)}
    if collection.time_start is None or min(times) < collection.time_start:
        changes["time_start"] = min(times)
    if collection.time_end is None or max(times) > collection.time_end:
        changes["time_end"] = max(times)

    boxes = [row.bounds for row in created_items if row.bounds]
    if collection.bounds and len(collection.bounds) == 4:
        boxes.append(list(collection.bounds))
    if boxes:
        bounds = [
            min(b[0] for b in boxes),
            min(b[1] for b in boxes),
            max(b[2] for b in boxes),
            max(b[3] for b in boxes),
        ]
        if bounds != collection.bounds:
            changes["bounds"] = bounds

    # An update, not a save: nothing else about the collection is this run's to write.
    Collection.objects.filter(pk=collection.pk).update(**changes)
