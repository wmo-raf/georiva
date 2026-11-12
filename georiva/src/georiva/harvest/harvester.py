"""
The harvest: one run of a Harvest feed, on the run-tracking spine (ADR 0042).

It lists the feed's window of an Upstream and indexes what it finds as ordinary
Published rows whose assets point upstream. No pixel is read and no file is
fetched: the run is a `FetchRun` with no `FetchedFile` rows, and its counts are
its own.

A completed harvest is also a Mirror of the feed's window (ADR 0042 §6): what
is gone upstream loses its rows here, and what changed upstream is re-synced.
Both happen only once the whole window has been listed, and only within the
Mirror's Jurisdiction (§7) — the assets that bear this feed's Provenance and
are still Remote. Every other row is left exactly as it is.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC

from django.db import transaction
from django.db.models import Exists, F, OuterRef, Q
from django.utils import timezone

from georiva.core.storage.asset_location import remote_q
from georiva.sources.fetch_run_spine import RunResult, RunStopped, run_tracked

from .asset_hosts import AssetHostAllowlist
from .upstream import UpstreamCatalogClient, UpstreamItem, UpstreamUnreachable, in_window

logger = logging.getLogger(__name__)

#: What a harvested asset is taken to be when the Upstream does not say (wave 1 is COGs).
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

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


def harvest(feed, *, upstream: UpstreamCatalogClient | None = None, resumed_from=None) -> HarvestResult:
    """
    Harvest `feed`'s window of its Upstream, as one `FetchRun`.

    `upstream` is the Upstream catalog client; the feed's own when not given.
    `resumed_from` is the run this one retries: a harvest's plan is its window,
    so a retry harvests the same window again (ADR 0042 §5).

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
        _index_window(feed, collection, upstream, result)

    return run_tracked(
        walk,
        result=result,
        data_feed=feed,
        collection=collection,
        resumed_from=resumed_from,
        logger=logger,
    )


def _index_window(feed, collection, upstream, result: HarvestResult) -> None:
    try:
        described = next((c for c in upstream.list_collections() if c.id == feed.upstream_collection_id), None)
        if described is None:
            raise RunStopped(f"source unavailable — upstream has no collection {feed.upstream_collection_id!r}")
        listed = list(upstream.list_items(feed.upstream_collection_id, feed.window))
    except UpstreamUnreachable as unreachable:
        raise RunStopped(f"source unavailable — {unreachable}") from unreachable

    _describe(collection, described, feed)

    variables = feed.mapped_variables(collection)
    allowlist = AssetHostAllowlist(
        feed.allowed_asset_hosts,
        allow_http=feed.allow_http,
        resolve=upstream.resolve_host,
    )
    # The Upstream is asked for the window and is not trusted to have kept to it.
    items = [item for item in listed if item.datetime is not None and in_window(item.datetime, feed.window)]
    indexed = _Index(collection)
    jurisdiction = _Jurisdiction(feed, collection)
    created_items = []
    # What the Upstream still has: the times of its items, and the places —
    # a time and a Variable — its mapped assets fill.
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
        upstream_times.add(item.datetime)
        admitted = []
        for key, variable in variables.items():
            asset = item.assets.get(key)
            if asset is None:
                continue
            place = (item.datetime, variable.pk)
            upstream_places.add(place)

            mine = jurisdiction.assets.get(place)
            if mine is not None:
                # In the Jurisdiction: re-synced when the item changed upstream, else not written.
                # Of two upstream items at one time, the first listed speaks for the place.
                if place in judged:
                    continue
                judged.add(place)
                if mine.is_current(item) or refused(key, asset, item):
                    continue
                result.updated += jurisdiction.resync(mine, asset, item)
            elif not indexed.has_asset(item.datetime, variable) and not refused(key, asset, item):
                admitted.append((variable, asset))

        # An item none of whose assets we index is not indexed either.
        if not admitted:
            continue

        with transaction.atomic():
            row, created = indexed.item_for(item, feed)
            for variable, asset in admitted:
                result.indexed += _create_asset(row, variable, asset, item, feed)
                indexed.note_asset(item.datetime, variable)
        if created:
            created_items.append(row)

    _extend_collection(collection, created_items)

    # Last, and only here: the window was listed whole and indexed (ADR 0042 §6).
    mapped = {variable.pk for variable in variables.values()}
    result.removed += jurisdiction.remove_gone(upstream_times, upstream_places, mapped)


class _Index:
    """What the collection already holds, read once: a second harvest of an
    unchanged window asks the database nothing per item."""

    def __init__(self, collection):
        from georiva.core.models import Asset

        self.collection = collection
        # A harvested item is an observation at a valid time: no reference time.
        self._assets = set(
            Asset.objects.filter(
                item__collection=collection, item__reference_time__isnull=True, format=Asset.Format.COG
            ).values_list("item__time", "variable_id")
        )

    def has_asset(self, when, variable) -> bool:
        """Whether an asset for `variable` is already there at `when` — a harvested
        one, a Materialized one, or one that was never ours. None is created over;
        only one in the Mirror's Jurisdiction is ever written again."""
        return (when, variable.pk) in self._assets

    def note_asset(self, when, variable) -> None:
        """Two upstream items at one time share an item row here; the first to
        bring a Variable's asset has it."""
        self._assets.add((when, variable.pk))

    def item_for(self, item: UpstreamItem, feed):
        """The item row at this upstream item's time, created with the feed's
        Provenance if the collection has none there yet. Returns `(row, created)`."""
        from georiva.core.models import Item

        row, created = Item.objects.get_or_create(
            collection=self.collection,
            time=item.datetime,
            reference_time=None,
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

    def is_current(self, item: UpstreamItem) -> bool:
        """Whether the upstream item's `updated` is the one this asset was last
        written from. An Upstream that changes an item and not its `updated`
        is not re-synced (ADR 0042, consequences)."""
        return self.extra_fields.get(UPSTREAM_UPDATED) == _updated_stamp(item)


class _Jurisdiction:
    """The feed's Jurisdiction within its window: the assets its Mirror may
    rewrite or remove (ADR 0042 §7).

    Mine, and still Remote — both, always. The two conditions pick the rows
    read here, once, and are applied again when a row is written: a rewrite is
    one guarded statement, and a removal locks the rows that still pass before
    deleting them. A row Materialized since the read is no longer the Mirror's.
    """

    def __init__(self, feed, collection):
        from georiva.core.models import Asset

        self.feed = feed
        self.collection = collection
        #: By place: the asset's time and its Variable's pk.
        self.assets = {
            (when, variable_pk): _Mine(pk, extra_fields)
            for pk, when, variable_pk, extra_fields in self._rows()
            .filter(
                _window_q(feed.window, "item__time"),
                item__collection=collection,
                item__reference_time__isnull=True,
                format=Asset.Format.COG,
            )
            .values_list("pk", "item__time", "variable_id", "extra_fields")
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

    def remove_gone(self, upstream_times: set, upstream_places: set, mapped: set) -> int:
        """Remove the assets that are gone upstream, and then every item of this
        feed's in the window that is left with no asset of any kind. Returns how
        many assets went.

        An asset is gone when the Upstream has no item at its time, or has one
        that no longer carries the asset its (still `mapped`) Variable is
        indexed from. Call it only with a window that was listed whole.
        """
        from georiva.core.models import Asset

        pks = [
            mine.pk
            for (when, variable_pk), mine in self.assets.items()
            if when not in upstream_times or (variable_pk in mapped and (when, variable_pk) not in upstream_places)
        ]
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
            _window_q(self.feed.window, "time"),
            ~Exists(Asset.objects.filter(item=OuterRef("pk"))),
            collection=self.collection,
            reference_time__isnull=True,
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


def _create_asset(row, variable, asset, item: UpstreamItem, feed) -> int:
    """Index one Remote asset. Returns how many rows that created: 1, or 0 when
    another asset took its place since the collection was read."""
    from georiva.core.models import Asset

    _, created = Asset.objects.get_or_create(
        item=row,
        variable=variable,
        format=Asset.Format.COG,
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
