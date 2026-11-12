"""
The harvest: one run of a Harvest feed, on the run-tracking spine (ADR 0042).

It lists the feed's window of an Upstream and indexes what it finds as ordinary
Published rows whose assets point upstream. No pixel is read and no file is
fetched: the run is a `FetchRun` with no `FetchedFile` rows, and its counts are
its own.

This is the create half. A harvest adds what is missing and leaves every row
that already exists exactly as it is; removing what is gone upstream and
re-syncing what changed is the Mirror's work, within its Jurisdiction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from django.db import transaction
from django.db.models import F

from georiva.sources.fetch_run_spine import RunResult, RunStopped, run_tracked

from .asset_hosts import AssetHostAllowlist
from .upstream import UpstreamCatalogClient, UpstreamItem, UpstreamUnreachable, in_window

logger = logging.getLogger(__name__)

#: What a harvested asset is taken to be when the Upstream does not say (wave 1 is COGs).
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

#: `Asset.checksum`'s column width. A longer upstream checksum is left out, not cut short.
MAX_CHECKSUM_LENGTH = 64


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
    no item or asset written: the whole window is listed before anything is
    indexed. The feed's own Collection is its setup, not the Upstream's
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
    if not variables:
        return

    allowlist = AssetHostAllowlist(
        feed.allowed_asset_hosts,
        allow_http=feed.allow_http,
        resolve=upstream.resolve_host,
    )
    # The Upstream is asked for the window and is not trusted to have kept to it.
    items = [item for item in listed if item.datetime is not None and in_window(item.datetime, feed.window)]
    indexed = _Index(collection)
    created_items = []

    for item in items:
        admitted = []
        for key, variable in variables.items():
            asset = item.assets.get(key)
            if asset is None or indexed.has_asset(item.datetime, variable):
                continue
            refusal = allowlist.refusal(asset.href)
            if refusal:
                result.rejected += 1
                logger.warning("Harvest %s: rejected asset %r of item %s — %s", feed.name, key, item.id, refusal)
                continue
            admitted.append((variable, asset))

        # An item none of whose assets we index is not indexed either.
        if not admitted:
            continue

        with transaction.atomic():
            row, created = indexed.item_for(item, feed)
            for variable, asset in admitted:
                result.indexed += _create_asset(row, variable, asset, feed)
                indexed.note_asset(item.datetime, variable)
        if created:
            created_items.append(row)

    _extend_collection(collection, created_items)


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
        one, a Materialized one, or one that was never ours. All are left alone."""
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


def _create_asset(row, variable, asset, feed) -> int:
    """Index one Remote asset. Returns how many rows that created: 1, or 0 when
    another asset took its place since the collection was read."""
    from georiva.core.models import Asset

    stats = asset.statistics
    _, created = Asset.objects.get_or_create(
        item=row,
        variable=variable,
        format=Asset.Format.COG,
        defaults={
            "href": asset.href,
            "media_type": asset.media_type or COG_MEDIA_TYPE,
            "roles": [Asset.Role.DATA],
            "file_size": asset.file_size,
            "checksum": asset.checksum if len(asset.checksum) <= MAX_CHECKSUM_LENGTH else "",
            "bands": asset.bands or 1,
            # Lifted from upstream metadata or left null; nothing reads pixels (ADR 0042 §10).
            "stats_min": stats.minimum if stats else None,
            "stats_max": stats.maximum if stats else None,
            "stats_mean": stats.mean if stats else None,
            "stats_std": stats.stddev if stats else None,
            "extra_fields": {"nodata": asset.nodata} if asset.nodata is not None else {},
            "harvested_by": feed,
        },
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
