"""An Upstream that lives in memory: the one fake a harvest test needs (ADR 0042 §13)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .upstream import (
    AssetProbe,
    UpstreamAsset,
    UpstreamCollection,
    UpstreamItem,
    UpstreamRoot,
    UpstreamUnreachable,
    Window,
    in_window,
)

#: What every host resolves to unless a test says otherwise: a public address.
PUBLIC_ADDRESS = "93.184.216.34"


class FakeUpstream:
    """An Upstream catalog client over canned collections and items.

    Honest by default: it answers `list_items` with the window it was asked
    for. Set `ignores_window` for an Upstream that returns everything it has,
    `unreachable` to a message for one that cannot be read at all, and
    `collections_unreachable` for one that lists its items and not its collections.
    """

    def __init__(self, collection_id: str = "chirps-daily", *, title: str = "", description: str = ""):
        self.collection_id = collection_id
        self.collections = [UpstreamCollection(id=collection_id, title=title, description=description)]
        self.items: list[UpstreamItem] = []
        self.addresses: dict[str, list[str]] = {}
        self.unreachable: str | None = None
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

    def drop_item(self, when: datetime) -> None:
        """The Upstream no longer has an item at `when`."""
        self.items = [item for item in self.items if item.datetime != when]

    def _answer(self):
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

    @staticmethod
    def _break_after(items, count):
        yield from items[:count]
        raise UpstreamUnreachable("upstream catalog unreachable: connection reset mid-listing")

    def resolve_host(self, host: str) -> list[str]:
        self.hosts_resolved.append(host)
        return self.addresses.get(host, [PUBLIC_ADDRESS])

    def describe_root(self) -> UpstreamRoot:
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
