"""An Upstream that lives in memory: the one fake a harvest test needs (ADR 0042 §13)."""

from __future__ import annotations

from datetime import datetime

from .upstream import (
    UpstreamAsset,
    UpstreamCollection,
    UpstreamItem,
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
    and `unreachable` to a message for one that cannot be read at all.
    """

    def __init__(self, collection_id: str = "chirps-daily", *, title: str = "", description: str = ""):
        self.collection_id = collection_id
        self.collections = [UpstreamCollection(id=collection_id, title=title, description=description)]
        self.items: list[UpstreamItem] = []
        self.addresses: dict[str, list[str]] = {}
        self.unreachable: str | None = None
        self.ignores_window = False
        #: The windows `list_items` was asked for, in order.
        self.windows_asked: list[Window] = []
        #: The hosts `resolve_host` was asked about, in order.
        self.hosts_resolved: list[str] = []

    def add_item(self, when: datetime, assets: dict[str, UpstreamAsset | str], **fields) -> UpstreamItem:
        """Add an item at `when`. An asset given as a string is its href."""
        item = UpstreamItem(
            id=fields.pop("id", f"item-{when:%Y%m%dT%H%M%S}"),
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

    def _answer(self):
        if self.unreachable:
            raise UpstreamUnreachable(self.unreachable)

    def list_collections(self):
        self._answer()
        return list(self.collections)

    def list_items(self, collection_id: str, window: Window):
        self._answer()
        self.windows_asked.append(window)
        if collection_id != self.collection_id:
            return []
        if self.ignores_window:
            return list(self.items)
        return [item for item in self.items if item.datetime is not None and in_window(item.datetime, window)]

    def resolve_host(self, host: str) -> list[str]:
        self.hosts_resolved.append(host)
        return self.addresses.get(host, [PUBLIC_ADDRESS])
