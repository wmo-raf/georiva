"""The endpoints the Connect panel lists for one variable (#580).

A row is a protocol, the URL a client pastes, and — when we have instructions
for it — the key of a how-to the side panel can open. Every URL is spelt by
``core/machine_plane/addresses.py`` (ADR 0013); this module only chooses which
ones to show and for whom.
"""

from dataclasses import dataclass

from django.urls import reverse

from georiva.core.machine_plane.addresses import (
    WMTS_REFTIME_DIMENSION,
    WMTS_TIME_DIMENSION,
    wmts_capabilities_url,
    wmts_kvp_endpoint,
    wmts_rest_tile_template,
)
from georiva.core.models import Collection
from georiva.core.utils import get_full_url_by_request

#: What a non-public collection's URLs carry where the caller's own key goes.
#: Keys are stored hashed, so the server has no real one to write here.
API_KEY_PLACEHOLDER = "YOUR_KEY"

#: The WMTS grid is WebMercatorQuad, whose rows count from the top — the same
#: scheme XYZ clients use — so an XYZ template is the WMTS one renamed.
_XYZ_PLACEHOLDERS = {"{TileMatrix}": "{z}", "{TileCol}": "{x}", "{TileRow}": "{y}"}


@dataclass(frozen=True)
class ConnectRow:
    key: str
    label: str
    url: str
    note: str = ""
    alt_label: str = ""
    alt_url: str = ""
    howto: str = ""
    openable: bool = True


def connect_rows(catalog, collection, variable, has_runs):
    """The Connect rows for ``variable``, keyed for the collection's audience."""
    private = collection.visibility != Collection.Visibility.PUBLIC
    api_key = API_KEY_PLACEHOLDER if private else None
    organisation = catalog.organisation

    # ``time`` is required by the tile route, and a layer with runs also needs
    # ``reftime``; the WMTS template builder spells both as placeholders.
    dimensions = (WMTS_TIME_DIMENSION, WMTS_REFTIME_DIMENSION) if has_runs else (WMTS_TIME_DIMENSION,)
    xyz = wmts_rest_tile_template(variable, dimensions=dimensions, api_key=api_key)
    for wmts_name, xyz_name in _XYZ_PLACEHOLDERS.items():
        xyz = xyz.replace(wmts_name, xyz_name)

    return [
        ConnectRow(
            key="stac",
            label="STAC",
            url=f"/api/stac/collections/{catalog.slug}/{collection.slug}/{variable.slug}/",
            howto="stac",
        ),
        ConnectRow(
            key="wmts",
            label="WMTS",
            url=wmts_kvp_endpoint(organisation, api_key=api_key),
            alt_label="Capabilities",
            alt_url=wmts_capabilities_url(organisation, api_key=api_key),
            howto="wmts",
        ),
        # A template, not an address: opening it would request the tile "{z}/{x}/{y}".
        ConnectRow(key="xyz", label="XYZ tiles", url=xyz, openable=False, howto="xyz"),
        ConnectRow(
            key="edr",
            label="EDR",
            url=f"/api/edr/collections/{collection.slug}/",
            note="metadata only",
            howto="edr",
        ),
    ]


def connect_context(request, catalog, collection, variable):
    """Template context for the panel; ``None`` when there is no variable.

    ``origin`` is the Site's canonical one, the same STAC links are built on, so
    the text a visitor selects, the copy button and the snippets all hold one
    absolute URL; rows stay path-only so the template can mute the origin.
    """
    if variable is None:
        return None
    # The same question EDR and WMTS answer: do the items carry runs? Not the
    # ``is_forecast`` flag, which a hindcast or reanalysis collection leaves off.
    has_runs = collection.has_runs()
    return {
        "origin": get_full_url_by_request(request, ""),
        "rows": connect_rows(catalog, collection, variable, has_runs),
        "has_runs": has_runs,
        "edr_url": f"/api/edr/collections/{collection.slug}/",
        "stac_root_url": "/api/stac/",
        "stac_browser_url": "/stac-browser/",
        "stac_collection_id": f"{catalog.slug}/{collection.slug}/{variable.slug}",
        "private": collection.visibility != Collection.Visibility.PUBLIC,
        "api_keys_url": reverse("api_keys"),
        "api_key_placeholder": API_KEY_PLACEHOLDER,
    }
