"""Core's Wagtail admin surface.

Everything here answers to a human in the admin: HTML views, the ModelViewSets
that generate the add/edit/delete routes behind them, and the small pieces of
chrome (table columns, dashboard tiles) those pages are assembled from.

The machine plane — the endpoints Titiler and nginx call, which serve no
templates and answer to no human — deliberately lives elsewhere, in
``core.machine_plane``.

The re-exports below are the surface the old top-level ``core/views.py``
offered, kept so its callers did not have to move with it. Everything else in
this package is imported by its own path (``core.views.viewsets``,
``core.views.tables``, ``core.views.summary_items``).
"""

from .admin import (  # noqa: F401
    CatalogIndexView,
    add_data_select,
    collection_items_list,
    get_collection_items_url,
    plugin_list,
)
