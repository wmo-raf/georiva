"""Events nobody owns: the one deliberately unscoped listing (ADR 0030).

A null organisation means *unattributed* — a delivery whose key names an
organisation or catalog that does not exist. Showing it on a tenant's surface
would leak one institution's file paths to another, so the tenancy dispatcher
reads it as "belongs to nobody" and every scoped panel omits it. The instance
administrator still has to be able to find and fix misrouted files, and this
page is where: superusers only, and unscoped on purpose.
"""

from django.core.exceptions import PermissionDenied
from django.db import connection
from django.shortcuts import render
from django.urls import reverse

from .history import HistoryRow, _row
from .registry import event_types

LIMIT = 200


def unattributed_events(request):
    if not request.user.is_superuser:
        raise PermissionDenied("Unattributed events are visible to the instance administrator only.")

    existing = set(connection.introspection.table_names())
    rows: list[HistoryRow] = []
    for model in event_types():
        if model._meta.db_table not in existing:
            continue
        # Deliberately not ``scope_rows``: these rows belong to nobody.
        for event in model._default_manager.filter(organisation__isnull=True).order_by("-occurred_at")[:LIMIT]:
            rows.append(_row(event))
    rows.sort(key=lambda row: row.occurred_at, reverse=True)

    return render(
        request,
        "georivaevents/unattributed.html",
        {
            "rows": rows[:LIMIT],
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": "Home"},
                {"url": None, "label": "Unattributed events"},
            ],
            "header_title": "Unattributed events",
            "header_icon": "warning",
        },
    )
