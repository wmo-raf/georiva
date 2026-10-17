"""Events nobody owns: the one deliberately unscoped listing (ADR 0030).

A null organisation means *unattributed* — a delivery whose key names an
organisation or catalog that does not exist. Showing it on a tenant's surface
would leak one institution's file paths to another, so the tenancy dispatcher
reads it as "belongs to nobody" and every scoped panel omits it. The instance
administrator still has to be able to find and fix misrouted files, and this
page is where: superusers only, and unscoped on purpose.
"""

from django.core.exceptions import PermissionDenied
from django.shortcuts import render
from django.urls import reverse
from django.utils.translation import gettext as _

from .history import HistoryRow, row_for
from .registry import event_types, installed_tables

LIMIT = 200


def unattributed_events(request):
    if not request.user.is_superuser:
        raise PermissionDenied(_("Unattributed events are visible to the instance administrator only."))

    existing = installed_tables()
    rows: list[HistoryRow] = []
    for model in event_types():
        if model._meta.db_table not in existing:
            continue
        # Deliberately not ``scope_rows``: these rows belong to nobody.
        for event in model._default_manager.filter(organisation__isnull=True).order_by("-occurred_at")[:LIMIT]:
            rows.append(row_for(event))
    rows.sort(key=lambda row: row.occurred_at, reverse=True)

    return render(
        request,
        "georivaevents/unattributed.html",
        {
            "rows": rows[:LIMIT],
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": _("Home")},
                {"url": None, "label": _("Unattributed events")},
            ],
            "header_title": _("Unattributed events"),
            "header_icon": "warning",
        },
    )


def timeline(request, correlation_id):
    """One whole chain, oldest first (ADR 0030): everything that shares a Correlation.

    Reads the index table and fetches details per type from the registry, so it
    works whichever plugins are installed — a type whose table is absent is
    skipped. Scoped through the tenancy dispatcher: a viewer sees their own
    organisation's events, and learns only that the chain continues elsewhere
    when rows exist beyond their scope.
    """
    from django.http import Http404

    from georiva.organisations.ownership import scope_rows

    from .models import EventIndex
    from .registry import model_for

    everything = EventIndex.objects.filter(correlation_id=correlation_id)
    visible = list(scope_rows(request, everything).order_by("occurred_at", "event_id"))
    if not visible:
        raise Http404(_("No such chain."))
    # Unattributed rows belong to nobody and are not hinted at; only another
    # organisation's part of the chain is.
    continues_elsewhere = everything.filter(organisation__isnull=False).count() > len(visible)

    existing = installed_tables()
    by_type: dict[str, list] = {}
    for row in visible:
        by_type.setdefault(row.event_type, []).append(row.event_id)

    events = {}
    for key, ids in by_type.items():
        model = model_for(key)
        if model is None or model._meta.db_table not in existing:
            continue
        for event in model._default_manager.filter(pk__in=ids):
            events[event.pk] = event

    rows = []
    for index_row in visible:
        event = events.get(index_row.event_id)
        if event is None:
            # Indexed, but its type is not installed here or its table is gone.
            rows.append(
                {"label": index_row.event_type, "summary": "", "occurred_at": index_row.occurred_at, "unreadable": True}
            )
            continue
        cause = events.get(event.caused_by_id) if event.caused_by_id else None
        rows.append(
            {
                "label": event.label,
                "summary": event.summary(),
                "occurred_at": event.occurred_at,
                "actor": event.actor,
                "outcome_class": event.outcome_class,
                "cause": f"{cause.label}: {cause.summary()}" if cause else None,
                "cause_hidden": bool(event.caused_by_id) and cause is None,
                "subject_url": event.subject_url(),
                "links": event.extra_links(),
            }
        )

    return render(
        request,
        "georivaevents/timeline.html",
        {
            "rows": rows,
            "continues_elsewhere": continues_elsewhere,
            "correlation_id": correlation_id,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": _("Home")},
                {"url": None, "label": _("Timeline %(id)s") % {"id": str(correlation_id)[:8]}},
            ],
            "header_title": _("Timeline %(id)s") % {"id": str(correlation_id)[:8]},
            "header_icon": "history",
        },
    )
