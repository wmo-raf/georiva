"""Recent failed and refused transitions for the active organisation (ADR 0030, ADR 0033).

The one deliberate org-wide listing in the design — an explicit exception to the
decision that retired the Monitoring menu (ADR 0019), justified by silent-failure
detection: a failure has to reach someone who does not already know which object
to open. It reads the index table's outcome class, so it needs no per-type query
to *find* failures; details are then fetched once per type that actually appears.

The dashboard shows one line per *failure group* — event type plus the event's
``failure_group_key`` (a feed, a collection, a derived product) — so a feed that
fails every twenty minutes is one line, not a page. The Failures page behind it
lists the events themselves.
"""

from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import urlencode

from django.conf import settings
from django.db.models import Count, Max, Q
from django.urls import reverse
from django.utils import timezone

from georiva.organisations.ownership import scope_rows

from .history import HistoryRow, row_for
from .models import EventIndex, OutcomeClass
from .registry import installed_tables, model_for

DEFAULT_DAYS = 7
GROUP_LIMIT = 10


def window_days() -> int:
    return int(getattr(settings, "GEORIVA_FAILURES_PANEL_DAYS", DEFAULT_DAYS))


def failure_index(request, *, days=None):
    """The active organisation's failed and refused index rows inside the window."""
    since = timezone.now() - timedelta(days=days if days is not None else window_days())
    return scope_rows(
        request,
        EventIndex.objects.filter(
            outcome_class__in=(OutcomeClass.FAILED, OutcomeClass.REFUSED), occurred_at__gte=since
        ),
    )


def rows_for(index_rows) -> list[HistoryRow]:
    """History rows for ``index_rows``, in their order; a type whose table is gone is skipped."""
    existing = installed_tables()
    wanted: dict[str, list] = {}
    for row in index_rows:
        wanted.setdefault(row.event_type, []).append(row.event_id)
    events = {}
    for key, ids in wanted.items():
        model = model_for(key)
        if model is None or model._meta.db_table not in existing:
            continue
        for event in model._default_manager.filter(pk__in=ids):
            events[event.pk] = event
    return [row_for(events[row.event_id]) for row in index_rows if row.event_id in events]


def failures_url(event_type=None, group_key=None) -> str:
    url = reverse("events_failures")
    if event_type is None:
        return url
    return f"{url}?{urlencode({'type': event_type, 'group': group_key})}"


@dataclass
class FailureGroupRow:
    label: str
    url: str | None
    failed: int
    refused: int
    latest: HistoryRow
    list_url: str


@dataclass
class FailureGroups:
    groups: list[FailureGroupRow]
    more: int


def failure_groups(request, *, days=None, limit=GROUP_LIMIT) -> FailureGroups:
    index = failure_index(request, days=days)
    counts = list(
        index.values("event_type", "failure_group_key")
        .annotate(
            failed=Count("event_id", filter=Q(outcome_class=OutcomeClass.FAILED)),
            refused=Count("event_id", filter=Q(outcome_class=OutcomeClass.REFUSED)),
            latest_at=Max("occurred_at"),
        )
        .order_by("-latest_at")
    )
    shown = counts[:limit]
    if not shown:
        return FailureGroups(groups=[], more=0)

    which = Q()
    for group in shown:
        which |= Q(event_type=group["event_type"], failure_group_key=group["failure_group_key"])
    newest = (
        index.filter(which)
        .order_by("event_type", "failure_group_key", "-occurred_at")
        .distinct("event_type", "failure_group_key")
    )
    latest = {(row.event_type, row.failure_group_key): row for row in newest}
    order = [latest[(g["event_type"], g["failure_group_key"])] for g in shown]
    history = {row.event.pk: row for row in rows_for(order)}

    groups = []
    for group, index_row in zip(shown, order, strict=True):
        row = history.get(index_row.event_id)
        if row is None:
            continue
        described = row.event.failure_group()
        groups.append(
            FailureGroupRow(
                label=described.label,
                url=described.url,
                failed=group["failed"],
                refused=group["refused"],
                latest=row,
                list_url=failures_url(group["event_type"], group["failure_group_key"]),
            )
        )
    return FailureGroups(groups=groups, more=len(counts) - len(shown))
