"""The shared History panel's data (ADR 0030): one subject's Domain events, newest first.

Reads each concrete event table it is asked about through the tenancy
dispatcher, so a panel can never show another organisation's events — nor an
unattributed one, which belongs to nobody. Events are rendered read-only; they
are never snippets and have no list or detail pages of their own.
"""

from dataclasses import dataclass, field

from django.db.models import Min

from georiva.organisations.ownership import scope_rows

from .models import EventIndex

DEFAULT_LIMIT = 50


@dataclass
class HistoryRow:
    event: object
    type_label: str
    occurred_at: object
    actor: str
    summary: str
    items: list = field(default_factory=list)
    outcome_class: str = "ok"
    correlation_id: object = None
    timeline_url: str | None = None
    subject_url: str | None = None
    links: list = field(default_factory=list)


@dataclass
class History:
    rows: list
    recorded_since: object  # when the first event of any kind was recorded on this instance, or None


def recorded_since():
    return EventIndex.objects.aggregate(first=Min("occurred_at"))["first"]


def build_history(request, sources, *, limit=DEFAULT_LIMIT) -> History:
    """``sources`` is ``[(event model, {filter: value}), ...]``; rows merge newest first."""
    rows = []
    for model, filters in sources:
        queryset = scope_rows(request, model._default_manager.filter(**filters))
        for event in queryset.order_by("-occurred_at")[:limit]:
            rows.append(row_for(event))
    rows.sort(key=lambda row: row.occurred_at, reverse=True)
    return History(rows=rows[:limit], recorded_since=recorded_since())


def row_for(event) -> HistoryRow:
    """The one shape every list of events renders from: History, failures, Trace."""
    from .timeline import timeline_url

    return HistoryRow(
        event=event,
        type_label=event.label,
        occurred_at=event.occurred_at,
        actor=event.actor,
        summary=event.summary(),
        items=event.snapshot_items(),
        outcome_class=event.outcome_class,
        correlation_id=event.correlation_id,
        timeline_url=timeline_url(event.correlation_id, event.pk),
        subject_url=event.subject_url(),
        links=event.extra_links(),
    )
