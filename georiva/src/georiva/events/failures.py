"""Recent failed and refused transitions for the active organisation (ADR 0030).

The one deliberate org-wide listing in the design — an explicit exception to the
decision that retired the Monitoring menu (ADR 0019), justified by silent-failure
detection: a failure has to reach someone who does not already know which object
to open. It reads the index table's outcome class, so it needs no per-type query
to *find* failures; details are then fetched once per type that actually appears,
which is bounded by the row limit however many event types are installed.
"""

from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from georiva.organisations.ownership import scope_rows

from .history import row_for
from .models import EventIndex, OutcomeClass
from .registry import installed_tables, model_for

DEFAULT_DAYS = 7
DEFAULT_LIMIT = 20


def window_days() -> int:
    return int(getattr(settings, "GEORIVA_FAILURES_PANEL_DAYS", DEFAULT_DAYS))


def recent_failures(request, *, days=None, limit=DEFAULT_LIMIT) -> list:
    since = timezone.now() - timedelta(days=days if days is not None else window_days())
    rows = list(
        scope_rows(
            request,
            EventIndex.objects.filter(
                outcome_class__in=(OutcomeClass.FAILED, OutcomeClass.REFUSED), occurred_at__gte=since
            ),
        ).order_by("-occurred_at")[:limit]
    )

    existing = installed_tables()
    wanted: dict[str, list] = {}
    for row in rows:
        wanted.setdefault(row.event_type, []).append(row.event_id)
    events = {}
    for key, ids in wanted.items():
        model = model_for(key)
        if model is None or model._meta.db_table not in existing:
            continue
        for event in model._default_manager.filter(pk__in=ids):
            events[event.pk] = event

    failures = []
    for row in rows:
        event = events.get(row.event_id)
        if event is None:
            continue
        failures.append(row_for(event))
    return failures
