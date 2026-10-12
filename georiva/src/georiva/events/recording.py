"""``record()``: the one way a Domain event is written (ADR 0030).

Called from inside the state-changing transition, after the conditional UPDATE
has been won, in the same atomic block. Never from a ``post_save`` receiver.
Part of the supported plugin API.
"""

from django.db import transaction

from .context import require_context
from .models import EventIndex
from .registry import type_key


def record(model, **fields):
    """Write one event of ``model`` and its index row; return the event.

    The base fields come from the context and raise
    :class:`~georiva.events.context.NoEventContext` when none is set. The index
    row is written in the same transaction, so the two never disagree.
    """
    context = require_context()
    with transaction.atomic():
        event = model(
            organisation_id=context.organisation_id,
            correlation_id=context.correlation_id,
            caused_by_id=context.caused_by,
            actor=context.actor,
            **fields,
        )
        event.save()
        EventIndex.objects.create(
            event_id=event.pk,
            event_type=type_key(model),
            correlation_id=event.correlation_id,
            occurred_at=event.occurred_at,
            organisation_id=event.organisation_id,
            outcome_class=event.outcome_class,
        )
    return event
