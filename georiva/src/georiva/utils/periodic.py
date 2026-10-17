"""Registering Celery beat entries without trusting ``IntervalSchedule`` to be unique.

``django_celery_beat.IntervalSchedule`` has no uniqueness constraint on
``(every, period)``, so duplicates accumulate. ``get_or_create`` does a ``get``
first and raises ``MultipleObjectsReturned`` on them — and a registration
handler that swallows the exception leaves its periodic tasks missing with
nothing but one startup warning. Any matching row is as good as any other, so
reuse the first one; and register each task on its own so one failure cannot
take the rest of the handler down with it.
"""

import logging
from typing import NamedTuple

logger = logging.getLogger(__name__)


class PeriodicSpec(NamedTuple):
    name: str
    task: str
    every: int
    period: str  # an ``IntervalSchedule`` period constant, e.g. ``IntervalSchedule.MINUTES``


def interval_schedule(every: int, period: str):
    from django_celery_beat.models import IntervalSchedule

    return IntervalSchedule.objects.filter(every=every, period=period).order_by(
        "pk"
    ).first() or IntervalSchedule.objects.create(every=every, period=period)


def register_periodic_tasks(specs, *, log=logger.warning) -> None:
    """Upsert one ``PeriodicTask`` per spec, isolating failures per task."""
    from django.db import transaction
    from django_celery_beat.models import PeriodicTask

    for spec in specs:
        try:
            with transaction.atomic():
                PeriodicTask.objects.update_or_create(
                    name=spec.name,
                    defaults={
                        "task": spec.task,
                        "interval": interval_schedule(spec.every, spec.period),
                        "enabled": True,
                    },
                )
        except Exception as exc:
            log("Could not register periodic task %s: %s", spec.name, exc)
