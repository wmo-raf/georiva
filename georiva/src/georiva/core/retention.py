"""Forecast retention: which of a forecast collection's Items are kept (#499).

A forecast collection that does not retain past forecasts loses every Item whose valid
time has passed, and, with ``retain_latest_run_only``, every Item of a run older than its
latest. ``prune`` deletes those Items; ``retires`` tells whether a file's Items fall
under the same rule, so the Sweep does not take a pruned file for data lost and ingest it
again, for the next prune to delete. One module holds the rule so the two cannot drift.
"""

from __future__ import annotations

from datetime import datetime

from georiva.core.models import Collection, Item


def prunable_collections():
    """The collections retention prunes (``_is_prunable`` is the same rule on one in hand)."""
    return Collection.objects.filter(is_forecast=True, retain_past_forecasts=False, is_active=True)


def _is_prunable(collection) -> bool:
    return collection.is_forecast and not collection.retain_past_forecasts and collection.is_active


def _latest_reference_time(collection) -> datetime | None:
    return (
        Item.objects.filter(collection=collection, reference_time__isnull=False)
        .order_by("-reference_time")
        .values_list("reference_time", flat=True)
        .first()
    )


def prune(collection, now: datetime) -> int:
    """Delete the Items of ``collection`` that retention does not keep; return how many."""
    deleted, _ = Item.objects.filter(collection=collection, time__lt=now).delete()

    if collection.retain_latest_run_only:
        latest = _latest_reference_time(collection)
        if latest:
            stale, _ = Item.objects.filter(
                collection=collection, reference_time__isnull=False, reference_time__lt=latest
            ).delete()
            deleted += stale
    return deleted


def retires(collection, *, valid_time_end: datetime | None, reference_time: datetime | None, now: datetime) -> bool:
    """Whether retention removes every Item a file produced in ``collection``.

    ``valid_time_end`` is the file's last valid time. Without it nothing can be said, and
    the answer is no — the Sweep then re-ingests as it would for data lost.
    """
    if not _is_prunable(collection):
        return False
    if valid_time_end is not None and valid_time_end < now:
        return True
    if collection.retain_latest_run_only and reference_time is not None:
        latest = _latest_reference_time(collection)
        return latest is not None and reference_time < latest
    return False
