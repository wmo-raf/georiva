"""
When a fetch run that ended with failures is retried (#480).

The backoff is measured from when the run finished and indexed by its depth in
the retry lineage; a `retry_after` hint from the source is a floor. Past the
lineage cap a run is parked: nothing retries it.
"""

from __future__ import annotations

from datetime import timedelta

from django.conf import settings

BACKOFF_MINUTES = (5, 15, 45, 120)


def max_retries() -> int:
    return settings.GEORIVA_FETCH_RUN_MAX_RETRIES


def retry_delay(depth: int, retry_after: float | None = None) -> timedelta | None:
    """How long after finishing a run `depth` retries into its lineage is retried,
    or None when the lineage has used its retries."""
    if depth >= max_retries():
        return None
    delay = timedelta(minutes=BACKOFF_MINUTES[min(depth, len(BACKOFF_MINUTES) - 1)])
    if retry_after:
        delay = max(delay, timedelta(seconds=retry_after))
    return delay
