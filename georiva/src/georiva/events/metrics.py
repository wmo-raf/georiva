"""A counter for work that ran without propagated context (ADR 0030).

Strict enforcement in production is switched on only once this has read zero for
a full release, so it has to survive a worker restart: it lives in the cache
(Redis in production) rather than in process memory.
"""

import logging

from django.core.cache import cache

logger = logging.getLogger(__name__)

UNPROPAGATED_KEY = "georiva:events:unpropagated_total"


def increment_unpropagated() -> None:
    try:
        cache.add(UNPROPAGATED_KEY, 0, timeout=None)
        cache.incr(UNPROPAGATED_KEY)
    except Exception:  # a metric must never fail the task it describes
        logger.debug("Could not increment the unpropagated-task metric", exc_info=True)


def unpropagated_count() -> int:
    try:
        return int(cache.get(UNPROPAGATED_KEY, 0))
    except Exception:
        return 0
