"""
Timeouts for a single derivation unit, in one place so the Celery task time
limit and the ``DerivationRun`` lock timeout can't drift apart.

A unit (``run_unit_task``) is bounded by a soft then hard Celery time limit; the
``DerivationRun`` lock timeout sits strictly above the hard limit. Because Celery
guarantees no task outlives the hard limit, a live task can never have its lock
stolen mid-run — while a worker that dies *without* releasing recovers a short
margin after the hard kill instead of after hours.
"""
# Soft limit: Celery raises ``SoftTimeLimitExceeded`` inside the task, which
# ``engine.run_unit`` catches (``except Exception``) → ``mark_failed`` → the lock
# is released immediately. This is the graceful recovery path for a task that has
# simply run too long (e.g. a pathologically large raster, or contention).
RUN_UNIT_SOFT_TIME_LIMIT_SECONDS = 13 * 60   # 780s

# Hard limit: the worker force-kills the task's process. No chance to release the
# lock, so the lock timeout below is the backstop. Only reached when the soft
# exception can't unwind — e.g. stuck in a native GDAL/rasterio call that ignores
# Python signals.
RUN_UNIT_HARD_TIME_LIMIT_SECONDS = 15 * 60   # 900s

# Grace after the hard kill before another worker may reclaim the lock, letting
# the killed task's DB state and broker redelivery settle.
LOCK_TIMEOUT_MARGIN_SECONDS = 5 * 60         # 300s

# The DerivationRun stale-lock timeout. Strictly greater than the hard task limit
# (so a live, time-limited task's lock is never stolen) plus the settle margin —
# 20 min total, versus the old fixed 2h that left a dead unit unrecoverable for
# hours.
DERIVATION_LOCK_TIMEOUT_SECONDS = (
    RUN_UNIT_HARD_TIME_LIMIT_SECONDS + LOCK_TIMEOUT_MARGIN_SECONDS
)  # 1200s = 20 min
