"""Bounded derivation units + an aligned lock timeout (Option 1).

A single derivation unit (``run_unit_task``) is bounded by a soft then hard
Celery time limit, and ``DerivationRun.LOCK_TIMEOUT`` sits just above the hard
limit. So a live, time-limited task can never have its lock stolen mid-run, while
a worker that dies without releasing recovers minutes after the hard kill instead
of after hours (the old fixed 2h). The soft limit is the graceful path: it raises
inside the task, ``run_unit`` catches it and marks the run failed, freeing the
lock immediately.
"""
from datetime import timedelta

from django.test import SimpleTestCase

from georiva.processing.constants import (
    DERIVATION_LOCK_TIMEOUT_SECONDS,
    RUN_UNIT_HARD_TIME_LIMIT_SECONDS,
    RUN_UNIT_SOFT_TIME_LIMIT_SECONDS,
)
from georiva.processing.models import DerivationRun
from georiva.processing.tasks import run_unit_task


class TaskTimeLimitInvariantTests(SimpleTestCase):
    def test_soft_below_hard_below_lock_timeout(self):
        # The correctness invariant: soft fires first (graceful cleanup), and the
        # lock only becomes stealable strictly *after* the hard kill — so Celery's
        # guarantee that no task outlives the hard limit means a live task's lock
        # can never be stolen.
        self.assertLess(
            RUN_UNIT_SOFT_TIME_LIMIT_SECONDS, RUN_UNIT_HARD_TIME_LIMIT_SECONDS
        )
        self.assertLess(
            RUN_UNIT_HARD_TIME_LIMIT_SECONDS, DERIVATION_LOCK_TIMEOUT_SECONDS
        )

    def test_run_unit_task_declares_both_time_limits(self):
        self.assertEqual(
            run_unit_task.soft_time_limit, RUN_UNIT_SOFT_TIME_LIMIT_SECONDS
        )
        self.assertEqual(run_unit_task.time_limit, RUN_UNIT_HARD_TIME_LIMIT_SECONDS)

    def test_lock_timeout_tracks_the_constant_and_is_far_below_the_old_2h(self):
        self.assertEqual(
            DerivationRun.LOCK_TIMEOUT,
            timedelta(seconds=DERIVATION_LOCK_TIMEOUT_SECONDS),
        )
        # Recovery in well under an hour (was 2h).
        self.assertLess(DerivationRun.LOCK_TIMEOUT, timedelta(hours=1))
