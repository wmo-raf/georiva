"""
Fetch-run recovery sweep — crash recovery and the retry of failed runs.

A hard worker death (power loss, OOM kill, forced restart) skips Loader.run()'s
finally block, stranding the FetchRun in RUNNING and its in-flight FetchedFile
in FETCHING forever, and can leave the wrapping task-ferry LoaderJob wedged in
'started' — where it counts against the per-user concurrency quota. This sweep
declares such runs dead on age and marks them INTERRUPTED.

Interrupted runs and runs that ended with failures share one way to carry on:
the finished run is given a `retry_due_at` (see retry_policy), and this sweep
dispatches every run whose time has passed as a system LoaderJob scoped to the
run's collection. The retry is a new FetchRun linked by `resumed_from` that
fetches the earlier run's failed requests (Loader plan_from_earlier_run). The
lineage cap lives in retry_policy: a deterministic crash (a poison file that
OOMs the worker every time) parks instead of looping forever.

The broker visibility_timeout is raised above the staleness threshold (see
settings.base) so acks-late redelivery never races this sweep; the dispatch
guard below makes the sweep idempotent against any concurrent recovery anyway.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

INTERRUPTED_FILE_ERROR = "interrupted — worker stopped mid-fetch"

# Reason codes returned by dispatch_retry / recover_run.
RESUME_QUEUED = "queued"
RESUME_CAP_REACHED = "cap_reached"
RESUME_ALREADY_UNDER_WAY = "already_under_way"
RESUME_ENQUEUE_FAILED = "enqueue_failed"

OPERATOR_SWEEP_REASON = "Interrupted — declared dead by an operator (targeted sweep)."


def stale_after_hours() -> int:
    return getattr(settings, "GEORIVA_FETCH_RUN_STALE_AFTER_HOURS", 6)


def stale_cutoff(now=None):
    return (now or timezone.now()) - timedelta(hours=stale_after_hours())


def sweep_stale_fetch_runs(
    *,
    stale_hours: float | None = None,
    run_ids: list[int] | None = None,
    resume: bool = True,
) -> dict:
    """Interrupt FetchRuns stuck in RUNNING past the staleness threshold, then
    dispatch every run whose retry is due.

    For each stale run: fail its dangling file records, freeze truthful
    counters, mark the run INTERRUPTED (which schedules its retry), and backfill
    the feed's last-run stats. Also reaps LoaderJobs wedged in 'started' so they
    stop eating the quota.

    Operator overrides (management command):
      stale_hours — replace the settings threshold for this invocation
                    (0 declares every running run dead — a hard sweep).
      run_ids     — sweep exactly these runs, ignoring age entirely; the
                    operator is asserting the runs are dead. Nothing else is
                    dispatched.
      resume      — False marks runs INTERRUPTED without scheduling retries,
                    and dispatches none.
    """
    from georiva.sources.models import FetchRun

    hours = stale_after_hours() if stale_hours is None else stale_hours
    cutoff = timezone.now() - timedelta(hours=hours)

    # Reap zombie jobs first so a wedged LoaderJob can't veto its own retry.
    jobs_reaped = _reap_stale_loader_jobs(cutoff)

    stale_runs = (
        FetchRun.objects.filter(status=FetchRun.Status.RUNNING).select_related("data_feed").order_by("started_at")
    )
    if run_ids is not None:
        stale_runs = stale_runs.filter(pk__in=run_ids)
        reason = OPERATOR_SWEEP_REASON
    else:
        stale_runs = stale_runs.filter(started_at__lt=cutoff)
        reason = f"Interrupted — worker stopped mid-run; still unfinished after {hours}h."

    swept = 0
    for run in stale_runs:
        _sweep_run(run, reason)
        if not resume:
            FetchRun.objects.filter(pk=run.pk).update(retry_due_at=None)
        swept += 1

    resumed = dispatch_due_retries() if resume and run_ids is None else 0

    if swept or resumed or jobs_reaped:
        logger.info(
            "Recovery sweep: %d run(s) interrupted, %d retry(ies) dispatched, %d zombie job(s) reaped",
            swept,
            resumed,
            jobs_reaped,
        )
    return {"swept": swept, "resumed": resumed, "jobs_reaped": jobs_reaped}


def _sweep_run(run, reason) -> None:
    from georiva.sources.fetch.base import FailureScope
    from georiva.sources.models import FetchedFile

    dangling = run.fetched_files.filter(
        status__in=[FetchedFile.Status.PENDING, FetchedFile.Status.FETCHING],
    )
    for fetched_file in dangling:
        # A row still Waiting was never reached; one Fetching was cut off mid-way.
        reached = fetched_file.status == FetchedFile.Status.FETCHING
        scope = FailureScope.TRANSIENT if reached else FailureScope.NOT_ATTEMPTED
        fetched_file.mark_failed(error=INTERRUPTED_FILE_ERROR, scope=scope)

    run.recompute_counters()
    run.mark_interrupted(error=reason)
    _backfill_feed_stats(run)
    logger.warning(
        "FetchRun %d (%s): marked interrupted — %s",
        run.pk,
        run.data_feed.name,
        reason,
    )


def _backfill_feed_stats(run) -> None:
    """A crashed run never reached _update_run_stats, so a first-ever run
    leaves the feed claiming "Never run". Record the attempt — unless a newer
    run has already reported fresher truth."""
    feed = run.data_feed
    if feed.last_run_at and feed.last_run_at >= run.started_at:
        return
    feed.last_run_at = run.started_at
    feed.last_run_status = "failed"
    feed.last_run_message = "Interrupted — worker stopped mid-run"
    feed.save(update_fields=["last_run_at", "last_run_status", "last_run_message"])


def recover_run(run) -> str:
    """Operator recovery of a single running FetchRun (the admin's Recover
    button): sweep it as interrupted, then dispatch its retry now rather than
    at its backoff time. Returns the reason code so the caller can tell the
    operator whether a new fetch actually started."""
    _sweep_run(run, OPERATOR_SWEEP_REASON)
    run.refresh_from_db()
    if run.parked:
        return RESUME_CAP_REACHED
    return dispatch_retry(run)


def dispatch_due_retries(now=None) -> int:
    """Dispatch the retry of every run whose `retry_due_at` has passed. Returns
    how many were enqueued."""
    from georiva.sources.models import FetchRun

    due = (
        FetchRun.objects.filter(retry_due_at__lte=now or timezone.now(), resumes__isnull=True)
        .select_related("data_feed")
        .order_by("retry_due_at")
    )
    return sum(dispatch_retry(run) == RESUME_QUEUED for run in due)


def dispatch_retry(run) -> str:
    """Enqueue a system LoaderJob that retries `run`, scoped to its collection.

    Returns RESUME_QUEUED on success, or why it was skipped: the run already has
    a retry (a run, or a job not yet started), or the enqueue failed. Whether a
    newer run exists for the feed is deliberately not asked — the schedule and a
    retry are independent (#480).
    """
    from task_ferry.handler import JobHandler
    from task_ferry.models import JOB_STATES_PENDING_OR_RUNNING

    from georiva.ingestion.models import LoaderJob

    if (
        run.resumes.exists()
        or LoaderJob.objects.filter(resume_of_run=run, state__in=JOB_STATES_PENDING_OR_RUNNING).exists()
    ):
        logger.info("FetchRun %d: already retried or being retried — not dispatching", run.pk)
        return RESUME_ALREADY_UNDER_WAY

    try:
        JobHandler.create_and_start(
            None,  # system job: exempt from the per-user max_count quota
            "data_source_load",
            data_feed_id=run.data_feed_id,
            collection_id=run.collection_id,
            resume_of_run_id=run.pk,
        )
    except Exception:
        logger.exception("FetchRun %d: failed to enqueue its retry", run.pk)
        return RESUME_ENQUEUE_FAILED

    logger.info("FetchRun %d: retry %d dispatched", run.pk, run.retry_number)
    return RESUME_QUEUED


def _reap_stale_loader_jobs(cutoff) -> int:
    """Fail LoaderJobs wedged in 'started' past the threshold. Their Celery
    message is gone (or covered by the same sweep), and every zombie counts
    against its user's max_count quota until it reaches a terminal state."""
    from task_ferry.models import JOB_STARTED

    from georiva.ingestion.models import LoaderJob

    reaped = 0
    for job in LoaderJob.objects.filter(state=JOB_STARTED, updated_at__lt=cutoff):
        job.mark_failed(
            error="interrupted — worker stopped mid-run (reaped by stale-run sweep)",
            human_readable_error="Interrupted — the worker stopped mid-run.",
        )
        reaped += 1
        logger.warning("LoaderJob %d: reaped (stuck in 'started')", job.pk)
    return reaped
