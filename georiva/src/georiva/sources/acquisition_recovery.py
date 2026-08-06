"""
Stale fetch-run recovery — the crash-recovery sweep.

A hard worker death (power loss, OOM kill, forced restart) skips Loader.run()'s
finally block, stranding the FetchRun in RUNNING and its in-flight FetchedFile
in FETCHING forever, and can leave the wrapping task-ferry LoaderJob wedged in
'started' — where it counts against the per-user concurrency quota. This module
is the deliberate recovery mechanism, mirroring ingestion's sweep_unprocessed
idiom: declare runs dead on age, mark them INTERRUPTED, and auto-resume by
enqueuing a fresh full loader run (skip_existing dedupes what already landed).

Auto-resume is capped at MAX_AUTO_RESUMES per original run — lineage is walked
via FetchRun.resumed_from — so a deterministic crash (a poison file that OOMs
the worker every time) parks after two attempts instead of looping forever.

The broker visibility_timeout is raised above the staleness threshold (see
settings.base) so acks-late redelivery never races this sweep; the resume
guard below makes the sweep idempotent against any concurrent recovery anyway.
"""
from __future__ import annotations

import logging
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

MAX_AUTO_RESUMES = 2
INTERRUPTED_FILE_ERROR = "interrupted — worker stopped mid-fetch"


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
    """Sweep FetchRuns stuck in RUNNING past the staleness threshold.

    For each: fail its dangling file records, freeze truthful counters, mark
    the run INTERRUPTED, backfill the feed's last-run stats, and enqueue an
    auto-resume (subject to the cap and the concurrent-recovery guard). Also
    reaps LoaderJobs wedged in 'started' so they stop eating the quota.

    Operator overrides (management command):
      stale_hours — replace the settings threshold for this invocation
                    (0 declares every running run dead — a hard sweep).
      run_ids     — sweep exactly these runs, ignoring age entirely; the
                    operator is asserting the runs are dead.
      resume      — False marks runs INTERRUPTED without enqueuing resumes.
    """
    from georiva.sources.models import FetchRun

    hours = stale_after_hours() if stale_hours is None else stale_hours
    cutoff = timezone.now() - timedelta(hours=hours)

    # Reap zombie jobs first so a wedged LoaderJob can't veto its own resume.
    jobs_reaped = _reap_stale_loader_jobs(cutoff)

    stale_runs = (
        FetchRun.objects
        .filter(status=FetchRun.Status.RUNNING)
        .select_related("data_feed")
        .order_by("started_at")
    )
    if run_ids is not None:
        stale_runs = stale_runs.filter(pk__in=run_ids)
        reason = "Interrupted — declared dead by an operator (targeted sweep)."
    else:
        stale_runs = stale_runs.filter(started_at__lt=cutoff)
        reason = (
            f"Interrupted — worker stopped mid-run; still unfinished after "
            f"{hours}h."
        )

    swept = resumed = 0
    for run in stale_runs:
        _sweep_run(run, reason)
        swept += 1
        if resume and _maybe_resume(run):
            resumed += 1

    if swept or jobs_reaped:
        logger.info(
            "Stale-run sweep: %d run(s) interrupted, %d resume(s) enqueued, "
            "%d zombie job(s) reaped",
            swept, resumed, jobs_reaped,
        )
    return {"swept": swept, "resumed": resumed, "jobs_reaped": jobs_reaped}


def _sweep_run(run, reason) -> None:
    from georiva.sources.models import FetchedFile

    dangling = run.fetched_files.filter(
        status__in=[FetchedFile.Status.PENDING, FetchedFile.Status.FETCHING],
    )
    for fetched_file in dangling:
        fetched_file.mark_failed(error=INTERRUPTED_FILE_ERROR)

    run.recompute_counters()
    run.mark_interrupted(error=reason)
    _backfill_feed_stats(run)
    logger.warning(
        "FetchRun %d (%s): marked interrupted — %s",
        run.pk, run.data_feed.name, reason,
    )


def _backfill_feed_stats(run) -> None:
    """A crashed run never reached _update_run_stats, so a first-ever run
    leaves the feed claiming "Never run". Record the attempt — unless a newer
    run has already reported fresher truth."""
    feed = run.data_feed
    if feed.last_run_at and feed.last_run_at >= run.started_at:
        return
    feed.last_run_at = run.started_at
    feed.last_run_status = 'failed'
    feed.last_run_message = 'Interrupted — worker stopped mid-run'
    feed.save(update_fields=['last_run_at', 'last_run_status', 'last_run_message'])


def _maybe_resume(run) -> bool:
    """Enqueue a fresh full loader run for the interrupted run's feed.

    Skipped when the lineage cap is reached, or when recovery is already under
    way — a newer FetchRun for the feed, or a pending/started LoaderJob.
    """
    from task_ferry.handler import JobHandler
    from task_ferry.models import JOB_STATES_PENDING_OR_RUNNING

    from georiva.ingestion.models import LoaderJob
    from georiva.sources.models import FetchRun

    generation = run.resume_generation()
    if generation >= MAX_AUTO_RESUMES:
        logger.warning(
            "FetchRun %d: auto-resume cap (%d) reached — leaving for a human",
            run.pk, MAX_AUTO_RESUMES,
        )
        return False

    feed = run.data_feed
    if FetchRun.objects.filter(
        data_feed=feed, started_at__gt=run.started_at,
    ).exists():
        logger.info(
            "FetchRun %d: newer run exists for feed %s — not resuming",
            run.pk, feed.pk,
        )
        return False
    if LoaderJob.objects.filter(
        data_feed=feed, state__in=JOB_STATES_PENDING_OR_RUNNING,
    ).exists():
        logger.info(
            "FetchRun %d: a loader job is already pending/running for feed %s "
            "— not resuming",
            run.pk, feed.pk,
        )
        return False

    try:
        JobHandler.create_and_start(
            None,  # system job: exempt from the per-user max_count quota
            "data_source_load",
            data_feed_id=feed.pk,
            resume_of_run_id=run.pk,
        )
    except Exception:
        logger.exception("FetchRun %d: failed to enqueue auto-resume", run.pk)
        return False

    logger.info(
        "FetchRun %d: auto-resume enqueued (attempt %d of %d)",
        run.pk, generation + 1, MAX_AUTO_RESUMES,
    )
    return True


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
