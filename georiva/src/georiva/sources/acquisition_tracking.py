"""
Acquisition run-tracking (PRD #217).

The read-side query module for the feed-scoped Acquisition Activity pages —
the acquisition analogue of derivation_tracking: query logic lives here,
the views stay dumb. Runs are collection-agnostic (ADR-0003): per-file
collection context is encoded in FetchedFile.file_path, not on the run.
"""
from __future__ import annotations


def feed_fetch_runs(feed, *, status=None):
    """A feed's FetchRuns for the Acquisition Activity list, newest first.

    An optional `status` narrows to a single run status.
    """
    runs = feed.fetch_runs.all()
    if status:
        runs = runs.filter(status=status)
    return runs.order_by("-started_at")


def with_live_counters(run):
    """Overlay derived counters onto an in-flight run, in memory only.

    Stored counters are a write-once completion summary, so a run still in
    RUNNING shows zeros while its FetchedFile rows tell the real story —
    aggregate those instead. Finished runs are returned untouched.
    """
    from georiva.sources.models import FetchRun

    if run.status == FetchRun.Status.RUNNING:
        for field, value in run.derive_counters().items():
            setattr(run, field, value)
    return run


def run_duration_seconds(run):
    """A run's wall-clock duration in seconds, or None if it never finished."""
    if run.started_at and run.finished_at:
        return (run.finished_at - run.started_at).total_seconds()
    return None
