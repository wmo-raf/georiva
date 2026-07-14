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


def run_duration_seconds(run):
    """A run's wall-clock duration in seconds, or None if it never finished."""
    if run.started_at and run.finished_at:
        return (run.finished_at - run.started_at).total_seconds()
    return None
