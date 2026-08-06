"""
Acquisition run-tracking (PRD #217).

The read-side query module for the feed-scoped Acquisition Activity pages —
the acquisition analogue of derivation_tracking: query logic lives here,
the views stay dumb. Runs are collection-agnostic (ADR-0003): per-file
collection context is encoded in FetchedFile.file_path, not on the run.
"""
from __future__ import annotations

from statistics import median

# Liveness verdict thresholds (run_liveness). Tuning knobs, not settings —
# nobody should have to configure these to use the Recover button.
LIVENESS_STUCK_MULTIPLIER = 5
LIVENESS_SLOW_MULTIPLIER = 2
# A fast feed's median can be seconds; without an absolute floor a brief
# network hiccup would read as a death.
LIVENESS_STUCK_MIN_SILENCE_SECONDS = 120
LIVENESS_MIN_SAMPLES = 3
# Cross-collection copies register mark_fetching+mark_stored back-to-back;
# sub-second "fetches" are copies, not evidence of real transfer speed.
LIVENESS_MIN_SAMPLE_SECONDS = 1.0


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


def run_liveness(run, now=None):
    """The stuck-vs-slow verdict for a RUNNING run, with its evidence.

    Self-calibrating: *silence* (time since the run's last file activity —
    a file starting, a file finishing, or failing that the run's own start)
    is judged against *typical* (the median duration of this run's completed
    real fetches). Advisory only — the Recover button never hides behind it.

    Returns None for a run that is not RUNNING. Otherwise a dict:
      verdict          — 'stuck' | 'slow' | 'normal' | 'unknown'
      run_elapsed_seconds, silence_seconds, median_seconds (None if unknown),
      sample_count, current_file (the in-flight FetchedFile, or None)
    """
    from django.utils import timezone

    from georiva.sources.models import FetchedFile, FetchRun

    if run.status != FetchRun.Status.RUNNING:
        return None
    now = now or timezone.now()

    files = list(run.fetched_files.all())
    samples = []
    for f in files:
        if f.status != FetchedFile.Status.STORED:
            continue
        if not (f.started_at and f.completed_at):
            continue
        duration = (f.completed_at - f.started_at).total_seconds()
        if duration >= LIVENESS_MIN_SAMPLE_SECONDS:
            samples.append(duration)

    last_activity = max(
        [run.started_at]
        + [f.started_at for f in files if f.started_at]
        + [f.completed_at for f in files if f.completed_at]
    )
    silence = (now - last_activity).total_seconds()

    liveness = {
        "run_elapsed_seconds": (now - run.started_at).total_seconds(),
        "silence_seconds": silence,
        "sample_count": len(samples),
        # No median below the sample floor: two data points make a guess,
        # not a baseline, and the panel must not dress a guess as one.
        "median_seconds": (
            median(samples) if len(samples) >= LIVENESS_MIN_SAMPLES else None
        ),
        "current_file": next(
            (f for f in files if f.status == FetchedFile.Status.FETCHING), None,
        ),
    }

    if len(samples) < LIVENESS_MIN_SAMPLES:
        liveness["verdict"] = "unknown"
    elif silence > max(
        LIVENESS_STUCK_MULTIPLIER * liveness["median_seconds"],
        LIVENESS_STUCK_MIN_SILENCE_SECONDS,
    ):
        liveness["verdict"] = "stuck"
    elif silence > LIVENESS_SLOW_MULTIPLIER * liveness["median_seconds"]:
        liveness["verdict"] = "slow"
    else:
        liveness["verdict"] = "normal"
    return liveness


def run_liveness_display(run, now=None):
    """run_liveness() shaped for templates: the same dict plus preformatted
    duration strings, so the panel stays free of formatting logic."""
    liveness = run_liveness(run, now=now)
    if liveness is None:
        return None
    liveness["elapsed_display"] = format_duration(liveness["run_elapsed_seconds"])
    liveness["silence_display"] = format_duration(liveness["silence_seconds"])
    liveness["median_display"] = format_duration(liveness["median_seconds"])
    return liveness


def format_duration(seconds):
    """Compact human duration for the liveness panel: 40s, 5m 20s, 2h 05m."""
    if seconds is None:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def run_duration_seconds(run):
    """A run's wall-clock duration in seconds, or None if it never finished."""
    if run.started_at and run.finished_at:
        return (run.finished_at - run.started_at).total_seconds()
    return None
