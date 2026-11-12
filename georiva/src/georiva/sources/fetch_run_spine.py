"""
The run-tracking spine: the `FetchRun` lifecycle, around a loop it does not own.

A run begins, a loop does the work, and the run ends `completed` or — when the
loop stopped or an exception escaped it — `failed` with its stop reason, due
its retry, its feed and collection link stamped, its events recorded (ADR 0031).
What the loop does is its own business: the Loader walks a plan of files; a
harvest lists an upstream window and has no `FetchedFile` rows at all
(ADR 0042 §4–5). Nothing here reads a plan row or a file count.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from django.utils import timezone

logger = logging.getLogger(__name__)


class RunStopped(Exception):
    """Raised by a loop when the source cannot serve it: the run ends early,
    FAILED, for `reason` (#478)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class RunResult:
    """What one run came to, as its caller sees it. A loop subclasses this to
    carry its own counts; the spine fills in how the run ended."""

    started_at: datetime = field(default_factory=timezone.now)
    finished_at: datetime | None = None

    errors: list[str] = field(default_factory=list)

    # Why the run stopped before its loop finished, if it did (#478)
    stop_reason: str | None = None
    # Set when it stopped because the source cannot serve us: the feed's other
    # collections are then not attempted in the same tick.
    source_unavailable: str | None = None
    # The longest wait the source asked for, in seconds: a floor under the
    # retry backoff (#480). The loop sets it.
    retry_after: float | None = None

    @property
    def success(self) -> bool:
        return self.stop_reason is None

    @property
    def status(self) -> str:
        """The word the feed keeps as its `last_run_status`."""
        return "failed" if self.stop_reason else "success"

    def counters(self) -> dict[str, int]:
        """The `FetchRun` counter fields this loop owns, as they stand now. A
        loop with nothing to count reports none."""
        return {}

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    def finish(self):
        self.finished_at = timezone.now()

    def add_error(self, error: str):
        self.errors.append(error)
        # Keep only last 50 errors
        if len(self.errors) > 50:
            self.errors = self.errors[-50:]


def run_tracked(
    walk: Callable,
    *,
    result: RunResult,
    data_feed,
    collection,
    resumed_from=None,
    settle: Callable | None = None,
    logger: logging.Logger = logger,
) -> RunResult:
    """
    Run `walk` inside a `FetchRun` for `collection`, and end the run by how it went.

    `walk(fetch_run)` is the loop. It returns when it has done all it meant to
    (the run ends COMPLETED, whatever failed along the way), raises `RunStopped`
    when the source cannot serve it, or lets an exception escape; both of those
    end the run FAILED, `Stopped: <reason>`, and neither propagates.

    `settle(fetch_run, stop_reason)`, if given, runs once the loop has ended,
    however it ended and before the run is: where the loop lets go of what it
    held and settles whatever rows it planned. `result.counters()` and
    `result.retry_after` are read after it.

    `resumed_from` is the run this one retries. With no `data_feed` there is
    nothing to track: the loop runs with `fetch_run=None` and only `result`
    says how it went.
    """
    from georiva.sources.models import FetchRun

    fetch_run = None
    began_at = timezone.now()
    if data_feed:
        fetch_run = FetchRun.begin(data_feed=data_feed, collection=collection, resumed_from=resumed_from)
        began_at = fetch_run.started_at

    try:
        walk(fetch_run)

    except RunStopped as stop:
        logger.warning(f"Run stopped: {stop.reason}")
        result.add_error(stop.reason)
        result.stop_reason = result.source_unavailable = stop.reason

    except Exception as e:
        logger.exception(f"Run failed: {e}")
        result.add_error(str(e))
        result.stop_reason = f"{type(e).__name__}: {e}"

    finally:
        result.finish()
        if settle:
            settle(fetch_run, result.stop_reason)

        if fetch_run:
            counters = result.counters()
            # A run whose loop finished completed, whatever failed inside it;
            # one that stopped before the end failed (#478).
            if result.stop_reason:
                fetch_run.mark_failed(
                    error=f"{FetchRun.STOPPED_PREFIX}{result.stop_reason}",
                    retry_after=result.retry_after,
                    **counters,
                )
            else:
                fetch_run.mark_completed(retry_after=result.retry_after, **counters)

        if data_feed:
            data_feed._update_run_stats(result, collection, started_at=began_at)

    return result


def record_not_attempted(
    result: RunResult,
    reason: str,
    *,
    data_feed,
    collection,
    resumed_from=None,
    logger: logging.Logger = logger,
) -> RunResult:
    """
    End a collection's run before it starts: a `FetchRun` FAILED at once with
    `reason`, never started, and no loop run. Nothing is asked of the source —
    the feed short-circuit after a sibling collection's run found it
    unavailable (ADR 0031 §5).
    """
    from georiva.sources.models import FetchRun

    result.stop_reason = reason
    result.add_error(reason)
    result.finish()
    if data_feed:
        fetch_run = FetchRun.objects.create(
            data_feed=data_feed,
            collection=collection,
            status=FetchRun.Status.RUNNING,
            resumed_from=resumed_from,
        )
        fetch_run.mark_failed(error=reason)
        data_feed._update_run_stats(result, collection, started_at=fetch_run.started_at)
    logger.info(f"Not attempted: {reason}")
    return result
