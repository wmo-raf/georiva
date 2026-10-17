"""Putting a build on a queue, and recording that it happened.

Every entry point here exists because doing the obvious thing is wrong in a way
that only shows up under load:

``dispatch_build``   claims *before* queueing, so a record waiting behind a
                     backlog is not dispatched again by the next sweep (#398)
``stand_down``       lets a copy holding a recycled claim exit rather than run
                     beside its replacement
``sweep_builds``     recovers abandoned claims before looking for work, so a
                     crashed build is picked up on the same pass
``build_attempt``    records a failed attempt with whatever it had learned
                     before it died, not just the exception
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SweepResult:
    """What one sweep pass did."""

    reset: int
    dispatched: int


def dispatch_build(model, pk, task, claimed_by: str = "dispatch", force: bool = False) -> bool:
    """Claim one record and queue its build; return whether it was queued.

    The only way a build should reach Celery. Claiming first is what stops a
    record waiting in the queue from being dispatched again by the next sweep.
    The claim is unique per dispatch and travels with the task, so the one case
    claiming alone does not cover — a queue wait long enough for
    ``reset_stale_locks`` to recycle the lock and a later sweep to dispatch a
    second copy — resolves in favour of the newer copy rather than running both.

    A claim we then fail to dispatch leaves the row BUILDING until
    ``reset_stale_locks`` recovers it, which is the same path a worker crash
    takes. ``force`` is the operator's rebuild of a record that does not need
    one.

    ``task`` is dispatched with ``delay(pk, claim)`` and never
    ``apply_async(queue=...)``: the queue a task declares is the only routing
    there is (ADR 0025), and an AST sweep in ``ingestion/tests/test_queue_routing.py``
    keeps it that way.
    """
    claim = f"{claimed_by}-{uuid4().hex[:8]}"
    if not model.claim_for_build(pk, claim, force=force):
        return False

    task.delay(pk, claim)
    return True


def stand_down(record, claim: str) -> bool:
    """Whether this copy of the build should exit without building.

    True when the claim it was dispatched under is no longer the one the row is
    held under: a queue wait longer than ``LOCK_TIMEOUT`` lets the sweep free the
    row and dispatch a replacement, and two builders writing one artefact is the
    failure the claim exists to prevent.
    """
    if record.holds_claim(claim):
        return False

    logger.info(
        "%s %s reclaimed (%s holds it, not %s) — standing down",
        type(record).__name__,
        record.pk,
        record.locked_by or "nobody",
        claim,
    )
    return True


def sweep_builds(model, task, claimed_by: str = "sweep") -> SweepResult:
    """Recover abandoned claims, then claim and dispatch everything buildable.

    The periodic safety net behind every trigger: whatever a signal failed to
    dispatch, or a worker died holding, is picked up on the next pass. Belongs
    on ``georiva-default`` with the other sweeps — never on ``georiva-ingestion``,
    which admits fetch and extraction only (ADR 0025).
    """
    reset = model.reset_stale_locks()
    if reset:
        logger.info("%s: reset %d stale lock(s)", model.__name__, reset)

    pks = list(model.get_buildable().values_list("pk", flat=True))
    dispatched = sum(dispatch_build(model, pk, task, claimed_by=claimed_by) for pk in pks)
    if dispatched:
        logger.info("%s: dispatched %d build task(s)", model.__name__, dispatched)

    return SweepResult(reset=reset, dispatched=dispatched)


@contextmanager
def build_attempt(log_model, target, kind=None):
    """Record one attempt, whichever way it ends.

    Yields a dict the build fills in as it goes, so an attempt that dies
    half-way still reports what it had established — how it classified the work,
    how much it had written — rather than an exception and nothing else. The
    exception propagates: marking the record FAILED is the caller's, because
    only it knows which record and what to say.

        try:
            with build_attempt(PublicationBuildLog, publication) as facts:
                facts["mode"] = plan.mode
                facts["points_written"] = write(plan)
        except Exception as exc:
            publication.mark_failed(str(exc))
    """
    from django.utils import timezone

    kind = kind or log_model.Kind.BUILD
    started_at = timezone.now()
    facts: dict = {}
    try:
        yield facts
    except Exception as exc:
        log_model.record(
            target,
            kind,
            log_model.Outcome.FAILURE,
            started_at,
            error=str(exc),
            **facts,
        )
        raise
    else:
        log_model.record(target, kind, log_model.Outcome.SUCCESS, started_at, **facts)
