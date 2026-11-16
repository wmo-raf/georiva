"""
A store harvest's model runs (ADR 0026, ADR 0044 §6).

A harvest writes no `FileIngestion`, so nothing else opens or closes the run
record of a collection harvested from a forecast store. The harvest keeps it
itself, from the rows it holds:

- a run opens on its first Held step;
- it closes, as `store_held`, once every mapped Variable has a row for every
  step its array is expected at — the run's expected steps from the array's
  own first Held step on, since an average is never held at a run's first
  step (#681). Which steps, not how many: a step held past the run's expected
  length gets a row, and cannot stand in for a missing one (#672);
- a closed run that is complete for every Variable mapped now is *settled*:
  the next harvest does not ask the store about it, and its rows stand;
- a run found no longer complete — a step gone upstream, found by a full
  re-check, or a Variable mapped since it closed — reopens at the next revision;
- a run that has aged out of a Rolling window loses its record with its rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from django.db.models import Count, Exists, OuterRef


@dataclass(frozen=True)
class Expected:
    """What the store expects of one run: how many steps of the run, and the
    lead times each mapped Variable whose array it holds a step of is expected
    at, by Variable pk. A Variable not in `by_variable` is not held whole: the
    store holds no step of its array, or did not say which it expects."""

    steps: int
    by_variable: dict[int, frozenset[timedelta]] = field(default_factory=dict)


def settled_runs(collection, variable_pks: set[int], window_q) -> set[datetime]:
    """The closed runs in the window that every mapped Variable holds whole:
    the ones a harvest need not ask about. `window_q` picks the runs in the
    window, by `reference_time`."""
    from georiva.ingestion.models import RunIngestion

    if not variable_pks:
        return set()
    closed = _recorded(
        RunIngestion.objects.filter(
            window_q,
            collection=collection,
            status=RunIngestion.Status.CLOSED,
            expected_file_count__gt=0,
        )
    )
    held = _held_by_variable(collection, closed, variable_pks)
    return {run for run, expected in closed.items() if _whole(held, run, variable_pks, expected)}


def keep_runs(
    collection, variable_pks: set[int], window_q, *, expected: dict[datetime, Expected], settled: set[datetime]
):
    """Bring the run records of the window up to the rows a harvest left.

    `expected` is what the store expects of each run it was asked about and
    holds a step of; `settled` the runs it was not asked about. A
    run in neither — one with a record whose every step is gone upstream — is
    judged as holding nothing. A run closed on other evidence — by an
    operator — is theirs, and is left as it is. Call it only once the window
    was listed whole and its rows written and removed.
    """
    from georiva.core.models import Item
    from georiva.ingestion.models import RunIngestion

    records = RunIngestion.objects.filter(window_q, collection=collection)
    recorded = _recorded(records)
    closed_elsewhere = set(
        records.filter(status=RunIngestion.Status.CLOSED)
        .exclude(closed_by=RunIngestion.Closer.STORE_HELD)
        .values_list("reference_time", flat=True)
    )
    judged = (set(expected) | set(recorded)) - settled - closed_elsewhere
    if not judged:
        return
    held = _held_by_variable(collection, judged, variable_pks)
    steps = dict(
        Item.objects.filter(collection=collection, reference_time__in=list(judged))
        .values("reference_time")
        .annotate(n=Count("pk"))
        .values_list("reference_time", "n")
    )
    for run in sorted(judged):
        asked = expected.get(run)
        complete = _whole(held, run, variable_pks, asked or recorded.get(run))
        RunIngestion.record_held_steps(
            collection,
            run,
            held=steps.get(run, 0),
            expected=asked.steps if asked else None,
            expected_by_variable=asked.by_variable if asked else None,
            complete=complete,
        )


def forget_aged_out(collection, start: datetime) -> None:
    """Remove the record of every run older than a Rolling window's `start`
    that has no row left. A run whose rows were not the Mirror's to remove
    keeps its record while they last."""
    from georiva.core.models import Item
    from georiva.ingestion.models import RunIngestion

    RunIngestion.objects.filter(collection=collection, reference_time__lt=start).exclude(
        Exists(Item.objects.filter(collection=collection, reference_time=OuterRef("reference_time")))
    ).delete()


def _whole(held, run, variable_pks, expected: Expected | None) -> bool:
    """Whether every one of `variable_pks` holds every step of `run` it is
    expected at. A Variable expected at none — its array held only past the
    run's expected length — holds none of the run, and is not whole."""
    if not expected or not expected.steps or not variable_pks:
        return False
    return all(
        expected.by_variable.get(pk) and expected.by_variable[pk] <= held.get((run, pk), frozenset())
        for pk in variable_pks
    )


def _recorded(records) -> dict[datetime, Expected | None]:
    """What the run records in `records` say the store expected of each run
    when last asked; `None` for a run it said nothing of."""
    from georiva.ingestion.models import RunIngestion

    return {
        run: Expected(steps, RunIngestion.decode_expected_by_variable(by_variable)) if steps else None
        for run, steps, by_variable in records.values_list(
            "reference_time", "expected_file_count", "expected_steps_by_variable"
        )
    }


def _held_by_variable(collection, runs, variable_pks) -> dict[tuple[datetime, int], frozenset[timedelta]]:
    """The lead times of each run that hold a row for each Variable: `(run, pk) → leads`."""
    from georiva.core.models import Asset

    if not runs or not variable_pks:
        return {}
    held: dict[tuple[datetime, int], set[timedelta]] = {}
    rows = (
        Asset.objects.filter(
            item__collection=collection, item__reference_time__in=list(runs), variable_id__in=variable_pks
        )
        .values_list("item__reference_time", "variable_id", "item__time")
        .distinct()
    )
    for run, pk, when in rows:
        held.setdefault((run, pk), set()).add(when - run)
    return {at: frozenset(leads) for at, leads in held.items()}
