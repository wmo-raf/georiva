"""
A store harvest's model runs (ADR 0026, ADR 0044 §6).

A harvest writes no `FileIngestion`, so nothing else opens or closes the run
record of a collection harvested from a forecast store. The harvest keeps it
itself, from the rows it holds:

- a run opens on its first Held step;
- it closes, as `store_held`, once every step the store expects of it has a
  row for every mapped Variable;
- a closed run that is complete for every Variable mapped now is *settled*:
  the next harvest does not ask the store about it, and its rows stand;
- a run found no longer complete — a step gone upstream, found by a full
  re-check, or a Variable mapped since it closed — reopens at the next revision;
- a run that has aged out of a Rolling window loses its record with its rows.
"""

from __future__ import annotations

from datetime import datetime

from django.db.models import Count, Exists, OuterRef


def settled_runs(collection, variable_pks: set[int], window_q) -> set[datetime]:
    """The closed runs in the window that every mapped Variable holds whole:
    the ones a harvest need not ask about. `window_q` picks the runs in the
    window, by `reference_time`."""
    from georiva.ingestion.models import RunIngestion

    if not variable_pks:
        return set()
    closed = dict(
        RunIngestion.objects.filter(
            window_q,
            collection=collection,
            status=RunIngestion.Status.CLOSED,
            expected_file_count__gt=0,
        ).values_list("reference_time", "expected_file_count")
    )
    held = _held_by_variable(collection, closed, variable_pks)
    return {run for run, expected in closed.items() if all(held.get((run, pk), 0) >= expected for pk in variable_pks)}


def keep_runs(collection, variable_pks: set[int], window_q, *, expected: dict[datetime, int], settled: set[datetime]):
    """Bring the run records of the window up to the rows a harvest left.

    `expected` is how many steps the store expects of each run it was asked
    about and holds a step of; `settled` the runs it was not asked about. A
    run in neither — one with a record whose every step is gone upstream — is
    judged as holding nothing. Call it only once the window was listed whole
    and its rows written and removed.
    """
    from georiva.ingestion.models import RunIngestion

    recorded = set(
        RunIngestion.objects.filter(window_q, collection=collection).values_list("reference_time", flat=True)
    )
    judged = (set(expected) | recorded) - settled
    if not judged:
        return
    held = _held_by_variable(collection, judged, variable_pks)
    steps = dict(
        _items(collection, judged).values("reference_time").annotate(n=Count("pk")).values_list("reference_time", "n")
    )
    for run in sorted(judged):
        known = expected.get(run)
        if known is None:
            known = (
                RunIngestion.objects.filter(collection=collection, reference_time=run)
                .values_list("expected_file_count", flat=True)
                .first()
            )
        complete = bool(known) and bool(variable_pks) and all(held.get((run, pk), 0) >= known for pk in variable_pks)
        RunIngestion.record_held_steps(
            collection, run, held=steps.get(run, 0), expected=expected.get(run), complete=complete
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


def _items(collection, runs):
    from georiva.core.models import Item

    return Item.objects.filter(collection=collection, reference_time__in=list(runs))


def _held_by_variable(collection, runs, variable_pks) -> dict[tuple[datetime, int], int]:
    """How many steps of each run hold a row for each Variable: `(run, pk) → n`."""
    from georiva.core.models import Asset

    if not runs or not variable_pks:
        return {}
    rows = (
        Asset.objects.filter(
            item__collection=collection, item__reference_time__in=list(runs), variable_id__in=variable_pks
        )
        .values("item__reference_time", "variable_id")
        .annotate(n=Count("item", distinct=True))
    )
    return {(row["item__reference_time"], row["variable_id"]): row["n"] for row in rows}
