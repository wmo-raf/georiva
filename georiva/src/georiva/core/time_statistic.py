"""
What a source says of a field's time statistic (#818, ADR 0051).

A field is either a value at its time (`instant`) or a statistic — a sum,
mean, maximum or minimum — over a window: since its run started, or over the
interval before each step. A Variable declares which (`Variable.time_statistic`
and `Variable.statistic_window`); the source sets it when the Variable is
created, from what it says here. Each reader returns `None` where its source
says nothing it understands: no declaration, not `instant`.

Pure: no Django, no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

INSTANT_STATISTIC = "instant"
RUN_START = "run_start"
PRECEDING_INTERVAL = "preceding_interval"


@dataclass(frozen=True)
class Declared:
    """A time statistic and its window; the window is empty for `instant`."""

    statistic: str
    window: str = ""

    @property
    def is_instant(self) -> bool:
        return self.statistic == INSTANT_STATISTIC

    def fields(self) -> dict[str, str]:
        """The Variable fields this declaration sets."""
        return {"time_statistic": self.statistic, "statistic_window": self.window}

    def __str__(self) -> str:
        return self.statistic if self.is_instant else f"{self.statistic} ({self.window.replace('_', ' ')})"


INSTANT = Declared(INSTANT_STATISTIC)

#: ecCodes' `stepType` — and the dynamical.org stores' `step_type`, which copies it — as a statistic.
_STEP_TYPES = {"accum": "sum", "avg": "mean", "max": "max", "min": "min"}

#: CF `cell_methods` methods over time, as a statistic.
_CELL_METHODS = {"sum": "sum", "mean": "mean", "maximum": "max", "minimum": "min"}

_CELL_METHOD_TIME = re.compile(r"\btime\s*:\s*(\w+)")


def from_store_array(path: str, attrs: Mapping) -> Declared | None:
    """An Upstream store array's declaration: its `step_type`, with its window
    from its name (AIFS ends a running total's in `_run_total`) or its
    `comment` (GFS names the window there)."""
    step_type = str(attrs.get("step_type") or "")
    if step_type == "instant":
        return INSTANT
    statistic = _STEP_TYPES.get(step_type)
    if statistic is None:
        return None
    comment = str(attrs.get("comment") or "").lower()
    since_start = "_run_total" in path.lower() or "initialization time" in comment or "since the start" in comment
    return Declared(statistic, RUN_START if since_start else PRECEDING_INTERVAL)


def from_grib_step_type(step_type: str | None) -> Declared | None:
    """A GRIB message's declaration, from cfgrib's `GRIB_stepType`. GRIB's
    stepType does not say where an accumulation's window starts; a forecast's
    (ECMWF `tp`, `ssrd`) runs from its start, and is taken to. An average or
    an extreme is over the interval before its step."""
    if step_type == "instant":
        return INSTANT
    statistic = _STEP_TYPES.get(step_type or "")
    if statistic is None:
        return None
    return Declared(statistic, RUN_START if statistic == "sum" else PRECEDING_INTERVAL)


def from_cell_methods(cell_methods: str | None) -> Declared | None:
    """A NetCDF variable's declaration, from its CF `cell_methods` over time.
    A CF statistic is over the cell's own interval: the one before its time."""
    found = _CELL_METHOD_TIME.search(cell_methods or "")
    if found is None:
        return None
    method = found.group(1)
    if method == "point":
        return INSTANT
    statistic = _CELL_METHODS.get(method)
    return Declared(statistic, PRECEDING_INTERVAL) if statistic else None
