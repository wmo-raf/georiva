"""
Append/rebuild classifier for Icechunk repo builds.

Watermark = arrival order: the committed watermark is the max ``Asset.modified``
seen at the last successful commit (read back from Icechunk commit metadata,
never the Django row).  At build time:

    * new items strictly after the committed ``time_end``  → APPEND
    * anything earlier/overlapping (backfill, out-of-order arrival,
      same-key COG overwrite, deletion)                     → REBUILD
    * nothing new and nothing changed                       → UP_TO_DATE

Pure functions only — no ORM, no I/O — so the decision table is unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class BuildMode(Enum):
    REBUILD = "rebuild"
    APPEND = "append"
    UP_TO_DATE = "up_to_date"
    # Set by the build task when the variable has no COG assets at all;
    # classify() itself never returns it (it requires at least one row).
    NO_DATA = "no_data"


@dataclass(frozen=True)
class SourceRow:
    """One COG asset feeding the repo: item time, chunk URL, arrival stamp."""

    time: datetime
    url: str
    modified: datetime


@dataclass(frozen=True)
class CommittedState:
    """What the repo's latest commit says it contains (decision 5)."""

    watermark: datetime
    time_end: datetime
    item_count: int


@dataclass(frozen=True)
class BuildPlan:
    mode: BuildMode
    rows: tuple[SourceRow, ...]
    watermark: datetime | None


def classify(
    committed: CommittedState | None,
    rows: list[SourceRow],
) -> BuildPlan:
    """
    Decide how this build cycle updates the repo.

    ``rows`` is the full current set of COG assets for the variable; the
    returned plan carries only the rows to write (all of them for a rebuild,
    the new tail for an append, none when up to date), sorted by time.
    """
    if not rows:
        raise ValueError("classify() needs at least one source row")

    ordered = tuple(sorted(rows, key=lambda r: r.time))
    watermark = max(r.modified for r in ordered)

    if committed is None:
        return BuildPlan(BuildMode.REBUILD, ordered, watermark)

    in_range = [r for r in ordered if r.time <= committed.time_end]

    # Backfill or deletion inside the committed range changes the in-range
    # item count — the committed axis no longer matches reality.
    if len(in_range) != committed.item_count:
        return BuildPlan(BuildMode.REBUILD, ordered, watermark)

    # Same-key overwrite: update_or_create rewrote a COG in place, bumping
    # Asset.modified past the committed watermark.
    if any(r.modified > committed.watermark for r in in_range):
        return BuildPlan(BuildMode.REBUILD, ordered, watermark)

    new_rows = tuple(r for r in ordered if r.time > committed.time_end)
    if not new_rows:
        return BuildPlan(BuildMode.UP_TO_DATE, (), None)

    return BuildPlan(BuildMode.APPEND, new_rows, watermark)
