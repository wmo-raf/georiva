"""
Coverage reporting for the virtual Zarr layer (spec #341).

The one service seam behind every monitoring surface: a structured report per
collection/variable comparing the catalog (Items joined to COG assets — the
same query the builder uses) against the repo (the Icechunk tip's time
coordinate, read live — the source of truth; nothing per-timestamp is
persisted).  Views are thin renderers over ``collection_coverage`` /
``variable_coverage``.

The timestamp-diff logic is pure functions in the style of ``classify`` —
no ORM, no I/O — so the arithmetic is unit-testable on its own.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from georiva.core.models import Variable

    from .models import VirtualZarrManifest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def as_utc(ts: datetime) -> datetime:
    """
    Normalise to tz-aware UTC.

    The builder writes tz-naive UTC into the repo's time axis while the
    catalog side is tz-aware — both sides pass through here so they compare
    equal.
    """
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt_timezone.utc)
    return ts.astimezone(dt_timezone.utc)


@dataclass(frozen=True)
class TimestampDiff:
    """Catalog vs repo time axes, both sorted ascending."""

    missing: tuple[datetime, ...]  # in the catalog, not in the repo
    extra: tuple[datetime, ...]    # in the repo, not in the catalog


def diff_timestamps(catalog_times, repo_times) -> TimestampDiff:
    catalog = {as_utc(t) for t in catalog_times}
    repo = {as_utc(t) for t in repo_times}
    return TimestampDiff(
        missing=tuple(sorted(catalog - repo)),
        extra=tuple(sorted(repo - catalog)),
    )


def freshness_lag(
        newest_asset_modified: datetime | None,
        watermark: datetime | None,
) -> timedelta | None:
    """
    How far the repo's committed watermark trails the newest ingested asset.

    None when either side is undefined (no assets, or no successful commit
    yet) — "never built" is a status, not a lag.  A watermark ahead of the
    newest asset (the asset feeding it was since deleted) clamps to zero.
    """
    if newest_asset_modified is None or watermark is None:
        return None
    return max(as_utc(newest_asset_modified) - as_utc(watermark), timedelta(0))


def is_lock_expired(
        locked_at: datetime | None,
        now: datetime,
        timeout: timedelta,
) -> bool:
    """
    Whether a build lock taken at ``locked_at`` has expired by ``now``.

    A missing stamp counts as expired: a BUILDING row without ``locked_at``
    was abandoned before the stamp landed, and the sweep's reset query
    (``locked_at__lt``) never matches it either.
    """
    if locked_at is None:
        return True
    return as_utc(locked_at) < as_utc(now) - timeout


# ---------------------------------------------------------------------------
# The structured report
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VariableCoverage:
    """Everything a monitoring surface shows about one variable's repo."""

    variable: "Variable"
    manifest: "VirtualZarrManifest | None"
    status: str            # manifest status value; "" when no manifest row
    stuck: bool            # BUILDING with an expired lock
    catalog_timestamps: tuple[datetime, ...]
    repo_timestamps: tuple[datetime, ...]
    missing: tuple[datetime, ...]
    extra: tuple[datetime, ...]
    items_without_cog: tuple[datetime, ...]
    newest_asset_modified: datetime | None
    watermark: datetime | None   # from the tip commit's metadata, read live
    lag: timedelta | None
    repo_size_bytes: int
    repo_object_count: int
    error: str             # manifest.error (last failure message)
    repo_read_error: str   # non-empty when the live repo read itself failed

    @property
    def display_status(self) -> str:
        """Status code for rendering: a stuck build is its own condition."""
        if self.manifest is None:
            return "no_manifest"
        if self.stuck:
            return "stuck"
        return self.status

    @property
    def catalog_count(self) -> int:
        return len(self.catalog_timestamps)

    @property
    def repo_count(self) -> int:
        return len(self.repo_timestamps)

    @property
    def missing_count(self) -> int:
        return len(self.missing)

    @property
    def extra_count(self) -> int:
        return len(self.extra)

    @property
    def skipped_count(self) -> int:
        return len(self.items_without_cog)

    @property
    def lag_label(self) -> str:
        """Compact human form of the lag, e.g. "3d 2h", "45m"."""
        if self.lag is None:
            return ""
        total = int(self.lag.total_seconds())
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes = rem // 60
        if days:
            return f"{days}d {hours}h"
        if hours:
            return f"{hours}h {minutes:02d}m"
        return f"{minutes}m"


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------

def collection_coverage(collection, *, now=None) -> list[VariableCoverage]:
    """One report per Variable in the collection, ordered by slug."""
    variables = list(collection.variables.order_by("slug"))
    return _coverage_for(collection, variables, now)


def variable_coverage(variable, *, now=None) -> VariableCoverage:
    """The per-variable report (drill-down / future status API)."""
    return _coverage_for(variable.collection, [variable], now)[0]


def _coverage_for(collection, variables, now) -> list[VariableCoverage]:
    from django.utils import timezone

    from .models import VirtualZarrManifest

    now = now or timezone.now()

    manifests = {
        m.variable_id: m
        for m in VirtualZarrManifest.objects.filter(
            variable__in=variables,
        )
    }
    catalog = _catalog_state(collection, variables)

    return [
        _one_variable(
            variable,
            manifests.get(variable.pk),
            catalog[variable.pk],
            now,
        )
        for variable in variables
    ]


@dataclass(frozen=True)
class _CatalogState:
    """The builder's view of one variable: Items joined to COG assets."""

    timestamps: tuple[datetime, ...]
    items_without_cog: tuple[datetime, ...]
    newest_asset_modified: datetime | None


def _catalog_state(collection, variables) -> dict[int, _CatalogState]:
    """Per-variable catalog state in two queries, however many variables."""
    from georiva.core.models import Asset, Item

    item_times = dict(
        Item.objects.filter(collection=collection).values_list("id", "time")
    )

    covered: dict[int, set[int]] = {v.pk: set() for v in variables}
    newest: dict[int, datetime] = {}
    cog_rows = Asset.objects.filter(
        item__collection=collection,
        variable__in=variables,
        format=Asset.Format.COG,
    ).values_list("variable_id", "item_id", "modified")
    for variable_id, item_id, modified in cog_rows:
        covered[variable_id].add(item_id)
        if variable_id not in newest or modified > newest[variable_id]:
            newest[variable_id] = modified

    states = {}
    for variable in variables:
        item_ids = covered[variable.pk]
        # Deduplicated: forecast Items may share a valid time across reference
        # times, but the repo's time axis holds each timestamp once — counts
        # must agree with the set arithmetic in diff_timestamps.
        states[variable.pk] = _CatalogState(
            timestamps=tuple(sorted(
                {as_utc(item_times[i]) for i in item_ids}
            )),
            items_without_cog=tuple(sorted(
                {as_utc(t) for i, t in item_times.items() if i not in item_ids}
            )),
            newest_asset_modified=newest.get(variable.pk),
        )
    return states


def _one_variable(variable, manifest, catalog: _CatalogState, now) -> VariableCoverage:
    from .models import VirtualZarrManifest

    repo_timestamps, watermark, repo_read_error = (
        _read_repo_state(manifest) if manifest is not None else ((), None, "")
    )
    diff = diff_timestamps(catalog.timestamps, repo_timestamps)

    stuck = bool(
        manifest is not None
        and manifest.status == VirtualZarrManifest.Status.BUILDING
        and is_lock_expired(
            manifest.locked_at, now, VirtualZarrManifest.LOCK_TIMEOUT
        )
    )

    return VariableCoverage(
        variable=variable,
        manifest=manifest,
        status=manifest.status if manifest is not None else "",
        stuck=stuck,
        catalog_timestamps=catalog.timestamps,
        repo_timestamps=repo_timestamps,
        missing=diff.missing,
        extra=diff.extra,
        items_without_cog=catalog.items_without_cog,
        newest_asset_modified=catalog.newest_asset_modified,
        watermark=watermark,
        lag=freshness_lag(catalog.newest_asset_modified, watermark),
        repo_size_bytes=manifest.repo_size_bytes if manifest is not None else 0,
        repo_object_count=(
            manifest.repo_object_count if manifest is not None else 0
        ),
        error=manifest.error if manifest is not None else "",
        repo_read_error=repo_read_error,
    )


def _read_repo_state(manifest):
    """
    The live truth at the Icechunk tip: (time axis, committed watermark,
    read error).

    ``repo_path`` is only ever set by a successful build (``mark_ready``), so
    a blank one means no repo can exist yet — skip the storage round-trip.
    A read failure is reported in the third slot rather than raised: one
    unreachable repo must not take down a whole collection's tab.
    """
    from .repo import latest_committed_state, open_repo, repo_exists

    if not manifest.repo_path:
        return (), None, ""

    try:
        import pandas as pd
        import xarray as xr

        repo_path = manifest.get_repo_path()
        if not repo_exists(repo_path):
            return (), None, ""
        repo = open_repo(repo_path)
        committed = latest_committed_state(repo)
        watermark = committed.watermark if committed else None

        session = repo.readonly_session(branch="main")
        ds = xr.open_zarr(session.store, consolidated=False, chunks=None)
        if "time" not in ds.coords:
            return (), watermark, ""
        timestamps = tuple(
            as_utc(pd.Timestamp(value).to_pydatetime())
            for value in ds["time"].values
        )
        return timestamps, watermark, ""
    except Exception as exc:
        logger.warning(
            "virtual_zarr coverage: repo read failed for manifest %s: %s",
            manifest.pk, exc,
        )
        return (), None, str(exc)
