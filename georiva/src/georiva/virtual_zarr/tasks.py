from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pandas as pd
from django.db.models import Prefetch
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule

from georiva.config.celery import app
from georiva.core.models import Asset, Item
from georiva.core.storage.asset_location import local_q
from georiva.events.tasks import EntryTask, ReadSideTask
from georiva.utils.periodic import PeriodicSpec, register_periodic_tasks
from georiva.virtual_zarr.classify import BuildMode, SourceRow, classify
from georiva.virtual_zarr.compat import (
    LeftOut,
    appends_cleanly,
    describe_left_out,
    left_out_count,
    select,
    spec_from_virtual_variable,
    spec_from_zarr_array,
)

from .models import VirtualZarrBuildLog, VirtualZarrManifest

if TYPE_CHECKING:
    from georiva.virtual_zarr.virtual_zarr import MinioStoreConfig

# ``georiva.virtual_zarr.repo`` (icechunk) and ``.virtual_zarr`` (obstore /
# async-tiff / virtualizarr) are imported inside the task bodies, never at
# module level: this module loads in the Celery worker *parent* at task
# discovery, and those libraries start a Rust/tokio runtime whose threads do
# not survive the prefork fork — the child then deadlocks on its first
# icechunk call.  Importing late keeps the parent runtime-free so each pool
# child initialises its own.

logger = logging.getLogger(__name__)


@dataclass
class BuildReport:
    """
    What one build cycle did, for the build log.

    Mutable and filled in as _run_build progresses, so a failing build still
    reports whatever it learned before dying (skip count, classified mode).
    """

    mode: str = ""
    items_written: int = 0
    items_skipped: int = 0
    snapshot_id: str = ""


# Retention for design decision 4: expire snapshots older than this (the
# latest is always kept) and garbage-collect unreachable icechunk objects.
SNAPSHOT_RETENTION = timedelta(days=7)


@app.task(
    name="georiva.virtual_zarr.tasks.build_virtual_zarr_manifest",
    base=ReadSideTask,
    bind=True,
    max_retries=0,  # failures go to FAILED status; sweep retries
    acks_late=True,
    # Deferrable bookkeeping: nothing a reader can observe waits on a manifest,
    # and the sweep re-dispatches anything dropped.  Keeping it off
    # georiva-ingestion keeps one large fetch's fan-out out of the way of the
    # next file to be extracted (#398).  Always dispatch via ``dispatch_build``.
    queue="georiva-bookkeeping",
)
def build_virtual_zarr_manifest(self, manifest_id: int, claim: str = "") -> None:
    """
    Build or update the Icechunk repo for one Variable.

    Steps
    -----
    1.  Load the VirtualZarrManifest record and lock it (BUILDING).
    2.  Query COG Asset hrefs (+ Asset.modified) for this variable, ordered
        by item time.
    3.  Read the committed watermark from the repo's latest commit metadata
        (source of truth — never the Django row) and classify the cycle:
        append (new tail only), rebuild (new commit in place), or up-to-date.
    4.  Write virtual refs via VirtualZarrBuilder + to_icechunk and commit
        with {watermark, item_count, time_start, time_end, left_out}
        metadata.  The repo holds the newest single-band asset's array
        signature; an asset of another, or of several bands, is left out
        and named in ``left_out`` (#732).
    5.  Mark READY and refresh the derived cache fields.

    On any exception — including icechunk.ConflictError, which the sweep's
    single-builder locking should make impossible — the manifest is marked
    FAILED with the error message; the sweep re-dispatches later.  No
    auto-rebase on conflicts.
    """
    try:
        manifest = VirtualZarrManifest.objects.select_related(
            "variable",
            "variable__collection",
            "variable__collection__catalog",
        ).get(pk=manifest_id)
    except VirtualZarrManifest.DoesNotExist:
        logger.error("build_virtual_zarr_manifest: manifest %d not found", manifest_id)
        return

    # Stand down if this copy's claim was recycled while it waited: a queue
    # wait longer than LOCK_TIMEOUT lets reset_stale_locks free the row and a
    # later sweep dispatch a replacement, and two writers on one Icechunk repo
    # conflict.  Blank claim means a task queued before claims existed.
    if claim and manifest.locked_by != claim:
        logger.info(
            "build_virtual_zarr_manifest: manifest %d reclaimed (%s now holds it, not %s) — standing down",
            manifest_id,
            manifest.locked_by or "nobody",
            claim,
        )
        return

    worker_id = f"celery-{self.request.id or 'unknown'}"
    manifest.refresh_build_lock(worker_id)

    col = manifest.variable.collection
    logger.info(
        "build_virtual_zarr_manifest: starting %s/%s/%s",
        col.catalog.slug,
        col.slug,
        manifest.variable.slug,
    )

    started_at = timezone.now()
    report = BuildReport()
    try:
        _run_build(manifest, report)
    except Exception as exc:
        logger.exception("build_virtual_zarr_manifest: failed for manifest %d", manifest_id)
        manifest.mark_failed(str(exc))
        VirtualZarrBuildLog.record(
            manifest,
            VirtualZarrBuildLog.Kind.BUILD,
            VirtualZarrBuildLog.Outcome.FAILURE,
            started_at,
            mode=report.mode,
            items_skipped=report.items_skipped,
            error=str(exc),
        )
    else:
        VirtualZarrBuildLog.record(
            manifest,
            VirtualZarrBuildLog.Kind.BUILD,
            VirtualZarrBuildLog.Outcome.SUCCESS,
            started_at,
            mode=report.mode,
            items_written=report.items_written,
            items_skipped=report.items_skipped,
            snapshot_id=report.snapshot_id,
        )
        # Only a real commit changes the repo prefix; skip the listing on
        # up-to-date and no-data cycles.
        if report.mode not in (
            BuildMode.UP_TO_DATE.value,
            BuildMode.NO_DATA.value,
        ):
            manifest.refresh_repo_stats()


def _collect_rows(
    manifest: VirtualZarrManifest,
    config: MinioStoreConfig,
) -> tuple[list[SourceRow], int]:
    """
    COG asset rows for this variable, one per Item, ordered by item time
    (newest run only for a forecast collection), plus the count of Items
    skipped for having no COG asset.
    """
    variable = manifest.variable
    collection = variable.collection

    cog_prefetch = Prefetch(
        "assets",
        # Remote assets are left out (ADR 0042 §11): a Mixed collection's
        # virtual store covers only its local window.
        queryset=Asset.objects.filter(
            local_q(),
            variable=variable,
            format=Asset.Format.COG,
        ).only("href", "item_id", "modified"),
        to_attr="cog_assets",
    )

    # A forecast item is (reference_time, time) and consecutive runs overlap, so
    # keying on valid time alone gives an axis that is neither append-only nor
    # unique. The virtual Zarr of a forecast collection is the newest run only:
    # a new run changes the in-range rows, which classify() answers with REBUILD.
    items_qs = Item.objects.filter(collection=collection)
    # Keyed on the data, not on Collection.is_forecast: that flag is set by hand
    # and an unflagged forecast collection hits the same append-only failure.
    # Newest run that has a COG for *this* variable, so a run still
    # ingesting elsewhere cannot park the variable as NO_DATA.
    latest_ref = (
        items_qs.filter(
            local_q("assets__"),
            reference_time__isnull=False,
            assets__variable=variable,
            assets__format=Asset.Format.COG,
        )
        .order_by("-reference_time")
        .values_list("reference_time", flat=True)
        .first()
    )
    if latest_ref is not None:
        unkeyed = items_qs.filter(reference_time__isnull=True).count()
        if unkeyed:
            logger.warning(
                "build_virtual_zarr_manifest: %d item(s) without a reference_time left out of %s/%s",
                unkeyed,
                collection.slug,
                variable.slug,
            )
        items_qs = items_qs.filter(reference_time=latest_ref)
    items_qs = items_qs.prefetch_related(cog_prefetch).only("time").order_by("time")

    rows = []
    skipped = 0
    for item in items_qs:
        if not item.cog_assets:
            # COG missing for this timestep (ingestion gap or partial failure)
            skipped += 1
            continue
        asset = item.cog_assets[0]
        rows.append(
            SourceRow(
                time=item.time,
                url=config.url_for(asset.location.key),
                modified=asset.modified,
            )
        )

    if skipped:
        logger.warning(
            "build_virtual_zarr_manifest: %d item(s) skipped (no COG asset) for %s/%s",
            skipped,
            collection.slug,
            variable.slug,
        )

    return rows, skipped


def _to_url_df(rows) -> pd.DataFrame:
    return pd.DataFrame([{"date": pd.Timestamp(r.time), "url": r.url} for r in rows])


def _run_build(
    manifest: VirtualZarrManifest,
    report: BuildReport | None = None,
) -> BuildReport:
    """
    Core build logic: classify against the repo's committed state, then
    append or rebuild as a new commit in place.

    ``report`` is filled in as the build progresses so the caller can log
    partial facts even when the build raises.
    """
    from georiva.virtual_zarr.repo import (
        commit_metadata,
        latest_committed_state,
        latest_snapshot_id,
        open_repo,
        repo_exists,
    )
    from georiva.virtual_zarr.virtual_zarr import MinioStoreConfig, VirtualZarrBuilder

    if report is None:
        report = BuildReport()
    variable = manifest.variable
    collection = variable.collection

    config = MinioStoreConfig.from_django_settings()
    repo_path = manifest.get_repo_path()

    rows, skipped = _collect_rows(manifest, config)
    report.items_skipped = skipped
    # Captured after row collection: to_icechunk's last_updated_at checksum
    # must be ≥ every included COG's Last-Modified, or reads fail instantly.
    build_start = timezone.now()
    if not rows:
        # Nothing to build and retrying cannot help — park the manifest as
        # NO_DATA so the sweep stops re-dispatching it.  The COG save signal
        # flips it back to STALE when data arrives.
        logger.info(
            "build_virtual_zarr_manifest: no COG assets for %s/%s — marking NO_DATA",
            collection.slug,
            variable.slug,
        )
        manifest.mark_no_data()
        report.mode = BuildMode.NO_DATA.value
        return report

    # ------------------------------------------------------------------
    # Classify against the repo's committed state (decision 5: the
    # watermark lives in commit metadata, never the Django row)
    # ------------------------------------------------------------------
    committed = None
    if repo_exists(repo_path):
        repo = open_repo(repo_path)
        committed = latest_committed_state(repo)
    else:
        repo = open_repo(repo_path, create=True)

    plan = classify(committed, rows)
    report.mode = plan.mode.value

    time_start = min(r.time for r in rows)
    time_end = max(r.time for r in rows)
    item_count = len(rows)

    if plan.mode == BuildMode.UP_TO_DATE:
        logger.info(
            "build_virtual_zarr_manifest: %s/%s already up to date",
            collection.slug,
            variable.slug,
        )
        manifest.mark_ready(
            repo_path=repo_path,
            item_count=item_count - left_out_count(committed.left_out),
            time_start=time_start,
            time_end=time_end,
            snapshot_id=latest_snapshot_id(repo) or "",
            watermark=committed.watermark,
            left_out=committed.left_out,
        )
        report.snapshot_id = latest_snapshot_id(repo) or ""
        return report

    builder = VirtualZarrBuilder(config)

    def metadata(left_out):
        return commit_metadata(
            watermark=plan.watermark,
            item_count=item_count,
            time_start=time_start,
            time_end=time_end,
            left_out=left_out,
        )

    written = None
    if plan.mode == BuildMode.APPEND:
        written = _run_append(repo, builder, plan, committed, variable.slug, build_start, metadata)
        if written is None:
            # A new signature, or a multi-band asset: what the repo holds is
            # chosen again over every asset (#732).
            logger.info(
                "build_virtual_zarr_manifest: %s/%s new assets do not append cleanly — rebuilding",
                collection.slug,
                variable.slug,
            )
            report.mode = BuildMode.REBUILD.value
    if written is None:
        ordered = sorted(rows, key=lambda row: row.time)
        written = _run_rebuild(repo, builder, ordered, variable.slug, build_start, metadata)
        if written.left_out:
            logger.warning(
                "build_virtual_zarr_manifest: %s/%s left %d asset(s) out: %s",
                collection.slug,
                variable.slug,
                left_out_count(written.left_out),
                describe_left_out(written.left_out),
            )
        if written.snapshot_id is None:
            logger.info(
                "build_virtual_zarr_manifest: %s/%s holds no asset — marking NO_DATA",
                collection.slug,
                variable.slug,
            )
            manifest.mark_no_data(left_out=written.left_out)
            report.mode = BuildMode.NO_DATA.value
            return report

    manifest.mark_ready(
        repo_path=repo_path,
        item_count=item_count - left_out_count(written.left_out),
        time_start=time_start,
        time_end=time_end,
        snapshot_id=written.snapshot_id,
        watermark=plan.watermark,
        left_out=written.left_out,
    )

    logger.info(
        "build_virtual_zarr_manifest: READY (%s) — %d items, %s → %s, snapshot %s",
        report.mode,
        item_count,
        time_start.date(),
        time_end.date(),
        written.snapshot_id,
    )

    report.items_written = written.items_written
    report.snapshot_id = written.snapshot_id
    return report


@dataclass(frozen=True)
class _Written:
    """One commit's outcome: its snapshot — None when nothing could be held —
    the timesteps it wrote, and the assets the repo leaves out."""

    snapshot_id: str | None
    items_written: int
    left_out: tuple[LeftOut, ...]


def _run_rebuild(repo, builder, rows, variable_name, build_start, metadata) -> _Written:
    """
    Rebuild path: hold the newest single-band asset's signature and leave
    every other asset out, by reason (#732), as a new commit in place.
    """
    from georiva.virtual_zarr.virtual_zarr import write_rebuild

    opened = builder.open(_to_url_df(rows))
    selection = select([spec_from_virtual_variable(cog.vds) for cog in opened])
    if selection.signature is None:
        return _Written(snapshot_id=None, items_written=0, left_out=selection.left_out)

    kept = [opened[position] for position in selection.kept]
    vds = builder.assemble(kept, variable_name)
    snapshot_id = write_rebuild(
        repo,
        vds,
        last_updated_at=build_start,
        metadata=metadata(selection.left_out),
        message=f"rebuild: {len(kept)} item(s)",
    )
    return _Written(snapshot_id=snapshot_id, items_written=len(kept), left_out=selection.left_out)


def _run_append(repo, builder, plan, committed, variable_name, build_start, metadata) -> _Written | None:
    """
    Append path: the new tail as a single append commit, carrying the
    committed record of assets left out. None when any new asset differs
    from the committed signature — the caller rebuilds, since the newest
    signature may now be another and an append never leaves anything out.
    """
    import zarr

    from georiva.virtual_zarr.virtual_zarr import last_committed_time, write_append

    ro = repo.readonly_session(branch="main")
    group = zarr.open_group(ro.store, mode="r")
    committed_spec = spec_from_zarr_array(group[variable_name])

    opened = builder.open(_to_url_df(plan.rows))
    if not appends_cleanly(committed_spec, [spec_from_virtual_variable(cog.vds) for cog in opened]):
        return None
    vds = builder.assemble(opened, variable_name)

    # Belt-and-braces (decision 5): appended timesteps must be strictly
    # after the last committed time coordinate, read from the snapshot
    # itself — independent of the commit-metadata watermark.
    committed_last = last_committed_time(repo)
    if committed_last is not None:
        first_new = pd.Timestamp(vds["time"].values[0])
        if first_new <= committed_last:
            raise ValueError(
                f"Append safety check failed: first new timestep {first_new} "
                f"is not strictly after the committed axis end {committed_last}."
            )

    snapshot_id = write_append(
        repo,
        vds,
        last_updated_at=build_start,
        metadata=metadata(committed.left_out),
        message=f"append: {len(plan.rows)} item(s)",
    )
    return _Written(snapshot_id=snapshot_id, items_written=len(opened), left_out=committed.left_out)


# =============================================================================
# Sweep task
# =============================================================================


def dispatch_build(manifest_id: int, claimed_by: str = "dispatch", force: bool = False) -> bool:
    """
    Claim a manifest and queue its build; return whether it was queued.

    The only way a build should reach Celery.  Claiming first is what stops a
    manifest waiting in the queue from being dispatched again by the next
    sweep — see ``VirtualZarrManifest.claim_for_build``.  The claim is unique
    per dispatch and travels with the task, so the one case claiming alone does
    not cover — a queue wait long enough for ``reset_stale_locks`` to recycle
    the lock and a later sweep to dispatch a second copy — resolves in favour
    of the newer copy rather than running both.

    A claim we then fail to dispatch leaves the row BUILDING until
    ``reset_stale_locks`` recovers it, which is the same path a worker crash
    takes.  ``force`` is the operator's rebuild of a manifest that does not
    need one.
    """
    claim = f"{claimed_by}-{uuid4().hex[:8]}"
    if not VirtualZarrManifest.claim_for_build(manifest_id, claim, force=force):
        return False

    build_virtual_zarr_manifest.delay(manifest_id, claim)
    return True


@app.task(
    name="georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_virtual_zarr_pending() -> None:
    """
    Periodic safety-net for virtual Zarr repo builds.

    Runs every 5 minutes:
      1. Reset stale BUILDING locks (crash recovery) → PENDING
      2. Claim and dispatch every buildable manifest
    """
    reset_count = VirtualZarrManifest.reset_stale_locks()
    if reset_count:
        logger.info("sweep_virtual_zarr_pending: reset %d stale lock(s)", reset_count)

    buildable = list(VirtualZarrManifest.get_buildable().values_list("pk", flat=True))
    dispatched = sum(dispatch_build(manifest_id, claimed_by="sweep") for manifest_id in buildable)

    if dispatched:
        logger.info("sweep_virtual_zarr_pending: dispatched %d build task(s)", dispatched)


# =============================================================================
# Retention task (design decision 4)
# =============================================================================


@app.task(
    name="georiva.virtual_zarr.tasks.gc_virtual_zarr_repos",
    base=EntryTask,
    bind=True,
    acks_late=True,
    queue="georiva-default",
)
def gc_virtual_zarr_repos(self) -> None:
    """
    Daily retention pass over every variable's Icechunk repo.

    Expires snapshots older than SNAPSHOT_RETENTION (the branch tip is always
    kept), then garbage-collects unreachable icechunk-native objects.  GC can
    never touch COG bytes — all chunks in these repos are virtual.
    """
    from georiva.virtual_zarr.repo import open_repo, repo_exists

    cutoff = timezone.now() - SNAPSHOT_RETENTION

    manifests = VirtualZarrManifest.objects.exclude(repo_path="").select_related(
        "variable",
        "variable__collection",
        "variable__collection__catalog",
    )

    for manifest in manifests:
        repo_path = manifest.get_repo_path()
        started_at = timezone.now()
        try:
            if not repo_exists(repo_path):
                # Nothing ran, nothing to record — the repo has simply not
                # been built yet.
                continue
            repo = open_repo(repo_path)
            expired = repo.expire_snapshots(older_than=cutoff)
            summary = repo.garbage_collect(delete_object_older_than=cutoff)
            logger.info(
                "gc_virtual_zarr_repos: %s — expired %d snapshot(s), gc: %s",
                repo_path,
                len(expired),
                summary,
            )
        except Exception as exc:
            logger.warning(
                "gc_virtual_zarr_repos: retention failed for %s: %s",
                repo_path,
                exc,
            )
            VirtualZarrBuildLog.record(
                manifest,
                VirtualZarrBuildLog.Kind.GC,
                VirtualZarrBuildLog.Outcome.FAILURE,
                started_at,
                error=str(exc),
            )
            # A partially completed GC may still have deleted objects.
            manifest.refresh_repo_stats()
        else:
            VirtualZarrBuildLog.record(
                manifest,
                VirtualZarrBuildLog.Kind.GC,
                VirtualZarrBuildLog.Outcome.SUCCESS,
                started_at,
            )
            manifest.refresh_repo_stats()

    pruned = VirtualZarrBuildLog.prune_expired()
    if pruned:
        logger.info("gc_virtual_zarr_repos: pruned %d expired build-log row(s)", pruned)


@app.on_after_finalize.connect
def setup_virtual_zarr_periodic_tasks(sender, **kwargs) -> None:
    """Register the 5-minute sweep and the daily retention pass."""
    register_periodic_tasks(
        [
            PeriodicSpec(
                "georiva.virtual_zarr.sweep_virtual_zarr_pending",
                "georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
                5,
                IntervalSchedule.MINUTES,
            ),
            PeriodicSpec(
                "georiva.virtual_zarr.gc_virtual_zarr_repos",
                "georiva.virtual_zarr.tasks.gc_virtual_zarr_repos",
                1,
                IntervalSchedule.DAYS,
            ),
        ]
    )
