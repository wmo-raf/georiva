import logging
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd
from django.db.models import Prefetch
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Asset, Item
from georiva.virtual_zarr.classify import BuildMode, SourceRow, classify
from georiva.virtual_zarr.compat import (
    assert_compatible,
    spec_from_virtual_variable,
    spec_from_zarr_array,
)
from georiva.virtual_zarr.repo import (
    commit_metadata,
    latest_committed_state,
    latest_snapshot_id,
    open_repo,
    repo_exists,
)
from georiva.virtual_zarr.virtual_zarr import (
    MinioStoreConfig,
    VirtualZarrBuilder,
    last_committed_time,
    write_append,
    write_rebuild,
)
from .models import VirtualZarrBuildLog, VirtualZarrManifest

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
    bind=True,
    max_retries=0,  # failures go to FAILED status; sweep retries
    acks_late=True,
    queue="georiva-ingestion",
)
def build_virtual_zarr_manifest(self, manifest_id: int) -> None:
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
        with {watermark, item_count, time_start, time_end} metadata.
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

    worker_id = f"celery-{self.request.id or 'unknown'}"
    manifest.mark_building(worker_id)

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
        logger.exception(
            "build_virtual_zarr_manifest: failed for manifest %d", manifest_id
        )
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
                BuildMode.UP_TO_DATE.value, BuildMode.NO_DATA.value,
        ):
            manifest.refresh_repo_stats()


def _collect_rows(
        manifest: VirtualZarrManifest, config: MinioStoreConfig,
) -> tuple[list[SourceRow], int]:
    """
    COG asset rows for this variable, one per Item, ordered by item time,
    plus the count of Items skipped for having no COG asset.
    """
    variable = manifest.variable
    collection = variable.collection

    cog_prefetch = Prefetch(
        "assets",
        queryset=Asset.objects.filter(
            variable=variable,
            format=Asset.Format.COG,
        ).only("href", "item_id", "modified"),
        to_attr="cog_assets",
    )

    items_qs = (
        Item.objects
        .filter(collection=collection)
        .prefetch_related(cog_prefetch)
        .only("time")
        .order_by("time")
    )

    rows = []
    skipped = 0
    for item in items_qs:
        if not item.cog_assets:
            # COG missing for this timestep (ingestion gap or partial failure)
            skipped += 1
            continue
        asset = item.cog_assets[0]
        rows.append(SourceRow(
            time=item.time,
            url=config.url_for(asset.href),
            modified=asset.modified,
        ))

    if skipped:
        logger.warning(
            "build_virtual_zarr_manifest: %d item(s) skipped (no COG asset) "
            "for %s/%s",
            skipped, collection.slug, variable.slug,
        )

    return rows, skipped


def _to_url_df(rows) -> pd.DataFrame:
    return pd.DataFrame(
        [{"date": pd.Timestamp(r.time), "url": r.url} for r in rows]
    )


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
            "build_virtual_zarr_manifest: no COG assets for %s/%s — "
            "marking NO_DATA",
            collection.slug, variable.slug,
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
            collection.slug, variable.slug,
        )
        manifest.mark_ready(
            repo_path=repo_path,
            item_count=item_count,
            time_start=time_start,
            time_end=time_end,
            snapshot_id=latest_snapshot_id(repo) or "",
            watermark=committed.watermark,
        )
        report.snapshot_id = latest_snapshot_id(repo) or ""
        return report

    builder = VirtualZarrBuilder(config)
    metadata = commit_metadata(
        watermark=plan.watermark,
        item_count=item_count,
        time_start=time_start,
        time_end=time_end,
    )

    if plan.mode == BuildMode.APPEND:
        snapshot_id = _run_append(
            repo, builder, plan, variable.slug, build_start, metadata
        )
    else:
        vds = builder.build(_to_url_df(plan.rows), variable_name=variable.slug)
        snapshot_id = write_rebuild(
            repo, vds,
            last_updated_at=build_start,
            metadata=metadata,
            message=f"rebuild: {item_count} item(s)",
        )

    manifest.mark_ready(
        repo_path=repo_path,
        item_count=item_count,
        time_start=time_start,
        time_end=time_end,
        snapshot_id=snapshot_id,
        watermark=plan.watermark,
    )

    logger.info(
        "build_virtual_zarr_manifest: READY (%s) — %d items, %s → %s, snapshot %s",
        plan.mode.value, item_count, time_start.date(), time_end.date(), snapshot_id,
    )

    report.items_written = len(plan.rows)
    report.snapshot_id = snapshot_id
    return report


def _run_append(repo, builder, plan, variable_name, build_start, metadata) -> str:
    """
    Append path: heterogeneity guard per COG, belt-and-braces time check,
    then a single append commit.
    """
    import zarr

    # Committed array signature for the hard heterogeneity guard (decision 7)
    ro = repo.readonly_session(branch="main")
    group = zarr.open_group(ro.store, mode="r")
    existing_spec = spec_from_zarr_array(group[variable_name])

    def guard(vds, url):
        raw_var = list(vds.data_vars)[0]
        assert_compatible(
            existing_spec, spec_from_virtual_variable(vds, raw_var), source=url
        )

    vds = builder.build(
        _to_url_df(plan.rows),
        variable_name=variable_name,
        per_cog_check=guard,
    )

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

    return write_append(
        repo, vds,
        last_updated_at=build_start,
        metadata=metadata,
        message=f"append: {len(plan.rows)} item(s)",
    )


# =============================================================================
# Sweep task
# =============================================================================

@app.task(
    name="georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
    queue="georiva-default",
)
def sweep_virtual_zarr_pending() -> None:
    """
    Periodic safety-net for virtual Zarr repo builds.

    Runs every 5 minutes:
      1. Reset stale BUILDING locks (crash recovery) → PENDING
      2. Dispatch build_virtual_zarr_manifest for every buildable manifest
    """
    reset_count = VirtualZarrManifest.reset_stale_locks()
    if reset_count:
        logger.info("sweep_virtual_zarr_pending: reset %d stale lock(s)", reset_count)

    buildable = list(VirtualZarrManifest.get_buildable().values_list("pk", flat=True))

    for manifest_id in buildable:
        build_virtual_zarr_manifest.apply_async(
            args=[manifest_id],
            queue="georiva-ingestion",
        )

    if buildable:
        logger.info(
            "sweep_virtual_zarr_pending: dispatched %d build task(s)", len(buildable)
        )


# =============================================================================
# Retention task (design decision 4)
# =============================================================================

@app.task(
    name="georiva.virtual_zarr.tasks.gc_virtual_zarr_repos",
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
    cutoff = timezone.now() - SNAPSHOT_RETENTION

    manifests = VirtualZarrManifest.objects.exclude(repo_path="").select_related(
        "variable", "variable__collection", "variable__collection__catalog",
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
                repo_path, len(expired), summary,
            )
        except Exception as exc:
            logger.warning(
                "gc_virtual_zarr_repos: retention failed for %s: %s",
                repo_path, exc,
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
        logger.info(
            "gc_virtual_zarr_repos: pruned %d expired build-log row(s)", pruned
        )


@app.on_after_finalize.connect
def setup_virtual_zarr_periodic_tasks(sender, **kwargs) -> None:
    """Register the 5-minute sweep and the daily retention pass."""
    try:
        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.virtual_zarr.sweep_virtual_zarr_pending",
            defaults={
                "task": "georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
                "interval": schedule_5min,
                "enabled": True,
            },
        )
        schedule_daily, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.DAYS
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.virtual_zarr.gc_virtual_zarr_repos",
            defaults={
                "task": "georiva.virtual_zarr.tasks.gc_virtual_zarr_repos",
                "interval": schedule_daily,
                "enabled": True,
            },
        )
    except Exception as exc:
        logger.warning(
            "Could not register virtual Zarr periodic tasks: %s", exc
        )
