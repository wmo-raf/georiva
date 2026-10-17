import logging
from datetime import timedelta

from georiva.config.celery import app
from georiva.events.tasks import EntryTask, ReadSideTask
from georiva.utils.periodic import PeriodicSpec, register_periodic_tasks

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.analysis.zonal_stats.tasks.compute_boundary_zonal_stats",
    base=ReadSideTask,
    bind=True,
    max_retries=3,
    acks_late=True,
    # Deferrable bookkeeping: an ingested COG is already servable without its
    # zonal statistics — tiles, STAC and EDR all answer without them — and one
    # large fetch fans this out per asset.  Keeping it off georiva-ingestion
    # stops that fan-out delaying the next file to be extracted (#398).
    # Idempotent — persist_stats upserts on its unique constraint — so it is
    # safe on a multi-slot worker.  A dropped run is re-issued by
    # sweep_missing_boundary_stats (#402).
    queue="georiva-processing",
)
def compute_boundary_zonal_stats(self, asset_id: int) -> None:
    """
    Compute and persist zonal statistics for one COG Asset.

    Steps
    -----
    1. Load the Asset, Item, Variable, and Collection.
    2. Check collection.boundary_stats_level — exit early if not set.
    3. Resolve AdminBoundary queryset for the configured level.
    4. Download COG bytes from MinIO.
    5. Compute stats via compute_stats_from_cog_bytes().
    6. Persist via persist_stats().

    Retries up to 3 times on transient failures (network, MinIO timeout).
    """
    from django.db import InterfaceError, OperationalError
    from rasterio.errors import RasterioError

    from georiva.core.models import Asset
    from georiva.core.storage import storage

    from .service import (
        compute_stats_from_cog_bytes,
        get_boundaries_for_collection,
        persist_stats,
    )

    try:
        asset = Asset.objects.select_related(
            "item",
            "item__collection",
            "item__collection__catalog",
            "variable",
        ).get(pk=asset_id, format=Asset.Format.COG)
    except Asset.DoesNotExist:
        logger.warning(
            "compute_boundary_zonal_stats: asset %d not found or not COG",
            asset_id,
        )
        return

    collection = asset.item.collection
    boundaries_by_level = get_boundaries_for_collection(collection)

    if not boundaries_by_level:
        logger.debug(
            "compute_boundary_zonal_stats: no boundary level configured for collection %s — skipping",
            collection.slug,
        )
        return

    try:
        cog_bytes = storage.assets.read_bytes(asset.href)
    except Exception as exc:
        logger.error(
            "compute_boundary_zonal_stats: failed to read COG %s: %s",
            asset.href,
            exc,
        )
        raise self.retry(exc=exc) from exc

    try:
        for level, boundaries in boundaries_by_level.items():
            stats_rows = compute_stats_from_cog_bytes(cog_bytes, boundaries)

            count = persist_stats(
                item=asset.item,
                variable=asset.variable,
                stats_rows=stats_rows,
            )

            logger.info(
                "compute_boundary_zonal_stats: %d row(s) at level %d for asset %d",
                count,
                level,
                asset_id,
            )
    except RasterioError:
        # An unreadable COG fails the same way every time — retrying only burns
        # the budget.  The sweep re-issues it, and it surfaces in the log.
        logger.exception("compute_boundary_zonal_stats: asset %d is not computable", asset_id)
        raise
    except (OSError, OperationalError, InterfaceError) as exc:
        # Transient: a DB blip or I/O hiccup.  Once retries are exhausted
        # sweep_missing_boundary_stats picks the asset up.
        logger.warning("compute_boundary_zonal_stats: asset %d failed (will retry): %s", asset_id, exc)
        raise self.retry(exc=exc) from exc


@app.task(
    name="georiva.analysis.zonal_stats.tasks.sweep_stale_boundary_stats",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_stale_boundary_stats() -> None:
    """
    Periodic cleanup for forecast boundary stats.

    Mirrors Collection forecast pruning logic:
      - retain_past_forecasts=False  → delete stats where valid_time < now
      - retain_latest_run_only=True  → delete stats not in the latest run

    Runs every 5 minutes via django-celery-beat.
    """
    from django.utils import timezone

    from georiva.core.models import Collection

    from .models import BoundaryZonalStats
    from .service import retained_items_q

    now = timezone.now()
    pruned = 0

    for collection in Collection.objects.filter(is_forecast=True, is_active=True):
        latest_ref = (
            BoundaryZonalStats.objects.filter(item__collection=collection)
            .order_by("-item__reference_time")
            .values_list("item__reference_time", flat=True)
            .first()
        )
        kept = retained_items_q(collection, now, latest_ref)
        if kept is None:
            continue
        deleted, _ = BoundaryZonalStats.objects.filter(item__collection=collection).exclude(kept).delete()
        pruned += deleted

    if pruned:
        logger.info("sweep_stale_boundary_stats: pruned %d stale row(s)", pruned)


#: How far back the recovery sweep looks.  Bounds the NOT EXISTS scan; older
#: gaps are the ``compute_boundary_stats`` backfill's job.
MISSING_STATS_WINDOW = timedelta(days=7)

#: An asset younger than this still has its own after-save run in flight.
MISSING_STATS_GRACE = timedelta(minutes=15)

#: How long a re-dispatched asset is left alone before the sweep may queue it again.
MISSING_STATS_CLAIM = timedelta(minutes=30)

#: Dispatch ceiling per pass, so a large outage drains over several passes
#: instead of flooding georiva-processing at once.
MISSING_STATS_BATCH = 500


@app.task(
    name="georiva.analysis.zonal_stats.tasks.sweep_missing_boundary_stats",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_missing_boundary_stats() -> None:
    """
    Recovery for a dropped ``compute_boundary_zonal_stats`` run (#402).

    The absence of ``BoundaryZonalStats`` rows for a COG asset is the signal:
    any asset in the recency window, past the grace period, with no row at one
    of its collection's configured levels is re-dispatched.  Every boundary gets
    a row (all-None when nothing intersects), so a finished run never looks
    missing.  ``persist_stats`` upserts, so a redundant run is harmless; a
    per-asset cache claim still keeps a backed-up queue from receiving the same
    asset on every pass.

    Retention comes from ``retained_items_q``, the rule the pruner deletes by.

    Runs every 5 minutes via django-celery-beat.
    """
    from adminboundarymanager.models import AdminBoundary
    from django.db.models import Exists, OuterRef, Q
    from django.utils import timezone

    from georiva.core.models import Asset, Collection

    from .models import BoundaryZonalStats
    from .service import retained_items_q

    now = timezone.now()
    since = now - MISSING_STATS_WINDOW
    settled = now - MISSING_STATS_GRACE
    dispatched = 0

    collections = Collection.objects.filter(is_active=True, boundary_stats_levels__isnull=False).exclude(
        boundary_stats_levels=[]
    )

    for collection in collections:
        if dispatched >= MISSING_STATS_BATCH:
            break

        # Levels with no boundaries loaded can never produce a row; without
        # this every pass would re-dispatch the same futile runs.
        levels = sorted(
            set(
                AdminBoundary.objects.filter(level__in=collection.boundary_stats_levels).values_list("level", flat=True)
            )
        )
        if not levels:
            continue

        assets = Asset.objects.filter(
            item__collection=collection,
            format=Asset.Format.COG,
            item__time__gte=since,
            created__lte=settled,
        )

        # Missing if ANY configured level has no row — a run that died between
        # levels leaves the earlier ones written.
        missing = Q()
        for level in levels:
            has_level = Exists(
                BoundaryZonalStats.objects.filter(
                    item=OuterRef("item"),
                    variable=OuterRef("variable"),
                    time__gte=since,
                    boundary__level=level,
                )
            )
            missing |= Q(pk__in=assets.exclude(has_level).values("pk"))
        assets = assets.filter(missing)

        latest_ref = (
            collection.items.order_by("-reference_time").values_list("reference_time", flat=True).first()
            if collection.is_forecast
            else None
        )
        kept = retained_items_q(collection, now, latest_ref)
        if kept is not None:
            assets = assets.filter(kept)

        room = MISSING_STATS_BATCH - dispatched
        for asset_id in assets.order_by("-item__time", "pk").values_list("pk", flat=True)[:room]:
            if not _claim_redispatch(asset_id):
                continue
            try:
                compute_boundary_zonal_stats.delay(asset_id)
                dispatched += 1
            except Exception as exc:
                logger.warning("sweep_missing_boundary_stats: enqueue failed for asset %d: %s", asset_id, exc)

    if dispatched:
        logger.info("sweep_missing_boundary_stats: re-dispatched %d run(s)", dispatched)


def _claim_redispatch(asset_id: int) -> bool:
    """
    True if no sweep has re-dispatched this asset within ``MISSING_STATS_CLAIM``.

    Without it, a deep ``georiva-processing`` backlog would receive the same
    asset every 5 minutes.  A cache outage fails open: a duplicate run is
    harmless, a skipped recovery is not.
    """
    from django.core.cache import cache

    try:
        return cache.add(f"zonal-stats-redispatch:{asset_id}", 1, timeout=int(MISSING_STATS_CLAIM.total_seconds()))
    except Exception:
        return True


@app.on_after_finalize.connect
def setup_zonal_stats_periodic_tasks(sender, **kwargs) -> None:
    """Register the two zonal-stats sweeps as periodic tasks (every 5 min)."""
    from django_celery_beat.models import IntervalSchedule

    register_periodic_tasks(
        [
            PeriodicSpec(
                f"georiva.analysis.zonal_stats.{name}",
                f"georiva.analysis.zonal_stats.tasks.{name}",
                5,
                IntervalSchedule.MINUTES,
            )
            for name in ("sweep_stale_boundary_stats", "sweep_missing_boundary_stats")
        ]
    )
