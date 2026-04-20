import logging

from georiva.config.celery import app

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.analysis.zonal_stats.tasks.compute_boundary_zonal_stats",
    bind=True,
    max_retries=3,
    acks_late=True,
    queue="georiva-ingestion",
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
    from georiva.core.models import Asset
    from georiva.core.storage import storage
    from .service import (
        compute_stats_from_cog_bytes,
        get_boundaries_for_collection,
        persist_stats,
    )
    
    try:
        asset = (
            Asset.objects
            .select_related(
                "item",
                "item__collection",
                "item__collection__catalog",
                "variable",
            )
            .get(pk=asset_id, format=Asset.Format.COG)
        )
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
            "compute_boundary_zonal_stats: no boundary level configured "
            "for collection %s — skipping",
            collection.slug,
        )
        return
    
    try:
        cog_bytes = storage.assets.read_bytes(asset.href)
    except Exception as exc:
        logger.error(
            "compute_boundary_zonal_stats: failed to read COG %s: %s",
            asset.href, exc,
        )
        raise self.retry(exc=exc)
    
    for level, boundaries in boundaries_by_level.items():
        stats_rows = compute_stats_from_cog_bytes(cog_bytes, boundaries)
        
        count = persist_stats(
            item=asset.item,
            variable=asset.variable,
            stats_rows=stats_rows,
        )
        
        logger.info(
            "compute_boundary_zonal_stats: %d row(s) at level %d for asset %d",
            count, level, asset_id,
        )


@app.task(
    name="georiva.analysis.zonal_stats.tasks.sweep_stale_boundary_stats",
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
    
    now = timezone.now()
    pruned = 0
    
    for collection in Collection.objects.filter(is_forecast=True, is_active=True):
        
        # Past forecast pruning
        if not collection.retain_past_forecasts:
            deleted, _ = (
                BoundaryZonalStats.objects
                .filter(
                    item__collection=collection,
                    time__lt=now,
                )
                .delete()
            )
            pruned += deleted
        
        # Latest-run-only pruning
        if collection.retain_latest_run_only:
            latest_ref = (
                BoundaryZonalStats.objects
                .filter(item__collection=collection)
                .order_by("-item__reference_time")
                .values_list("item__reference_time", flat=True)
                .first()
            )
            if latest_ref:
                deleted, _ = (
                    BoundaryZonalStats.objects
                    .filter(item__collection=collection)
                    .exclude(item__reference_time=latest_ref)
                    .delete()
                )
                pruned += deleted
    
    if pruned:
        logger.info(
            "sweep_stale_boundary_stats: pruned %d stale row(s)", pruned
        )


@app.on_after_finalize.connect
def setup_zonal_stats_periodic_tasks(sender, **kwargs) -> None:
    """Register sweep_stale_boundary_stats as a periodic task (every 5 min)."""
    try:
        from django_celery_beat.models import IntervalSchedule, PeriodicTask
        
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.analysis.zonal_stats.sweep_stale_boundary_stats",
            defaults={
                "task": "georiva.analysis.zonal_stats.tasks.sweep_stale_boundary_stats",
                "interval": schedule,
                "enabled": True,
            },
        )
    except Exception as exc:
        logger.warning(
            "Could not register zonal stats periodic task: %s", exc
        )
