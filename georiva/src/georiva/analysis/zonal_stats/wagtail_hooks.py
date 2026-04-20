import logging

from wagtail import hooks

from georiva.ingestion.service import GEORIVA_AFTER_SAVE_ASSET

logger = logging.getLogger(__name__)


@hooks.register(GEORIVA_AFTER_SAVE_ASSET)
def enqueue_zonal_stats_on_cog_save(asset) -> None:
    """
    Enqueue compute_boundary_zonal_stats after a COG asset is saved.

    Guards:
    - Only fires for COG assets.
    - Only fires if collection.boundary_stats_levels is set.
    - Enqueue failure never propagates — the ingestion pipeline must not
      fail because of a downstream analytics hook.
    """
    from georiva.core.models import Asset
    
    if asset.format != Asset.Format.COG:
        return
    
    collection = asset.item.collection
    level = getattr(collection, "boundary_stats_levels", None)
    if level is None:
        return
    
    try:
        from .tasks import compute_boundary_zonal_stats
        
        compute_boundary_zonal_stats.apply_async(
            args=[asset.pk],
            queue="georiva-ingestion",
        )
        logger.debug(
            "Enqueued zonal stats for asset %d (%s @ %s)",
            asset.pk, asset.variable.slug, asset.item.time,
        )
    except Exception as exc:
        logger.warning(
            "Failed to enqueue zonal stats for asset %d: %s",
            asset.pk, exc,
        )
