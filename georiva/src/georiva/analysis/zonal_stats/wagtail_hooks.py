from wagtail import hooks

from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET


@hooks.register(GEORIVA_AFTER_SAVE_ASSET)
def enqueue_zonal_stats_on_cog_save(asset) -> None:
    """
    Enqueue compute_boundary_zonal_stats after a COG asset is saved, once the
    write commits: a derivation saves its assets inside a transaction, and a
    worker that ran first would not find the row.

    Only for a COG, and only where the collection computes a boundary level
    with boundaries loaded (``computes_boundary_stats``). Enqueue failure never
    propagates — the ingestion pipeline must not fail because of a downstream
    analytics hook.
    """
    from georiva.core.models import Asset

    from .tasks import computes_boundary_stats, enqueue_boundary_stats

    if asset.format != Asset.Format.COG:
        return
    if not computes_boundary_stats(asset.item.collection):
        return
    enqueue_boundary_stats(asset.pk)
