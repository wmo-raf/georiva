import logging

from wagtail import hooks

from georiva.core.models import Asset
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET

logger = logging.getLogger(__name__)
from georiva.virtual_zarr.models import VirtualZarrManifest


@hooks.register(GEORIVA_AFTER_SAVE_ASSET)
def after_save_asset(asset, **kwargs):
    """
    After an Asset is confirmed written:
      1. If COG, Mark the virtual Zarr manifest stale so the next sweep rebuilds it
    """
    
    if asset.format == Asset.Format.COG:
        variable = asset.variable
        
        try:
            VirtualZarrManifest.objects.filter(
                variable=variable,
                status=VirtualZarrManifest.Status.READY,
            ).update(status=VirtualZarrManifest.Status.STALE)
        except Exception as exc:
            logger.warning(
                "Virtual Zarr stale mark failed for %s: %s", variable.slug, exc
            )
