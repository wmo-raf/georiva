import logging

from django.db.models.signals import post_save
from django.dispatch import receiver

from georiva.core.models import Asset
from .models import VirtualZarrManifest

logger = logging.getLogger(__name__)


@receiver(post_save, sender=Asset)
def mark_manifest_stale_on_cog_save(sender, instance: Asset, created: bool, **kwargs):
    """
    When a COG Asset is saved (created or updated), mark the corresponding
    VirtualZarrManifest as STALE so the next sweep triggers a rebuild.

    The manifest is keyed on Variable (OneToOneField), so no collection
    lookup is needed — variable carries its collection via ParentalKey.
    """
    if instance.format != Asset.Format.COG:
        return
    
    try:
        manifest = VirtualZarrManifest.objects.get(
            variable=instance.variable,
            status=VirtualZarrManifest.Status.READY,
        )
        manifest.mark_stale()
        col = instance.variable.collection
        logger.debug(
            "Marked manifest stale: %s/%s/%s",
            col.catalog.slug,
            col.slug,
            instance.variable.slug,
        )
    except VirtualZarrManifest.DoesNotExist:
        pass
    except Exception as exc:
        logger.warning(
            "Failed to mark manifest stale for asset %s: %s",
            instance.pk, exc,
        )
