import logging

from django.db.models.signals import post_delete, post_save
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
        manifest, created = VirtualZarrManifest.objects.get_or_create(
            variable=instance.variable,
            defaults={
                "repo_path": VirtualZarrManifest.make_repo_path(instance.variable),
            },
        )
        col = instance.variable.collection
        if created:
            logger.debug(
                "Created manifest record (PENDING): %s/%s/%s",
                col.catalog.slug,
                col.slug,
                instance.variable.slug,
            )
        else:
            manifest.mark_stale()
            logger.debug(
                "Marked manifest stale: %s/%s/%s",
                col.catalog.slug,
                col.slug,
                instance.variable.slug,
            )
    except Exception as exc:
        logger.warning(
            "Failed to update manifest for asset %s: %s",
            instance.pk,
            exc,
        )


@receiver(post_delete, sender=Asset)
def mark_manifest_stale_on_cog_delete(sender, instance: Asset, **kwargs):
    """
    When a COG Asset is deleted, mark the manifest STALE so the next sweep
    reconciles the repo: classify() detects the shrunken axis and rebuilds,
    or — if this was the variable's last COG — the build parks the manifest
    as NO_DATA instead of failing forever.

    Filtered by variable_id (never the related object): during a cascade
    delete of the Variable the related rows may already be gone, and the
    manifest itself cascades away with them anyway.
    """
    if instance.format != Asset.Format.COG:
        return

    try:
        VirtualZarrManifest.objects.filter(
            variable_id=instance.variable_id,
            status__in=[
                VirtualZarrManifest.Status.READY,
                VirtualZarrManifest.Status.NO_DATA,
            ],
        ).update(status=VirtualZarrManifest.Status.STALE)
    except Exception as exc:
        logger.warning(
            "Failed to mark manifest stale on delete of asset %s: %s",
            instance.pk,
            exc,
        )
