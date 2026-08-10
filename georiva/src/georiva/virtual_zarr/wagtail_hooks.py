import logging

from wagtail import hooks
from wagtail.admin.panels import FieldPanel
from wagtail.snippets.models import register_snippet
from wagtail.snippets.views.snippets import SnippetViewSet

from georiva.core.models import Asset
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.organisations.scoping import OrgScopedViewSetMixin

logger = logging.getLogger(__name__)
from georiva.virtual_zarr.models import VirtualZarrManifest


class VirtualZarrManifestViewSet(OrgScopedViewSetMixin, SnippetViewSet):
    """The manifest admin: a low-level escape hatch, org-scoped like Items/Assets.

    The state machine is driven by signals and the sweep; the one thing an
    operator legitimately writes here is ``status`` (e.g. forcing STALE to
    trigger a rebuild). Everything else — coverage caches, lock bookkeeping,
    the derived repo path — is output, so the form does not offer it.
    """

    model = VirtualZarrManifest
    icon = "table"
    list_display = ["variable", "status", "built_at", "item_count"]
    list_filter = ["status"]
    panels = [
        FieldPanel("variable"),
        FieldPanel("status"),
        FieldPanel("error", read_only=True),
    ]


register_snippet(VirtualZarrManifestViewSet)


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
                status__in=[
                    VirtualZarrManifest.Status.READY,
                    VirtualZarrManifest.Status.NO_DATA,
                ],
            ).update(status=VirtualZarrManifest.Status.STALE)
        except Exception as exc:
            logger.warning(
                "Virtual Zarr stale mark failed for %s: %s", variable.slug, exc
            )
