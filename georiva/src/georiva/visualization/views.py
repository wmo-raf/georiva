from django.shortcuts import render
from django.urls import reverse_lazy
from django.utils.translation import gettext as _

from georiva.core.machine_plane import titiler_encoded_preview_url
from georiva.core.models import Item
from georiva.organisations.access import get_org_object_or_404


def item_preview(request, item_id):
    item = get_org_object_or_404(request, Item, pk=item_id)

    breadcrumbs_items = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": "#", "label": _("Preview")},
    ]

    assets = item.assets.all()

    # The map texture is derived on demand by Titiler from the COG at the
    # variable's current render range (ADR 0021) — no stored visual asset.
    for asset in assets:
        asset.texture_url = (
            titiler_encoded_preview_url(item, asset.variable)
            if asset.format == asset.Format.COG else ""
        )
    
    context = {
        "breadcrumbs_items": breadcrumbs_items,
        "header_title": "Item Preview - {}".format(item),
        'item': item,
        "assets": assets
    }
    
    return render(request, 'visualization/item_preview.html', context)
