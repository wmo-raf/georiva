from django.shortcuts import render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _

from georiva.core.machine_plane import titiler_encoded_preview_url
from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Item
from georiva.organisations.access import get_org_object_or_404


def item_preview(request, item_id):
    item = get_org_object_or_404(request, Item, pk=item_id)

    breadcrumbs_items = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse("wagtailsnippets_georivacore_item:list"), "label": _("Items")},
        {"url": None, "label": f"{item.collection.name} · {item.time:%Y-%m-%d %H:%M}"},
    ]

    assets = item.assets.all()

    # The map texture is derived on demand by Titiler at the variable's
    # current render range (ADR 0021) — no stored visual asset — from the COG,
    # or from the Upstream store a Store asset names (ADR 0044 §9).
    for asset in assets:
        asset.texture_url = titiler_encoded_preview_url(item, asset.variable) if asset.is_drawable else ""
        asset.render_contract = render_contract(asset.variable)

    context = {
        "breadcrumbs_items": breadcrumbs_items,
        "header_title": f"Item Preview - {item}",
        "item": item,
        "assets": assets,
    }

    return render(request, "visualization/item_preview.html", context)
