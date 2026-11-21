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

    assets = item.assets.select_related("variable__unit")

    for asset in assets:
        asset.texture_url = titiler_encoded_preview_url(item, asset.variable) if asset.is_drawable else ""
        asset.render_contract = render_contract(asset.variable)
        # Statistics are stored in the source unit and shown served (ADR 0046 §15).
        variable = asset.variable
        asset.served_unit = variable.unit.symbol
        asset.served_stats = {
            "min": variable.served(asset.stats_min),
            "max": variable.served(asset.stats_max),
            "mean": variable.served(asset.stats_mean),
            "std": variable.served_spread(asset.stats_std),
        }

    context = {
        "breadcrumbs_items": breadcrumbs_items,
        "header_title": f"Item Preview - {item}",
        "item": item,
        "assets": assets,
    }

    return render(request, "visualization/item_preview.html", context)
