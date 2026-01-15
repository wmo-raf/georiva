from django.shortcuts import render, get_object_or_404
from django.urls import reverse_lazy
from django.utils.translation import gettext as _

from georiva.core.models import Item


def item_preview(request, item_id):
    # Logic to retrieve item details can be added here
    
    item = get_object_or_404(Item, pk=item_id)
    
    breadcrumbs_items = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": "#", "label": _("Preview")},
    ]
    
    context = {
        "breadcrumbs_items": breadcrumbs_items,
        "header_title": "Item Preview - {}".format(item),
        'item': item,
        "assets": item.assets.all()
    }
    
    return render(request, 'visualization/item_preview.html', context)
