from django.shortcuts import render
from django.urls import reverse_lazy, reverse
from django.utils.translation import gettext as _
from wagtail.admin.ui.tables import ButtonsColumnMixin, TitleColumn, Table
from wagtail.admin.widgets import ListingButton, HeaderButton

from georiva.core.models import Catalog
from .viewsets import CatalogViewSet, CollectionViewSet


def catalog_index(request):
    catalogs = Catalog.objects.all()
    
    catalog_viewset = CatalogViewSet()
    collection_viewset = CollectionViewSet()
    
    data = []
    
    class CollectionButtonsColumn(ButtonsColumnMixin, TitleColumn):
        def get_buttons(self, instance, parent_context):
            more_buttons = []
            buttons = []
            
            edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
            delete_url = reverse(collection_viewset.get_url_name("delete"), kwargs={"pk": instance.pk})
            
            more_buttons.append(
                ListingButton(
                    _("Edit"),
                    url=edit_url,
                    icon_name="edit",
                    attrs={
                        "aria-label": _("Edit '%(title)s'") % {"title": str(instance)}
                    },
                    priority=10,
                )
            )
            
            more_buttons.append(
                ListingButton(
                    _("Delete"),
                    url=delete_url,
                    icon_name="bin",
                    attrs={
                        "aria-label": _("Delete '%(title)s'") % {"title": str(instance)}
                    },
                    priority=30,
                )
            )
            
            return buttons
    
    def get_url(instance):
        edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
        return edit_url
    
    for catalog in catalogs:
        columns = [
            CollectionButtonsColumn("name", label=_("Collection "), get_url=get_url),
        ]
        
        data.append({
            "catalog": catalog,
            "edit_url": reverse(catalog_viewset.get_url_name("edit"), kwargs={"pk": catalog.pk}),
            "add_collection_url": reverse(collection_viewset.get_url_name("add")),
            "collections_table": Table(columns, catalog.collections.all()),
        })
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Catalogs")},
        ],
        "header_buttons": [
            HeaderButton(
                label=_('Add Catalog'),
                url=reverse(catalog_viewset.get_url_name("add")),
                icon_name="plus",
            ),
        ],
        "catalogs": data,
    }
    
    return render(request, 'core/catalog_list.html', context)
