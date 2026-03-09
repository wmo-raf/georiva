from django.shortcuts import render
from django.urls import reverse_lazy, reverse
from django.utils.translation import gettext as _
from wagtail.admin.ui.tables import ButtonsColumnMixin, TitleColumn, Table, BooleanColumn
from wagtail.admin.widgets import ListingButton, HeaderButton, ButtonWithDropdown

from georiva.core.models import Catalog
from .table import LinkColumnWithIcon
from .viewsets import CatalogViewSet, CollectionViewSet


def get_collection_items_url(collection):
    return "#"


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
            
            if more_buttons:
                buttons.append(
                    ButtonWithDropdown(
                        buttons=more_buttons,
                        icon_name="dots-horizontal",
                        attrs={
                            "aria-label": _("More options for '%(title)s'")
                                          % {"title": str(instance)},
                        },
                    )
                )
            
            return buttons
    
    def get_url(instance):
        edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
        return edit_url
    
    for catalog in catalogs:
        columns = [
            CollectionButtonsColumn("name", label=_("Collection "), get_url=get_url),
            BooleanColumn("is_active", label=_("Active")),
            LinkColumnWithIcon("Items", label=_("Items"), icon_name="view", get_url=get_collection_items_url),
        ]
        
        collections = catalog.collections.all()
        
        data.append({
            "catalog": catalog,
            "edit_url": reverse(catalog_viewset.get_url_name("edit"), kwargs={"pk": catalog.pk}),
            "add_collection_url": reverse(collection_viewset.get_url_name("add")),
            "has_collections": collections.exists(),
            "collections_table": Table(columns, collections),
        })
    
    catalog_add_url = reverse(catalog_viewset.get_url_name("add"))
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Catalogs")},
        ],
        "header_buttons": [
            HeaderButton(
                label=_('Add Catalog'),
                url=catalog_add_url,
                icon_name="plus",
            ),
        ],
        "catalogs": data,
        "catalog_add_url": catalog_add_url,
    }
    
    return render(request, 'core/catalog_list.html', context)



