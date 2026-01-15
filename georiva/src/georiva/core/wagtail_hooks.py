from adminboundarymanager.wagtail_hooks import AdminBoundaryViewSetGroup
from django.urls import reverse
from wagtail import hooks
from wagtail.admin.views import generic
from django.utils.translation import gettext_lazy as _
from wagtail.admin.viewsets import ViewSetGroup
from wagtail.admin.viewsets.chooser import ChooserViewSet
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import ListingButton

from georiva.core.models import Item, Catalog, Collection


class BoundaryChooserViewSet(ChooserViewSet):
    model = "adminboundarymanager.AdminBoundary"
    
    icon = "map"
    choose_one_text = "Choose a boundary"
    choose_another_text = "Choose another boundary"
    edit_item_text = "Edit this boundary"


class CatalogViewSet(ModelViewSet):
    model = Catalog
    icon = "folder-open-inverse"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]


class CollectionViewSet(ModelViewSet):
    model = Collection
    icon = "folder-open-inverse"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]


class ItemIndexView(generic.IndexView):
    def get_list_more_buttons(self, instance):
        buttons = super().get_list_more_buttons(instance)
        
        label = _("View")
        url = reverse("item_preview", args=[instance.id])
        icon_name = "view"
        attrs = {}
        if label and url:
            buttons.append(
                ListingButton(
                    label,
                    url=url,
                    icon_name=icon_name,
                    attrs=attrs,
                )
            )
        
        return buttons


class ItemViewSet(ModelViewSet):
    model = Item
    icon = "snippet"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = ItemIndexView


class GeorivaViewSetGroup(ViewSetGroup):
    menu_label = "GeoRIVA"
    menu_icon = "globe"
    
    items = [
        CatalogViewSet(),
        CollectionViewSet(),
        ItemViewSet(),
    ]


@hooks.register("register_admin_viewset")
def register_viewset():
    return [
        AdminBoundaryViewSetGroup(),
        BoundaryChooserViewSet("boundary_chooser"),
        GeorivaViewSetGroup()
    ]
