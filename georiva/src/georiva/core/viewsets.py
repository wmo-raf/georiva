from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin.views import generic
from wagtail.admin.viewsets.chooser import ChooserViewSet
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import ListingButton

from georiva.core.models import Item, Catalog, Collection, ColorPalette


class BoundaryChooserViewSet(ChooserViewSet):
    model = "adminboundarymanager.AdminBoundary"
    
    icon = "map"
    choose_one_text = "Choose a boundary"
    choose_another_text = "Choose another boundary"
    edit_item_text = "Edit this boundary"


class CatalogCreateView(generic.CreateView):
    def get_success_url(self):
        url = reverse("catalog_index")
        return self._set_locale_query_param(url)


class CatalogEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog_index")


class CatalogDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog_index")


class CatalogViewSet(ModelViewSet):
    model = Catalog
    icon = "globe"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]
    add_view_class = CatalogCreateView
    edit_view_class = CatalogEditView
    delete_view_class = CatalogDeleteView


class CatalogChooserViewSet(ChooserViewSet):
    model = Catalog
    
    icon = "globe"
    choose_one_text = "Choose a catalog"
    choose_another_text = "Choose another catalog"
    edit_item_text = "Edit this catalog"


class CollectionCreateView(generic.CreateView):
    def get_success_url(self):
        return reverse("catalog_index")


class CollectionEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog_index")


class CollectionDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog_index")


class CollectionViewSet(ModelViewSet):
    model = Collection
    icon = "folder-open-inverse"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]
    add_view_class = CollectionCreateView
    edit_view_class = CollectionEditView
    delete_view_class = CollectionDeleteView


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
    add_to_admin_menu = True
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = ItemIndexView


class ColorPaletteModelViewSet(ModelViewSet):
    model = ColorPalette
    icon = "palette"
    add_to_admin_menu = True
    menu_order = 600
    exclude_form_fields = ["created_at", "updated_at"]


CatalogChooserViewSetObject = CatalogChooserViewSet("catalog_chooser")

admin_viewsets = [
    CatalogViewSet(),
    CatalogChooserViewSetObject,
    CollectionViewSet(),
    ColorPaletteModelViewSet()
]
