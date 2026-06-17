from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin.filters import WagtailFilterSet
from wagtail.admin.views import generic
from wagtail.admin.viewsets.chooser import ChooserViewSet
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import ListingButton
from wagtail.snippets.views.snippets import SnippetViewSet, IndexView

from georiva.core.models import Item, Catalog, Collection, ColorPalette, Asset
from georiva.core.models.catalog import Topic
from georiva.core.views import CatalogIndexView


class BoundaryChooserViewSet(ChooserViewSet):
    model = "adminboundarymanager.AdminBoundary"
    
    icon = "map"
    choose_one_text = "Choose a boundary"
    choose_another_text = "Choose another boundary"
    edit_item_text = "Edit this boundary"


class TopicViewSet(ModelViewSet):
    model = Topic
    add_to_admin_menu = True
    menu_order = 300
    menu_icon = "hashtag"
    exclude_form_fields = ["created_at", "updated_at"]


class CatalogCreateView(generic.CreateView):
    def get_success_url(self):
        url = reverse("catalog:index")
        return self._set_locale_query_param(url)


class CatalogEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog:index")


class CatalogDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog:index")


class CatalogViewSet(ModelViewSet):
    model = Catalog
    icon = "globe"
    menu_label = _("Catalogs")
    menu_icon = "globe"
    menu_order = 400
    add_to_admin_menu = True
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = CatalogIndexView
    index_template_name = "core/catalog_index.html"
    index_results_template_name = "core/catalog_index_results.html"
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
        return reverse("catalog:index")


class CollectionEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog:index")


class CollectionDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog:index")


class CollectionIndexView(generic.IndexView):
    def get_list_more_buttons(self, instance):
        buttons = super().get_list_more_buttons(instance)
        # buttons.append(
        #     ListingButton(
        #         _("Zarr Store"),
        #         url=reverse("zarr_collection_detail", args=[instance.pk]),
        #         icon_name="resubmit",
        #         attrs={"title": _("View Zarr Store Details")},
        #     )
        # )
        return buttons


class CollectionViewSet(ModelViewSet):
    model = Collection
    icon = "folder-open-inverse"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]
    add_view_class = CollectionCreateView
    edit_view_class = CollectionEditView
    delete_view_class = CollectionDeleteView
    index_view_class = CollectionIndexView


class ItemIndexView(IndexView):
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


class ItemFilterSet(WagtailFilterSet):
    class Meta:
        model = Item
        fields = ["collection"]


class ItemViewSet(SnippetViewSet):
    model = Item
    icon = "snippet"
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = ItemIndexView
    list_filter = ["collection"]


class AssetViewSet(SnippetViewSet):
    model = Asset
    exclude_form_fields = ["created_at", "updated_at"]
    list_filter = ["format", "variable"]


class ColorPaletteModelViewSet(ModelViewSet):
    model = ColorPalette
    icon = "palette"
    add_to_admin_menu = True
    menu_order = 600
    exclude_form_fields = ["created_at", "updated_at"]


CatalogChooserViewSetObject = CatalogChooserViewSet("catalog_chooser")

admin_viewsets = [
    TopicViewSet(),
    CatalogViewSet(),
    CatalogChooserViewSetObject,
    CollectionViewSet(),
    ColorPaletteModelViewSet()
]
