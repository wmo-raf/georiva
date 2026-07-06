from django.urls import path, reverse
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import MenuItem
from wagtail.admin.viewsets.model import ModelViewSet

from .models import DataFeed
from .registry import data_feed_viewset_registry
from .utils import get_all_child_models
from .views import (
    data_feed_list,
    data_feed_add_select,
    data_feed_detail,
    data_feed_edit,
    feed_product_toggle,
    feed_product_run,
    feed_product_edit,
    feed_product_enable_new,
    feed_product_delete_orphan,
    definition_collection_add,
    definition_collection_edit,
    definition_collection_remove_confirm,
    definition_collection_vars_edit,
    setup_wizard_select,
    wizard_step1_catalog,
    wizard_step2_feed,
    wizard_step3_collections,
    wizard_step4_products,
    wizard_provision,
    derived_product_tracking,
    derived_product_chain,
    item_lineage,
)
from .viewsets import (
    admin_viewsets,
    DataFeedCreateView,
    DataFeedEditView,
    DataFeedDeleteView,
)


@hooks.register('register_admin_urls')
def urlconf_georivasources():
    return [
        path('data-feeds/', data_feed_list, name="data_feed_list"),
        path('data-feeds/derived-products/', derived_product_tracking, name="derived_product_tracking"),
        path('data-feeds/<int:feed_pk>/chain/', derived_product_chain, name="derived_product_chain"),
        path('items/<int:item_pk>/lineage/', item_lineage, name="item_lineage"),
        path('data-feeds/select/', data_feed_add_select, name="data_feed_add_select"),
        path('data-feeds/<int:pk>/', data_feed_detail, name="data_feed_detail"),
        path('data-feeds/<int:pk>/edit/', data_feed_edit, name="data_feed_edit"),
        path('data-feeds/<int:feed_pk>/products/<int:product_pk>/toggle/',
             feed_product_toggle, name="feed_product_toggle"),
        path('data-feeds/<int:feed_pk>/products/<int:product_pk>/run/',
             feed_product_run, name="feed_product_run"),
        path('data-feeds/<int:feed_pk>/products/<int:product_pk>/edit/',
             feed_product_edit, name="feed_product_edit"),
        path('data-feeds/<int:feed_pk>/products/enable/<str:definition_key>/',
             feed_product_enable_new, name="feed_product_enable_new"),
        path('data-feeds/<int:feed_pk>/products/<int:product_pk>/delete/',
             feed_product_delete_orphan, name="feed_product_delete_orphan"),
        path('data-feeds/setup-wizard/', setup_wizard_select, name="setup_wizard_select"),
        path('data-feeds/setup-wizard/<str:model_name>/catalog/', wizard_step1_catalog, name="wizard_step1_catalog"),
        path('data-feeds/setup-wizard/<str:model_name>/feed/', wizard_step2_feed, name="wizard_step2_feed"),
        path('data-feeds/setup-wizard/<str:model_name>/collections/', wizard_step3_collections,
             name="wizard_step3_collections"),
        path('data-feeds/setup-wizard/<str:model_name>/products/', wizard_step4_products,
             name="wizard_step4_products"),
        path('data-feeds/setup-wizard/<str:model_name>/provision/', wizard_provision, name="wizard_provision"),
        path('data-feeds/<int:feed_pk>/collections/add/<str:definition_key>/', definition_collection_add,
             name="definition_collection_add"),
        path('data-feeds/<int:feed_pk>/collections/<int:link_pk>/edit/', definition_collection_edit,
             name="definition_collection_edit"),
        path('data-feeds/<int:feed_pk>/collections/<int:link_pk>/remove/', definition_collection_remove_confirm,
             name="definition_collection_remove"),
        path('data-feeds/<int:feed_pk>/collections/<int:link_pk>/variables/', definition_collection_vars_edit,
             name="definition_collection_vars_edit"),
    ]


@hooks.register('register_admin_menu_item')
def register_sources_menu():
    return MenuItem(
        _("Automated Sources"),
        reverse('data_feed_list'),
        icon_name='file-import',
        order=800,
    )


@hooks.register('register_admin_menu_item')
def register_derived_products_menu():
    return MenuItem(
        _("Derived Products"),
        reverse('derived_product_tracking'),
        icon_name='cogs',
        order=810,
    )


def get_data_feed_viewsets():
    data_feed_model_cls = get_all_child_models(DataFeed)
    
    data_feed_viewsets = []
    
    for model_cls in data_feed_model_cls:
        model_name = model_cls._meta.model_name
        
        attrs = {
            "model": model_cls,
            "type": model_name,
            "add_view_class": DataFeedCreateView,
            "edit_view_class": DataFeedEditView,
            "delete_view_class": DataFeedDeleteView,
        }
        
        viewset = type(
            f"{model_cls.__name__}ViewSet",
            (ModelViewSet,),
            attrs
        )
        
        viewset_cls = viewset()
        
        data_feed_viewsets.append(viewset_cls)
        data_feed_viewset_registry.register(viewset_cls)
    
    return data_feed_viewsets


@hooks.register("register_admin_viewset")
def register_viewset():
    data_feed_viewsets = get_data_feed_viewsets()
    return admin_viewsets + data_feed_viewsets
