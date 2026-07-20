from django.urls import path
from wagtail import hooks
from wagtail.admin.viewsets.model import ModelViewSet

from .models import DataFeed
from .registry import data_feed_viewset_registry
from .utils import get_all_child_models
from .views import (
    data_feed_list,
    data_feed_add_select,
    data_feed_detail,
    data_feed_edit,
    data_feed_delete,
    feed_product_toggle,
    feed_product_run,
    feed_product_edit,
    feed_product_enable_new,
    feed_product_delete_orphan,
    feed_product_rebind,
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
    data_feed_fetch_runs,
    data_feed_fetch_run_detail,
    data_feed_ingestions,
    derived_product_runs,
    derived_product_run_detail,
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
        path('data-feeds/derived-products/<int:product_pk>/runs/', derived_product_runs,
             name="derived_product_runs"),
        path('data-feeds/derived-products/<int:product_pk>/runs/<int:run_pk>/',
             derived_product_run_detail, name="derived_product_run_detail"),
        path('data-feeds/<int:feed_pk>/chain/', derived_product_chain, name="derived_product_chain"),
        path('items/<int:item_pk>/lineage/', item_lineage, name="item_lineage"),
        path('data-feeds/select/', data_feed_add_select, name="data_feed_add_select"),
        path('data-feeds/<int:pk>/', data_feed_detail, name="data_feed_detail"),
        path('data-feeds/<int:feed_pk>/fetch-runs/', data_feed_fetch_runs, name="data_feed_fetch_runs"),
        path('data-feeds/<int:feed_pk>/fetch-runs/<int:run_pk>/', data_feed_fetch_run_detail,
             name="data_feed_fetch_run_detail"),
        path('data-feeds/<int:feed_pk>/ingestions/', data_feed_ingestions,
             name="data_feed_ingestions"),
        path('data-feeds/<int:pk>/edit/', data_feed_edit, name="data_feed_edit"),
        path('data-feeds/<int:pk>/delete/', data_feed_delete, name="data_feed_delete"),
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
        path('data-feeds/<int:feed_pk>/products/<int:product_pk>/rebind/',
             feed_product_rebind, name="feed_product_rebind"),
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


# "Automated Sources" is reached via the "Data" menu group registered in
# core/wagtail_hooks.py, not a top-level menu item. Derived products have no
# menu surface at all — each feed's dashboard is their single home, with
# per-product Runs links drilling into run history.


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
