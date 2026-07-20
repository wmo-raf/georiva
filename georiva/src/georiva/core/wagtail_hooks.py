from adminboundarymanager.wagtail_hooks import AdminBoundaryViewSetGroup
from django.urls import path, reverse_lazy
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import Menu, MenuItem, SubmenuMenuItem
from wagtail.snippets.models import register_snippet

from .summary_items import CatalogSummaryItem, CollectionSummaryItem, PluginSummaryItem
from .views import add_data_select, collection_items_list, plugin_list
from .viewsets import BoundaryChooserViewSet, admin_viewsets
from .viewsets import ItemViewSet, AssetViewSet


@hooks.register('register_admin_urls')
def urlconf_georivacore():
    return [
        path('data/add/', add_data_select, name="add_data"),
        path('collection/<int:collection_pk>/items/', collection_items_list, name="collection_items_list"),
        path('plugins/', plugin_list, name="plugin_list"),
    ]


# The "Data" menu group: creation (Add Data) first, then the acquisition
# surfaces. Catalogs sits above this group as a top-level entry (the Catalog
# viewset's add_to_admin_menu, order 390); derived products are reached from
# each Data Feed's dashboard, not from a menu item.
@hooks.register("register_admin_menu_item")
def register_data_menu():
    return SubmenuMenuItem(
        _("Data"),
        Menu(items=[
            MenuItem(_("Add Data"), reverse_lazy("add_data"), icon_name="plus", order=10),
            MenuItem(_("Automated Sources"), reverse_lazy("data_feed_list"), icon_name="file-import", order=30),
            MenuItem(_("Manual Uploads"), reverse_lazy("manual_upload_config_list"), icon_name="upload", order=40),
        ]),
        icon_name="folder-open-inverse",
        order=400,
    )


@hooks.register("register_admin_viewset")
def register_viewset():
    return admin_viewsets + [
        AdminBoundaryViewSetGroup(),
        BoundaryChooserViewSet("boundary_chooser"),
    ]


register_snippet(ItemViewSet)
register_snippet(AssetViewSet)


@hooks.register('construct_main_menu')
def hide_some_menus(request, menu_items):
    hidden_menus = ["documents", "help", "snippets", "reports"]
    
    menu_items[:] = [item for item in menu_items if item.name not in hidden_menus]


@hooks.register('construct_homepage_summary_items')
def construct_homepage_summary_items(request, summary_items):
    hidden_summary_items = ["PagesSummaryItem", "DocumentsSummaryItem", "ImagesSummaryItem"]
    
    summary_items[:] = [item for item in summary_items if item.__class__.__name__ not in hidden_summary_items]
    
    summary_items[:] = [
        CatalogSummaryItem(request),
        CollectionSummaryItem(request),
        PluginSummaryItem(request),
    ]


# wagtailadmin/generic/base.html wraps main_content in a bare .nice-padding,
# which pads the sides only — Wagtail's own pages add w-mt-8 where they need
# top space. Our function-based-view pages render straight into that wrapper,
# so give the slim-header + bare-nice-padding pairing the same breathing room.
@hooks.register("insert_global_admin_css")
def main_content_breathing_space():
    return mark_safe(
        "<style>.w-sticky.w-z-header + .nice-padding { margin-top: 2rem; }</style>"
    )


@hooks.register("register_icons")
def register_icons(icons):
    return icons + [
        'wagtailfontawesomesvg/solid/circle-nodes.svg',
        'wagtailfontawesomesvg/solid/map-pin.svg',
        'wagtailfontawesomesvg/solid/location-pin.svg',
        'wagtailfontawesomesvg/solid/location-dot.svg',
        'wagtailfontawesomesvg/solid/plug.svg',
        'wagtailfontawesomesvg/solid/hourglass-start.svg',
        'wagtailfontawesomesvg/solid/hourglass-end.svg',
        'wagtailfontawesomesvg/solid/hourglass-half.svg',
        'wagtailfontawesomesvg/solid/paper-plane.svg',
        'wagtailfontawesomesvg/solid/puzzle-piece.svg',
        'wagtailfontawesomesvg/solid/up-down.svg',
        'wagtailfontawesomesvg/solid/plus-minus.svg',
        'wagtailfontawesomesvg/solid/palette.svg',
        'wagtailfontawesomesvg/solid/map.svg',
        'wagtailfontawesomesvg/solid/file-import.svg',
        'wagtailfontawesomesvg/solid/hashtag.svg',
        'wagtailfontawesomesvg/solid/save.svg',
    ]
