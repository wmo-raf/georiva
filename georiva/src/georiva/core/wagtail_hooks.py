from adminboundarymanager.wagtail_hooks import AdminBoundaryViewSetGroup
from django.urls import path, reverse
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import MenuItem

from .views import catalog_index
from .viewsets import BoundaryChooserViewSet, admin_viewsets


@hooks.register('register_admin_urls')
def urlconf_georivacore():
    return [
        path('catalogs/', catalog_index, name="catalog_index"),
    ]


@hooks.register('register_admin_menu_item')
def register_catalogs_menu():
    list_url = reverse('catalog_index')
    label = _("Catalogs")
    return MenuItem(label, list_url, icon_name='globe', order=400)


@hooks.register("register_admin_viewset")
def register_viewset():
    return admin_viewsets + [
        AdminBoundaryViewSetGroup(),
        BoundaryChooserViewSet("boundary_chooser"),
    ]


@hooks.register('construct_main_menu')
def hide_some_menus(request, menu_items):
    hidden_menus = ["documents", "help", "snippets", "reports"]
    
    menu_items[:] = [item for item in menu_items if item.name not in hidden_menus]


@hooks.register('construct_homepage_summary_items')
def construct_homepage_summary_items(request, summary_items):
    hidden_summary_items = ["PagesSummaryItem", "DocumentsSummaryItem", "ImagesSummaryItem"]
    
    summary_items[:] = [item for item in summary_items if item.__class__.__name__ not in hidden_summary_items]
    
    summary_items[:] = []


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
    ]
