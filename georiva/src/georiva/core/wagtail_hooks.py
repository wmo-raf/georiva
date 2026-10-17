from functools import wraps

from adminboundarymanager.models import AdminBoundarySettings
from adminboundarymanager.views import load_boundary, preview_boundary
from django.contrib.auth.decorators import user_passes_test
from django.urls import path, reverse, reverse_lazy
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import Menu, MenuItem, SubmenuMenuItem
from wagtail.models import Site
from wagtail.snippets.models import register_snippet

from .menus import publications_menu
from .views import (
    add_data_select,
    collection_items_list,
    collection_styling,
    plugin_list,
    variable_style_stops,
    variable_styling,
)
from .views.summary_items import CatalogSummaryItem, CollectionSummaryItem, PluginSummaryItem
from .views.trace import item_trace
from .views.viewsets import AssetViewSet, BoundaryChooserViewSet, ItemViewSet, admin_viewsets


@hooks.register("register_admin_urls")
def urlconf_georivacore():
    return [
        path("data/add/", add_data_select, name="add_data"),
        path("collection/<int:collection_pk>/items/", collection_items_list, name="collection_items_list"),
        path("item/<int:item_id>/trace/", item_trace, name="item_trace"),
        path("collection/<int:collection_pk>/styling/", collection_styling, name="collection_styling"),
        path(
            "collection/<int:collection_pk>/styling/<int:variable_pk>/",
            variable_styling,
            name="variable_styling",
        ),
        path(
            "collection/<int:collection_pk>/styling/<int:variable_pk>/stops/",
            variable_style_stops,
            name="variable_style_stops",
        ),
        path("plugins/", plugin_list, name="plugin_list"),
    ]


# The "Data" menu group: creation (Add Data) first, then the acquisition
# surfaces. Catalogs sits above this group as a top-level entry (the Catalog
# viewset's add_to_admin_menu, order 390); derived products are reached from
# each Data Feed's dashboard, not from a menu item.
@hooks.register("register_admin_menu_item")
def register_data_menu():
    return SubmenuMenuItem(
        _("Data"),
        Menu(
            items=[
                MenuItem(_("Add Data"), reverse_lazy("add_data"), icon_name="plus", order=10),
                MenuItem(_("Automated Sources"), reverse_lazy("data_feed_list"), icon_name="file-import", order=30),
                MenuItem(_("Manual Uploads"), reverse_lazy("manual_upload_config_list"), icon_name="upload", order=40),
            ]
        ),
        icon_name="folder-open-inverse",
        order=400,
    )


# The "Publications" menu group: what goes *out*, after Data, which is what
# comes in. Empty by construction — every child arrives through the hook above —
# and a SubmenuMenuItem hides itself when its menu has nothing to show, so an
# instance with no publisher installed has no "Publications" entry at all.
@hooks.register("register_admin_menu_item")
def register_publications_menu():
    return SubmenuMenuItem(
        _("Publications"),
        publications_menu,
        icon_name="globe",
        order=500,
    )


@hooks.register("register_admin_viewset")
def register_viewset():
    return admin_viewsets + [
        BoundaryChooserViewSet("boundary_chooser"),
    ]


def instance_boundary_site():
    """The one site whose ``AdminBoundarySettings`` governs boundary loads."""
    return Site.objects.get(is_default_site=True)


def on_instance_boundary_settings(view):
    """Make ``AdminBoundarySettings.for_request`` resolve to the instance site.

    The package reads ``data_source`` and the country list from the settings row
    of the request's site, i.e. the host's organisation, but writes the single
    instance-wide ``AdminBoundary`` table. Wagtail caches the request's site on
    the request, so pinning it before the view runs pins every settings read in
    it without copying the view.
    """

    @wraps(view)
    def pinned(request, *args, **kwargs):
        request._wagtail_site = instance_boundary_site()
        return view(request, *args, **kwargs)

    return pinned


# The package gates its two views on "holds any page permission", which every
# editor does, yet a load rewrites the instance-wide AdminBoundary table
# (SHARED_REFERENCE_DATA, written by the instance admin alone, ADR 0011). The
# menu below is superuser-only; these keep the URLs in step with it. order=-1
# runs this hook before the package's, so Django resolves our wrapped views
# first; the paths and names are the package's own, so reverse() is unchanged.
@hooks.register("register_admin_urls", order=-1)
def urlconf_boundary_loader_superuser_only():
    superuser_only = user_passes_test(lambda user: user.is_superuser)
    return [
        path(
            "load-boundary/",
            superuser_only(on_instance_boundary_settings(load_boundary)),
            name="adminboundarymanager_load_boundary",
        ),
        path("preview-boundary/", superuser_only(preview_boundary), name="adminboundarymanager_preview_boundary"),
    ]


def boundary_settings_url(site=None):
    """Where Wagtail edits ``AdminBoundarySettings`` (for ``site``, if given).

    Derived from the model's own meta rather than spelled out, so it stays
    correct if the app or model is ever renamed — and so it matches, character
    for character, the URL ``adminboundarymanager`` builds for the same page.
    """
    args = [AdminBoundarySettings._meta.app_label, AdminBoundarySettings._meta.model_name]
    if site is not None:
        args.append(site.pk)
    return reverse("wagtailsettings:edit", args=args)


class SuperuserMenuItem(MenuItem):
    """Shown to the instance admin alone.

    Administrative boundaries are SHARED_REFERENCE_DATA (ADR 0011): no
    organisation owns them and every organisation reads them, so a reload
    rewrites the dataset the whole instance depends on. That is the instance
    admin's act, not an org admin's — and not a page editor's.
    """

    def is_shown(self, request):
        return bool(request.user.is_superuser)


class InstanceBoundarySettingsMenuItem(SuperuserMenuItem):
    """Links to the instance site's settings, the row the loader actually reads."""

    @property
    def url(self):
        return boundary_settings_url(instance_boundary_site())

    @url.setter
    def url(self, value):
        pass


# Boundaries are configuration, not data anybody browses: a Settings submenu,
# not a top-level sidebar group. The two pages come from adminboundarymanager,
# whose own ViewSetGroup existed only to draw the menu we are replacing — we no
# longer register it, so its duplicate /boundary-loader/ URL is gone too.
@hooks.register("register_settings_menu_item")
def register_boundaries_menu_item():
    return SubmenuMenuItem(
        _("Boundaries"),
        Menu(
            items=[
                SuperuserMenuItem(
                    _("Load Data"), reverse_lazy("adminboundarymanager_preview_boundary"), icon_name="upload", order=10
                ),
                InstanceBoundarySettingsMenuItem(
                    _("Settings"),
                    "#",
                    icon_name="cogs",
                    order=20,
                ),
            ]
        ),
        icon_name="map",
        order=120,
    )


# ``@register_setting`` puts its own "Admin boundary settings" entry in this
# menu, which would sit beside ours pointing at the same page. Drop it, matching
# on the URL so there is no label string to drift. Our submenu survives the
# filter: a SubmenuMenuItem's url is "#", never the settings URL.
@hooks.register("construct_settings_menu")
def hide_duplicate_boundary_settings(request, menu_items):
    settings_url = boundary_settings_url()
    menu_items[:] = [item for item in menu_items if item.url != settings_url]


register_snippet(ItemViewSet)
register_snippet(AssetViewSet)


@hooks.register("construct_main_menu")
def hide_some_menus(request, menu_items):
    hidden_menus = ["documents", "help", "snippets", "reports"]

    menu_items[:] = [item for item in menu_items if item.name not in hidden_menus]


@hooks.register("construct_homepage_summary_items")
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
    return mark_safe("<style>.w-sticky.w-z-header + .nice-padding { margin-top: 2rem; }</style>")


@hooks.register("register_icons")
def register_icons(icons):
    return icons + [
        "wagtailfontawesomesvg/solid/circle-nodes.svg",
        "wagtailfontawesomesvg/solid/map-pin.svg",
        "wagtailfontawesomesvg/solid/location-pin.svg",
        "wagtailfontawesomesvg/solid/location-dot.svg",
        "wagtailfontawesomesvg/solid/plug.svg",
        "wagtailfontawesomesvg/solid/hourglass-start.svg",
        "wagtailfontawesomesvg/solid/hourglass-end.svg",
        "wagtailfontawesomesvg/solid/hourglass-half.svg",
        "wagtailfontawesomesvg/solid/paper-plane.svg",
        "wagtailfontawesomesvg/solid/puzzle-piece.svg",
        "wagtailfontawesomesvg/solid/up-down.svg",
        "wagtailfontawesomesvg/solid/plus-minus.svg",
        "wagtailfontawesomesvg/solid/palette.svg",
        "wagtailfontawesomesvg/solid/map.svg",
        "wagtailfontawesomesvg/solid/file-import.svg",
        "wagtailfontawesomesvg/solid/hashtag.svg",
        "wagtailfontawesomesvg/solid/save.svg",
    ]
