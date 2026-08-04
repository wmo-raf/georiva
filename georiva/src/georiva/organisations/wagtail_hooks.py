from django.templatetags.static import static
from django.urls import path, reverse, reverse_lazy
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import Menu, MenuItem, SubmenuMenuItem

from .access import is_org_admin
from .dashboard import scope_dashboard_panels
from .pages import scope_pages
from .views import (
    organisation_member_add,
    organisation_member_edit,
    organisation_member_remove,
    organisation_members,
    organisation_settings,
)
from .viewsets import organisation_membership_viewset, organisation_viewset


@hooks.register("register_admin_viewset")
def register_organisation_viewsets():
    return [organisation_viewset, organisation_membership_viewset]


@hooks.register("register_admin_urls")
def register_organisation_urls():
    return [
        path("org-settings/", organisation_settings, name="organisation_settings"),
        path("org-members/", organisation_members, name="organisation_members"),
        path("org-members/add/", organisation_member_add, name="organisation_member_add"),
        path("org-members/<int:pk>/", organisation_member_edit, name="organisation_member_edit"),
        path("org-members/<int:pk>/remove/", organisation_member_remove,
             name="organisation_member_remove"),
    ]


@hooks.register("insert_global_admin_css")
def org_hopper_css():
    return format_html(
        '<link rel="stylesheet" href="{}">', static("organisations/org_hopper.css")
    )


@hooks.register("insert_global_admin_js")
def org_hopper_js():
    """The two halves of the org-hopper, in the order they have to run.

    The behaviour is a static file, cacheable and identical for everyone. The
    block it mounts is not — it names the organisations *this* user may hop to —
    so it comes from a view, for the reason ADR 0017 gives. Both are ``defer``,
    so they run in document order: the mount function exists before the markup
    calls it.
    """
    return format_html(
        '<script src="{}" defer></script><script src="{}" defer></script>',
        static("organisations/org_hopper.js"),
        reverse("organisation_hopper_script"),
    )


@hooks.register("construct_explorer_page_queryset")
def scope_explorer_to_this_organisation(parent_page, pages, request):
    """The page explorer, and the sidebar's page browser through the admin API.

    Both run every listing through this hook, so the explorer started at the tree
    root — where a superuser lands, and where every organisation's root sits side
    by side — lists only the root of the organisation whose host was dialled.
    """
    return scope_pages(request, pages)


@hooks.register("construct_page_chooser_queryset")
def scope_page_chooser_to_this_organisation(pages, request):
    return scope_pages(request, pages)


@hooks.register("construct_homepage_panels")
def scope_the_dashboard_to_this_organisation(request, panels):
    """The admin dashboard's page-resolving panels — see ``dashboard``.

    Registered here rather than anywhere earlier on purpose: this app is listed
    after the apps that append panels of their own, so this hook runs last and
    sees the finished list.
    """
    scope_dashboard_panels(panels)


class OrgAdminMenuItem(MenuItem):
    """Shown only to the people the views themselves admit.

    Menu visibility and each view's own gate read the same role, so a member
    never sees an entry that would turn them away — and the menu is decoration,
    not the gate: every view calls ``require_org_admin`` regardless.
    """

    def is_shown(self, request):
        return is_org_admin(request)


class OrgAdminSubmenuItem(SubmenuMenuItem, OrgAdminMenuItem):
    pass


@hooks.register("register_settings_menu_item")
def register_organisation_menu_item():
    return OrgAdminSubmenuItem(
        _("Organisation"),
        Menu(items=[
            OrgAdminMenuItem(_("Settings"), reverse_lazy("organisation_settings"),
                             icon_name="cogs", order=10),
            OrgAdminMenuItem(_("Members"), reverse_lazy("organisation_members"),
                             icon_name="user", order=20),
        ]),
        icon_name="group",
        order=100,
    )
