from django.urls import path, reverse_lazy
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import Menu, MenuItem, SubmenuMenuItem

from .access import is_org_admin
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
