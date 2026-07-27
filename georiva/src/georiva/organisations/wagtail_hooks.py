from django.urls import path, reverse_lazy
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import MenuItem

from .access import is_org_admin
from .views import organisation_settings
from .viewsets import organisation_membership_viewset, organisation_viewset


@hooks.register("register_admin_viewset")
def register_organisation_viewsets():
    return [organisation_viewset, organisation_membership_viewset]


@hooks.register("register_admin_urls")
def register_organisation_urls():
    return [
        path("org-settings/", organisation_settings, name="organisation_settings"),
    ]


class OrgAdminMenuItem(MenuItem):
    """Shown only to the people the view itself admits.

    Menu visibility and the view's own gate read the same role, so a member
    never sees an entry that would turn them away.
    """

    def is_shown(self, request):
        return is_org_admin(request)


@hooks.register("register_settings_menu_item")
def register_organisation_settings_menu_item():
    return OrgAdminMenuItem(
        _("Organisation"),
        reverse_lazy("organisation_settings"),
        icon_name="group",
        order=100,
    )
