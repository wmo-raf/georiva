"""The instance admin's organisation surface.

Deliberately thin: an organisation's *contents* are managed from its own host,
by its own admins. This viewset only creates organisations and edits their lean
settings, and only a superuser can reach it.
"""
from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from wagtail.admin.views.generic import EditView
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import HeaderButton

from .forms import OrganisationCreateForm, OrganisationEditForm
from .models import Organisation, OrganisationMembership
from .permissions import SuperuserOnlyPermissionPolicy


class OrganisationEditView(EditView):
    """Adds the one hop this page was missing: over to the people in this org.

    The membership list is instance-wide, so the link carries the organisation
    as a filter rather than landing on every membership on the instance. Both
    ends are superuser-only, so the button needs no gate of its own — nobody who
    reaches this page is barred from where it points.
    """

    @cached_property
    def header_buttons(self):
        members_url = (
            f"{reverse('organisation_membership:index')}"
            f"?organisation={self.object.pk}"
        )
        return [
            *super().header_buttons,
            HeaderButton(_("View members"), url=members_url, icon_name="user"),
        ]


class OrganisationViewSet(ModelViewSet):
    model = Organisation
    icon = "group"
    menu_label = "Organisations"
    menu_icon = "group"
    add_to_admin_menu = True
    # Below the data menus — an instance-admin surface, not a daily one.
    menu_order = 800
    list_display = ["name", "slug", "hostname", "country"]
    search_fields = ["name", "slug"]
    inspect_view_enabled = True
    copy_view_enabled = False
    edit_view_class = OrganisationEditView

    @cached_property
    def permission_policy(self):
        # Deleting an organisation from here would strand its Site, page tree,
        # storage prefix and group with no way to put them back; teardown is a
        # database-level act for the instance admin.
        return SuperuserOnlyPermissionPolicy(self.model, denied_actions=["delete"])

    def get_form_class(self, for_update=False):
        return OrganisationEditForm if for_update else OrganisationCreateForm


class OrganisationMembershipViewSet(ModelViewSet):
    """Who belongs to which organisation — including an org's first org admin.

    Instance-admin only for now: org admins managing their own members is a
    separate surface, on the org's own host.
    """

    model = OrganisationMembership
    icon = "user"
    menu_label = "Organisation members"
    menu_icon = "user"
    add_to_admin_menu = True
    menu_order = 810
    form_fields = ["user", "organisation", "role"]
    list_display = ["user", "organisation", "role"]
    list_filter = ["organisation"]
    search_fields = ["user__username", "organisation__name"]

    @cached_property
    def permission_policy(self):
        return SuperuserOnlyPermissionPolicy(self.model)


organisation_viewset = OrganisationViewSet("organisation")
organisation_membership_viewset = OrganisationMembershipViewSet("organisation_membership")
