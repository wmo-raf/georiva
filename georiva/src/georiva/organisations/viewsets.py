"""The instance admin's organisation surface.

Deliberately thin: an organisation's *contents* are managed from its own host,
by its own admins. This viewset only creates organisations and edits their lean
settings, and only a superuser can reach it.
"""
from django.utils.functional import cached_property
from wagtail.admin.viewsets.model import ModelViewSet

from .forms import OrganisationCreateForm, OrganisationEditForm
from .models import Organisation
from .permissions import SuperuserOnlyPermissionPolicy


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

    # Organisations are deleted by the instance admin at the database level, not
    # from the admin: deleting one would strand its Site, pages and storage.
    copy_view_enabled = False

    @cached_property
    def permission_policy(self):
        return SuperuserOnlyPermissionPolicy(self.model)

    def get_form_class(self, for_update=False):
        return OrganisationEditForm if for_update else OrganisationCreateForm


organisation_viewset = OrganisationViewSet("organisation")
