from wagtail import hooks

from .viewsets import organisation_membership_viewset, organisation_viewset


@hooks.register("register_admin_viewset")
def register_organisation_viewsets():
    return [organisation_viewset, organisation_membership_viewset]
