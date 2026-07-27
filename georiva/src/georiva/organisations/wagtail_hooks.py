from wagtail import hooks

from .viewsets import organisation_viewset


@hooks.register("register_admin_viewset")
def register_organisation_viewset():
    return organisation_viewset
