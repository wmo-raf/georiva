from django.urls import path, reverse_lazy
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import MenuItem

from .views import api_key_revoke, api_keys


@hooks.register("register_admin_urls")
def register_api_key_urls():
    return [
        path("api-keys/", api_keys, name="api_keys"),
        path("api-keys/<int:pk>/revoke/", api_key_revoke, name="api_key_revoke"),
    ]


@hooks.register("register_settings_menu_item")
def register_api_keys_menu_item():
    """Shown to everybody who reaches the admin at all.

    No role gate, unlike the organisation menu next to it: a key grants its
    holder exactly what they already have, so a member managing their own
    credentials is not an administrative act.
    """
    return MenuItem(_("API keys"), reverse_lazy("api_keys"), icon_name="key", order=110)
