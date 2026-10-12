from django.templatetags.static import static
from django.urls import path, reverse
from django.utils.html import format_html
from wagtail import hooks
from wagtail.admin.menu import MenuItem


@hooks.register("insert_global_admin_css")
def history_panel_css():
    return format_html('<link rel="stylesheet" href="{}">', static("georivaevents/history.css"))


@hooks.register("register_admin_urls")
def register_event_urls():
    from .views import unattributed_events

    return [path("events/unattributed/", unattributed_events, name="events_unattributed")]


class UnattributedEventsMenuItem(MenuItem):
    def is_shown(self, request):
        return request.user.is_superuser


@hooks.register("register_settings_menu_item")
def register_unattributed_events_menu_item():
    return UnattributedEventsMenuItem(
        "Unattributed events", reverse("events_unattributed"), icon_name="warning", order=900
    )
