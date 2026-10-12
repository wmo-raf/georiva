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
    from .views import timeline, unattributed_events

    # No menu entry for the Timeline: it is reached by link, from a History row.
    return [
        path("events/unattributed/", unattributed_events, name="events_unattributed"),
        path("events/timeline/<uuid:correlation_id>/", timeline, name="events_timeline"),
    ]


class UnattributedEventsMenuItem(MenuItem):
    def is_shown(self, request):
        return request.user.is_superuser


@hooks.register("register_settings_menu_item")
def register_unattributed_events_menu_item():
    return UnattributedEventsMenuItem(
        "Unattributed events", reverse("events_unattributed"), icon_name="warning", order=900
    )


@hooks.register("construct_homepage_panels")
def add_failures_panel(request, panels):
    """Failed and refused transitions, org-wide — the one deliberate exception to ADR 0019."""
    from .panels import FailuresPanel

    panels.append(FailuresPanel())
