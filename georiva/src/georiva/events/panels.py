from wagtail.admin.ui.components import Component

from .failures import failure_groups, failures_url, window_days


class FailuresPanel(Component):
    name = "event_failures"
    template_name = "georivaevents/failures_panel.html"
    order = 150

    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        context.update(
            {
                "failures": failure_groups(parent_context["request"]),
                "failures_url": failures_url(),
                "days": window_days(),
            }
        )
        return context
