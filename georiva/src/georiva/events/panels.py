from wagtail.admin.ui.components import Component

from .failures import recent_failures, window_days


class FailuresPanel(Component):
    """Failed and refused transitions on the admin homepage, beside the Collection Health Panel."""

    name = "event_failures"
    template_name = "georivaevents/failures_panel.html"
    order = 150

    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        context.update(
            {
                "failures": recent_failures(parent_context["request"]),
                "days": window_days(),
            }
        )
        return context
