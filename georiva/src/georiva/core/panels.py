"""Read-only styling panel for the demoted admin surfaces (issue #323).

The Wagtail collection form's inline variables panel used to edit the value
range beside the identity fields. Since ADR 0022 made the Styling surface the
one place that tunes styling, this panel replaces those fields: it *shows* the
range and the default style's swatch and links to the Styling page, editing
nothing.

Runtime imports throughout: this module is imported by ``core.models.variable``
at model-definition time, so importing models or views at module level would be
circular.
"""
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin.panels import Panel


class StylingSummaryPanel(Panel):
    """Range + default-style swatch, read-only, linking to the Styling page."""

    class BoundPanel(Panel.BoundPanel):
        template_name = "core/panels/styling_summary.html"

        def get_context_data(self, parent_context=None):
            from georiva.core.views.styling import GRAYSCALE_GRADIENT

            context = super().get_context_data(parent_context)
            variable = self.instance
            context["variable"] = variable
            if variable is not None and variable.pk:
                default = variable.default_style
                context["default_style"] = default
                context["gradient"] = (
                    default.css_gradient() if default else GRAYSCALE_GRADIENT
                )
                context["is_grayscale"] = default is None
                context["styling_url"] = reverse(
                    "variable_styling",
                    args=[variable.collection_id, variable.pk],
                )
            else:
                context["styling_note"] = _(
                    "Save the collection first, then set the range and style "
                    "on the Styling page."
                )
            return context
