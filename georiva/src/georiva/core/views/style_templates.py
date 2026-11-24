"""The Style template catalogue (#786): both tiers listed, shipped ones read-only.

Built on the ramp catalog's tier rules (``GlobalTierCreateView`` /
``GlobalTierIndexView``) with one rule added: a template with an ``origin`` was
shipped by GeoRiva and nobody edits or deletes it — the instance admin
included — so a later release can refresh it. Anyone adapts one by copying it
into their organisation.
"""

from django import forms
from django.contrib import messages
from django.http import Http404
from django.shortcuts import redirect
from django.urls import path, reverse
from django.utils.functional import cached_property
from django.views.generic import TemplateView
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.panels import FieldPanel, FieldRowPanel, ObjectList
from wagtail.admin.views import generic
from wagtail.admin.views.generic.base import WagtailAdminTemplateMixin
from wagtail.admin.views.generic.permissions import PermissionCheckedMixin
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import ListingButton

from georiva.core.models import StyleTemplate
from georiva.organisations.access import get_org_object_or_404, require_active_org
from georiva.organisations.scoping import OrgScopedViewSetMixin

from .viewsets import GlobalTierCreateView, GlobalTierIndexView


def _number(text):
    value = float(text)
    return int(value) if value.is_integer() else value


class StyleTemplateForm(WagtailAdminModelForm):
    """Levels and colours as the comma-separated lists an operator types,
    stored as the JSON lists the model validates."""

    levels = forms.CharField(
        help_text="Band boundaries, ascending and comma-separated, e.g. 0.5, 1, 2, 5, 10.",
    )
    colors = forms.CharField(
        label="Colours",
        widget=forms.Textarea(attrs={"rows": 3}),
        help_text="One hex colour per band — one fewer than the levels — comma-separated.",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            self.initial["levels"] = ", ".join(f"{v:g}" for v in self.instance.levels or [])
            self.initial["colors"] = ", ".join(self.instance.colors or [])

    def clean_levels(self):
        try:
            return [_number(part) for part in self.cleaned_data["levels"].split(",") if part.strip()]
        except ValueError as exc:
            raise forms.ValidationError("Levels must be numbers separated by commas.") from exc

    def clean_colors(self):
        return [part.strip() for part in self.cleaned_data["colors"].split(",") if part.strip()]


class StyleTemplateIndexView(GlobalTierIndexView):
    def get_base_queryset(self):
        return super().get_base_queryset().select_related("unit", "organisation").in_catalogue_order()

    def _writable(self, instance):
        return not instance.is_shipped and super()._writable(instance)

    def get_list_more_buttons(self, instance):
        buttons = super().get_list_more_buttons(instance)
        if self.user_has_permission("add"):
            buttons.append(
                ListingButton(
                    "Copy to my organisation",
                    url=reverse("styletemplate:copy_to_organisation", args=[instance.pk]),
                    icon_name="copy",
                    priority=20,
                )
            )
        return buttons


class ShippedReadOnlyMixin:
    """A shipped template is not there to change, for anyone.

    Reported as absent rather than forbidden, the way the read-only tier is
    (``access.require_writable_org_object``).
    """

    def get_object(self, *args, **kwargs):
        obj = super().get_object(*args, **kwargs)
        if obj.is_shipped:
            raise Http404("A shipped Style template is read-only; copy it to your organisation to adapt it.")
        return obj


class StyleTemplateEditView(ShippedReadOnlyMixin, generic.EditView):
    pass


class StyleTemplateDeleteView(ShippedReadOnlyMixin, generic.DeleteView):
    pass


class StyleTemplateCopyView(PermissionCheckedMixin, WagtailAdminTemplateMixin, TemplateView):
    """ "Copy to my organisation": an editable copy of any template you can see.

    The source is resolved through the read scope — this organisation's
    templates plus the instance-wide tier — so another organisation's template
    is absent here. The copy lands in the host's organisation, for the
    instance admin too: the instance-wide tier is curated by creating there,
    not by copying into it.
    """

    template_name = "core/style_template_copy.html"
    permission_required = "add"
    permission_policy = None
    header_icon = "copy"

    def dispatch(self, request, *args, **kwargs):
        self.source = get_org_object_or_404(request, StyleTemplate, pk=kwargs["pk"])
        return super().dispatch(request, *args, **kwargs)

    def get_page_title(self):
        return f"Copy “{self.source}” to my organisation"

    def get_breadcrumbs_items(self):
        return self.breadcrumbs_items + [
            {"url": reverse("styletemplate:index"), "label": "Style Templates"},
            {"url": None, "label": self.get_page_title()},
        ]

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["source"] = self.source
        context["organisation"] = require_active_org(self.request)
        return context

    def post(self, request, *args, **kwargs):
        copy = self.source.copy_to(require_active_org(request))
        messages.success(request, f"Copied “{self.source}” to your organisation. This copy is yours to edit.")
        return redirect(reverse("styletemplate:edit", args=[copy.pk]))


class StyleTemplateViewSet(OrgScopedViewSetMixin, ModelViewSet):
    """The Style template catalogue, beside the Color ramp catalog under Settings."""

    model = StyleTemplate
    icon = "table"
    add_to_settings_menu = True
    menu_order = 135
    add_view_class = GlobalTierCreateView
    index_view_class = StyleTemplateIndexView
    edit_view_class = StyleTemplateEditView
    delete_view_class = StyleTemplateDeleteView
    # Wagtail's copy view saves over its source (see ColorRampModelViewSet);
    # templates are copied by "Copy to my organisation" instead.
    copy_view_enabled = False
    list_display = ["name", "quantity", "preview", "levels_label", "under", "over", "owner_label"]
    list_filter = ["quantity"]
    search_fields = ["name", "slug", "source_name"]

    def get_urlpatterns(self):
        copy_view = StyleTemplateCopyView.as_view(permission_policy=self.permission_policy)
        return super().get_urlpatterns() + [
            path(f"copy-to-organisation/<{self.pk_path_converter}:pk>/", copy_view, name="copy_to_organisation"),
        ]

    @cached_property
    def edit_handler(self):
        return ObjectList(
            [
                FieldPanel("name"),
                FieldPanel("slug"),
                FieldPanel("quantity"),
                FieldPanel("unit"),
                FieldPanel("levels"),
                FieldPanel("colors"),
                FieldRowPanel([FieldPanel("under"), FieldPanel("over")]),
                FieldPanel("scale_type"),
            ],
            base_form_class=StyleTemplateForm,
        )
