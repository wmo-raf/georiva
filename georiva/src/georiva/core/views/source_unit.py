from django import forms
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin import messages

from georiva.core.models import Collection, SourceUnitCorrected, Unit, Variable
from georiva.core.unit_utils import ConversionRefused
from georiva.events.recording import record
from georiva.organisations.access import get_org_object_or_404


class CorrectSourceUnitForm(forms.Form):
    source_unit = forms.ModelChoiceField(
        queryset=Unit.objects.all(),
        label=_("What this variable's data actually holds"),
    )

    def __init__(self, *args, variable, **kwargs):
        super().__init__(*args, **kwargs)
        self.variable = variable
        self.fields["source_unit"].initial = variable.source_unit_id

    def clean_source_unit(self):
        source_unit = self.cleaned_data["source_unit"]
        if source_unit.pk == self.variable.source_unit_id:
            raise ValidationError(_("That is already this variable's source unit."))
        return source_unit

    def clean(self):
        cleaned = super().clean()
        source_unit = cleaned.get("source_unit")
        if source_unit is not None:
            corrected = Variable(source_unit=source_unit, unit=self.variable.unit, water=self.variable.water)
            try:
                corrected.conversion()
            except ConversionRefused as e:
                self.add_error("source_unit", str(e))
        return cleaned


def reinterpreted(variable) -> dict:
    """What a correction changes the meaning of, counted, for the confirmation page."""
    from georiva.analysis.zonal_stats.models import BoundaryZonalStats

    styles = list(variable.styles.all())
    return {
        "assets": variable.assets.count(),
        "value_min": variable.value_min,
        "value_max": variable.value_max,
        "styles": len(styles),
        "stops": sum(len(style.stops or []) for style in styles),
        "asset_statistics": variable.assets.exclude(stats_min__isnull=True).count(),
        "zonal_statistics": BoundaryZonalStats.objects.filter(variable=variable).count(),
    }


def correct_source_unit(request, collection_pk, variable_pk):
    collection = get_org_object_or_404(request, Collection.objects.select_related("catalog"), pk=collection_pk)
    variable = get_org_object_or_404(
        request,
        Variable.objects.select_related("source_unit", "unit"),
        pk=variable_pk,
        collection=collection,
    )
    if not request.user.has_perm("georivacore.change_collection"):
        raise PermissionDenied

    edit_url = reverse("collection:edit", args=[collection.pk])
    form = CorrectSourceUnitForm(request.POST or None, variable=variable)
    if request.method == "POST" and form.is_valid():
        before = variable.source_unit.symbol
        with transaction.atomic():
            variable.correct_source_unit(form.cleaned_data["source_unit"])
            record(
                SourceUnitCorrected,
                variable=variable,
                collection=collection,
                catalog_slug=collection.catalog.slug,
                collection_slug=collection.slug,
                variable_slug=variable.slug,
                from_unit=before,
                to_unit=variable.source_unit.symbol,
            )
        messages.success(
            request,
            _("%(variable)s's source unit is now %(unit)s. No data was converted.")
            % {"variable": variable.name, "unit": variable.source_unit},
        )
        return redirect(edit_url)

    return render(
        request,
        "core/correct_source_unit.html",
        {
            "collection": collection,
            "variable": variable,
            "form": form,
            "reinterpreted": reinterpreted(variable),
            "cancel_url": edit_url,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": _("Home")},
                {"url": edit_url, "label": collection.name},
                {"url": None, "label": _("Correct source unit — %(variable)s") % {"variable": variable.name}},
            ],
            "header_title": _("Correct source unit"),
        },
    )
