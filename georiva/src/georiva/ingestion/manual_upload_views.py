from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _


def manual_upload_config_list(request):
    from georiva.ingestion.models import ManualUploadConfig

    configs = ManualUploadConfig.objects.select_related("catalog").prefetch_related("variables").order_by(
        "catalog__name", "name"
    )

    return render(request, "georivaingestion/manual_upload_config_list.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Manual Uploads")},
        ],
        "add_url": reverse("upload_wizard_step1"),
        "configs": configs,
    })


def manual_upload_config_edit(request, pk):
    from django.forms import ModelForm
    from georiva.ingestion.models import ManualUploadConfig

    config = get_object_or_404(ManualUploadConfig, pk=pk)

    class EditForm(ModelForm):
        class Meta:
            model = ManualUploadConfig
            fields = ["name", "is_forecast", "valid_time_format"]

    if request.method == "POST":
        form = EditForm(request.POST, instance=config)
        if form.is_valid():
            form.save()
            messages.success(request, _("Configuration '%s' updated.") % config.name)
            return redirect("manual_upload_config_list")
    else:
        form = EditForm(instance=config)

    variables = config.variables.select_related("collection").order_by(
        "collection__name", "variable_name"
    )

    return render(request, "georivaingestion/manual_upload_config_edit.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("manual_upload_config_list"), "label": _("Manual Uploads")},
            {"url": "", "label": config.name},
        ],
        "config": config,
        "form": form,
        "variables": variables,
    })


def manual_upload_config_delete(request, pk):
    from georiva.ingestion.models import ManualUploadConfig

    config = get_object_or_404(ManualUploadConfig, pk=pk)

    if request.method == "POST":
        name = config.name
        config.delete()
        messages.success(request, _("Configuration '%s' deleted.") % name)
        return redirect("manual_upload_config_list")

    return render(request, "georivaingestion/manual_upload_config_confirm_delete.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("manual_upload_config_list"), "label": _("Manual Uploads")},
            {"url": "", "label": _("Delete")},
        ],
        "config": config,
    })


# =============================================================================
# Variable editor — data managers tune variables on manually-provisioned
# Collections from here (the raw Collection form is permission-gated).
# Operator-is-truth: edits are authoritative; nothing resets them.
# =============================================================================

def _core_variable_for(mcv):
    """The core Variable a ManualUploadConfigVariable routes to (by collection + slug)."""
    from django.utils.text import slugify

    from georiva.core.models import Variable

    return Variable.objects.filter(
        collection=mcv.collection, slug=slugify(mcv.variable_name)
    ).first()


def _variable_breadcrumbs(config, leaf):
    return [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse("manual_upload_config_list"), "label": _("Manual Uploads")},
        {"url": reverse("manual_upload_config_edit", args=[config.pk]), "label": config.name},
        {"url": "", "label": leaf},
    ]


def manual_upload_variable_edit(request, pk, var_pk):
    from django.forms import ModelForm

    from georiva.core.models import Variable
    from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

    config = get_object_or_404(ManualUploadConfig, pk=pk)
    mcv = get_object_or_404(ManualUploadConfigVariable, pk=var_pk, config=config)
    variable = _core_variable_for(mcv)
    if variable is None:
        messages.error(request, _("No variable record found for '%s'.") % mcv.variable_name)
        return redirect("manual_upload_config_edit", pk=config.pk)

    class VariableEditForm(ModelForm):
        class Meta:
            model = Variable
            fields = ["name", "unit", "value_min", "value_max", "palette"]

        def clean(self):
            cleaned = super().clean()
            value_min, value_max = cleaned.get("value_min"), cleaned.get("value_max")
            if value_min is not None and value_max is not None and value_min >= value_max:
                self.add_error("value_max", _("Maximum must be greater than minimum."))
            return cleaned

    if request.method == "POST":
        form = VariableEditForm(request.POST, instance=variable)
        if form.is_valid():
            form.save()
            # Keep the config's own display name consistent with the Variable
            mcv.long_name = variable.name
            mcv.save(update_fields=["long_name"])
            messages.success(request, _("Variable '%s' updated.") % variable.name)
            return redirect("manual_upload_config_edit", pk=config.pk)
    else:
        form = VariableEditForm(instance=variable)

    return render(request, "georivaingestion/manual_upload_variable_edit.html", {
        "breadcrumbs_items": _variable_breadcrumbs(config, mcv.variable_name),
        "config": config,
        "mcv": mcv,
        "variable": variable,
        "form": form,
    })


def manual_upload_variable_add(request, pk):
    from django import forms
    from django.utils.text import slugify

    from georiva.core.models import Unit, Variable
    from georiva.core.provisioning import passthrough_sources, resolve_unit
    from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

    config = get_object_or_404(ManualUploadConfig, pk=pk)
    collections = config.catalog.collections.order_by("name")

    class VariableAddForm(forms.Form):
        variable_name = forms.CharField(
            label=_("Variable name in files"),
            help_text=_("The name this variable has inside uploaded files, e.g. 2t or band_1."),
        )
        long_name = forms.CharField(label=_("Display name"), required=False)
        collection = forms.ModelChoiceField(label=_("Collection"), queryset=collections)
        unit = forms.ModelChoiceField(
            label=_("Unit"), queryset=Unit.objects.order_by("name"), required=False,
        )
        new_unit_symbol = forms.CharField(
            label=_("Or create a unit"), required=False,
            help_text=_("A unit symbol such as hPa or mm — used when the unit is not in the list."),
        )
        value_min = forms.FloatField(label=_("Minimum value"))
        value_max = forms.FloatField(label=_("Maximum value"))

        def clean(self):
            cleaned = super().clean()
            if not cleaned.get("unit") and not (cleaned.get("new_unit_symbol") or "").strip():
                self.add_error("unit", _("Choose a unit or enter a symbol to create one."))
            value_min, value_max = cleaned.get("value_min"), cleaned.get("value_max")
            if value_min is not None and value_max is not None and value_min >= value_max:
                self.add_error("value_max", _("Maximum must be greater than minimum."))
            variable_name = (cleaned.get("variable_name") or "").strip()
            collection = cleaned.get("collection")
            if variable_name and collection:
                slug = slugify(variable_name)
                if collection.variables.filter(slug=slug).exists():
                    self.add_error(
                        "variable_name",
                        _("'%s' already exists in this collection.") % variable_name,
                    )
            return cleaned

    if request.method == "POST":
        form = VariableAddForm(request.POST)
        if form.is_valid():
            data = form.cleaned_data
            variable_name = data["variable_name"].strip()
            collection = data["collection"]
            unit = data["unit"] or resolve_unit(data["new_unit_symbol"].strip())

            Variable.objects.create(
                collection=collection,
                slug=slugify(variable_name),
                name=data["long_name"] or variable_name,
                transform_type=Variable.TransformType.PASSTHROUGH,
                unit=unit,
                value_min=data["value_min"],
                value_max=data["value_max"],
                sources=passthrough_sources(variable_name),
            )
            ManualUploadConfigVariable.objects.create(
                config=config,
                collection=collection,
                variable_name=variable_name,
                long_name=data["long_name"],
                units=unit.symbol[:50],
            )
            messages.success(request, _("Variable '%s' added.") % variable_name)
            return redirect("manual_upload_config_edit", pk=config.pk)
    else:
        form = VariableAddForm()

    return render(request, "georivaingestion/manual_upload_variable_add.html", {
        "breadcrumbs_items": _variable_breadcrumbs(config, _("Add variable")),
        "config": config,
        "form": form,
    })


def manual_upload_variable_remove(request, pk, var_pk):
    from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

    config = get_object_or_404(ManualUploadConfig, pk=pk)
    mcv = get_object_or_404(ManualUploadConfigVariable, pk=var_pk, config=config)

    if request.method == "POST":
        name = mcv.variable_name
        variable = _core_variable_for(mcv)
        if variable is not None:
            variable.delete()
        mcv.delete()
        messages.success(request, _("Variable '%s' removed.") % name)
        return redirect("manual_upload_config_edit", pk=config.pk)

    return render(request, "georivaingestion/manual_upload_variable_confirm_remove.html", {
        "breadcrumbs_items": _variable_breadcrumbs(config, _("Remove variable")),
        "config": config,
        "mcv": mcv,
    })
