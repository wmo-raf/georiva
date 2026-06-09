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
