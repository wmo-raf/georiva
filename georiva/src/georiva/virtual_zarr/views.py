"""Thin renderers over the coverage service (ADR-0019: monitoring lives
where the objects live — this page hangs off the collection, not a menu)."""

from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_POST
from wagtail.admin.auth import permission_denied
from wagtail.permissions import ModelPermissionPolicy

from georiva.core.models import Collection, Variable
from georiva.organisations.access import get_org_object_or_404

from .coverage import collection_coverage, variable_detail
from .models import VirtualZarrManifest


def _can_queue_rebuild(user) -> bool:
    """The action gate, shared by the POST view and the buttons' visibility."""
    return ModelPermissionPolicy(VirtualZarrManifest).user_has_permission(
        user, "change"
    )


def collection_virtual_zarr(request, collection_pk):
    """The per-collection Virtual Zarr tab: one coverage row per Variable."""
    collection = get_org_object_or_404(
        request,
        Collection.objects.select_related("catalog"),
        pk=collection_pk,
    )

    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog:index"), "label": _("Catalogs")},
            {
                "url": reverse("collection_items_list", args=[collection.pk]),
                "label": collection.name,
            },
            {"url": None, "label": _("Virtual Zarr")},
        ],
        "header_title": _("Virtual Zarr — %s") % collection.name,
        "header_icon": "table",
        "collection": collection,
        "reports": collection_coverage(collection),
        "can_queue_rebuild": _can_queue_rebuild(request.user),
    }
    return render(request, "virtual_zarr/collection_virtual_zarr.html", context)


def variable_virtual_zarr(request, variable_pk):
    """The per-variable drill-down: what exactly is wrong with this repo."""
    variable = get_org_object_or_404(
        request,
        Variable.objects.select_related("collection", "collection__catalog"),
        pk=variable_pk,
    )
    collection = variable.collection
    detail = variable_detail(variable)

    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog:index"), "label": _("Catalogs")},
            {
                "url": reverse("collection_items_list", args=[collection.pk]),
                "label": collection.name,
            },
            {
                "url": reverse("collection_virtual_zarr", args=[collection.pk]),
                "label": _("Virtual Zarr"),
            },
            {"url": None, "label": variable.slug},
        ],
        "header_title": _("Virtual Zarr — %s") % variable.slug,
        "header_icon": "table",
        "collection": collection,
        "variable": variable,
        "detail": detail,
        "report": detail.coverage,
        "can_queue_rebuild": _can_queue_rebuild(request.user),
    }
    return render(request, "virtual_zarr/variable_virtual_zarr.html", context)


@require_POST
def variable_virtual_zarr_queue_rebuild(request, variable_pk):
    """
    Queue a rebuild for one variable's manifest (issue #346).

    Flips the manifest to PENDING and stops there — the 5-minute sweep
    dispatches the actual build through its normal locking, so this action
    cannot race an in-flight build.  A manifest actively BUILDING under a
    fresh lock is refused with an explanation instead of being yanked out
    from under its worker.
    """
    variable = get_org_object_or_404(
        request,
        Variable.objects.select_related("collection", "collection__catalog"),
        pk=variable_pk,
    )
    if not _can_queue_rebuild(request.user):
        return permission_denied(request)

    # A variable ingested before the manifest signal existed (or with no COG
    # yet) may have no row — create one; the default status is PENDING.
    manifest, _created = VirtualZarrManifest.objects.get_or_create(
        variable=variable,
        defaults={"repo_path": VirtualZarrManifest.make_repo_path(variable)},
    )

    if manifest.queue_rebuild():
        messages.success(
            request,
            _(
                "Rebuild queued for '%s' — the sweep dispatches it within "
                "5 minutes."
            ) % variable.slug,
        )
    else:
        messages.warning(
            request,
            _(
                "'%s' is already building under a fresh lock — leave the "
                "worker to finish. If it dies, the sweep resets and retries "
                "it automatically."
            ) % variable.slug,
        )

    next_url = request.POST.get("next", "")
    if next_url and url_has_allowed_host_and_scheme(
            next_url, allowed_hosts={request.get_host()},
    ):
        return redirect(next_url)
    return redirect(reverse("variable_virtual_zarr", args=[variable.pk]))
