"""Thin renderers over the coverage service (ADR-0019: monitoring lives
where the objects live — this page hangs off the collection, not a menu)."""

from django.shortcuts import render
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from georiva.core.models import Collection, Variable
from georiva.organisations.access import get_org_object_or_404

from .coverage import collection_coverage, variable_detail


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
    }
    return render(request, "virtual_zarr/variable_virtual_zarr.html", context)
