from collections import defaultdict

from django.core.paginator import InvalidPage
from django.db.models import Count, OuterRef, Subquery
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin import messages
from wagtail.admin.paginator import WagtailPaginator
from wagtail.admin.ui.tables import ButtonsColumnMixin, TitleColumn, Table, BooleanColumn
from wagtail.admin.views.generic import IndexView
from wagtail.admin.widgets import Button, ButtonWithDropdown

from georiva.core.models import Catalog
from georiva.core.models import Collection, Item
from .table import LinkColumnWithIcon


def get_collection_items_url(collection):
    return reverse("collection_items_list", args=[collection.pk])


def _build_collection_columns(collection_viewset, perms):
    """Columns for the per-catalog collections table rendered inside each
    accordion panel. Affordances the user cannot use are not rendered —
    ``perms`` carries the booleans computed from the viewset permission
    policies (the same objects that enforce server-side)."""

    class CollectionButtonsColumn(ButtonsColumnMixin, TitleColumn):
        def get_buttons(self, instance, parent_context):
            more_buttons = []
            if perms["can_change_collection"]:
                edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
                more_buttons.append(Button(
                    _("Edit"),
                    url=edit_url,
                    icon_name="edit",
                    attrs={"aria-label": _("Edit '%(title)s'") % {"title": str(instance)}},
                    priority=10,
                ))
            if perms["can_delete_collection"]:
                delete_url = reverse(collection_viewset.get_url_name("delete"), kwargs={"pk": instance.pk})
                more_buttons.append(Button(
                    _("Delete"),
                    url=delete_url,
                    icon_name="bin",
                    attrs={"aria-label": _("Delete '%(title)s'") % {"title": str(instance)}},
                    priority=20,
                ))

            if not more_buttons:
                return []

            return [
                ButtonWithDropdown(
                    buttons=more_buttons,
                    icon_name="dots-horizontal",
                    attrs={
                        "aria-label": _("More options for '%(title)s'") % {"title": str(instance)},
                    },
                )
            ]

    def get_url(instance):
        # View-only users get a useful destination instead of the gated edit form.
        if perms["can_change_collection"]:
            return reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
        return get_collection_items_url(instance)

    return [
        CollectionButtonsColumn("name", label=_("Collection"), get_url=get_url),
        BooleanColumn("is_active", label=_("Active")),
        LinkColumnWithIcon("Items", label=_("Items"), icon_name="view", get_url=get_collection_items_url),
    ]


class CatalogIndexView(IndexView):
    """Catalog listing rendered as an accordion of catalog panels, each
    containing its collections. Built on Wagtail's generic IndexView so the
    admin header search (AJAX results swap) and server-side pagination work
    out of the box. Search matches catalog names and collection names — see
    Catalog.search_fields."""
    
    model = Catalog
    paginate_by = 20
    page_title = _("Catalogs")
    
    def _build_catalog_panels(self, catalogs):
        """For the catalogs on the current page, build the render context for
        each accordion panel. Issues at most two queries (the page of catalogs
        plus one batched query for all their collections) regardless of how
        many catalogs are on the page — no N+1."""
        # Local import to avoid a circular import (viewsets imports this view).
        from .viewsets import CatalogViewSet, CollectionViewSet
        
        catalog_viewset = CatalogViewSet()
        collection_viewset = CollectionViewSet()

        # Computed once per request from the same permission policies that
        # enforce server-side, so visibility and enforcement cannot drift.
        user = self.request.user
        catalog_policy = catalog_viewset.permission_policy
        collection_policy = collection_viewset.permission_policy
        perms = {
            "can_change_catalog": catalog_policy.user_has_permission(user, "change"),
            "can_delete_catalog": catalog_policy.user_has_permission(user, "delete"),
            "can_add_collection": collection_policy.user_has_permission(user, "add"),
            "can_change_collection": collection_policy.user_has_permission(user, "change"),
            "can_delete_collection": collection_policy.user_has_permission(user, "delete"),
        }

        columns = _build_collection_columns(collection_viewset, perms)
        add_collection_url = (
            reverse(collection_viewset.get_url_name("add"))
            if perms["can_add_collection"] else None
        )
        
        ids = [c.pk for c in catalogs]
        cols_by_cat = defaultdict(list)
        if ids:
            # select_related("catalog") because Collection.__str__ (used in the
            # row buttons' aria-labels) dereferences self.catalog — without it
            # rendering the tables would trigger one query per collection.
            collections = (
                Collection.objects
                .filter(catalog_id__in=ids)
                .select_related("catalog")
                .order_by("name")
            )
            for col in collections:
                cols_by_cat[col.catalog_id].append(col)
        
        panels = []
        for catalog in catalogs:
            collections = cols_by_cat.get(catalog.pk, [])
            active_count = sum(1 for c in collections if c.is_active)
            panels.append({
                "catalog": catalog,
                "edit_url": (
                    reverse(catalog_viewset.get_url_name("edit"), kwargs={"pk": catalog.pk})
                    if perms["can_change_catalog"] else None
                ),
                "delete_url": (
                    reverse(catalog_viewset.get_url_name("delete"), kwargs={"pk": catalog.pk})
                    if perms["can_delete_catalog"] else None
                ),
                "add_collection_url": add_collection_url,
                "collection_count": len(collections),
                "active_count": active_count,
                "has_collections": bool(collections),
                "collections_table": Table(columns, collections),
            })
        return panels
    
    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context["catalog_panels"] = self._build_catalog_panels(list(context["object_list"]))
        return context


def collection_items_list(request, collection_pk):
    collection = get_object_or_404(
        Collection.objects.select_related("catalog", "catalog__boundary"),
        pk=collection_pk,
    )
    
    # ------------------------------------------------------------------
    # POST actions
    # ------------------------------------------------------------------
    if request.method == "POST":
        action = request.POST.get("action")
        
        if action == "trigger_ingestion":
            # TODO: wire up real Celery task, e.g.:
            # from georiva.ingestion.tasks import run_loader_for_collection
            # run_loader_for_collection.delay(collection.pk)
            messages.success(request, _("Ingestion queued (placeholder)."))
            return redirect(request.path)
    
    # ------------------------------------------------------------------
    # Variables
    # ------------------------------------------------------------------
    variable_list = list(
        collection.variables.filter(is_active=True).select_related("palette")
    )
    
    # ------------------------------------------------------------------
    # Active variable tab
    # ------------------------------------------------------------------
    active_var_slug = request.GET.get("var", "")
    active_variable = None
    if active_var_slug:
        active_variable = next(
            (v for v in variable_list if v.slug == active_var_slug), None
        )
        # Unrecognised slug — fall back to "All"
        if not active_variable:
            active_var_slug = ""
    
    # ------------------------------------------------------------------
    # Items queryset
    # Use a Subquery for asset_count rather than annotate(Count(...)).
    # annotate + COUNT triggers GROUP BY which conflicts with TimescaleModel's
    # extra fields (created, modified) on PostgreSQL.
    # ------------------------------------------------------------------
    from georiva.core.models import Asset
    
    asset_count_sq = (
        Asset.objects.filter(item=OuterRef("pk"))
        .order_by()
        .values("item")
        .annotate(c=Count("pk"))
        .values("c")
    )
    
    items = (
        Item.objects.filter(collection=collection)
        .annotate(asset_count=Subquery(asset_count_sq))
        .order_by("-time")
    )
    
    if active_variable:
        # Only items that have at least one asset for this variable
        items = items.filter(assets__variable=active_variable).distinct()
    
    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------
    try:
        page_num = int(request.GET.get("p", 1))
    except ValueError:
        page_num = 0
    
    paginator = WagtailPaginator(items, 25)
    
    try:
        page_obj = paginator.page(page_num)
    except InvalidPage:
        page_obj = paginator.page(1)
    
    elided_page_range = paginator.get_elided_page_range(page_obj.number)
    
    # ------------------------------------------------------------------
    # Attach ingestion_log to each item on this page.
    # Bulk lookup by source_file (convention: "{bucket}:{file_path}") so
    # every item — including GRIB/NetCDF items that are not the last one
    # written — gets the correct status badge.
    # ------------------------------------------------------------------
    from django.db.models import F, Value
    from django.db.models.functions import Concat
    from georiva.ingestion.models import FileIngestion
    
    source_files = {item.source_file for item in page_obj.object_list if item.source_file}
    fi_by_source_file = {}
    if source_files:
        for fi in (
                FileIngestion.objects
                        .annotate(_sf=Concat(F("bucket"), Value(":"), F("file_path")))
                        .filter(_sf__in=source_files)
                        .order_by("-created_at")
        ):
            key = f"{fi.bucket}:{fi.file_path}"
            if key not in fi_by_source_file:
                fi_by_source_file[key] = fi
    
    for item in page_obj.object_list:
        item.ingestion_log = fi_by_source_file.get(item.source_file)
    
    # ------------------------------------------------------------------
    # Context
    # ------------------------------------------------------------------
    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog:index"), "label": _("Catalogs")},
            {"url": None, "label": collection.name},
        ],
        "collection": collection,
        "catalog": collection.catalog,
        "variable_list": variable_list,
        "variable_count": len(variable_list),
        "active_var_slug": active_var_slug,
        "active_variable": active_variable,
        "page_obj": page_obj,
        "paginator": paginator,
        "elided_page_range": elided_page_range,
    }
    
    return render(request, "core/collection_items.html", context)


def plugin_list(request):
    """Admin page listing all installed GeoRiva plugins and their metadata."""
    from .plugins import get_installed_plugins
    
    context = {
        "header_title": _("Installed Plugins"),
        "header_icon": "puzzle-piece",
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": None, "label": _("Plugins")},
        ],
        "plugins": get_installed_plugins(),
    }
    return render(request, "core/plugin_list.html", context)


def add_data_select(request):
    """The Add Data front door: route to the setup wizard matching how the data arrives."""
    return render(request, "core/add_data.html", {
        # Rendered by the slim header via wagtailadmin/generic/base.html.
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": None, "label": _("Add Data")},
        ],
        "header_title": _("Add Data"),
        "header_icon": "plus",
        "automated_url": reverse("data_feed_add_select"),
        "upload_url": reverse("upload_wizard_step1"),
    })
