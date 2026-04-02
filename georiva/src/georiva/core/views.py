from django.contrib import messages
from django.core.paginator import InvalidPage
from django.db.models import Count, OuterRef, Q, Subquery, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.urls import reverse_lazy
from django.utils.translation import gettext as _
from django.utils.translation import gettext_lazy as _
from wagtail.admin import messages
from wagtail.admin.paginator import WagtailPaginator
from wagtail.admin.ui.tables import ButtonsColumnMixin, TitleColumn, Table, BooleanColumn
from wagtail.admin.widgets import ListingButton, HeaderButton, ButtonWithDropdown

from georiva.core.models import Catalog
from georiva.core.models import Collection, Item
from georiva.ingestion.models import IngestionLog
from georiva.sources.models import LoaderRun
from .table import LinkColumnWithIcon
from .viewsets import CatalogViewSet, CollectionViewSet


def get_collection_items_url(collection):
    return reverse("collection_items_list", args=[collection.pk])


def catalog_index(request):
    catalogs = Catalog.objects.all()
    
    catalog_viewset = CatalogViewSet()
    collection_viewset = CollectionViewSet()
    
    data = []
    
    class CollectionButtonsColumn(ButtonsColumnMixin, TitleColumn):
        def get_buttons(self, instance, parent_context):
            more_buttons = []
            buttons = []
            
            edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
            delete_url = reverse(collection_viewset.get_url_name("delete"), kwargs={"pk": instance.pk})
            zarr_store_url = reverse("zarr_collection_detail", args=[instance.pk])
            
            more_buttons.append(
                ListingButton(
                    _("Edit"),
                    url=edit_url,
                    icon_name="edit",
                    attrs={
                        "aria-label": _("Edit '%(title)s'") % {"title": str(instance)}
                    },
                    priority=10,
                )
            )
            
            more_buttons.append(
                ListingButton(
                    _("Delete"),
                    url=delete_url,
                    icon_name="bin",
                    attrs={
                        "aria-label": _("Delete '%(title)s'") % {"title": str(instance)}
                    },
                    priority=20,
                )
            )
            
            more_buttons.append(
                ListingButton(
                    _("Zarr Store"),
                    url=zarr_store_url,
                    icon_name="resubmit",
                    attrs={
                        "title": _("Queue Zarr sync for all COG assets in this collection")
                    },
                    priority=30,
                )
            )
            
            if more_buttons:
                buttons.append(
                    ButtonWithDropdown(
                        buttons=more_buttons,
                        icon_name="dots-horizontal",
                        attrs={
                            "aria-label": _("More options for '%(title)s'")
                                          % {"title": str(instance)},
                        },
                    )
                )
            
            return buttons
    
    def get_url(instance):
        edit_url = reverse(collection_viewset.get_url_name("edit"), kwargs={"pk": instance.pk})
        return edit_url
    
    for catalog in catalogs:
        columns = [
            CollectionButtonsColumn("name", label=_("Collection "), get_url=get_url),
            BooleanColumn("is_active", label=_("Active")),
            LinkColumnWithIcon("Items", label=_("Items"), icon_name="view", get_url=get_collection_items_url),
        ]
        
        collections = catalog.collections.all()
        
        data.append({
            "catalog": catalog,
            "edit_url": reverse(catalog_viewset.get_url_name("edit"), kwargs={"pk": catalog.pk}),
            "add_collection_url": reverse(collection_viewset.get_url_name("add")),
            "has_collections": collections.exists(),
            "collections_table": Table(columns, collections),
        })
    
    catalog_add_url = reverse(catalog_viewset.get_url_name("add"))
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Catalogs")},
        ],
        "header_buttons": [
            HeaderButton(
                label=_('Add Catalog'),
                url=catalog_add_url,
                icon_name="plus",
            ),
        ],
        "catalogs": data,
        "catalog_add_url": catalog_add_url,
    }
    
    return render(request, 'core/catalog_list.html', context)


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
        .select_related("ingestion_log")
        .order_by("-time")
    )
    
    if active_variable:
        # Only items that have at least one asset for this variable
        items = items.filter(assets__variable=active_variable).distinct()
    
    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------
    try:
        page_num = int(request.GET.get("p", 0))
    except ValueError:
        page_num = 0
    
    paginator = WagtailPaginator(items, 25)
    
    try:
        page_obj = paginator.page(page_num + 1)
    except InvalidPage:
        page_obj = paginator.page(1)
    
    elided_page_range = paginator.get_elided_page_range(page_obj.number)
    
    # ------------------------------------------------------------------
    # Automation section (only rendered if loader_profile is set)
    # ------------------------------------------------------------------
    loader_profile = None
    recent_runs = []
    ingestion_summary = {}
    
    if collection.loader_profile_id:
        loader_profile = collection.loader_profile
        
        recent_runs = list(
            LoaderRun.objects.filter(collection=collection)
            .order_by("-started_at")[:5]
        )
        
        logs_qs = IngestionLog.objects.filter(
            collection_slug=collection.slug,
            catalog_slug=collection.catalog.slug,
        )
        ingestion_summary = logs_qs.aggregate(
            total=Count("id"),
            completed=Count("id", filter=Q(status=IngestionLog.Status.COMPLETED)),
            failed=Count("id", filter=Q(status=IngestionLog.Status.FAILED)),
            pending=Count("id", filter=Q(status=IngestionLog.Status.PENDING)),
            processing=Count("id", filter=Q(status=IngestionLog.Status.PROCESSING)),
            total_items_created=Sum("items_created"),
            total_assets_created=Sum("assets_created"),
        )
    
    # ------------------------------------------------------------------
    # Context
    # ------------------------------------------------------------------
    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog_index"), "label": _("Catalogs")},
            {"url": "", "label": collection.name},
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
        "loader_profile": loader_profile,
        "recent_runs": recent_runs,
        "ingestion_summary": ingestion_summary,
    }
    
    return render(request, "core/collection_items.html", context)
