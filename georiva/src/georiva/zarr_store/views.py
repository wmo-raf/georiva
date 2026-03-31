import csv

from django.core.paginator import InvalidPage, Paginator
from django.db.models import Count, Max, Min, Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.shortcuts import render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext_lazy as _
from wagtail.admin import messages

from georiva.core.models import Collection
from .models import ZarrSyncLog
from .tasks import zarr_sync_store
from .utils import rebuild_zarr_for_collection

PAGE_SIZE = 50


def zarr_rebuild_collection_view(request, pk):
    """Admin action: queue Zarr sync for all COG assets in a collection."""
    
    collection = get_object_or_404(Collection, pk=pk)
    dry_run = request.GET.get('dry_run') == '1'
    
    count = rebuild_zarr_for_collection(collection, dry_run=dry_run)
    
    if dry_run:
        messages.info(
            request,
            _(f"Dry run: {count} Zarr sync record(s) would be queued for "
              f"{collection.catalog.slug}/{collection.slug}."),
        )
    else:
        messages.success(
            request,
            _(f"Queued {count} Zarr sync record(s) for "
              f"{collection.catalog.slug}/{collection.slug}."),
        )
    
    return redirect(request.META.get('HTTP_REFERER', reverse('wagtailadmin_home')))
    
    # =============================================================================
    # Zarr collection detail view
    # =============================================================================
    
    # =============================================================================
    # Zarr collection detail view
    # =============================================================================


def zarr_collection_detail_view(request, pk):
    """
    Custom Wagtail admin view showing Zarr store state for a collection.
 
    Displays:
      - Per-store summary (one row per variable): record counts by status,
        time range of completed records, last completed timestamp.
      - Record-level table filtered by status query param, paginated at
        PAGE_SIZE rows per page.
 
    Actions:
      - Rebuild Zarr Store  — queues missing ZarrSyncLog records
      - Reset failed        — resets permanently-failed records for retry
      - Export CSV          — exports the full filtered record set (unpaginated)
    """
    collection = get_object_or_404(
        Collection.objects.select_related('catalog'),
        pk=pk,
    )
    
    # -------------------------------------------------------------------------
    # Actions
    # -------------------------------------------------------------------------
    
    if request.method == 'POST':
        action = request.POST.get('action')
        
        if action == 'rebuild':
            dry_run = request.POST.get('dry_run') == '1'
            count = rebuild_zarr_for_collection(collection, dry_run=dry_run)
            if dry_run:
                messages.info(
                    request,
                    _(f"Dry run: {count} record(s) would be queued for "
                      f"{collection.catalog.slug}/{collection.slug}."),
                )
            else:
                messages.success(
                    request,
                    _(f"Queued {count} record(s) for "
                      f"{collection.catalog.slug}/{collection.slug}."),
                )
        
        elif action == 'reset_failed':
            # Reset permanently-failed records (retry_count >= MAX_RETRIES)
            # back to PENDING so the sweep will re-dispatch them.
            reset_count = (
                ZarrSyncLog.objects
                .filter(
                    item__collection=collection,
                    status=ZarrSyncLog.Status.FAILED,
                    retry_count__gte=ZarrSyncLog.MAX_RETRIES,
                )
                .update(
                    status=ZarrSyncLog.Status.PENDING,
                    retry_count=0,
                    locked_at=None,
                    locked_by='',
                    error='',
                )
            )
            # Re-dispatch affected store paths
            store_paths = list(
                ZarrSyncLog.objects
                .filter(item__collection=collection, status=ZarrSyncLog.Status.PENDING)
                .values_list('store_path', flat=True)
                .distinct()
            )
            for store_path in store_paths:
                zarr_sync_store.apply_async(args=[store_path], queue='georiva-ingestion')
            
            messages.success(
                request,
                _(f"Reset {reset_count} permanently-failed record(s) for retry."),
            )
        
        return redirect(request.path)
    
    # -------------------------------------------------------------------------
    # Store summary — one row per store_path
    # -------------------------------------------------------------------------
    
    store_summary = (
        ZarrSyncLog.objects
        .filter(item__collection=collection)
        .values('store_path')
        .annotate(
            total=Count('pk'),
            completed=Count('pk', filter=Q(status=ZarrSyncLog.Status.COMPLETED)),
            failed=Count('pk', filter=Q(status=ZarrSyncLog.Status.FAILED)),
            pending=Count('pk', filter=Q(status=ZarrSyncLog.Status.PENDING)),
            processing=Count('pk', filter=Q(status=ZarrSyncLog.Status.PROCESSING)),
            last_completed=Max(
                'completed_at',
                filter=Q(status=ZarrSyncLog.Status.COMPLETED),
            ),
            time_start=Min(
                'item__time',
                filter=Q(status=ZarrSyncLog.Status.COMPLETED),
            ),
            time_end=Max(
                'item__time',
                filter=Q(status=ZarrSyncLog.Status.COMPLETED),
            ),
        )
        .order_by('store_path')
    )
    
    # Whether any permanently-failed records exist (retry_count exhausted)
    has_permanently_failed = ZarrSyncLog.objects.filter(
        item__collection=collection,
        status=ZarrSyncLog.Status.FAILED,
        retry_count__gte=ZarrSyncLog.MAX_RETRIES,
    ).exists()
    
    # -------------------------------------------------------------------------
    # Record-level table — filterable by status
    # -------------------------------------------------------------------------
    
    status_filter = request.GET.get('status', '')
    records_qs = (
        ZarrSyncLog.objects
        .filter(item__collection=collection)
        .select_related('item', 'variable')
        .order_by('store_path', 'item__time')
    )
    if status_filter and status_filter in ZarrSyncLog.Status.values:
        records_qs = records_qs.filter(status=status_filter)
    
    # -------------------------------------------------------------------------
    # CSV export — full filtered set, unpaginated
    # -------------------------------------------------------------------------
    
    if request.GET.get('export') == 'csv':
        return _export_csv(collection, records_qs)
    
    # -------------------------------------------------------------------------
    # Counts for filter tabs
    # -------------------------------------------------------------------------
    
    status_counts = {
        s: ZarrSyncLog.objects.filter(item__collection=collection, status=s).count()
        for s in ZarrSyncLog.Status.values
    }
    status_counts['all'] = sum(status_counts.values())
    
    # -------------------------------------------------------------------------
    # Pagination
    # -------------------------------------------------------------------------
    
    paginator = Paginator(records_qs, PAGE_SIZE)
    try:
        page = paginator.page(request.GET.get('p', 1))
    except InvalidPage:
        page = paginator.page(1)
    
    # Preserved query params for page links — template appends &p=N.
    # Keeps status filter active when navigating between pages.
    filter_params = f"status={status_filter}&" if status_filter else ""
    
    # Pre-compute the page range with ellipsis markers so the template
    # doesn't need to do numeric comparisons with |add: filters.
    # Yields integers for page links and None as an ellipsis sentinel.
    page_range = _page_range_with_ellipsis(page.number, paginator.num_pages, window=2)
    
    page_title = _(f"Zarr Store: {collection.catalog.slug} / {collection.slug}")
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("catalog_index"), "label": _("Catalogs")},
            {"url": "", "label": page_title},
        ],
        'collection': collection,
        'store_summary': store_summary,
        'page': page,
        'paginator': paginator,
        'page_range': page_range,
        'filter_params': filter_params,
        'status_filter': status_filter,
        'status_counts': status_counts,
        'has_permanently_failed': has_permanently_failed,
        'status_choices': ZarrSyncLog.Status.choices,
    }
    
    return render(request, 'zarr_store/zarr_collection_detail.html', context)


def _page_range_with_ellipsis(current: int, num_pages: int, window: int = 2):
    """
    Yield page numbers and None (ellipsis sentinel) for a pagination control.
 
    Always includes page 1 and num_pages. Shows `window` pages either side of
    current. Inserts None where there is a gap of more than one page.
 
    Example (current=6, num_pages=15, window=2):
        1 … 4 5 [6] 7 8 … 15
    """
    pages = sorted({1, num_pages} | {
        p for p in range(current - window, current + window + 1)
        if 1 <= p <= num_pages
    })
    result = []
    prev = None
    for p in pages:
        if prev is not None and p - prev > 1:
            result.append(None)  # ellipsis
        result.append(p)
        prev = p
    return result


def _export_csv(collection, records_qs) -> HttpResponse:
    """Stream the record-level queryset as a CSV download."""
    response = HttpResponse(content_type='text/csv')
    filename = f"zarr_{collection.catalog.slug}_{collection.slug}.csv"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    
    writer = csv.writer(response)
    writer.writerow([
        'store_path', 'item_time', 'variable', 'status',
        'retry_count', 'locked_by', 'completed_at', 'error',
    ])
    for record in records_qs.iterator():
        writer.writerow([
            record.store_path,
            record.item.time.isoformat() if record.item.time else '',
            record.variable.slug,
            record.status,
            record.retry_count,
            record.locked_by,
            record.completed_at.isoformat() if record.completed_at else '',
            record.error,
        ])
    return response
