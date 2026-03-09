import logging
from collections import defaultdict
from datetime import timedelta

from django.http import JsonResponse, Http404
from django.utils import timezone

logger = logging.getLogger(__name__)


# =============================================================================
# Collection list API
# =============================================================================

def ingestion_dashboard_api(request):
    """
    Returns collection list with ingestion health data for the dashboard.
    """
    from georiva.core.models import Collection
    from georiva.ingestion.models import IngestionLog
    from georiva.sources.models import LoaderRun
    
    collections = (
        Collection.objects
        .select_related("catalog", "loader_profile")
        .filter(is_active=True)
        .order_by("catalog__slug", "sort_order", "name")
    )
    
    today = timezone.now().date()
    thirty_days_ago = today - timedelta(days=29)
    
    recent_logs = (
        IngestionLog.objects
        .filter(created_at__date__gte=thirty_days_ago)
        .values("collection_slug", "catalog_slug", "status", "created_at")
        .order_by("created_at")
    )
    
    logs_by_collection = defaultdict(list)
    for log in recent_logs:
        key = (log["catalog_slug"], log["collection_slug"])
        logs_by_collection[key].append(log)
    
    latest_loader_runs = {}
    loader_run_qs = (
        LoaderRun.objects
        .filter(
            collection__in=collections,
            collection__loader_profile__isnull=False,
        )
        .order_by("collection_id", "-started_at")
        .distinct("collection_id")
        .select_related("collection")
    )
    for run in loader_run_qs:
        latest_loader_runs[run.collection_id] = run
    
    result = []
    
    for collection in collections:
        is_automated = collection.loader_profile_id is not None
        key = (collection.catalog.slug, collection.slug)
        logs = logs_by_collection.get(key, [])
        
        sparkline = _build_sparkline(logs, today)
        
        last_run_at = None
        last_run_status = None
        
        if is_automated:
            run = latest_loader_runs.get(collection.pk)
            if run:
                last_run_at = run.started_at.isoformat()
                last_run_status = run.status
        else:
            if logs:
                latest_log = max(logs, key=lambda l: l["created_at"])
                last_run_at = latest_log["created_at"].isoformat()
                last_run_status = latest_log["status"]
        
        status = _derive_status(sparkline, last_run_status)
        
        result.append({
            "id": collection.pk,
            "slug": collection.slug,
            "name": collection.name,
            "catalog": collection.catalog.slug,
            "catalog_name": collection.catalog.name,
            "type": "automated" if is_automated else "manual",
            "is_active": collection.is_active,
            "item_count": collection.item_count,
            "last_run_at": last_run_at,
            "last_run_status": last_run_status,
            "status": status,
            "sparkline": sparkline,
        })
    
    return JsonResponse({"collections": result})


# =============================================================================
# Drawer detail APIs
# =============================================================================

def collection_loader_runs_api(request, collection_id):
    """
    Returns LoaderRun history for one collection (automated only).
    """
    from georiva.core.models import Collection
    from georiva.sources.models import LoaderRun
    
    try:
        collection = Collection.objects.get(pk=collection_id)
    except Collection.DoesNotExist:
        raise Http404
    
    runs = (
        LoaderRun.objects
        .filter(collection=collection)
        .order_by("-started_at")[:100]
    )
    
    result = []
    for run in runs:
        result.append({
            "id": run.pk,
            "status": run.status,
            "started_at": run.started_at.isoformat(),
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "duration_seconds": run.duration_seconds,
            "run_time": run.run_time.isoformat() if run.run_time else None,
            "files_requested": run.files_requested,
            "files_fetched": run.files_fetched,
            "files_skipped": run.files_skipped,
            "files_failed": run.files_failed,
            "files_queued": run.files_queued,
            "bytes_transferred": run.bytes_transferred,
            "errors": run.errors or [],
        })
    
    return JsonResponse({"runs": result})


def collection_ingestion_logs_api(request, collection_id):
    """
    Returns IngestionLog entries for one collection.
    Works for both manual and automated collections.
    """
    from georiva.core.models import Collection
    from georiva.ingestion.models import IngestionLog
    
    try:
        collection = Collection.objects.select_related("catalog").get(pk=collection_id)
    except Collection.DoesNotExist:
        raise Http404
    
    logs = (
        IngestionLog.objects
        .filter(
            catalog_slug=collection.catalog.slug,
            collection_slug=collection.slug,
        )
        .order_by("-created_at")[:200]
    )
    
    result = []
    for log in logs:
        result.append({
            "id": log.pk,
            "status": log.status,
            "file_path": log.file_path,
            "reference_time": log.reference_time.isoformat() if log.reference_time else None,
            "items_created": log.items_created,
            "assets_created": log.assets_created,
            "retry_count": log.retry_count,
            "error": log.error or "",
            "created_at": log.created_at.isoformat(),
            "completed_at": log.completed_at.isoformat() if log.completed_at else None,
        })
    
    return JsonResponse({"logs": result})


# =============================================================================
# Helpers
# =============================================================================

def _build_sparkline(logs, today):
    daily = defaultdict(lambda: {"success": 0, "failed": 0})
    
    for log in logs:
        d = log["created_at"].date()
        if log["status"] == "completed":
            daily[d]["success"] += 1
        elif log["status"] == "failed":
            daily[d]["failed"] += 1
    
    sparkline = []
    for i in range(29, -1, -1):
        d = today - timedelta(days=i)
        counts = daily.get(d)
        if not counts:
            status = "empty"
        elif counts["success"] > 0:
            status = "success"
        else:
            status = "failed"
        sparkline.append({"date": str(d), "status": status})
    
    return sparkline


def _derive_status(sparkline, last_run_status):
    if not any(s["status"] != "empty" for s in sparkline):
        return "empty"
    
    if last_run_status in ("completed", "success"):
        return "ok"
    
    if last_run_status == "failed":
        return "failed"
    
    recent = [s["status"] for s in sparkline[-3:]]
    if all(s == "empty" for s in recent):
        return "warning"
    
    return "ok"
