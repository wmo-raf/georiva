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
    from georiva.ingestion.models import FileIngestion
    from georiva.sources.models import DataFeedCollectionLink
    
    collections = list(
        Collection.objects
        .select_related("catalog")
        .filter(is_active=True)
        .order_by("catalog__slug", "sort_order", "name")
    )
    
    automated_collection_ids = set(
        DataFeedCollectionLink.objects.values_list('collection_id', flat=True)
    )
    
    today = timezone.now().date()
    thirty_days_ago = today - timedelta(days=29)
    
    # Sparkline data: one row per (FileIngestion × Collection) pair via M2M join.
    recent_logs = (
        FileIngestion.objects
        .filter(created_at__date__gte=thirty_days_ago)
        .filter(collections__isnull=False)
        .values("collections", "status", "created_at")
        .order_by("created_at")
    )
    
    logs_by_collection = defaultdict(list)
    for log in recent_logs:
        logs_by_collection[log["collections"]].append(log)
    
    # Latest FileIngestion per collection (for last_run_at / last_run_status).
    collection_ids = [c.pk for c in collections]
    latest_fi_by_collection = {}
    for fi in (
            FileIngestion.objects
                    .filter(collections__in=collection_ids)
                    .values("collections", "status", "created_at")
                    .order_by("collections", "-created_at")
                    .distinct("collections")
    ):
        latest_fi_by_collection[fi["collections"]] = fi
    
    result = []
    
    for collection in collections:
        is_automated = collection.pk in automated_collection_ids
        logs = logs_by_collection.get(collection.pk, [])
        
        sparkline = _build_sparkline(logs, today)
        
        last_run_at = None
        last_run_status = None
        
        latest_fi = latest_fi_by_collection.get(collection.pk)
        if latest_fi:
            last_run_at = latest_fi["created_at"].isoformat()
            last_run_status = latest_fi["status"]
        
        status = _derive_status(sparkline)
        
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


def collection_ingestion_jobs_api(request, collection_id):
    """
    Returns FileIngestionJob history for one collection with live progress from Redis cache.

    All ingestion jobs are system-triggered (user=None) so the task_ferry user-scoped
    API cannot be used. Active jobs are sorted first; the response includes has_active
    so the frontend knows whether to keep polling.
    """
    from django.db.models import Case, IntegerField, When
    
    from georiva.core.models import Collection
    from georiva.ingestion.models import FileIngestionJob
    
    try:
        collection = Collection.objects.select_related("catalog").get(pk=collection_id)
    except Collection.DoesNotExist:
        raise Http404
    
    active_states = ("pending", "started")
    
    jobs = (
        FileIngestionJob.objects
        .filter(file_ingestion__collections=collection)
        .annotate(
            _active=Case(
                When(state__in=active_states, then=0),
                default=1,
                output_field=IntegerField(),
            )
        )
        .order_by("_active", "-created_at")
        .select_related("file_ingestion")[:50]
    )
    
    result = []
    has_active = False
    for job in jobs:
        state = job.get_cached_state()
        if state in active_states:
            has_active = True
        fi = job.file_ingestion
        result.append({
            "id": job.pk,
            "state": state,
            "progress_percentage": job.get_cached_progress_percentage(),
            "progress_state": job.get_cached_progress_state(),
            "file_path": job.file_path,
            "bucket": job.bucket,
            "items_created": fi.items_created if fi else job.items_created,
            "assets_created": fi.assets_created if fi else job.assets_created,
            "error": job.error or "",
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
        })
    
    return JsonResponse({"jobs": result, "has_active": has_active})


def collection_ingestion_logs_api(request, collection_id):
    """
    Returns FileIngestion entries for one collection.
    Works for both manual and automated collections.
    """
    from georiva.core.models import Collection
    from georiva.ingestion.models import FileIngestion
    
    try:
        collection = Collection.objects.select_related("catalog").get(pk=collection_id)
    except Collection.DoesNotExist:
        raise Http404
    
    logs = (
        FileIngestion.objects
        .filter(collections=collection)
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


def collection_fetch_runs_api(request, collection_id):
    """
    Returns FetchRun history for all DataFeeds linked to one collection.
    Used by CollectionDrawer "Loader Runs" tab for automated collections.
    """
    from georiva.core.models import Collection
    from georiva.sources.models import DataFeedCollectionLink, FetchRun
    
    try:
        Collection.objects.get(pk=collection_id)
    except Collection.DoesNotExist:
        raise Http404
    
    feed_ids = list(
        DataFeedCollectionLink.objects
        .filter(collection_id=collection_id)
        .values_list("data_feed_id", flat=True)
    )
    
    runs = (
        FetchRun.objects
        .filter(data_feed_id__in=feed_ids)
        .select_related("data_feed")
        .order_by("-started_at")[:100]
    )
    
    result = []
    for run in runs:
        duration = None
        if run.finished_at and run.started_at:
            duration = (run.finished_at - run.started_at).total_seconds()
        
        errors = [run.error_message] if run.error_message else []
        
        result.append({
            "id": run.pk,
            "status": run.status,
            "started_at": run.started_at.isoformat(),
            "duration_seconds": duration,
            "run_time": None,
            "data_feed_name": run.data_feed.name,
            "files_fetched": run.files_fetched,
            "files_skipped": run.files_skipped,
            "files_failed": run.files_failed,
            "bytes_transferred": run.bytes_transferred,
            "errors": errors,
        })
    
    return JsonResponse({"fetch_runs": result})


def upload_session_status_api(request, session_id):
    """Returns {id, status, files} for a single UploadSession."""
    from georiva.ingestion.models import UploadSession
    
    try:
        session = UploadSession.objects.prefetch_related('uploaded_files').get(pk=session_id)
    except UploadSession.DoesNotExist:
        raise Http404
    
    return JsonResponse({
        "id": session.pk,
        "status": session.status,
        "files": [
            {
                "id": uf.pk,
                "status": uf.status,
                "file_path": uf.file_path,
                "error": uf.error,
            }
            for uf in session.uploaded_files.all()
        ],
    })


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


def _derive_status(sparkline):
    for entry in reversed(sparkline):
        if entry["status"] == "failed":
            return "failed"
        if entry["status"] == "success":
            return "ok"
    return "empty"
