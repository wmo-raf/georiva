"""
GeoRiva MinIO Webhook View

Receives S3 event notifications from georiva-incoming and georiva-sources
buckets, registers files in the IngestionLog, and queues Celery tasks.

The webhook does the following:
    - Validates the catalog exists
    - Registers the file in IngestionLog
    - Queues a Celery task

Collection resolution happens in the ingestion service
"""

import json
import logging
from functools import lru_cache
from urllib.parse import unquote

from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from georiva.core.filename import validate_path
from georiva.core.storage import BucketType, get_bucket_config
from georiva.ingestion.models import IngestionLog
from georiva.ingestion.tasks import process_incoming_file

logger = logging.getLogger(__name__)

MINIO_WEBHOOK_BEARER_TOKEN = getattr(settings, "MINIO_WEBHOOK_BEARER_TOKEN", None)


# =============================================================================
# Helpers
# =============================================================================

def _get_bearer_token(request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    return auth.split(" ", 1)[1].strip()


def _resolve_origin(bucket_name: str) -> str | None:
    """
    Map MinIO bucket name to BucketType.

    Only returns a value for ingest-enabled buckets (incoming, sources).
    Returns None for archive, assets, or unknown buckets.
    """
    buckets = get_bucket_config()
    ingest_buckets = {
        buckets[BucketType.INCOMING]["name"]: BucketType.INCOMING,
        buckets[BucketType.SOURCES]["name"]: BucketType.SOURCES,
    }
    
    return ingest_buckets.get(bucket_name)


@lru_cache(maxsize=64)
def _catalog_exists(catalog_slug: str) -> bool:
    """
    Cached check that a catalog exists and is active.

    Prevents queuing tasks for files in unknown catalogs.
    Cache lives in process memory — cleared on restart/deploy.
    """
    from georiva.core.models import Catalog
    return Catalog.objects.filter(slug=catalog_slug, is_active=True).exists()


# =============================================================================
# Webhook endpoint
# =============================================================================

@csrf_exempt
@require_POST
def minio_event_webhook(request):
    """
    Receive MinIO S3 event notifications.

    For each event:
    1. Resolve which bucket it came from
    2. Parse the file path for catalog/collection/reference_time
    3. Validate the catalog exists
    4. Register in IngestionLog (skip if already processing/completed)
    5. Queue a Celery task for processing
    """
    # Auth
    if MINIO_WEBHOOK_BEARER_TOKEN:
        token = _get_bearer_token(request)
        if not token or token != MINIO_WEBHOOK_BEARER_TOKEN:
            return HttpResponseForbidden("Forbidden")
    
    # Parse payload
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Invalid webhook payload: %s", e)
        return JsonResponse({"status": "error", "message": "Invalid payload"}, status=400)
    
    events = (
        payload if isinstance(payload, list)
        else payload.get("Records", [payload])
    )
    
    queued = 0
    skipped = 0
    
    for ev in events:
        bucket_name = ev.get("s3", {}).get("bucket", {}).get("name", "")
        key_raw = ev.get("s3", {}).get("object", {}).get("key", "")
        
        if not key_raw or not bucket_name:
            continue
        
        key = unquote(key_raw)
        
        # 1. Only process ingest-enabled buckets
        origin_bucket = _resolve_origin(bucket_name)
        if not origin_bucket:
            continue
        
        # 2. Parse and validate path (minimum: catalog/filename)
        try:
            meta = validate_path(key)
        except ValueError as e:
            logger.warning("Invalid path %s: %s", key, e)
            continue
        
        catalog_slug = meta['catalog']
        collection_slug = meta.get('collection')
        
        # 3. Validate catalog exists
        if not _catalog_exists(catalog_slug):
            logger.warning("Unknown catalog '%s': %s", catalog_slug, key)
            continue
        
        # 4. Register in IngestionLog — skip if already tracked
        log, created = IngestionLog.register(
            bucket=origin_bucket,
            file_path=key,
            catalog_slug=catalog_slug,
            collection_slug=collection_slug or '',
            reference_time=meta.get('reference_time'),
        )
        
        if not created and log.status in (
                IngestionLog.Status.PROCESSING,
                IngestionLog.Status.COMPLETED,
        ):
            logger.debug("Already %s: %s", log.status, key)
            skipped += 1
            continue
        
        # 5. Queue Celery task — ingestion service resolves collection
        process_incoming_file.delay(
            file_path=key,
            origin_bucket=origin_bucket,
            reference_time=(
                meta['reference_time'].isoformat()
                if meta['reference_time'] else None
            ),
        )
        queued += 1
        
        logger.info(
            "Queued: %s/%s (catalog=%s, collection=%s, ref=%s)",
            bucket_name, key,
            catalog_slug,
            collection_slug or '(to resolve)',
            meta.get('reference_time'),
        )
    
    return JsonResponse({
        "status": "ok",
        "queued": queued,
        "skipped": skipped,
    })
