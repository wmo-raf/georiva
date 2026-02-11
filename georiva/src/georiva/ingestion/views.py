"""
GeoRiva MinIO Webhook View

Receives S3 event notifications from georiva-incoming and georiva-sources
buckets, registers files in the IngestionLog, and queues Celery tasks.
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
from georiva.core.models import Collection
from georiva.core.storage import BucketType, get_bucket_config
from georiva.ingestion.tasks import process_incoming_file
from .models import IngestionLog

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
        buckets[BucketType.INCOMING]: BucketType.INCOMING,
        buckets[BucketType.SOURCES]: BucketType.SOURCES,
    }
    return ingest_buckets.get(bucket_name)


@lru_cache(maxsize=128)
def _get_collection_id(catalog_slug: str, collection_slug: str) -> int | None:
    """
    Cached collection lookup.

    Avoids hitting the database for every event when lots of files
    arrive for the same collection.
    """
    try:
        return Collection.objects.values_list('id', flat=True).get(
            catalog__slug=catalog_slug,
            slug=collection_slug,
            is_active=True,
        )
    except Collection.DoesNotExist:
        return None


@lru_cache(maxsize=64)
def _get_default_collection_id(catalog_slug: str) -> int | None:
    """
    Get the default collection for a catalog when no collection is in the path.

    Looks for the catalog's default_collection or falls back to the only
    active collection if there's exactly one.
    """
    from georiva.core.models import Catalog
    
    try:
        catalog = Catalog.objects.get(slug=catalog_slug, is_active=True)
    except Catalog.DoesNotExist:
        return None
    
    # Check for a configured default
    if hasattr(catalog, 'default_collection') and catalog.default_collection:
        try:
            return Collection.objects.values_list('id', flat=True).get(
                catalog=catalog,
                slug=catalog.default_collection,
                is_active=True,
            )
        except Collection.DoesNotExist:
            pass
    
    # Fall back: if catalog has exactly one active collection, use it
    collections = list(
        Collection.objects.filter(
            catalog=catalog, is_active=True
        ).values_list('id', flat=True)[:2]
    )
    
    if len(collections) == 1:
        return collections[0]
    
    return None


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
    3. Resolve collection (from path or catalog default)
    4. Register in IngestionLog (skip if already known)
    5. Queue a Celery task for processing
    """
    # Auth
    if MINIO_WEBHOOK_BEARER_TOKEN:
        token = _get_bearer_token(request)
        if not token or token != MINIO_WEBHOOK_BEARER_TOKEN:
            return HttpResponseForbidden("Forbidden")
    
    # Parse payload
    payload = json.loads(request.body.decode("utf-8"))
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
        
        # 2. Parse and validate path
        try:
            meta = validate_path(key)
        except ValueError as e:
            logger.warning("Invalid path %s: %s", key, e)
            continue
        
        # 3. Resolve collection
        catalog_slug = meta['catalog']
        collection_slug = meta.get('collection')
        
        if collection_slug:
            # Path has collection: {catalog}/{collection}/{filename}
            collection_id = _get_collection_id(catalog_slug, collection_slug)
        else:
            # Path has no collection: {catalog}/{filename}
            # Try to resolve from catalog default
            collection_id = _get_default_collection_id(catalog_slug)
        
        if not collection_id:
            if collection_slug:
                logger.warning(
                    "Unknown collection: %s/%s from %s",
                    catalog_slug, collection_slug, key,
                )
            else:
                logger.warning(
                    "No collection in path and no default for catalog '%s': %s",
                    catalog_slug, key,
                )
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
        
        # 5. Queue Celery task
        process_incoming_file.delay(
            collection_id=collection_id,
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
            collection_slug or '(default)',
            meta.get('reference_time'),
        )
    
    return JsonResponse({
        "status": "ok",
        "queued": queued,
        "skipped": skipped,
    })
