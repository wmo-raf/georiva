import json
import logging
from pathlib import PurePosixPath
from urllib.parse import unquote

from django.conf import settings
from django.http import JsonResponse, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from georiva.core.models import Collection

logger = logging.getLogger(__name__)

MINIO_WEBHOOK_BEARER_TOKEN = getattr(settings, "MINIO_WEBHOOK_BEARER_TOKEN", None)


def _get_bearer_token(request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    return auth.split(" ", 1)[1].strip()


@csrf_exempt
@require_POST
def minio_event_webhook(request):
    if MINIO_WEBHOOK_BEARER_TOKEN:
        token = _get_bearer_token(request)
        if not token or token != settings.MINIO_WEBHOOK_BEARER_TOKEN:
            return HttpResponseForbidden("Forbidden")
    
    payload = json.loads(request.body.decode("utf-8"))
    events = payload if isinstance(payload, list) else payload.get("Records", [payload])
    
    for ev in events:
        key_raw = ev.get("s3", {}).get("object", {}).get("key", "")
        if not key_raw:
            continue
        
        key = unquote(key_raw)
        file_path = PurePosixPath(key)
        parts = file_path.parts
        
        if len(parts) < 3:
            logger.warning(f"Event {key} has less than 3 parts. Skipping....")
            continue
        
        action_directory = parts[0]
        
        if action_directory != "incoming":
            logger.info(f"Event {key} is not in 'incoming' directory. Skipping....")
            continue
        
        catalog_slug = parts[1]
        collection_slug = parts[2]
        
        # Look up the collection
        try:
            collection = Collection.objects.select_related('catalog').get(
                catalog__slug=catalog_slug,
                slug=collection_slug,
                is_active=True
            )
        except Collection.DoesNotExist:
            # Log and skip unknown paths
            logger.warning(f"Event {key} refers to unknown collection {catalog_slug}/{collection_slug}. Skipping....")
            continue
        
        logger.info(f"Event {key} is in collection {collection_slug}")
        
        # Queue processing task
        # process_incoming_file.delay(collection.id, str(file_path))
    
    return JsonResponse({"status": "queued"})
