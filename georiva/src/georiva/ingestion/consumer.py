import json
import logging
import time
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote

import redis
from django.conf import settings

from georiva.core.filename import validate_path
from georiva.core.models import Catalog
from georiva.core.storage import BucketType, get_bucket_config
from georiva.ingestion.models import DataArrival, FileIngestion
from georiva.ingestion.tasks import process_incoming_file

logger = logging.getLogger(__name__)

REDIS_KEY = getattr(settings, "MINIO_REDIS_KEY", "georiva:minio:events")


# Cache bucket config — it's static for the lifetime of the process
@lru_cache(maxsize=1)
def _get_ingest_buckets() -> dict:
    buckets = get_bucket_config()
    return {
        buckets[BucketType.INCOMING]["name"]: BucketType.INCOMING,
        buckets[BucketType.SOURCES]["name"]: BucketType.SOURCES,
    }


def _resolve_origin(bucket_name: str):
    return _get_ingest_buckets().get(bucket_name)


def _catalog_exists(catalog_slug: str) -> bool:
    return Catalog.objects.filter(slug=catalog_slug, is_active=True).exists()


def _required_time_error(catalog_slug: str, key: str) -> str | None:
    """
    For formats with no native time dimension (time_from_filename), a file's
    valid time must be parseable from its filename. Returns a descriptive
    error string when extraction fails, None when the file is fine.

    Only enforced when the catalog has ManualUploadConfigs to supply a
    valid_time_format — catalogs fed purely by DataFeeds are unaffected.
    """
    from georiva.formats.registry import format_registry
    from georiva.ingestion.models import ManualUploadConfig
    from georiva.ingestion.time_extraction import extract_times

    catalog = Catalog.objects.filter(slug=catalog_slug).only("file_format").first()
    if catalog is None:
        return None
    plugin = format_registry.get(catalog.file_format)
    if plugin is None or not plugin.time_from_filename:
        return None  # time is read from file content during ingestion

    formats = list(
        ManualUploadConfig.objects.filter(catalog=catalog)
        .values_list("valid_time_format", flat=True)
        .distinct()
    )
    if not formats:
        return None

    filename = Path(key).name
    for fmt in formats:
        if extract_times(filename, fmt).get("valid_time"):
            return None

    return (
        f"Could not extract a valid time from filename '{filename}'. "
        f"Expected the filename stem to match one of: {', '.join(formats)} "
        f"(optionally with a GR--YYYYMMDDTHHMM-- reference time prefix)."
    )


def _should_stop(stop_event) -> bool:
    return stop_event is not None and stop_event.is_set()


def _handle_event(ev: dict):
    bucket_name = ev.get("s3", {}).get("bucket", {}).get("name", "")
    key_raw = ev.get("s3", {}).get("object", {}).get("key", "")
    
    if not key_raw or not bucket_name:
        return
    
    key = unquote(key_raw)

    # Skip placeholder and hidden files (.keep, .gitkeep, etc.)
    if Path(key).name.startswith('.'):
        return

    origin_bucket = _resolve_origin(bucket_name)
    if not origin_bucket:
        return
    
    try:
        meta = validate_path(key)
    except ValueError as e:
        logger.warning("Invalid path %s: %s", key, e)
        return
    
    catalog_slug = meta["catalog"]
    collection_slug = meta.get("collection")
    
    if not _catalog_exists(catalog_slug):
        logger.warning("Unknown catalog '%s': %s", catalog_slug, key)
        return
    
    arrival, arrival_created = DataArrival.find_or_create(
        file_path=key,
        trigger=DataArrival.Trigger.MANUAL_UPLOAD,
    )
    if not arrival_created and arrival.status == DataArrival.Status.UPLOADING:
        arrival.status = DataArrival.Status.PENDING
        arrival.save(update_fields=['status', 'updated_at'])

    log, created = FileIngestion.register(
        bucket=origin_bucket,
        file_path=key,
        catalog_slug=catalog_slug,
        collection_slug=collection_slug or "",
        reference_time=meta.get("reference_time"),
        data_arrival=arrival,
    )

    # Only genuine direct drops: admin uploads pre-create the DataArrival and
    # have already validated times server-side (the date may have been entered
    # in the form rather than encoded in the filename).
    if arrival_created:
        time_error = _required_time_error(catalog_slug, key)
        if time_error:
            logger.warning("Time extraction failed for %s: %s", key, time_error)
            FileIngestion.mark_failed(origin_bucket, key, time_error)
            arrival.status = DataArrival.Status.FAILED
            arrival.error_message = time_error
            arrival.save(update_fields=['status', 'error_message', 'updated_at'])
            return

    if not created:
        if log.status == FileIngestion.Status.PROCESSING:
            logger.debug("Already processing: %s/%s", origin_bucket, key)
            return
        if log.status == FileIngestion.Status.COMPLETED and log.has_live_data:
            return
        if log.status == FileIngestion.Status.COMPLETED and not log.has_live_data:
            logger.warning("Completed but no live data, re-ingesting: %s", key)
            FileIngestion.reset_for_reingest(origin_bucket, key)
    
    process_incoming_file.delay(
        file_path=key,
        origin_bucket=origin_bucket,
        reference_time=(
            meta["reference_time"].isoformat() if meta["reference_time"] else None
        ),
    )
    logger.info(
        "Queued: %s/%s (catalog=%s, collection=%s, ref=%s)",
        bucket_name, key, catalog_slug,
        collection_slug or "(to resolve)",
        meta.get("reference_time"),
    )


def _consume_loop(stop_event=None):
    r = redis.from_url(settings.REDIS_URL)
    
    while not _should_stop(stop_event):
        try:
            result = r.blpop(REDIS_KEY, timeout=5)
        except redis.RedisError as e:
            logger.error("Redis error in consumer, retrying in 5s: %s", e)
            time.sleep(5)
            continue
        
        if result is None:
            continue
        
        _, raw = result
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning("Invalid event JSON: %s", e)
            continue
        
        # MinIO Redis access format: [{"Event": [{...}], "EventTime": "..."}]
        # Fallback to webhook-style {"Records": [...]} just in case
        if isinstance(payload, list):
            records = [ev for item in payload for ev in item.get("Event", [])]
        else:
            records = payload.get("Records", [payload])
        
        for ev in records:
            try:
                _handle_event(ev)
            except Exception as e:
                logger.exception("Error handling event: %s", e)


def run_minio_consumer(stop_event=None):
    """
    Block on the MinIO Redis event list and dispatch Celery tasks.

    Intended to run in a daemon thread inside the ingestion worker.
    stop_event is a threading.Event; set it to trigger a clean shutdown.
    """
    logger.info("MinIO event consumer started, listening on key: %s", REDIS_KEY)
    
    while not _should_stop(stop_event):
        try:
            _consume_loop(stop_event)
        except Exception as e:
            logger.exception("MinIO event consumer crashed, restarting in 5s: %s", e)
            time.sleep(5)
    
    logger.info("MinIO event consumer stopped.")
