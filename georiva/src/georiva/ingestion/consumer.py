import json
import logging
import time
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote

import redis
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from georiva.core.storage import BucketType, get_bucket_config
from georiva.core.storage.filename import validate_path
from georiva.core.storage.path_resolution import resolve_org_catalog
from georiva.events.context import use_context
from georiva.ingestion.chains import context_for_arrival
from georiva.ingestion.models import FileIngestion, FileIngestionFinished
from georiva.ingestion.tasks import process_incoming_file

logger = logging.getLogger(__name__)

# How long a Delivery GeoRiva recorded for its own write stays the explanation for that key.
# A person's upload is recorded when the page asks to send it, before the browser has
# sent a byte, so this has to outlast the upload itself, not just the write.
WAITING_WINDOW = timedelta(hours=6)

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


def _required_time_error(catalog, key: str) -> str | None:
    """
    For formats with no native time dimension (time_from_filename), a file's
    valid time must be parseable from its filename. Returns a descriptive
    error string when extraction fails, None when the file is fine.

    Only enforced when the catalog has a push feed to supply a
    valid_time_format — catalogs fed by a DataFeed are unaffected.
    """
    from georiva.formats.registry import format_registry
    from georiva.ingestion.models import PushFeed
    from georiva.ingestion.time_extraction import extract_times

    plugin = format_registry.get(catalog.file_format)
    if plugin is None or not plugin.time_from_filename:
        return None  # time is read from file content during ingestion

    feed = PushFeed.objects.filter(catalog=catalog).first()
    if feed is None:
        return None

    filename = Path(key).name
    if extract_times(filename, feed.valid_time_format).get("valid_time"):
        return None

    return (
        f"Could not extract a valid time from filename '{filename}'. "
        f"Expected the filename stem to match: {feed.valid_time_format} "
        f"(optionally with a GR--YYYYMMDDTHHMM-- reference time prefix)."
    )


def _principal_of(ev: dict) -> str:
    return (ev.get("userIdentity") or {}).get("principalId", "")


def _write_id_of(ev: dict) -> str:
    """Which write a notification reports: MinIO's sequencer, set once per write, else the ETag."""
    obj = ev.get("s3", {}).get("object", {})
    return obj.get("sequencer") or obj.get("eTag") or ""


def _delivered_since_ingested(log) -> bool:
    """True when a delivery to ``log``'s key was accepted after its ingestion finished.

    A completed ingestion deletes its file from ``incoming``, so such a delivery is a
    later write the ingestion never read: a sender resending a corrected run.
    """
    from georiva.ingestion.models import Delivery

    if log.bucket != BucketType.INCOMING or log.completed_at is None:
        return False
    return Delivery.objects.filter(
        file_path=log.file_path,
        status=Delivery.Status.ACCEPTED,
        accepted_at__gt=log.completed_at,
    ).exists()


def _waiting_person_delivery(key: str):
    """The Delivery GeoRiva itself recorded for a file it just wrote: a person's upload, or a Retry.

    It is waiting while still ``received``, or while its file's ingestion has not yet
    finished registering; a later write by anyone else to the same key is not covered by it.
    """
    from georiva.ingestion.models import Delivery

    delivery = Delivery.objects.filter(file_path=key, sent_by__isnull=False).order_by("-id").first()
    if delivery is None or delivery.status == Delivery.Status.REJECTED:
        return None
    if delivery.status == Delivery.Status.RECEIVED:
        # Recorded before the write, so a row older than the window covers nobody.
        return delivery if timezone.now() - delivery.received_at < WAITING_WINDOW else None
    in_flight = FileIngestion.objects.filter(
        bucket=BucketType.INCOMING,
        file_path=key,
        status__in=[FileIngestion.Status.PENDING, FileIngestion.Status.PROCESSING],
    ).exists()
    return delivery if in_flight else None


def _awaits_completion(key: str) -> bool:
    """True when a person's browser is sending ``key`` straight to storage and will start its ingestion.

    Such an upload is told apart by the delivery recorded for it before the first byte moved,
    since its write carries GeoRiva's own credentials like any other. Starting the ingestion
    from here as well would race the browser's, and the page would follow the run that lost.
    """
    from georiva.ingestion.models import Delivery

    latest = Delivery.objects.filter(file_path=key, sent_by__isnull=False).order_by("-id").first()
    return latest is not None and latest.awaits_completion


def _no_feed_reason(org_slug: str, catalog_slug: str):
    """Why ``org/catalog`` is not a push feed's area, or ``(feed, None)`` when it is."""
    from georiva.ingestion.models import PushFeed

    catalog, error = resolve_org_catalog(org_slug, catalog_slug, require_active=False)
    if catalog is None:
        return None, error
    feed = PushFeed.objects.filter(catalog=catalog).select_related("catalog__organisation").first()
    if feed is not None:
        return feed, None
    if hasattr(catalog, "data_feed"):
        return None, f"Catalog '{org_slug}/{catalog_slug}' is fed by a pull feed, which nothing is sent to."
    return None, f"Catalog '{org_slug}/{catalog_slug}' has no push feed."


def _admit_incoming(ev: dict, key: str) -> bool:
    """Decide whether a file written into ``incoming`` may be ingested (ADR 0034).

    Admitted: a file GeoRiva wrote itself on a person's behalf, and a machine sender's
    write to its own feed — which is recorded as an accepted Delivery here, starting the
    chain, so ``context_for_arrival`` adopts it by key. Everything else is moved to
    quarantine, with a rejected Delivery when the key names a push feed.
    """
    from georiva.ingestion.models import Delivery
    from georiva.ingestion.quarantine import NOT_A_SENDER, quarantine

    size = ev.get("s3", {}).get("object", {}).get("size") or 0

    try:
        meta = validate_path(key)
    except ValueError as e:
        quarantine(key, f"Path check failed: {e}", size=size)
        return False

    feed, reason = _no_feed_reason(meta["org"], meta["catalog"])
    if feed is None:
        quarantine(key, reason, size=size)
        return False

    waiting = _waiting_person_delivery(key)
    sender = None
    if waiting is None:
        sender = feed.senders.filter(access_key=_principal_of(ev)).first()
        if sender is None:
            quarantine(key, NOT_A_SENDER, feed=feed, size=size)
            return False

    time_error = _required_time_error(feed.catalog, key)
    if time_error:
        quarantine(key, time_error, feed=feed, sender=sender, size=size)
        return False

    if waiting is not None:
        if waiting.status == Delivery.Status.RECEIVED:
            waiting.mark_accepted(file_path=key, bytes=size)
        return True

    # MinIO can notify more than once for one write; the repeat is the same delivery, not
    # a second one. A notification that names no write is a repeat while one is in flight.
    write_id = _write_id_of(ev)
    if write_id:
        if Delivery.objects.filter(file_path=key, write_id=write_id).exists():
            return True
    else:
        in_flight = FileIngestion.objects.filter(
            bucket=BucketType.INCOMING,
            file_path=key,
            status__in=[FileIngestion.Status.PENDING, FileIngestion.Status.PROCESSING],
        ).exists()
        if in_flight and Delivery.objects.filter(file_path=key, push_sender=sender).exists():
            return True

    with transaction.atomic():
        delivery = Delivery.receive(
            push_feed=feed, filename=Path(key).name, file_path=key, push_sender=sender, write_id=write_id
        )
        delivery.mark_accepted(file_path=key, bytes=size)
    return True


def admit_found_in_incoming(key: str) -> bool:
    """Decide whether a file the Sweep found in ``incoming`` may be ingested (ADR 0034).

    The same attribution as a notification, with no principal: a key inside a push feed's
    area is safe to attribute by prefix, since only that feed's senders and GeoRiva can
    write there, and becomes an accepted Delivery found by sweep. Anything else is
    quarantined. A file a delivery already covers is not recorded again.
    """
    from georiva.ingestion.models import Delivery
    from georiva.ingestion.quarantine import quarantine

    try:
        meta = validate_path(key)
    except ValueError as e:
        quarantine(key, f"Path check failed: {e}")
        return False

    feed, reason = _no_feed_reason(meta["org"], meta["catalog"])
    if feed is None:
        quarantine(key, reason)
        return False

    time_error = _required_time_error(feed.catalog, key)
    if time_error:
        quarantine(key, time_error, feed=feed)
        return False

    if (
        _waiting_person_delivery(key) is not None
        or Delivery.objects.filter(
            file_path=key, status__in=[Delivery.Status.RECEIVED, Delivery.Status.ACCEPTED]
        ).exists()
    ):
        return True

    with transaction.atomic():
        delivery = Delivery.receive(push_feed=feed, filename=Path(key).name, file_path=key, found_by_sweep=True)
        delivery.mark_accepted(file_path=key)
    return True


def _should_stop(stop_event) -> bool:
    return stop_event is not None and stop_event.is_set()


def _handle_event(ev: dict):
    bucket_name = ev.get("s3", {}).get("bucket", {}).get("name", "")
    key_raw = ev.get("s3", {}).get("object", {}).get("key", "")

    if not key_raw or not bucket_name:
        return

    key = unquote(key_raw)

    # Skip placeholder and hidden files (.keep, .gitkeep, etc.)
    if Path(key).name.startswith("."):
        return

    origin_bucket = _resolve_origin(bucket_name)
    if not origin_bucket:
        return

    if origin_bucket == BucketType.INCOMING:
        # Nothing reaches a catalog from incoming except through its push feed and a known
        # sender; a refused file leaves the bucket here, so the Sweep never revives it.
        if not _admit_incoming(ev, key):
            return

    try:
        meta = validate_path(key)
    except ValueError as e:
        logger.warning("Invalid path %s: %s", key, e)
        return

    org_slug = meta["org"]
    catalog_slug = meta["catalog"]
    collection_slug = meta.get("collection")

    # A Bucket notification belongs to the chain of the fetch (or upload) that stored
    # the file, found by lookup; with none behind it, it is a drop-zone arrival and
    # starts its own. The organisation is the key's first segment, and may name
    # nobody: that arrival is recorded as unattributed.
    with use_context(context_for_arrival(key, actor="system:bucket")):
        catalog, resolution_error = resolve_org_catalog(org_slug, catalog_slug)

        # Register before deciding: a file we refuse to ingest must still leave a
        # visible, actionable record. It is left in place in its bucket — never
        # guessed into some other organisation, never deleted.
        log, created = FileIngestion.register(
            bucket=origin_bucket,
            file_path=key,
            reference_time=meta.get("reference_time"),
        )

        if catalog is None:
            logger.warning("Refusing %s: %s", key, resolution_error)
            # A key naming an organisation or catalog that does not exist is
            # nobody's: the event is unattributed, visible to the instance
            # administrator only.
            FileIngestion.mark_failed(
                origin_bucket,
                key,
                resolution_error,
                outcome=FileIngestionFinished.Outcome.REFUSED,
                unattributed=True,
            )
            return

        time_error = _required_time_error(catalog, key)
        if time_error:
            logger.warning("Time extraction failed for %s: %s", key, time_error)
            # Refused inside a known organisation: attributed to it.
            FileIngestion.mark_failed(origin_bucket, key, time_error, outcome=FileIngestionFinished.Outcome.REFUSED)
            return

        # Registered above, so the Sweep still revives it if the browser never reports back.
        if origin_bucket == BucketType.INCOMING and _awaits_completion(key):
            logger.debug("Awaiting the uploader's completion: %s/%s", origin_bucket, key)
            return

        if not created:
            if log.status == FileIngestion.Status.PROCESSING:
                logger.debug("Already processing: %s/%s", origin_bucket, key)
                return
            if log.status == FileIngestion.Status.COMPLETED and log.has_live_data:
                if not _delivered_since_ingested(log):
                    return
                logger.info("Written again since it was ingested, re-ingesting: %s", key)
                FileIngestion.reset_for_reingest(origin_bucket, key)
            if log.status == FileIngestion.Status.COMPLETED and not log.has_live_data:
                logger.warning("Completed but no live data, re-ingesting: %s", key)
                FileIngestion.reset_for_reingest(origin_bucket, key)

        process_incoming_file.delay(
            file_path=key,
            origin_bucket=origin_bucket,
            reference_time=(meta["reference_time"].isoformat() if meta["reference_time"] else None),
        )
        logger.info(
            "Queued: %s/%s (org=%s, catalog=%s, collection=%s, ref=%s)",
            bucket_name,
            key,
            org_slug,
            catalog_slug,
            collection_slug or "(to resolve)",
            meta.get("reference_time"),
        )


def _consume_loop(stop_event=None):
    r = redis.from_url(settings.REDIS_URL)

    while not _should_stop(stop_event):
        try:
            result = r.blpop(REDIS_KEY, timeout=5)
        except redis.exceptions.TimeoutError:
            # redis-py 8 enforces the block timeout as a socket read deadline,
            # so an idle wait raises instead of returning None. Same meaning:
            # nothing arrived, go around again.
            continue
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
