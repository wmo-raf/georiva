"""
Staging consumer — registers raw files landed in the STAGING bucket as
StagingItems, without materializing any served layers.

This is the parallel of the published MinIO consumer (``consumer.py``), with
two deliberate differences:

1. It listens on a **separate** Redis list (``MINIO_STAGING_REDIS_KEY``), fed by
   a separate MinIO notification target, so the destructive ``blpop`` of the
   published consumer never steals (and drops) staging events.
2. It does **not** shred multi-temporal files into per-timestep items. One file
   becomes exactly **one** ``StagingItem`` with a temporal *extent* and one
   ``source``-role ``StagingAsset``.

See docs/adr/0004-staging-tier-and-abstract-stac-models.md.
"""

import hashlib
import json
import logging
import time
from pathlib import Path
from urllib.parse import unquote

import redis
from django.conf import settings

from georiva.core.filename import validate_path
from georiva.core.models import Catalog
from georiva.core.models.base import AbstractAsset
from georiva.core.storage import BucketType, storage

logger = logging.getLogger(__name__)

STAGING_REDIS_KEY = getattr(
    settings, "MINIO_STAGING_REDIS_KEY", "georiva:minio:staging-events"
)

# catalog.file_format → asset Format enum value
_FORMAT_MAP = {
    "grib2": AbstractAsset.Format.GRIB2,
    "grib": AbstractAsset.Format.GRIB2,
    "netcdf": AbstractAsset.Format.NETCDF,
    "nc": AbstractAsset.Format.NETCDF,
    "geotiff": AbstractAsset.Format.GEOTIFF,
    "tif": AbstractAsset.Format.GEOTIFF,
    "tiff": AbstractAsset.Format.GEOTIFF,
    "zarr": AbstractAsset.Format.ZARR,
}


def _checksum_and_size(local_path: Path) -> tuple[str, int]:
    """sha256 hex digest (fits checksum's 64 chars) and byte size."""
    h = hashlib.sha256()
    size = 0
    with open(local_path, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _temporal_extent(timestamps: list):
    """
    Collapse a file's timestamps into a STAC extent.

    One timestamp  → (datetime, None, None).
    Many           → (None, start, end) — the item's approximate index bounds.
    None/empty     → (None, None, None) — time comes from filename/derivation.
    """
    if not timestamps:
        return None, None, None
    ordered = sorted(timestamps)
    if len(ordered) == 1:
        return ordered[0], None, None
    return None, ordered[0], ordered[-1]


def register_staging_file(bucket: str, key: str):
    """
    Register a single raw file in the STAGING bucket as one StagingItem +
    one source asset. Returns the StagingItem, or None if the file is skipped.

    The authoritative time and calendar are NOT trusted from these fields —
    they are approximate Gregorian index bounds. The real time is read from
    file content at derivation time.
    """
    from georiva.formats.registry import format_registry
    from georiva.ingestion.handlers.source_file_manager import SourceFileManager
    from georiva.staging.models import (
        StagingAsset,
        StagingCollection,
        StagingItem,
    )

    meta = validate_path(key)
    catalog_slug = meta["catalog"]
    collection_slug = meta.get("collection")

    catalog = Catalog.objects.filter(slug=catalog_slug, is_active=True).first()
    if catalog is None:
        logger.warning("Staging: unknown catalog '%s': %s", catalog_slug, key)
        return None
    if not collection_slug:
        logger.warning("Staging: no collection in path: %s", key)
        return None

    plugin = format_registry.get(catalog.file_format)
    if plugin is None:
        logger.warning(
            "Staging: no format plugin for '%s': %s", catalog.file_format, key
        )
        return None

    origin = storage.bucket(bucket)
    sfm = SourceFileManager()

    with sfm.download_to_temp(origin, key) as local_path:
        checksum, file_size = _checksum_and_size(local_path)

        variables = plugin.list_variables(local_path) or []
        first_var = variables[0]["name"] if variables else None

        timestamps = []
        spatial = {}
        if first_var:
            try:
                timestamps = plugin.get_timestamps(local_path, first_var) or []
            except Exception as e:  # time is best-effort; derivation re-reads it
                logger.warning("Staging: get_timestamps failed for %s: %s", key, e)
            try:
                spatial = plugin.get_metadata_for_variable(local_path, first_var) or {}
            except Exception as e:
                logger.warning("Staging: metadata scan failed for %s: %s", key, e)

    dt, start_dt, end_dt = _temporal_extent(timestamps)

    collection, _ = StagingCollection.objects.get_or_create(
        catalog=catalog,
        slug=collection_slug,
        defaults={"name": collection_slug},
    )

    item = StagingItem.objects.create(
        collection=collection,
        source_file=f"{bucket}:{key}",
        datetime=dt,
        start_datetime=start_dt,
        end_datetime=end_dt,
        reference_time=meta.get("reference_time"),
        bounds=spatial.get("bounds"),
        crs=spatial.get("crs") or "EPSG:4326",
        width=spatial.get("width"),
        height=spatial.get("height"),
    )

    StagingAsset.objects.create(
        item=item,
        href=key,
        roles=[AbstractAsset.Role.SOURCE],
        format=_FORMAT_MAP.get(catalog.file_format, ""),
        checksum=checksum,
        file_size=file_size,
    )

    logger.info(
        "Staged: %s/%s → StagingItem(%s) [%s timestep(s), no shredding]",
        bucket, key, item.pk, len(timestamps),
    )
    return item


# =============================================================================
# Redis event loop (mirror of consumer.py, separate list)
# =============================================================================

def _handle_event(ev: dict):
    bucket_name = ev.get("s3", {}).get("bucket", {}).get("name", "")
    key_raw = ev.get("s3", {}).get("object", {}).get("key", "")

    if not key_raw or not bucket_name:
        return

    key = unquote(key_raw)
    if Path(key).name.startswith("."):  # .keep, hidden files
        return

    # Only act on the STAGING bucket.
    staging_name = storage.bucket(BucketType.STAGING).bucket_name
    if bucket_name != staging_name:
        return

    try:
        validate_path(key)
    except ValueError as e:
        logger.warning("Staging: invalid path %s: %s", key, e)
        return

    from georiva.ingestion.tasks import process_staging_file
    process_staging_file.delay(bucket=BucketType.STAGING, key=key)


def _consume_loop(stop_event=None):
    r = redis.from_url(settings.REDIS_URL)

    while not (stop_event is not None and stop_event.is_set()):
        try:
            result = r.blpop(STAGING_REDIS_KEY, timeout=5)
        except redis.RedisError as e:
            logger.error("Staging consumer Redis error, retry in 5s: %s", e)
            time.sleep(5)
            continue

        if result is None:
            continue

        _, raw = result
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning("Staging: invalid event JSON: %s", e)
            continue

        if isinstance(payload, list):
            records = [ev for item in payload for ev in item.get("Event", [])]
        else:
            records = payload.get("Records", [payload])

        for ev in records:
            try:
                _handle_event(ev)
            except Exception as e:
                logger.exception("Staging: error handling event: %s", e)


def run_staging_consumer(stop_event=None):
    """Block on the staging Redis list and register StagingItems."""
    logger.info("Staging consumer started, listening on key: %s", STAGING_REDIS_KEY)

    while not (stop_event is not None and stop_event.is_set()):
        try:
            _consume_loop(stop_event)
        except Exception as e:
            logger.exception("Staging consumer crashed, restarting in 5s: %s", e)
            time.sleep(5)

    logger.info("Staging consumer stopped.")
