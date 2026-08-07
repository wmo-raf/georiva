import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Optional

import httpx
import redis
from fastapi import Depends, HTTPException, Path, Query
from rio_tiler.models import ImageData
from rio_tiler.types import ColorMapType
from titiler.core.algorithm.base import BaseAlgorithm

from app.config import (
    DJANGO_BASE_URL,
    MINIO_BUCKET_NAME,
    MINIO_HOST,
    PALETTE_KEY_PREFIX,
    PATH_RE,
    REDIS_URL,
)

logger = logging.getLogger(__name__)

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Django fallback
# ---------------------------------------------------------------------------

def _fetch_config_from_django(
        org: str, catalog: str, collection: str, variable: str,
        style: Optional[str] = None,
) -> tuple[Optional[int], Optional[dict]]:
    """Fetch rendering config from Django internal API on Redis miss.

    The org segment is forwarded, not resolved: this service is dialled on an
    internal container name that belongs to no organisation, so Django cannot
    work one out from the Host and reads it from the path instead. The style
    query param is forwarded the same way — which styles exist is Django's
    knowledge, not this service's.

    Returns ``(status_code, payload)``; the status is ``None`` when Django
    could not be reached at all, and the payload only ever accompanies a 200.
    """
    url = f"{DJANGO_BASE_URL}/api/tile-config/{org}/{catalog}/{collection}/{variable}/"
    try:
        resp = httpx.get(url, params={"style": style} if style else None, timeout=5.0)
        if resp.status_code == 200:
            return resp.status_code, resp.json()
        logger.warning(
            "Django tile-config returned %d for %s/%s/%s/%s (style=%s)",
            resp.status_code, org, catalog, collection, variable, style,
        )
        return resp.status_code, None
    except Exception as e:
        logger.warning(
            "Django tile-config fallback failed for %s/%s/%s/%s: %s",
            org, catalog, collection, variable, e,
        )
    return None, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_cog_url(
        org: str,
        catalog: str,
        collection: str,
        variable: str,
        time_dt: datetime,
        reftime_dt: Optional[datetime],
) -> str:
    """
    Construct the MinIO COG URL from path components.

    Path convention (org-first, matching the storage grammar every bucket key
    follows — see georiva/core/path_resolution.py):
      {org}/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{variable}_{HHMMSS}.tif
      {org}/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{variable}_{HHMMSS}__ref{YYYYMMDDTHHmmss}.tif

    The org is concatenated, never interpreted: catalog slugs repeat across
    organisations, so a key missing its leading org segment is ambiguous — but
    which org a request may ask for is Django's decision, made when it wrote the
    URL, not this service's.
    """
    date_path = time_dt.strftime("%Y/%m/%d")
    time_str = time_dt.strftime("%H%M%S")

    if reftime_dt is not None:
        ref_str = reftime_dt.strftime("%Y%m%dT%H%M%S")
        filename = f"{variable}_{time_str}__ref{ref_str}.tif"
    else:
        filename = f"{variable}_{time_str}.tif"

    dataset_path = f"{org}/{catalog}/{collection}/{variable}/{date_path}/{filename}"

    if not PATH_RE.match(dataset_path):
        raise HTTPException(status_code=400, detail=f"Invalid constructed path: {dataset_path}")
    
    return f"{MINIO_HOST}/{MINIO_BUCKET_NAME}/{dataset_path}"


def parse_iso_datetime(value: str, param_name: str) -> datetime:
    """Parse ISO 8601 UTC datetime string, raising 400 on failure."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} — expected ISO 8601 UTC (e.g. 2026-03-23T12:00:00Z)",
        )


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

def SemanticTileConfig(
        org_slug: Annotated[str, Path(...)],
        catalog_slug: Annotated[str, Path(...)],
        collection_slug: Annotated[str, Path(...)],
        variable_slug: Annotated[str, Path(...)],
        style: Optional[str] = Query(
            None,
            description="Named style to render with; omission means the variable's default style",
        ),
) -> dict:
    """Resolve rendering config for a variable.

    Resolution order: Redis → Django internal API → HTTP 404/503.
    FastAPI caches this result per request so colormap and rescale
    dependencies share a single Redis call.

    The Redis key is org-first because catalog slugs are only unique within an
    organisation: without that segment two institutions running a catalog of the
    same name would share one entry, and whichever Django warmed last would
    decide how both render. The optional ``style`` param is resolved into the
    key's trailing segment and nothing more (ADR 0023): which styles exist,
    and which is default, stays Django's knowledge — a styleless request reads
    the alias key Django keeps mirroring the default. A style Django answers
    404 for is a 404 here too, never a silent fall back to the default:
    serving the wrong style would be worse than failing.
    """
    address = f"{org_slug}/{catalog_slug}/{collection_slug}/{variable_slug}"
    key = f"{PALETTE_KEY_PREFIX}:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}"
    if style:
        key = f"{key}:{style}"

    raw = redis_client.get(key)
    if raw:
        logger.debug("Redis cache hit: %s (style=%s)", address, style)
        return json.loads(raw)

    status, config = _fetch_config_from_django(
        org_slug, catalog_slug, collection_slug, variable_slug, style,
    )
    if config is not None:
        logger.debug("Django fallback hit: %s (style=%s)", address, style)
        return config
    if status == 404:
        raise HTTPException(
            status_code=404,
            detail="No rendering config for this address — unknown variable or style",
        )

    logger.warning("Tile config unavailable: %s (style=%s)", address, style)
    raise HTTPException(
        status_code=503,
        detail="Tile config unavailable — Redis cold and Django fallback failed",
    )


def SemanticPathParams(
        org_slug: Annotated[str, Path(...)],
        catalog_slug: Annotated[str, Path(...)],
        collection_slug: Annotated[str, Path(...)],
        variable_slug: Annotated[str, Path(...)],
        time: str = Query(..., description="Valid time in ISO 8601 UTC (e.g. 2026-03-23T12:00:00Z)"),
        reftime: Optional[str] = Query(None, description="Forecast reference time in ISO 8601 UTC"),
) -> str:
    """Resolve the COG URL from semantic path params and time query params."""
    time_dt = parse_iso_datetime(time, "time")
    reftime_dt = parse_iso_datetime(reftime, "reftime") if reftime else None
    url = build_cog_url(org_slug, catalog_slug, collection_slug, variable_slug, time_dt, reftime_dt)
    logger.debug("Resolved COG URL: %s", url)
    return url


def SemanticColorMap(
        tile_config: dict = Depends(SemanticTileConfig),
) -> Optional[ColorMapType]:
    """Return the 256-entry colormap from Redis config, or grayscale fallback."""
    raw = tile_config.get("colormap")
    if raw:
        return {int(k): tuple(v) for k, v in raw.items()}
    return {i: (i, i, i, 255) for i in range(256)}


class RescaleAlgorithm(BaseAlgorithm):
    """Rescale raw float COG data to 0-255 before colormap lookup."""
    
    vmin: float
    vmax: float
    
    def __call__(self, img: ImageData) -> ImageData:
        img.rescale(in_range=[(self.vmin, self.vmax)], out_range=[(0, 255)])
        return img


def SemanticRescale(
        tile_config: dict = Depends(SemanticTileConfig),
) -> Optional[BaseAlgorithm]:
    """Return a rescale algorithm using vmin/vmax from the tile config."""
    return RescaleAlgorithm(vmin=tile_config["vmin"], vmax=tile_config["vmax"])
