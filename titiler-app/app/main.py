import json
import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Annotated, Optional

import httpx
import redis
from fastapi import Depends, FastAPI, HTTPException, Path, Query
from rio_tiler.models import ImageData
from rio_tiler.types import ColorMapType
from starlette.middleware.cors import CORSMiddleware
from titiler.core.algorithm.base import BaseAlgorithm
from titiler.core.factory import TilerFactory
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINIO_HOST = os.getenv("MINIO_HOST", "http://georiva-minio:9000")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "georiva-assets")
REDIS_URL = os.getenv("REDIS_URL", "redis://georiva-redis:6379/0")
DJANGO_BASE_URL = os.getenv("DJANGO_BASE_URL", "http://georiva:8000")

PALETTE_KEY_PREFIX = "georiva:palette"
PATH_RE = re.compile(r"^[\w/.-]+\.tif$")

# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Django fallback (lru_cache — only used on Redis cache miss)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=512)
def _fetch_config_from_django(catalog: str, collection: str, variable: str) -> Optional[dict]:
    """Fetch rendering config from Django internal API on Redis miss.
    Result is cached in-process for the process lifetime."""
    url = f"{DJANGO_BASE_URL}/api/tile-config/{catalog}/{collection}/{variable}/"
    try:
        resp = httpx.get(url, timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.warning(
            "Django tile-config fallback failed for %s/%s/%s: %s",
            catalog, collection, variable, e,
        )
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_cog_url(
        catalog: str,
        collection: str,
        variable: str,
        time_dt: datetime,
        reftime_dt: Optional[datetime],
) -> str:
    """
    Construct the MinIO COG URL from path components.

    Path convention (matches ingestion/service.py:626-640):
      {catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{variable}_{HHMMSS}.tif
      {catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{variable}_{HHMMSS}__ref{YYYYMMDDTHHmmss}.tif
    """
    date_path = time_dt.strftime("%Y/%m/%d")
    time_str = time_dt.strftime("%H%M%S")
    
    if reftime_dt is not None:
        ref_str = reftime_dt.strftime("%Y%m%dT%H%M%S")
        filename = f"{variable}_{time_str}__ref{ref_str}.tif"
    else:
        filename = f"{variable}_{time_str}.tif"
    
    dataset_path = f"{catalog}/{collection}/{variable}/{date_path}/{filename}"
    
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
# Custom TilerFactory dependencies
# ---------------------------------------------------------------------------

def SemanticTileConfig(
        catalog_slug: Annotated[str, Path(...)],
        collection_slug: Annotated[str, Path(...)],
        variable_slug: Annotated[str, Path(...)],
) -> dict:
    """Resolve rendering config for a variable.

    Resolution order: Redis → Django internal API → HTTP 503.
    FastAPI caches this result per request so colormap and rescale
    dependencies share a single Redis call.
    """
    raw = redis_client.get(f"{PALETTE_KEY_PREFIX}:{catalog_slug}:{collection_slug}:{variable_slug}")
    if raw:
        return json.loads(raw)
    
    config = _fetch_config_from_django(catalog_slug, collection_slug, variable_slug)
    if config is not None:
        return config
    
    raise HTTPException(
        status_code=503,
        detail="Tile config unavailable — Redis cold and Django fallback failed",
    )


def SemanticPathParams(
        catalog_slug: Annotated[str, Path(...)],
        collection_slug: Annotated[str, Path(...)],
        variable_slug: Annotated[str, Path(...)],
        time: str = Query(..., description="Valid time in ISO 8601 UTC (e.g. 2026-03-23T12:00:00Z)"),
        reftime: Optional[str] = Query(None, description="Forecast reference time in ISO 8601 UTC"),
) -> str:
    """Resolve the COG URL from semantic path params and time query params."""
    time_dt = parse_iso_datetime(time, "time")
    
    reftime_dt = parse_iso_datetime(reftime, "reftime") if reftime else None
    return build_cog_url(catalog_slug, collection_slug, variable_slug, time_dt, reftime_dt)


def SemanticColorMap(
        tile_config: dict = Depends(SemanticTileConfig),
) -> Optional[ColorMapType]:
    """Return the 256-entry colormap from Redis config, or grayscale fallback."""
    raw = tile_config.get("colormap")
    if raw:
        return {int(k): tuple(v) for k, v in raw.items()}
    # Grayscale fallback for variables without an assigned palette
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


# ---------------------------------------------------------------------------
# TilerFactory with semantic dependencies
# ---------------------------------------------------------------------------

cog = TilerFactory(
    path_dependency=SemanticPathParams,
    colormap_dependency=SemanticColorMap,
    process_dependency=SemanticRescale,
    router_prefix="/{catalog_slug}/{collection_slug}/{variable_slug}",
)

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(title="GeoRiva Tile Server")

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

app.include_router(
    cog.router,
    prefix="/{catalog_slug}/{collection_slug}/{variable_slug}",
)
