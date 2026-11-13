import json
import logging
from datetime import UTC, datetime
from typing import Annotated

import httpx
import redis
from fastapi import Depends, HTTPException, Path, Query
from rio_tiler.models import ImageData
from rio_tiler.types import ColorMapType
from titiler.core.algorithm.base import BaseAlgorithm

from app.boundary import crop
from app.config import (
    DJANGO_BASE_URL,
    MINIO_BUCKET_NAME,
    MINIO_HOST,
    PALETTE_KEY_PREFIX,
    PATH_RE,
    REDIS_URL,
    REMOTE_ASSETS_KEY_PREFIX,
    REMOTE_ASSETS_PUBLISHED_FIELD,
)
from app.store import StoreError, StoreSlice

logger = logging.getLogger(__name__)

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Django fallback
# ---------------------------------------------------------------------------


def _fetch_config_from_django(
    org: str,
    catalog: str,
    collection: str,
    variable: str,
    style: str | None = None,
    time: str | None = None,
    reftime: str | None = None,
) -> tuple[int | None, dict | None]:
    """Fetch rendering config from Django internal API on Redis miss.

    ``time`` (and ``reftime``) name one timestep; Django then adds
    ``remote_href`` to its answer when that timestep's asset is a Remote COG,
    or ``remote_store`` when it is a Store asset (see :func:`remote_source`).

    The org segment is forwarded, not resolved: this service is dialled on an
    internal container name that belongs to no organisation, so Django cannot
    work one out from the Host and reads it from the path instead. The style
    query param is forwarded the same way — which styles exist is Django's
    knowledge, not this service's.

    Returns ``(status_code, payload)``; the status is ``None`` when Django
    could not be reached at all, and the payload only ever accompanies a 200.
    """
    url = f"{DJANGO_BASE_URL}/api/tile-config/{org}/{catalog}/{collection}/{variable}/"
    params = {name: value for name, value in (("style", style), ("time", time), ("reftime", reftime)) if value}
    try:
        resp = httpx.get(url, params=params or None, timeout=5.0)
        if resp.status_code == 200:
            return resp.status_code, resp.json()
        logger.warning(
            "Django tile-config returned %d for %s/%s/%s/%s (style=%s)",
            resp.status_code,
            org,
            catalog,
            collection,
            variable,
            style,
        )
        return resp.status_code, None
    except Exception as e:
        logger.warning(
            "Django tile-config fallback failed for %s/%s/%s/%s: %s",
            org,
            catalog,
            collection,
            variable,
            e,
        )
    return None, None


def django_client() -> httpx.AsyncClient:
    """The client an async route makes its own call to Django through.

    Separate from the fetch above only because that one runs inside a *sync*
    FastAPI dependency and can await nothing; both dial the same internal base
    URL, and this is the seam a test swaps a transport in at.

    Bounding the call is the point of it. This service must answer the client
    it is holding even when Django does not answer this one, so both ends are
    timed: a short connect, because an unreachable container should fail at
    once rather than hold a legacy client open, and a long read, because the
    one document fetched through here enumerates a whole archive's worth of
    timestamps and is deliberately uncapped (#354).
    """
    return httpx.AsyncClient(
        base_url=DJANGO_BASE_URL,
        timeout=httpx.Timeout(30.0, connect=5.0),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_cog_url(
    org: str,
    catalog: str,
    collection: str,
    variable: str,
    time_dt: datetime,
    reftime_dt: datetime | None,
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


#: How a timestep is spelled in a remote-assets hash field. Django writes the
#: same spelling (``remote_assets.timestep_field``).
_TIMESTEP_STAMP = "%Y%m%dT%H%M%S"


def remote_source(
    org: str,
    catalog: str,
    collection: str,
    variable: str,
    time_dt: datetime,
    reftime_dt: datetime | None,
) -> str | StoreSlice | None:
    """Where this timestep's asset is read if it is Remote, else ``None``.

    A Remote COG is its upstream URL. A Store asset (ADR 0044 §9) is a
    :class:`StoreSlice`: the store's address, the array's path, this timestep
    as its run and step, and the feed's Pinned locations for the store's data.

    A Remote asset's bytes were never copied into our storage (ADR 0042): they
    are wherever the upstream put them, and no path grammar leads there. Only
    Django knows the address, and it says so in Redis, for every tier, the way
    it says how a variable renders. Resolution order: Redis, then Django.

    Redis settles nearly every request. A hash carrying the published marker is
    Django's whole word on the variable, so a timestep missing from it is not
    Remote and Django is not asked. Only a hash with no marker (Redis was
    flushed, or the variable was never warmed) falls back to the tile-config
    callback, which answers for ``public`` collections.

    Which organisation, and whether the caller may see it, is not decided here:
    nginx asked Django before this request arrived (ADR 0015). The one thing
    checked is that the address is an http(s) URL, because it is about to be
    handed to GDAL or to the store reader, either of which would as readily
    open something else.
    """
    field = time_dt.strftime(_TIMESTEP_STAMP)
    if reftime_dt is not None:
        field = f"{field}/{reftime_dt.strftime(_TIMESTEP_STAMP)}"
    key = f"{REMOTE_ASSETS_KEY_PREFIX}:{org}:{catalog}:{collection}:{variable}"

    value, published = redis_client.hmget(key, [field, REMOTE_ASSETS_PUBLISHED_FIELD])
    entry = None
    if value is not None and value.startswith("{"):
        try:
            entry = json.loads(value)
        except ValueError:
            entry = {}
        value = None
    elif value is None and not published:
        _, config = _fetch_config_from_django(
            org,
            catalog,
            collection,
            variable,
            time=_iso_utc(time_dt),
            reftime=_iso_utc(reftime_dt) if reftime_dt is not None else None,
        )
        value = (config or {}).get("remote_href")
        entry = (config or {}).get("remote_store")

    if entry is not None:
        try:
            return StoreSlice.published(entry if isinstance(entry, dict) else {}, time_dt, reftime_dt)
        except StoreError as exc:
            logger.error("Refusing a published store entry for %s: %s", key, exc.detail)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    if value is None:
        return None
    if not value.startswith(("https://", "http://")):
        logger.error("Refusing a remote asset address that is not an http(s) URL: %s", value)
        raise HTTPException(status_code=502, detail="The remote asset's address is not an http(s) URL.")
    return value


def resolve_source(
    org: str,
    catalog: str,
    collection: str,
    variable: str,
    time_dt: datetime,
    reftime_dt: datetime | None,
) -> str | StoreSlice:
    """Where this timestep is read from: upstream if its asset is Remote — a
    COG's URL, or a field of a store — else the address in our storage that
    the path spells (ADR 0043 §3, ADR 0044 §9).

    The one function every raster route resolves its dataset through, so a
    Mixed collection is served timestep by timestep from wherever each one is.
    """
    return remote_source(org, catalog, collection, variable, time_dt, reftime_dt) or build_cog_url(
        org, catalog, collection, variable, time_dt, reftime_dt
    )


def _iso_utc(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_datetime(value: str, param_name: str) -> datetime:
    """Parse ISO 8601 UTC datetime string, raising 400 on failure."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(UTC)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} — expected ISO 8601 UTC (e.g. 2026-03-23T12:00:00Z)",
        ) from None


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------


def SemanticTileConfig(
    org_slug: Annotated[str, Path(...)],
    catalog_slug: Annotated[str, Path(...)],
    collection_slug: Annotated[str, Path(...)],
    variable_slug: Annotated[str, Path(...)],
    style: str | None = Query(
        None,
        description="Named style to render with; omission means the variable's default style",
    ),
) -> dict:
    """Resolve rendering config for the variable the path names (see :func:`resolve_tile_config`)."""
    return resolve_tile_config(org_slug, catalog_slug, collection_slug, variable_slug, style)


def resolve_tile_config(
    org_slug: str,
    catalog_slug: str,
    collection_slug: str,
    variable_slug: str,
    style: str | None = None,
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
        org_slug,
        catalog_slug,
        collection_slug,
        variable_slug,
        style,
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
    reftime: str | None = Query(None, description="Forecast reference time in ISO 8601 UTC"),
) -> str | StoreSlice:
    """Resolve what the reader opens — a COG's URL, or a store's field — from
    semantic path params and time query params."""
    time_dt = parse_iso_datetime(time, "time")
    reftime_dt = parse_iso_datetime(reftime, "reftime") if reftime else None
    source = resolve_source(org_slug, catalog_slug, collection_slug, variable_slug, time_dt, reftime_dt)
    logger.debug("Resolved dataset: %s", source)
    return source


def SemanticColorMap(
    tile_config: dict = Depends(SemanticTileConfig),
) -> ColorMapType | None:
    """Return the 256-entry colormap from Redis config, or grayscale fallback."""
    raw = tile_config.get("colormap")
    if raw:
        return {int(k): tuple(v) for k, v in raw.items()}
    return {i: (i, i, i, 255) for i in range(256)}


class RescaleAlgorithm(BaseAlgorithm):
    """Rescale raw float COG data to 0-255 before colormap lookup.

    When the layer's catalog crops its maps to a boundary (``display`` clip mode,
    ADR 0038) the tile is also masked to it here, so the colormap step paints
    nothing outside. The stored COG is untouched: only this drawing is cropped.
    """

    vmin: float
    vmax: float
    boundary: dict | None = None

    def __call__(self, img: ImageData) -> ImageData:
        crop(img, self.boundary)
        img.rescale(in_range=[(self.vmin, self.vmax)], out_range=[(0, 255)])
        return img


def SemanticRescale(
    tile_config: dict = Depends(SemanticTileConfig),
) -> BaseAlgorithm | None:
    """Return a rescale algorithm using vmin/vmax from the tile config."""
    return RescaleAlgorithm(vmin=tile_config["vmin"], vmax=tile_config["vmax"], boundary=tile_config.get("boundary"))
