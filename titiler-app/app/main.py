from app.logging_config import configure_logging

configure_logging()

import logging

import numpy as np
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from rasterio.errors import RasterioIOError
from rio_tiler.models import ImageData
from starlette.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.boundary import crop
from app.config import TTL_ROOT_PATH
from app.dependencies import (
    SemanticColorMap,
    SemanticPathParams,
    SemanticRescale,
    SemanticTileConfig,
    parse_iso_datetime,
    resolve_cog_url,
    resolve_tile_config,
)
from app.middleware import PreviewCacheMiddleware, RequestLoggingMiddleware
from app.reader import ResilientReader, is_not_found
from app.wmts import rest_router as wmts_rest_router
from app.wmts import router as wmts_router

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TilerFactory
# ---------------------------------------------------------------------------

#: Every tile route opens with the organisation. This service has no way to work
#: one out — it is dialled through nginx on whatever host the portal runs on and
#: reads storage directly — so Django, which does know, puts it in the path and
#: the segment is carried through to the storage key and the palette cache key
#: unchanged. No tenancy decision is taken here.
TILE_ROUTE_PREFIX = "/{org_slug}/{catalog_slug}/{collection_slug}/{variable_slug}"

cog = TilerFactory(
    reader=ResilientReader,
    path_dependency=SemanticPathParams,
    colormap_dependency=SemanticColorMap,
    process_dependency=SemanticRescale,
    router_prefix=TILE_ROUTE_PREFIX,
)

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(title="GeoRiva Tile Server", root_path=TTL_ROOT_PATH)

app.add_middleware(PreviewCacheMiddleware)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#: No cache may hold on to a storage failure. GDAL's own copy of one is dropped
#: by the reader before a request ever reaches this handler (#400); this says
#: the same thing to nginx and to the browser, so that a request made a second
#: after the COG lands is answered from storage rather than from a record of
#: the moment before it existed.
_NEVER_CACHED = {"Cache-Control": "no-store"}


@app.exception_handler(RasterioIOError)
async def rasterio_io_error_handler(request: Request, exc: RasterioIOError) -> JSONResponse:
    msg = str(exc)
    if is_not_found(exc):
        logger.warning("COG not found: %s", request.url)
        return JSONResponse(
            status_code=404,
            content={"detail": "File not found in storage — check that the time/reftime parameters are correct."},
            headers=_NEVER_CACHED,
        )
    logger.error("RasterioIOError: %s | path: %s", msg, request.url)
    return JSONResponse(
        status_code=502,
        content={"detail": f"Storage read error: {msg}"},
        headers=_NEVER_CACHED,
    )


app.include_router(
    cog.router,
    prefix=TILE_ROUTE_PREFIX,
)

#: The KVP WMTS endpoint, `/{org}/wmts` — two segments, so it can never be
#: read as the four-segment tile grammar above. Like every other route here
#: the org is carried, never resolved (ADR 0013).
app.include_router(wmts_router)

#: RESTful GetFeatureInfo (#379), on the same prefix the tiles are on: the
#: identify address is the tile address with the clicked pixel appended, which
#: is why it mounts here rather than under the KVP endpoint. Two segments
#: deeper than the deepest route the factory registers, so it shadows none of
#: them, and the auth gate scopes it by the four segments it already reads.
app.include_router(wmts_rest_router, prefix=TILE_ROUTE_PREFIX)


# ---------------------------------------------------------------------------
# Encoded texture (ADR 0021)
# ---------------------------------------------------------------------------


@app.get(TILE_ROUTE_PREFIX + "/encoded-preview.png")
def encoded_preview(
    src_path: str = Depends(SemanticPathParams),
    tile_config: dict = Depends(SemanticTileConfig),
    max_size: int = Query(4096, ge=1, description="Cap on the longest image side (native grid if smaller)"),
    v: str | None = Query(
        None,
        description=(
            "Render-config version token; varies the URL so caches never serve a texture scaled to a superseded range"
        ),
    ),
) -> Response:
    """The whole extent as one value-encoded texture: pixel = rescale(value, vmin→vmax, 0→255).

    Never colormapped — this is the machine texture WeatherLayers unscales
    client-side with ``imageUnscale=[vmin, vmax]``. The range comes from the
    same always-current tile config the tile routes resolve per request, so
    there is no encode-time state to go stale (ADR 0021); ``v`` is opaque to
    this service and exists only to key caches. The immutable cache header is
    safe for exactly that reason: a range change arrives as a different URL.
    """
    with ResilientReader(src_path) as src:
        img = src.preview(max_size=max_size)

    # A catalog that crops its maps to a boundary (ADR 0038) crops the texture
    # the same way: masked pixels render with alpha 0, which is how a layer
    # already says "no data here" and what keeps hover readouts blank outside it.
    crop(img, tile_config.get("boundary"))
    img.rescale(in_range=((tile_config["vmin"], tile_config["vmax"]),))

    return Response(
        img.render(img_format="PNG"),
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=31536000, immutable",
            "X-Image-Unscale": f"{tile_config['vmin']},{tile_config['vmax']}",
            "X-Image-Bounds": ",".join(str(b) for b in img.bounds) if img.bounds else "",
        },
    )


# ---------------------------------------------------------------------------
# Two-band vector texture (#593)
# ---------------------------------------------------------------------------


@app.get("/{org_slug}/{catalog_slug}/{collection_slug}/vector-preview.png")
def vector_preview(
    org_slug: str = Path(...),
    catalog_slug: str = Path(...),
    collection_slug: str = Path(...),
    u_variable: str = Query(..., description="Slug of the eastward (u) component"),
    v_variable: str = Query(..., description="Slug of the northward (v) component"),
    time: str = Query(..., description="Valid time in ISO 8601 UTC (e.g. 2026-03-23T12:00:00Z)"),
    reftime: str | None = Query(None, description="Forecast reference time in ISO 8601 UTC"),
    max_size: int = Query(4096, ge=1, description="Cap on the longest image side (native grid if smaller)"),
    v: str | None = Query(None, description="Render-config version token; varies the URL for caches"),
) -> Response:
    """A u/v pair as one texture: R = u, G = v, B = 0, alpha = both components valid.

    Both bands are rescaled over one range, the union of the two variables'
    current ranges, so ``atan2(v, u)`` of the decoded pixel is the direction
    in the source data; per-band ranges would bend every angle off the axes.
    Like ``encoded-preview.png`` this is derived per request and never stored
    (ADR 0021); ``v`` is opaque here and exists to key caches, and the
    collection-scoped address is gated by nginx like every other tile route.
    """
    time_dt = parse_iso_datetime(time, "time")
    reftime_dt = parse_iso_datetime(reftime, "reftime") if reftime else None

    if u_variable == v_variable:
        raise HTTPException(status_code=422, detail="u_variable and v_variable must name two different variables.")

    configs, images = [], []
    for variable in (u_variable, v_variable):
        configs.append(resolve_tile_config(org_slug, catalog_slug, collection_slug, variable))
        src_path = resolve_cog_url(org_slug, catalog_slug, collection_slug, variable, time_dt, reftime_dt)
        with ResilientReader(src_path) as src:
            images.append(src.preview(max_size=max_size))
    u_img, v_img = images

    if u_img.array.shape != v_img.array.shape or tuple(u_img.bounds) != tuple(v_img.bounds) or u_img.crs != v_img.crs:
        raise HTTPException(
            status_code=422,
            detail="The u and v components are on different grids — a vector texture needs one shared grid.",
        )

    ends = [(c.get("vmin"), c.get("vmax")) for c in configs]
    if any(a is None or b is None for a, b in ends):
        raise HTTPException(status_code=422, detail="Both components need a configured value range.")
    lo = min(a for a, _ in ends)
    hi = max(b for _, b in ends)
    if not lo < hi:
        raise HTTPException(status_code=422, detail="The shared value range is empty.")

    bands = []
    for img in (u_img, v_img):
        img.rescale(in_range=((lo, hi),))
        bands.append(img.array[0])
    zero = np.ma.masked_array(np.zeros_like(bands[0].data), mask=False)
    mask = np.ma.getmaskarray(bands[0]) | np.ma.getmaskarray(bands[1])
    array = np.ma.masked_array(
        np.ma.stack([bands[0], bands[1], zero]).data,
        mask=np.broadcast_to(mask, (3, *mask.shape)),
    )
    out = ImageData(array, assets=u_img.assets, crs=u_img.crs, bounds=u_img.bounds)
    # Same crop as every other output (ADR 0038); the catalog is one, so u's config speaks for both.
    crop(out, configs[0].get("boundary"))

    return Response(
        out.render(img_format="PNG"),
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=31536000, immutable",
            "X-Image-Unscale": f"{lo},{hi}",
            "X-Image-Bounds": ",".join(str(b) for b in out.bounds) if out.bounds else "",
        },
    )
