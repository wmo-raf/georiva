from app.logging_config import configure_logging

configure_logging()

import logging

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse
from rasterio.errors import RasterioIOError
from starlette.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.config import TTL_ROOT_PATH
from app.dependencies import (
    SemanticColorMap,
    SemanticPathParams,
    SemanticRescale,
    SemanticTileConfig,
)
from app.middleware import RequestLoggingMiddleware
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
