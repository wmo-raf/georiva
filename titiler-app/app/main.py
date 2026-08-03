from app.logging_config import configure_logging

configure_logging()

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from rasterio.errors import RasterioIOError
from starlette.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.config import TTL_ROOT_PATH
from app.dependencies import SemanticColorMap, SemanticPathParams, SemanticRescale
from app.middleware import RequestLoggingMiddleware

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


@app.exception_handler(RasterioIOError)
async def rasterio_io_error_handler(request: Request, exc: RasterioIOError) -> JSONResponse:
    msg = str(exc)
    if "404" in msg or "HTTP response code: 404" in msg:
        logger.warning("COG not found: %s", request.url)
        return JSONResponse(
            status_code=404,
            content={"detail": "File not found in storage — check that the time/reftime parameters are correct."},
        )
    logger.error("RasterioIOError: %s | path: %s", msg, request.url)
    return JSONResponse(
        status_code=502,
        content={"detail": f"Storage read error: {msg}"},
    )


app.include_router(
    cog.router,
    prefix=TILE_ROUTE_PREFIX,
)
