import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "%s %s → %d (%.1fms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response


#: Same header the encoded texture sends: the URL names everything the pixels
#: depend on, so the response never changes under it.
_IMMUTABLE = "public, max-age=31536000, immutable"


class PreviewCacheMiddleware(BaseHTTPMiddleware):
    """Declares the colorized ``preview.webp`` cacheable — when its URL is tokened.

    The ``v`` param is the version token Django puts on every preview URL it
    builds (style, range and data version). Only a URL carrying it can honestly
    be called immutable; an untokened one gets no header, so nginx stores
    nothing. Errors are left alone so a failure is never cached.
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if (
            request.url.path.endswith("/preview.webp")
            and response.status_code == 200
            and request.query_params.get("v")
            and "cache-control" not in response.headers
        ):
            response.headers["Cache-Control"] = _IMMUTABLE
        return response
