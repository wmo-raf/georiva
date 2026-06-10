import json
import logging

from django.http import StreamingHttpResponse

logger = logging.getLogger(__name__)


def ingestion_events_sse(request):
    """
    Async SSE endpoint streaming ingestion events to connected browsers.

    Auth is enforced by Wagtail's require_admin_access before this view runs.
    The outer function is intentionally sync so Wagtail's sync decorator chain
    works correctly; the inner generator is async to use redis.asyncio under ASGI.
    """
    return StreamingHttpResponse(
        _event_stream(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _event_stream():
    from django.conf import settings
    import redis.asyncio as aioredis

    from .events import CHANNEL
    from .snapshot import build_arrival_snapshot

    snapshot = await build_arrival_snapshot()
    yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"

    r = aioredis.from_url(settings.REDIS_URL)
    try:
        pubsub = r.pubsub()
        await pubsub.subscribe(CHANNEL)
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    raw = message["data"]
                    data = raw.decode() if isinstance(raw, bytes) else raw
                    try:
                        payload = json.loads(data)
                        event_type = payload.get("type", "ingestion")
                    except (ValueError, AttributeError):
                        event_type = "ingestion"
                    yield f"event: {event_type}\ndata: {data}\n\n"
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
    finally:
        await r.aclose()
