import json
import logging

from django.http import StreamingHttpResponse

logger = logging.getLogger(__name__)

_KEEPALIVE_SECS = 25


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

    # Subscribe to Redis BEFORE fetching the snapshot so that events published
    # during the snapshot query are queued in pubsub and not lost.
    r = aioredis.from_url(settings.REDIS_URL)
    try:
        pubsub = r.pubsub()
        await pubsub.subscribe(CHANNEL)

        snapshot = await build_arrival_snapshot()
        yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"

        try:
            while True:
                # get_message with a timeout is used instead of pubsub.listen() so that:
                # 1. We can send periodic keepalive comments to prevent proxy timeouts.
                # 2. We avoid the block=True socket read inside listen() which can stall
                #    under some ASGI event loop configurations (Daphne/Uvicorn).
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=_KEEPALIVE_SECS,
                )
                if message is None:
                    yield ": keepalive\n\n"
                    continue

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
