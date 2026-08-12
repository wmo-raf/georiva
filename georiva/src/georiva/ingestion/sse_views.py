import json
import logging

from django.http import StreamingHttpResponse

from georiva.organisations.access import require_active_org

logger = logging.getLogger(__name__)

_KEEPALIVE_SECS = 25

_INGESTION_EVENT_TYPES = frozenset(
    [
        "file_ingestion.created",
        "file_ingestion.status_changed",
        "job.state_changed",
        "job.progress_updated",
        "snapshot",
    ]
)


def ingestion_events_sse(request):
    """Async SSE endpoint streaming ingestion (FileIngestion/Job) events."""
    # Resolved here, while the request is still in hand: the stream itself
    # outlives the view and has no request to ask.
    org = require_active_org(request)
    return StreamingHttpResponse(
        _event_stream(_INGESTION_EVENT_TYPES, _build_ingestion_snapshot, org.slug),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _build_ingestion_snapshot(org_slug):
    from .snapshot import build_ingestion_snapshot

    return await build_ingestion_snapshot(org_slug)


def should_forward(payload, allowed_types, org_slug):
    """Whether one published event belongs on this listener's stream.

    Two independent reasons to drop: the event type is not one this stream
    carries, or it is for another organisation. A single Redis channel carries
    every organisation's events and the SSE endpoint subscribes to all of it.

    An event carrying no organisation reaches nobody. That is not a fallback to
    "everybody" — an event whose owner could not be determined is exactly the one
    it would be wrong to broadcast.
    """
    if not isinstance(payload, dict):
        return False
    if payload.get("type", "ingestion") not in allowed_types:
        return False
    return payload.get("org") == org_slug


async def _event_stream(allowed_types, snapshot_fn, org_slug):
    import redis.asyncio as aioredis
    from django.conf import settings

    from .events import CHANNEL

    r = aioredis.from_url(settings.REDIS_URL)
    try:
        pubsub = r.pubsub()
        await pubsub.subscribe(CHANNEL)

        snapshot = await snapshot_fn(org_slug)
        yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"

        try:
            while True:
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
                except (ValueError, TypeError):
                    continue

                if not should_forward(payload, allowed_types, org_slug):
                    continue

                yield f"event: {payload['type']}\ndata: {data}\n\n"
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
    finally:
        await r.aclose()
