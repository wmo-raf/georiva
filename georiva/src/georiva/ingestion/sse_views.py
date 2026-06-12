import json
import logging

from django.http import StreamingHttpResponse

logger = logging.getLogger(__name__)

_KEEPALIVE_SECS = 25

_INGESTION_EVENT_TYPES = frozenset([
    "file_ingestion.created",
    "file_ingestion.status_changed",
    "job.state_changed",
    "job.progress_updated",
    "snapshot",
])

_ACQUISITION_EVENT_TYPES = frozenset([
    "fetch_run.created",
    "fetch_run.status_changed",
    "fetched_file.status_changed",
    "upload_session.created",
    "upload_session.status_changed",
    "uploaded_file.status_changed",
])


def ingestion_events_sse(request):
    """Async SSE endpoint streaming ingestion (FileIngestion/Job) events."""
    return StreamingHttpResponse(
        _event_stream(_INGESTION_EVENT_TYPES, _build_ingestion_snapshot),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def acquisition_events_sse(request):
    """Async SSE endpoint streaming acquisition (FetchRun/UploadSession) events."""
    return StreamingHttpResponse(
        _event_stream(_ACQUISITION_EVENT_TYPES, _build_acquisition_snapshot),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _build_ingestion_snapshot():
    from .snapshot import build_ingestion_snapshot
    return await build_ingestion_snapshot()


async def _build_acquisition_snapshot():
    from .acquisition_snapshot import build_acquisition_snapshot
    return await build_acquisition_snapshot()


async def _event_stream(allowed_types, snapshot_fn):
    from django.conf import settings
    import redis.asyncio as aioredis

    from .events import CHANNEL

    r = aioredis.from_url(settings.REDIS_URL)
    try:
        pubsub = r.pubsub()
        await pubsub.subscribe(CHANNEL)

        snapshot = await snapshot_fn()
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
                    event_type = payload.get("type", "ingestion")
                except (ValueError, AttributeError):
                    event_type = "ingestion"

                # Each SSE endpoint only forwards events relevant to its feed.
                if event_type not in allowed_types:
                    continue

                yield f"event: {event_type}\ndata: {data}\n\n"
        finally:
            await pubsub.unsubscribe(CHANNEL)
            await pubsub.aclose()
    finally:
        await r.aclose()
