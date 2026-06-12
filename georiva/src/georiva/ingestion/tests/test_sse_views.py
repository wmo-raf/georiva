from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()


# =============================================================================
# Cycle 1: Unauthenticated requests are rejected
# =============================================================================

class SSEAuthTests(TestCase):

    def test_unauthenticated_is_rejected(self):
        response = self.client.get(
            "/admin/api/ingestion/events/",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 403)


# =============================================================================
# Cycle 2: Ingestion snapshot shape (FileIngestion-keyed)
# =============================================================================

class IngestionSnapshotShapeTests(TestCase):

    def setUp(self):
        from georiva.ingestion.models import FileIngestion

        self.fi_completed = FileIngestion.objects.create(
            bucket="incoming",
            file_path="cat/col/file.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        self.fi_pending = FileIngestion.objects.create(
            bucket="incoming",
            file_path="cat/col/file2.grib2",
            status=FileIngestion.Status.PENDING,
        )

    def test_snapshot_returns_list(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)()
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)

    def test_snapshot_includes_active_file_ingestions(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)()
        ids = [r["id"] for r in result]
        self.assertIn(self.fi_pending.pk, ids)

    def test_snapshot_item_has_required_fields(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)()
        item = next(r for r in result if r["id"] == self.fi_pending.pk)
        for field in ("id", "status", "bucket", "file_path", "created_at", "job_id", "job_state"):
            self.assertIn(field, item)

    def test_snapshot_item_has_summary_fields(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)()
        item = next(r for r in result if r["id"] == self.fi_completed.pk)
        for field in ("variables_discovered", "valid_time_start", "valid_time_end", "timestep_count"):
            self.assertIn(field, item)

    def test_snapshot_caps_terminal_file_ingestions(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(terminal_limit=0)
        statuses = {r["status"] for r in result}
        self.assertNotIn("completed", statuses)
        self.assertIn("pending", statuses)


# =============================================================================
# Cycle 3: Authenticated connect delivers snapshot as first SSE message
# =============================================================================

class SSESnapshotOnConnectTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "admin@test.com", "pw")

    @staticmethod
    def _parse_sse_events(raw: bytes) -> list[dict]:
        import json
        events = []
        current = {}
        for line in raw.decode().splitlines():
            if line.startswith("event:"):
                current["event"] = line[len("event:"):].strip()
            elif line.startswith("data:"):
                current["data"] = json.loads(line[len("data:"):].strip())
            elif line == "" and current:
                events.append(current)
                current = {}
        return events

    @staticmethod
    async def _read_snapshot_chunk(response) -> bytes:
        async for chunk in response.streaming_content:
            if b"event: snapshot" in chunk:
                return chunk
        return b""

    async def test_authenticated_connect_returns_streaming_response(self):
        await self.async_client.aforce_login(self.user)
        response = await self.async_client.get("/admin/api/ingestion/events/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get("Content-Type"), "text/event-stream")

    async def test_first_event_is_snapshot(self):
        await self.async_client.aforce_login(self.user)
        response = await self.async_client.get("/admin/api/ingestion/events/")

        chunk = await self._read_snapshot_chunk(response)
        events = self._parse_sse_events(chunk)

        self.assertGreater(len(events), 0)
        self.assertEqual(events[0]["event"], "snapshot")
        self.assertIsInstance(events[0]["data"], list)


# =============================================================================
# Cycle 4: Live Redis events are forwarded as typed SSE messages
# =============================================================================

class SSELiveEventForwardingTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "admin2@test.com", "pw")

    async def test_published_event_appears_in_stream(self):
        import asyncio
        import json
        import redis.asyncio as aioredis
        from django.conf import settings
        from georiva.ingestion.events import CHANNEL

        await self.async_client.aforce_login(self.user)
        response = await self.async_client.get("/admin/api/ingestion/events/")

        async def _collect_next_event_after_snapshot():
            skipped_snapshot = False
            async for chunk in response.streaming_content:
                decoded = chunk.decode()
                if not skipped_snapshot:
                    if "event: snapshot" in decoded:
                        skipped_snapshot = True
                    continue
                stripped = decoded.strip()
                if stripped and not stripped.startswith(":"):
                    return decoded
            return ""

        async def _publish_after_delay():
            await asyncio.sleep(0.1)
            r = aioredis.from_url(settings.REDIS_URL)
            payload = json.dumps({"type": "file_ingestion.status_changed", "id": 99, "status": "completed"})
            await r.publish(CHANNEL, payload)
            await r.aclose()

        _, chunk = await asyncio.gather(
            _publish_after_delay(),
            _collect_next_event_after_snapshot(),
        )

        self.assertIn("event: file_ingestion.status_changed", chunk)
        self.assertIn('"status": "completed"', chunk)
