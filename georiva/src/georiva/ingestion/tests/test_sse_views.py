from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.ingestion.sse_views import _INGESTION_EVENT_TYPES, should_forward
from georiva.organisations.models import Organisation
from georiva.organisations.testing import DEFAULT_TEST_ORG_SLUG, dial_org

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
            file_path=f"{DEFAULT_TEST_ORG_SLUG}/cat/col/file.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        self.fi_pending = FileIngestion.objects.create(
            bucket="incoming",
            file_path=f"{DEFAULT_TEST_ORG_SLUG}/cat/col/file2.grib2",
            status=FileIngestion.Status.PENDING,
        )

    def test_snapshot_returns_list(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(DEFAULT_TEST_ORG_SLUG)
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)

    def test_snapshot_includes_active_file_ingestions(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(DEFAULT_TEST_ORG_SLUG)
        ids = [r["id"] for r in result]
        self.assertIn(self.fi_pending.pk, ids)

    def test_snapshot_item_has_required_fields(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(DEFAULT_TEST_ORG_SLUG)
        item = next(r for r in result if r["id"] == self.fi_pending.pk)
        for field in ("id", "status", "bucket", "file_path", "created_at", "job_id", "job_state"):
            self.assertIn(field, item)

    def test_snapshot_item_has_summary_fields(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(DEFAULT_TEST_ORG_SLUG)
        item = next(r for r in result if r["id"] == self.fi_completed.pk)
        for field in ("variables_discovered", "valid_time_start", "valid_time_end", "timestep_count"):
            self.assertIn(field, item)

    def test_snapshot_caps_terminal_file_ingestions(self):
        from georiva.ingestion.snapshot import build_ingestion_snapshot

        result = async_to_sync(build_ingestion_snapshot)(DEFAULT_TEST_ORG_SLUG, terminal_limit=0)
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
                current["event"] = line[len("event:") :].strip()
            elif line.startswith("data:"):
                current["data"] = json.loads(line[len("data:") :].strip())
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


class EventForwardingRuleTests(TestCase):
    """The rule itself: one channel, one organisation's events per listener.

    Unit-tested here rather than only through a live stream — the stream emits
    keepalives forever, so an over-eager filter shows up as a hang rather than a
    failure, and this is the assertion that names what went wrong.
    """

    def test_an_event_for_this_organisation_is_forwarded(self):
        self.assertTrue(
            should_forward(
                {"type": "file_ingestion.created", "org": "kenya"},
                _INGESTION_EVENT_TYPES,
                "kenya",
            )
        )

    def test_another_organisations_event_is_dropped(self):
        self.assertFalse(
            should_forward(
                {"type": "file_ingestion.created", "org": "uganda"},
                _INGESTION_EVENT_TYPES,
                "kenya",
            )
        )

    def test_an_unattributed_event_reaches_nobody(self):
        self.assertFalse(
            should_forward(
                {"type": "file_ingestion.created"},
                _INGESTION_EVENT_TYPES,
                "kenya",
            )
        )

    def test_an_event_of_an_uncarried_type_is_dropped(self):
        self.assertFalse(
            should_forward(
                {"type": "some.other_event", "org": "kenya"},
                _INGESTION_EVENT_TYPES,
                "kenya",
            )
        )

    def test_an_unreadable_payload_is_dropped(self):
        self.assertFalse(should_forward("not json at all", _INGESTION_EVENT_TYPES, "kenya"))


class SSELiveEventForwardingTests(TestCase):
    """End to end: a published event reaches the stream of the org it belongs to."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "admin2@test.com", "pw")
        self.org_slugs = list(Organisation.objects.values_list("slug", flat=True))

    async def test_only_this_organisations_event_appears_in_the_stream(self):
        import asyncio
        import json

        import redis.asyncio as aioredis
        from django.conf import settings

        from georiva.ingestion.events import CHANNEL

        await self.async_client.aforce_login(self.user)
        response = await self.async_client.get("/admin/api/ingestion/events/")

        async def _collect():
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

        async def _publish():
            await asyncio.sleep(0.1)
            r = aioredis.from_url(settings.REDIS_URL)
            # A foreign event first, then one for each organisation that exists —
            # whichever of those this host serves is the one that must arrive, so
            # the test never has to guess which organisation the stream resolved.
            await r.publish(
                CHANNEL,
                json.dumps(
                    {
                        "type": "file_ingestion.status_changed",
                        "org": "somebody-else",
                        "id": 1,
                        "status": "failed",
                    }
                ),
            )
            for slug in self.org_slugs:
                await r.publish(
                    CHANNEL,
                    json.dumps(
                        {
                            "type": "file_ingestion.status_changed",
                            "org": slug,
                            "id": 2,
                            "status": "completed",
                        }
                    ),
                )
            await r.aclose()

        # Bounded: a dropped event that should have arrived would otherwise leave
        # the stream emitting keepalives forever and hang the suite.
        _, chunk = await asyncio.wait_for(asyncio.gather(_publish(), _collect()), timeout=20)

        self.assertIn('"id": 2', chunk)
        self.assertNotIn('"id": 1', chunk)
