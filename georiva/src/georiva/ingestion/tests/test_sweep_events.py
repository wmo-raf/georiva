"""Sweep interventions are recorded and keep the chain (ADR 0030, #428)."""

from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.utils import timezone as dj_timezone

from georiva.core.storage import BucketType
from georiva.events.context import current_context
from georiva.events.models import SweepIntervened
from georiva.events.testing import event_context
from georiva.ingestion.models import FileIngestion, FileIngestionFinished
from georiva.ingestion.tasks import sweep_unprocessed
from georiva.ingestion.unprocessed import UnprocessedFile
from georiva.organisations.testing import make_organisation

KEY = "kenya/chirps/rainfall/rain.tif"


def _sweep(found=()):
    seen = []
    with (
        patch("georiva.ingestion.unprocessed.find_unprocessed", return_value=list(found)),
        patch("georiva.ingestion.tasks.process_incoming_file") as task,
    ):
        task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
        sweep_unprocessed.apply()
    return seen


class IngestionSweepTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")

    def _failed_file(self):
        with event_context(organisation_id=self.kenya.pk) as ctx:
            FileIngestion.register(BucketType.SOURCES, KEY)
            FileIngestion.acquire(BucketType.SOURCES, KEY, "w")
            FileIngestion.mark_failed(BucketType.SOURCES, KEY, "boom")
        return ctx

    def test_a_retried_failed_file_is_recorded_and_stays_in_its_chain(self):
        ctx = self._failed_file()

        (context,) = _sweep()

        intervention = SweepIntervened.objects.get()
        self.assertEqual((intervention.sweep_name, intervention.reason), ("unprocessed", "failed_file_retried"))
        self.assertEqual(intervention.target_kind, "file_ingestion")
        self.assertEqual(intervention.target_label, f"sources:{KEY}")
        self.assertEqual(intervention.correlation_id, ctx.correlation_id)
        self.assertEqual(intervention.organisation_id, self.kenya.pk)
        self.assertEqual(intervention.actor, "sweep:unprocessed")
        self.assertIsNone(intervention.caused_by_id)
        self.assertEqual((context.correlation_id, context.actor), (ctx.correlation_id, "sweep:unprocessed"))

    def test_an_untracked_file_found_in_the_bucket_is_recorded(self):
        found = [UnprocessedFile(bucket=BucketType.SOURCES, file_path=KEY, reason="untracked")]

        _sweep(found)

        intervention = SweepIntervened.objects.get()
        self.assertEqual(intervention.reason, "unprocessed_file_found")
        self.assertEqual(intervention.organisation_id, self.kenya.pk)  # a fresh chain, owned by the key's org

    def test_a_stale_lock_reset_is_recorded_per_file(self):
        ctx = self._failed_file()
        FileIngestion.objects.update(
            status=FileIngestion.Status.PROCESSING, locked_at=dj_timezone.now() - timedelta(hours=2)
        )

        FileIngestion.reset_stale_locks(sweep="unprocessed")

        intervention = SweepIntervened.objects.get()
        self.assertEqual(intervention.reason, "stale_lock_reclaimed")
        self.assertEqual(intervention.correlation_id, ctx.correlation_id)

    def test_a_pass_that_redispatches_nothing_writes_nothing(self):
        _sweep()
        FileIngestion.reset_stale_locks(sweep="unprocessed")
        self.assertFalse(SweepIntervened.objects.exists())

    def test_the_revived_files_next_event_shares_the_chain_and_carries_the_sweeps_actor(self):
        ctx = self._failed_file()
        (context,) = _sweep()

        with event_context(
            actor=context.actor, organisation_id=context.organisation_id, correlation_id=context.correlation_id
        ):
            FileIngestion.acquire(BucketType.SOURCES, KEY, "w2")
            FileIngestion.mark_completed(BucketType.SOURCES, KEY, items_created=1)

        events = FileIngestionFinished.objects.order_by("occurred_at")
        self.assertEqual({e.correlation_id for e in events}, {ctx.correlation_id})
        self.assertEqual(events.last().actor, "sweep:unprocessed")
