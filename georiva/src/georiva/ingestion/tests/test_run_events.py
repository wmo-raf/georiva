"""Model-run history as Domain events (ADR 0030, #420)."""

from unittest import mock

from django.test import TestCase

from georiva.events.models import EventIndex
from georiva.ingestion.domain_signals import run_ingestion_closed, run_ingestion_reopened
from georiva.ingestion.models import RunIngestion, RunIngestionClosed, RunIngestionReopened

from .test_run_ingestion import _collection, _complete_file


class RunEventTests(TestCase):
    def setUp(self):
        self.collection = _collection()
        _complete_file(self.collection, 0)
        self.run = RunIngestion.objects.get()

    def test_closing_writes_exactly_one_event_and_one_index_row(self):
        self.assertTrue(self.run.close(RunIngestion.Closer.DECLARED_SET))

        event = RunIngestionClosed.objects.get()
        self.assertEqual(event.run, self.run)
        self.assertEqual(event.closer, "declared_set")
        self.assertEqual(event.revision, 0)
        self.assertEqual(event.collection_slug, self.collection.slug)
        self.assertEqual(event.catalog_slug, self.collection.catalog.slug)
        self.assertEqual(event.reference_time, self.run.reference_time)
        self.assertEqual(event.organisation_id, self.collection.catalog.organisation_id)
        self.assertEqual(EventIndex.objects.filter(event_id=event.pk).count(), 1)

    def test_two_racing_closers_write_one_event(self):
        first = RunIngestion.objects.get()
        second = RunIngestion.objects.get()

        self.assertTrue(first.close(RunIngestion.Closer.QUIET_PERIOD))
        self.assertFalse(second.close(RunIngestion.Closer.DECLARED_SET))

        self.assertEqual(RunIngestionClosed.objects.count(), 1)

    def test_the_run_row_keeps_revision_and_closer(self):
        self.run.close(RunIngestion.Closer.UPLOAD_SESSION)
        self.run.refresh_from_db()
        self.assertEqual(self.run.closed_by, "upload_session")
        self.assertEqual(self.run.revision, 0)

    def test_a_late_file_reopens_with_the_bumped_revision(self):
        self.run.close(RunIngestion.Closer.QUIET_PERIOD)
        _complete_file(self.collection, 6)

        event = RunIngestionReopened.objects.get()
        self.assertEqual(event.revision, 1)
        self.assertEqual(event.run_id, self.run.pk)

    def test_a_failing_block_leaves_no_state_change_event_or_index_row(self):
        with mock.patch("georiva.ingestion.models.record", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.run.close(RunIngestion.Closer.QUIET_PERIOD)

        self.run.refresh_from_db()
        self.assertEqual(self.run.status, RunIngestion.Status.OPEN)
        self.assertFalse(RunIngestionClosed.objects.exists())
        self.assertFalse(EventIndex.objects.filter(event_type__contains="runingestionclosed").exists())

    def test_deleting_the_collection_keeps_the_history_readable(self):
        self.run.close(RunIngestion.Closer.QUIET_PERIOD)
        self.collection.delete()

        event = RunIngestionClosed.objects.get()
        self.assertIsNone(event.run)
        self.assertEqual(event.collection_slug, self.collection.slug)


class SignalContractTests(TestCase):
    """A receiver shaped like georiva-publisher-forti's still fires, once, after commit."""

    def setUp(self):
        self.collection = _collection()
        self.calls = []

        def plugin_receiver(sender, run, **kwargs):
            self.calls.append((run, kwargs.get("event")))

        self.receiver = plugin_receiver  # keep a strong reference: signals hold weak ones
        run_ingestion_closed.connect(self.receiver, dispatch_uid="plugin-closed")
        run_ingestion_reopened.connect(self.receiver, dispatch_uid="plugin-reopened")
        self.addCleanup(run_ingestion_closed.disconnect, dispatch_uid="plugin-closed")
        self.addCleanup(run_ingestion_reopened.disconnect, dispatch_uid="plugin-reopened")

    def test_closed_fires_once_after_commit_carrying_the_event(self):
        _complete_file(self.collection, 0)
        run = RunIngestion.objects.get()

        with self.captureOnCommitCallbacks(execute=False) as callbacks:
            run.close(RunIngestion.Closer.QUIET_PERIOD)
        self.assertEqual(self.calls, [])

        for callback in callbacks:
            callback()
        self.assertEqual(len(self.calls), 1)
        sent_run, event = self.calls[0]
        self.assertEqual(sent_run.pk, run.pk)
        self.assertEqual(event, RunIngestionClosed.objects.get())

    def test_reopened_fires_once_after_commit_carrying_the_event(self):
        _complete_file(self.collection, 0)
        RunIngestion.objects.get().close(RunIngestion.Closer.QUIET_PERIOD)
        self.calls.clear()

        with self.captureOnCommitCallbacks(execute=True):
            _complete_file(self.collection, 6)

        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1], RunIngestionReopened.objects.get())

    def test_nothing_fires_when_the_transaction_rolls_back(self):
        _complete_file(self.collection, 0)
        run = RunIngestion.objects.get()
        with mock.patch("georiva.ingestion.models.record", side_effect=RuntimeError("boom")):
            with self.captureOnCommitCallbacks(execute=True):
                with self.assertRaises(RuntimeError):
                    run.close(RunIngestion.Closer.QUIET_PERIOD)
        self.assertEqual(self.calls, [])
