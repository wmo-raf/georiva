"""The recording contract (ADR 0030): what an outside observer sees after record()."""

import datetime

from django.db import transaction
from django.test import TestCase

from georiva.events.context import NoEventContext, current_context
from georiva.events.models import AppendOnlyError, EventIndex
from georiva.events.recording import record
from georiva.events.testing import event_context
from georiva.ingestion.models import RunIngestionClosed
from georiva.organisations.testing import make_organisation


def _closed(**overrides):
    fields = dict(
        collection_slug="c",
        catalog_slug="k",
        reference_time=datetime.datetime(2026, 9, 2, 12, tzinfo=datetime.UTC),
        revision=0,
        closer="manual",
    )
    fields.update(overrides)
    return record(RunIngestionClosed, **fields)


class RecordTests(TestCase):
    def test_recording_without_a_context_raises(self):
        with self.assertRaises(NoEventContext):
            _closed()

    def test_the_event_takes_its_base_fields_from_the_context(self):
        org = make_organisation()
        with event_context(actor="user:7", organisation_id=org.pk) as ctx:
            event = _closed()
        self.assertEqual(event.correlation_id, ctx.correlation_id)
        self.assertEqual(event.actor, "user:7")
        self.assertEqual(event.organisation_id, org.pk)
        self.assertIsNone(event.caused_by_id)

    def test_an_index_row_is_written_beside_the_event(self):
        with event_context():
            event = _closed()
        row = EventIndex.objects.get(pk=event.pk)
        self.assertEqual(row.event_type, "georivaingestion.runingestionclosed")
        self.assertEqual(row.correlation_id, event.correlation_id)
        self.assertEqual(row.outcome_class, "ok")

    def test_event_and_index_row_roll_back_together(self):
        with event_context():
            try:
                with transaction.atomic():
                    _closed()
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
        self.assertFalse(RunIngestionClosed.objects.exists())
        self.assertFalse(EventIndex.objects.exists())

    def test_the_context_is_restored_when_the_block_ends(self):
        self.assertIsNone(current_context())
        with event_context():
            self.assertIsNotNone(current_context())
        self.assertIsNone(current_context())


class AppendOnlyGuardTests(TestCase):
    def setUp(self):
        with event_context():
            self.event = _closed()

    def test_saving_an_existing_event_raises(self):
        self.event.revision = 5
        with self.assertRaises(AppendOnlyError):
            self.event.save()

    def test_deleting_an_event_raises(self):
        with self.assertRaises(AppendOnlyError):
            self.event.delete()

    def test_queryset_update_raises(self):
        with self.assertRaises(AppendOnlyError):
            RunIngestionClosed.objects.update(revision=9)

    def test_queryset_delete_raises(self):
        with self.assertRaises(AppendOnlyError):
            RunIngestionClosed.objects.all().delete()

    def test_bulk_update_raises(self):
        with self.assertRaises(AppendOnlyError):
            RunIngestionClosed.objects.bulk_update([self.event], ["revision"])

    def test_the_index_is_guarded_too(self):
        with self.assertRaises(AppendOnlyError):
            EventIndex.objects.all().delete()
