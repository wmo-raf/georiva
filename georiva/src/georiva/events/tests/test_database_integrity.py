"""Append-only in Postgres itself (ADR 0030, #421).

These tests go through a raw cursor on purpose: the model guard stops the ORM
before the trigger is ever reached, so only SQL can show the trigger is there.
"""

import datetime
from io import StringIO

from django.core.management import call_command
from django.db import DatabaseError, connection, transaction
from django.db.models.deletion import ProtectedError
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.events.models import EventIndex, EventsPurged
from georiva.ingestion.models import RunIngestion, RunIngestionClosed
from georiva.organisations.provisioning import provision_organisation

REF = datetime.datetime(2026, 9, 2, 12, tzinfo=datetime.UTC)


def _closed_run(organisation, slug="forecast"):
    catalog = Catalog.objects.create(organisation=organisation, name=slug, slug=slug, file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name=slug, slug=slug, is_forecast=True)
    run = RunIngestion.objects.create(collection=collection, reference_time=REF)
    run.close(RunIngestion.Closer.QUIET_PERIOD)
    return run


def _raw(sql, params=()):
    """Run ``sql`` in a savepoint so a rejection does not poison the test's transaction."""
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount


class TriggerTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.org = provision_organisation(name="Kenya Met", slug="kenya")
        cls.model_run = _closed_run(cls.org)
        cls.event = RunIngestionClosed.objects.get()

    def test_raw_update_of_a_data_column_is_rejected(self):
        with self.assertRaises(DatabaseError):
            _raw("UPDATE georivaingestion_runingestionclosed SET revision = 9")

    def test_raw_delete_is_rejected(self):
        with self.assertRaises(DatabaseError):
            _raw("DELETE FROM georivaingestion_runingestionclosed")

    def test_raw_update_of_the_organisation_is_rejected(self):
        with self.assertRaises(DatabaseError):
            _raw("UPDATE georivaingestion_runingestionclosed SET organisation_id = NULL")

    def test_the_index_table_is_protected_too(self):
        with self.assertRaises(DatabaseError):
            _raw("UPDATE georivaevents_eventindex SET outcome_class = 'failed'")
        with self.assertRaises(DatabaseError):
            _raw("DELETE FROM georivaevents_eventindex")

    def test_setting_a_nullable_reference_to_null_is_accepted(self):
        self.assertEqual(_raw("UPDATE georivaingestion_runingestionclosed SET run_id = NULL"), 1)

    def test_setting_a_nullable_reference_to_something_else_is_rejected(self):
        other = RunIngestion.objects.create(collection=self.model_run.collection, reference_time=REF.replace(hour=18))
        with self.assertRaises(DatabaseError):
            _raw("UPDATE georivaingestion_runingestionclosed SET run_id = %s", [other.pk])

    def test_deleting_a_collection_keeps_its_events_with_the_link_nulled(self):
        self.model_run.collection.delete()

        event = RunIngestionClosed.objects.get(pk=self.event.pk)
        self.assertIsNone(event.run_id)
        self.assertEqual(event.collection_slug, "forecast")

    def test_an_organisation_with_events_cannot_be_deleted(self):
        with self.assertRaises(ProtectedError):
            self.org.delete()


class PurgeTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        _closed_run(cls.kenya)
        _closed_run(cls.uganda)

    def _purge(self, before="2999-01-01", organisation="kenya", **kwargs):
        call_command("purge_events", organisation=organisation, before=before, stdout=StringIO(), **kwargs)

    def test_removes_exactly_the_scoped_events_and_index_rows(self):
        self._purge()

        self.assertFalse(RunIngestionClosed.objects.filter(organisation=self.kenya).exists())
        self.assertEqual(RunIngestionClosed.objects.filter(organisation=self.uganda).count(), 1)
        self.assertEqual(EventIndex.objects.filter(organisation=self.kenya, event_type__contains="closed").count(), 0)
        self.assertEqual(EventIndex.objects.filter(organisation=self.uganda, event_type__contains="closed").count(), 1)

    def test_events_after_the_cutoff_survive(self):
        self._purge(before="2000-01-01")
        self.assertEqual(RunIngestionClosed.objects.filter(organisation=self.kenya).count(), 1)

    def test_leaves_one_purged_event_naming_organisation_cutoff_counts_and_actor(self):
        self._purge()

        purged = EventsPurged.objects.get()
        self.assertEqual(purged.organisation, self.kenya)
        self.assertEqual(purged.cutoff.date(), datetime.date(2999, 1, 1))
        self.assertEqual(purged.counts, {"georivaingestion.runingestionclosed": 1})
        self.assertEqual(purged.actor, "system:purge_events")
        self.assertTrue(EventIndex.objects.filter(event_id=purged.pk).exists())

    def test_the_trigger_is_active_again_afterwards(self):
        self._purge()
        with self.assertRaises(DatabaseError):
            _raw("DELETE FROM georivaingestion_runingestionclosed")
        with self.assertRaises(DatabaseError):
            _raw("DELETE FROM georivaevents_eventspurged")

    def test_purge_events_are_themselves_never_purged(self):
        self._purge()
        self._purge()
        self.assertEqual(EventsPurged.objects.count(), 2)
