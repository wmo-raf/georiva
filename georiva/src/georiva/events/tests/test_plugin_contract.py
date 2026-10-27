"""The plugin extension point, proved with a type defined outside the events app (ADR 0030, #432).

Core's tests never import a real plugin: a minimal event type declared here
stands in for one, exactly as a plugin would declare its own.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import patch

from django.db import DatabaseError, connection, transaction
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.events import registry
from georiva.events.context import context_after, use_context
from georiva.events.history import build_history
from georiva.events.models import EventIndex
from georiva.events.recording import record
from georiva.events.testing import event_context
from georiva.events.tests.plugin_event import PluginThingHappened
from georiva.ingestion.domain_signals import run_ingestion_closed
from georiva.ingestion.models import RunIngestion
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user


class PluginContractBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        registry.populate()  # app-ready did this once; a type declared later needs the same call


class PluginEventTests(PluginContractBase):
    def test_the_registry_knows_both_core_and_plugin_types(self):
        keys = {registry.type_key(m) for m in registry.event_types()}
        self.assertIn("georivaevents.pluginthinghappened", keys)
        self.assertIn("georivaingestion.runingestionclosed", keys)

    def test_it_records_through_the_helper_and_gets_an_index_row(self):
        with event_context():
            event = record(PluginThingHappened, note="hello")

        self.assertEqual(EventIndex.objects.get(pk=event.pk).event_type, "georivaevents.pluginthinghappened")
        self.assertEqual(event.summary(), "thing: hello")

    def test_its_table_is_protected_by_the_shared_trigger(self):
        with event_context():
            record(PluginThingHappened, note="x")
        for sql in (
            "UPDATE georivaevents_pluginthinghappened SET note = 'y'",
            "DELETE FROM georivaevents_pluginthinghappened",
        ):
            with self.subTest(sql=sql), self.assertRaises(DatabaseError), transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute(sql)

    def test_it_appears_in_a_history_panel(self):
        org = provision_organisation(name="Kenya Met", slug="kenya")
        with event_context(organisation_id=org.pk):
            record(PluginThingHappened, note="shown")
        request = RequestFactory().get("/admin/")
        request.active_org = org
        request.user = make_user("u")

        history = build_history(request, [(PluginThingHappened, {})])

        self.assertEqual([row.summary for row in history.rows], ["thing: shown"])


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class PluginTimelineAndSignalTests(PluginContractBase):
    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        from django.contrib.auth.models import Group, Permission

        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def test_a_plugin_event_appears_in_the_timeline_beside_core_events(self):
        with event_context(organisation_id=self.kenya.pk) as ctx:
            record(PluginThingHappened, note="from a plugin")

        response = self.client.get(reverse("events_timeline", args=[ctx.correlation_id]))

        labels = [row.entry.row.type_label for row in response.context["layout"].rows]
        self.assertEqual(labels, ["Plugin thing happened"])
        self.assertContains(response, "thing: from a plugin")

    def test_the_timeline_renders_when_a_registered_types_table_is_absent(self):
        absent = uuid.uuid4()
        # A registered type whose table does not exist: a plugin that was uninstalled.
        ghost = SimpleNamespace(_meta=SimpleNamespace(db_table="gone_plugin_ghost"))
        self.enterContext(patch.dict(registry._types, {"gone_plugin.ghost": ghost}))
        EventIndex.objects.create(
            event_id=uuid.uuid4(),
            event_type="gone_plugin.ghost",
            correlation_id=absent,
            occurred_at=timezone.now(),
            organisation=self.kenya,
        )

        response = self.client.get(reverse("events_timeline", args=[absent]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "unavailable on this installation")

    def test_a_signal_receiver_can_record_its_own_event_caused_by_the_one_it_was_handed(self):
        """The shape georiva-publisher-forti uses: ``(sender, run, **kwargs)`` plus the event."""
        calls = []

        def plugin_receiver(sender, run, **kwargs):
            event = kwargs.get("event")
            with use_context(context_after(event, actor="system:plugin")):
                calls.append(record(PluginThingHappened, note=f"published run {run.pk}"))

        run_ingestion_closed.connect(plugin_receiver, dispatch_uid="plugin-contract")
        self.addCleanup(run_ingestion_closed.disconnect, dispatch_uid="plugin-contract")
        catalog = Catalog.objects.create(organisation=self.kenya, name="M", slug="m", file_format="grib2")
        collection = Collection.objects.create(catalog=catalog, name="s", slug="s", is_forecast=True)
        run = RunIngestion.objects.create(collection=collection, reference_time="2026-09-02T12:00:00Z")

        with self.captureOnCommitCallbacks(execute=True):
            with event_context(organisation_id=self.kenya.pk):
                run.close(RunIngestion.Closer.MANUAL)

        from georiva.ingestion.models import RunIngestionClosed

        closed = RunIngestionClosed.objects.get()
        (published,) = calls  # exactly once
        self.assertEqual(published.caused_by_id, closed.pk)
        self.assertEqual(published.correlation_id, closed.correlation_id)
        self.assertEqual(published.organisation_id, self.kenya.pk)
        self.assertEqual(published.actor, "system:plugin")
