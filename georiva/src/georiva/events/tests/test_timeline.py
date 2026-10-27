"""The Timeline page for one chain (ADR 0030, #429)."""

import datetime
import uuid

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.events.models import EventIndex
from georiva.events.testing import event_context
from georiva.events.timeline import timeline_url
from georiva.ingestion.models import RunIngestion, RunIngestionClosed
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.processing.models import DerivationRun

REF = datetime.datetime(2026, 9, 2, 12, tzinfo=datetime.UTC)


def _run(organisation, slug="forecast"):
    catalog = Catalog.objects.create(organisation=organisation, name=slug, slug=slug, file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name=slug, slug=slug, is_forecast=True)
    return RunIngestion.objects.create(collection=collection, reference_time=REF)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class TimelineTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_run = _run(cls.kenya)
        cls.uganda_run = _run(cls.uganda)
        cls.chain = uuid.uuid4()
        with event_context(actor="system:bucket", organisation_id=cls.kenya.pk, correlation_id=cls.chain):
            cls.kenya_run.close(RunIngestion.Closer.MANUAL)
            cls.attempt = DerivationRun.acquire(
                recipe_type="promotion", recipe_version="1", unit_key={}, unit_hash="k" * 64
            ).mark_failed("boom")
        with event_context(actor="system:bucket", organisation_id=cls.uganda.pk, correlation_id=cls.chain):
            cls.uganda_run.close(RunIngestion.Closer.MANUAL)
        cls.uganda_only = uuid.uuid4()
        with event_context(organisation_id=cls.uganda.pk, correlation_id=cls.uganda_only):
            _run(cls.uganda, "other").close(RunIngestion.Closer.MANUAL)

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _get(self, correlation):
        return self.client.get(reverse("events_timeline", args=[correlation]))

    def test_it_draws_the_chain_oldest_first_with_actor_and_links(self):
        response = self._get(self.chain)

        self.assertEqual(response.status_code, 200)
        rows = [row.entry.row for row in response.context["layout"].rows]
        self.assertEqual([row.type_label for row in rows], ["Run closed", "Derivation attempt"])
        self.assertEqual(rows[0].actor, "system:bucket")
        self.assertEqual(rows[0].subject_url, reverse("run_ingestion_detail", args=[self.kenya_run.pk]))
        # A failed attempt produced nothing, so there is no lineage to offer.
        self.assertEqual(rows[1].links, [])
        self.assertContains(response, "1 failed")

    def test_a_derivation_attempt_that_produced_an_item_links_to_its_inputs(self):
        from georiva.events.recording import record
        from georiva.processing.models import DerivationAttemptFinished

        chain = uuid.uuid4()
        with event_context(organisation_id=self.kenya.pk, correlation_id=chain):
            attempt = record(
                DerivationAttemptFinished,
                outcome="completed",
                recipe_type="promotion",
                recipe_version="1",
                unit_hash="c" * 64,
                attempt_number=1,
                retry_reason="initial",
                produced_item_id=7,
            )

        response = self._get(chain)

        self.assertContains(response, "See inputs")
        self.assertContains(response, reverse("derivation_attempt_inputs", args=[attempt.pk]))

    def test_another_organisations_events_are_never_shown_only_a_marker(self):
        response = self._get(self.chain)

        self.assertTrue(response.context["continues_elsewhere"])
        self.assertContains(response, "continues elsewhere")
        self.assertNotContains(response, "uganda")
        self.assertEqual(response.context["layout"].size, 2)

    def test_a_chain_entirely_outside_the_viewers_scope_is_not_found(self):
        self.assertEqual(self._get(self.uganda_only).status_code, 404)

    def test_an_unknown_correlation_is_not_found(self):
        self.assertEqual(self._get(uuid.uuid4()).status_code, 404)

    def test_a_row_whose_subject_was_deleted_still_reads_from_its_snapshot(self):
        self.kenya_run.collection.delete()

        response = self._get(self.chain)

        self.assertEqual(response.status_code, 200)
        closed = response.context["layout"].rows[0].entry.row
        self.assertIsNone(closed.subject_url)
        self.assertIn("Closed at revision 0", closed.summary)

    def test_an_indexed_event_whose_type_is_not_installed_still_renders(self):
        EventIndex.objects.create(
            event_id=uuid.uuid4(),
            event_type="gone_plugin.somethingthathappened",
            correlation_id=self.chain,
            occurred_at=timezone.now(),
            organisation=self.kenya,
        )

        response = self._get(self.chain)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "unavailable on this installation")

    def test_history_rows_link_to_their_own_event_on_the_timeline(self):
        closed = RunIngestionClosed.objects.get(organisation=self.kenya, correlation_id=self.chain)

        response = self.client.get(reverse("run_ingestion_detail", args=[self.kenya_run.pk]))

        self.assertContains(response, timeline_url(self.chain, closed.pk))

    def test_it_has_no_menu_entry(self):
        from wagtail.admin.menu import admin_menu

        urls = [item.url for item in admin_menu.registered_menu_items if getattr(item, "url", None)]
        self.assertFalse([url for url in urls if "/events/timeline" in url], urls)

    def test_a_cause_that_is_not_visible_is_not_named(self):
        # Caused-by pointing at an event of another organisation reveals nothing.
        other = RunIngestionClosed.objects.get(organisation=self.uganda, correlation_id=self.chain)
        with event_context(organisation_id=self.kenya.pk, correlation_id=self.chain, caused_by=other.pk):
            _run(self.kenya, "third").close(RunIngestion.Closer.MANUAL)

        response = self._get(self.chain)

        self.assertEqual(len([row for row in response.context["layout"].rows if row.stub]), 1)
        self.assertContains(response, "caused by an event outside this view")

    def test_events_hang_from_their_cause(self):
        closed = RunIngestionClosed.objects.get(organisation=self.kenya, correlation_id=self.chain)
        with event_context(organisation_id=self.kenya.pk, correlation_id=self.chain, caused_by=closed.pk):
            _run(self.kenya, "third").close(RunIngestion.Closer.MANUAL)

        rows = self._get(self.chain).context["layout"].rows

        effect = rows[-1]
        self.assertEqual(effect.entry.caused_by_id, closed.pk)
        self.assertFalse(effect.stub)

    def test_a_target_event_is_marked(self):
        from georiva.processing.models import DerivationAttemptFinished

        attempt = DerivationAttemptFinished.objects.get(correlation_id=self.chain)

        response = self.client.get(reverse("events_timeline", args=[self.chain]), {"event": str(attempt.pk)})

        targets = [row.entry.id for row in response.context["layout"].rows if row.target]
        self.assertEqual(targets, [attempt.pk])

    def test_focus_draws_only_what_one_event_set_in_motion(self):
        closed = RunIngestionClosed.objects.get(organisation=self.kenya, correlation_id=self.chain)

        response = self.client.get(reverse("events_timeline", args=[self.chain]), {"from": str(closed.pk)})

        self.assertEqual([row.entry.id for row in response.context["layout"].rows], [closed.pk])
        self.assertContains(response, "Show the whole chain")

    def test_focus_on_an_event_outside_the_chain_is_not_found(self):
        response = self.client.get(reverse("events_timeline", args=[self.chain]), {"from": str(uuid.uuid4())})

        self.assertEqual(response.status_code, 404)

    def test_a_malformed_parameter_is_ignored(self):
        response = self.client.get(reverse("events_timeline", args=[self.chain]), {"from": "nope", "event": "x"})

        self.assertEqual(response.status_code, 200)

    def test_a_fetch_that_brought_many_files_folds_them(self):
        from georiva.events.recording import record
        from georiva.ingestion.models import FileIngestionFinished

        chain = uuid.uuid4()
        with event_context(organisation_id=self.kenya.pk, correlation_id=chain):
            for step in range(5):
                record(
                    FileIngestionFinished,
                    bucket="incoming",
                    file_path=f"kenya/forecast/f{step:03d}.grib2",
                    outcome=FileIngestionFinished.Outcome.COMPLETED,
                )

        response = self._get(chain)

        group = response.context["layout"].rows[0].group
        self.assertEqual(len(group.members), 5)
        self.assertContains(response, "5 × ")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class AttemptInputsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        with event_context(organisation_id=cls.kenya.pk):
            cls.kenya_attempt = DerivationRun.acquire(
                recipe_type="promotion", recipe_version="1", unit_key={}, unit_hash="a" * 64
            ).mark_failed("x")
        with event_context(organisation_id=cls.uganda.pk):
            cls.uganda_attempt = DerivationRun.acquire(
                recipe_type="promotion", recipe_version="1", unit_key={}, unit_hash="b" * 64
            ).mark_failed("x")

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def test_an_attempt_without_a_product_has_a_clear_empty_state(self):
        response = self.client.get(reverse("derivation_attempt_inputs", args=[self.kenya_attempt.pk]))
        self.assertContains(response, "No inputs are recorded")

    def test_another_organisations_attempt_is_not_found(self):
        response = self.client.get(reverse("derivation_attempt_inputs", args=[self.uganda_attempt.pk]))
        self.assertEqual(response.status_code, 404)
