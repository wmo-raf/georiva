"""The History panel on the model-run page (ADR 0030, #420)."""

from datetime import UTC, datetime

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import RunIngestion
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user

REF = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def _run(organisation, slug="forecast"):
    catalog = Catalog.objects.create(organisation=organisation, name=slug, slug=slug, file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name=slug, slug=slug, is_forecast=True)
    return RunIngestion.objects.create(collection=collection, reference_time=REF)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class RunHistoryPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_run = _run(cls.kenya)
        cls.uganda_run = _run(cls.uganda)

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _url(self, run):
        return reverse("run_ingestion_detail", args=[run.pk])

    def test_lists_the_runs_events_newest_first(self):
        self.kenya_run.close(RunIngestion.Closer.QUIET_PERIOD)
        # Reopen: a late file arrives for the closed run.
        RunIngestion.record_file(self.kenya_run.collection, REF)

        response = self.client.get(self._url(self.kenya_run))

        self.assertEqual(response.status_code, 200)
        rows = response.context["history"].rows
        self.assertEqual([row.type_label for row in rows], ["Run reopened", "Run closed"])
        self.assertContains(response, "Closed at revision 0 by quiet period")

    def test_states_when_history_begins(self):
        self.kenya_run.close(RunIngestion.Closer.QUIET_PERIOD)
        response = self.client.get(self._url(self.kenya_run))
        self.assertContains(response, "History is recorded from")

    def test_states_that_nothing_is_recorded_before_the_first_event(self):
        response = self.client.get(self._url(self.kenya_run))
        self.assertContains(response, "No history has been recorded yet")

    def test_shows_nothing_belonging_to_another_organisation(self):
        self.uganda_run.close(RunIngestion.Closer.QUIET_PERIOD)
        self.kenya_run.close(RunIngestion.Closer.QUIET_PERIOD)

        response = self.client.get(self._url(self.kenya_run))

        slugs = {row.event.organisation_id for row in response.context["history"].rows}
        self.assertEqual(slugs, {self.kenya.pk})

    def test_another_organisations_run_page_is_not_found(self):
        self.assertEqual(self.client.get(self._url(self.uganda_run)).status_code, 404)
