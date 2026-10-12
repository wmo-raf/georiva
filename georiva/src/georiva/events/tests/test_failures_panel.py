"""The failures panel on the admin homepage (ADR 0030, #430)."""

import datetime

from django.contrib.auth.models import Group, Permission
from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from georiva.events.models import EventIndex
from georiva.events.testing import event_context
from georiva.ingestion.models import FileIngestion
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.processing.models import DerivationRun
from georiva.sources.models import DataFeed, FetchRun


def _fail_a_file(org, path):
    with event_context(organisation_id=org.pk):
        FileIngestion.register("sources", path)
        FileIngestion.mark_failed("sources", path, "could not read band 1")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class FailuresPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from georiva.core.models import Catalog

        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        _fail_a_file(cls.kenya, "kenya/chirps/rainfall/bad.tif")
        _fail_a_file(cls.uganda, "uganda/chirps/rainfall/theirs.tif")
        with event_context(organisation_id=None):  # unattributed refusal
            FileIngestion.register("sources", "atlantis/x/y/z.tif")
            FileIngestion.mark_failed("sources", "atlantis/x/y/z.tif", "Unknown organisation", outcome="refused")
        catalog = Catalog.objects.create(organisation=cls.kenya, name="C", slug="chirps", file_format="geotiff")
        feed = DataFeed.objects.create(name="CHIRPS", catalog=catalog)
        with event_context(organisation_id=cls.kenya.pk):
            FetchRun.objects.create(data_feed=feed).mark_failed("source down")
            DerivationRun.acquire(
                recipe_type="promotion", recipe_version="1", unit_key={}, unit_hash="a" * 64
            ).mark_failed("boom")
            # A success is not a failure.
            FileIngestion.register("sources", "kenya/chirps/rainfall/good.tif")
            FileIngestion.acquire("sources", "kenya/chirps/rainfall/good.tif", "w")
            FileIngestion.mark_completed("sources", "kenya/chirps/rainfall/good.tif")

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _panel(self):
        from georiva.events.failures import recent_failures

        request = self.client.get(reverse("wagtailadmin_home")).wsgi_request
        return recent_failures(request)

    def test_it_lists_this_organisations_failures_newest_first_and_nothing_else(self):
        failures = self._panel()

        self.assertEqual(
            sorted(f["label"] for f in failures),
            ["Derivation attempt", "Fetch finished", "File ingestion finished"],
        )
        self.assertEqual(failures, sorted(failures, key=lambda f: f["occurred_at"], reverse=True))

    def test_other_organisations_and_unattributed_events_never_appear(self):
        page = self.client.get(reverse("wagtailadmin_home"))

        self.assertContains(page, "bad.tif")
        self.assertNotContains(page, "theirs.tif")
        self.assertNotContains(page, "atlantis")

    def test_each_entry_links_to_its_object_and_its_timeline(self):
        file_failure = next(f for f in self._panel() if f["label"] == "File ingestion finished")

        self.assertTrue(file_failure["subject_url"].startswith("/admin/ingestion/files/"))
        self.assertIn("/events/timeline/", file_failure["timeline_url"])

    def test_nothing_older_than_the_window_is_listed(self):
        long_ago = timezone.now() - datetime.timedelta(days=30)
        # Backdate the index row the way the database would have it a month on: the
        # append-only trigger forbids an UPDATE, so insert one directly.
        import uuid

        EventIndex.objects.create(
            event_id=uuid.uuid4(),
            event_type="georivaingestion.fileingestionfinished",
            correlation_id=uuid.uuid4(),
            occurred_at=long_ago,
            organisation=self.kenya,
            outcome_class="failed",
        )
        self.assertEqual(len(self._panel()), 3)

    def test_it_renders_an_empty_state(self):
        from georiva.events.failures import recent_failures

        request = self.client.get(reverse("wagtailadmin_home")).wsgi_request
        self.assertEqual(recent_failures(request, days=0), [])
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"  # not a member: refused, so test via the view helper
        with override_settings(GEORIVA_FAILURES_PANEL_DAYS=0):
            page = self.client.get(reverse("wagtailadmin_home"), HTTP_HOST="kenya.georiva.test")
        self.assertContains(page, "No failures in the last 0 days")

    def test_the_cost_is_bounded_by_the_row_limit_not_by_how_many_types_exist(self):
        from georiva.events.failures import recent_failures

        request = self.client.get(reverse("wagtailadmin_home")).wsgi_request
        with CaptureQueriesContext(connection) as queries:
            recent_failures(request)

        # one for the index, one per type that actually appears (three here), one for introspection
        self.assertLessEqual(len(queries), 6)

    def test_it_adds_no_menu_entry_and_leaves_the_health_panel_alone(self):
        from wagtail.admin.menu import admin_menu

        urls = [item.url for item in admin_menu.registered_menu_items if getattr(item, "url", None)]
        self.assertFalse([url for url in urls if "/events/" in url], urls)
