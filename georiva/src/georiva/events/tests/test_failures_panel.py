"""The failures panel on the admin homepage and the Failures page behind it (ADR 0030, ADR 0033)."""

import datetime
import uuid

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


def _fail_a_file(org, path, outcome="failed", error="could not read band 1"):
    with event_context(organisation_id=org.pk if org else None):
        FileIngestion.register("sources", path)
        FileIngestion.mark_failed("sources", path, error, outcome=outcome)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class FailuresTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from georiva.core.models import Catalog

        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        _fail_a_file(cls.kenya, "kenya/chirps/rainfall/bad.tif")
        _fail_a_file(cls.uganda, "uganda/chirps/rainfall/theirs.tif")
        # unattributed refusal
        _fail_a_file(None, "atlantis/x/y/z.tif", outcome="refused", error="Unknown organisation")
        catalog = Catalog.objects.create(organisation=cls.kenya, name="C", slug="chirps", file_format="geotiff")
        cls.feed = DataFeed.objects.create(name="CHIRPS", catalog=catalog)
        with event_context(organisation_id=cls.kenya.pk):
            FetchRun.objects.create(data_feed=cls.feed).mark_failed("source down")
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

    def _request(self):
        return self.client.get(reverse("wagtailadmin_home")).wsgi_request

    def _panel(self, **kwargs):
        from georiva.events.failures import failure_groups

        return failure_groups(self._request(), **kwargs)

    def _group(self, label):
        return next(g for g in self._panel().groups if g.label == label)


class FailureGroupKeyTests(FailuresTestCase):
    """``record()`` writes each event's failure group key onto its index row."""

    def _key(self, event_type):
        return EventIndex.objects.get(
            event_type=event_type, organisation=self.kenya, outcome_class="failed"
        ).failure_group_key

    def test_a_file_failure_rolls_up_under_its_collection(self):
        self.assertEqual(self._key("georivaingestion.fileingestionfinished"), "collection:chirps/rainfall")

    def test_a_fetch_failure_rolls_up_under_its_feed(self):
        self.assertEqual(self._key("georivasources.fetchrunfinished"), f"feed:{self.feed.pk}")

    def test_a_derivation_failure_without_a_product_rolls_up_under_its_recipe(self):
        self.assertEqual(self._key("georivaprocessing.derivationattemptfinished"), "recipe:promotion")

    def test_successes_carry_a_key_too(self):
        completed = EventIndex.objects.get(
            event_type="georivaingestion.fileingestionfinished", organisation=self.kenya, outcome_class="ok"
        )
        self.assertEqual(completed.failure_group_key, "collection:chirps/rainfall")


class FailuresPanelTests(FailuresTestCase):
    def test_it_lists_one_line_per_group_for_this_organisation_newest_first(self):
        groups = self._panel().groups

        self.assertEqual(sorted(g.label for g in groups), ["CHIRPS", "chirps/rainfall", "promotion"])
        self.assertEqual(groups, sorted(groups, key=lambda g: g.latest.occurred_at, reverse=True))

    def test_repeated_failures_of_one_subject_collapse_into_a_count(self):
        for name in ("a", "b", "c"):
            _fail_a_file(self.kenya, f"kenya/chirps/rainfall/{name}.tif")

        group = self._group("chirps/rainfall")
        self.assertEqual((group.failed, group.refused), (4, 0))
        self.assertTrue(group.latest.summary.startswith("Failed kenya/chirps/rainfall/c.tif"))

    def test_refusals_are_counted_apart_from_failures(self):
        _fail_a_file(self.kenya, "kenya/chirps/rainfall/old.tif", outcome="refused", error="No timestamps found")

        group = self._group("chirps/rainfall")
        self.assertEqual((group.failed, group.refused), (1, 1))

    def test_different_collections_are_different_groups(self):
        _fail_a_file(self.kenya, "kenya/chirps/tmax/bad.tif")

        labels = [g.label for g in self._panel().groups]
        self.assertIn("chirps/rainfall", labels)
        self.assertIn("chirps/tmax", labels)

    def test_each_group_links_to_its_subject_its_list_and_its_latest_timeline(self):
        feed = self._group("CHIRPS")

        self.assertEqual(feed.url, reverse("data_feed_detail", args=[self.feed.pk]))
        self.assertEqual(
            feed.list_url,
            reverse("events_failures") + f"?type=georivasources.fetchrunfinished&group=feed%3A{self.feed.pk}",
        )
        self.assertIn("/events/timeline/", feed.latest.timeline_url)

    def test_other_organisations_and_unattributed_events_never_appear(self):
        page = self.client.get(reverse("wagtailadmin_home"))

        self.assertContains(page, "bad.tif")
        self.assertNotContains(page, "theirs.tif")
        self.assertNotContains(page, "atlantis")

    def test_another_organisations_failures_never_join_this_ones_group(self):
        self.assertEqual(self._group("chirps/rainfall").failed, 1)

    def test_nothing_older_than_the_window_is_counted(self):
        # The append-only trigger forbids backdating an UPDATE, so insert an old row directly.
        EventIndex.objects.create(
            event_id=uuid.uuid4(),
            event_type="georivaingestion.fileingestionfinished",
            correlation_id=uuid.uuid4(),
            occurred_at=timezone.now() - datetime.timedelta(days=30),
            organisation=self.kenya,
            outcome_class="failed",
            failure_group_key="collection:chirps/rainfall",
        )
        self.assertEqual(self._group("chirps/rainfall").failed, 1)

    def test_the_number_of_lines_is_capped_and_the_rest_is_counted(self):
        for i in range(12):
            _fail_a_file(self.kenya, f"kenya/chirps/c{i}/bad.tif")

        panel = self._panel()
        self.assertEqual(len(panel.groups), 10)
        self.assertEqual(panel.more, 5)
        self.assertContains(self.client.get(reverse("wagtailadmin_home")), "and 5 more")

    def test_it_renders_an_empty_state(self):
        self.assertEqual(self._panel(days=0).groups, [])
        with override_settings(GEORIVA_FAILURES_PANEL_DAYS=0):
            page = self.client.get(reverse("wagtailadmin_home"))
        self.assertContains(page, "No failures in the last 0 days")

    def test_it_links_to_the_failures_page(self):
        self.assertContains(self.client.get(reverse("wagtailadmin_home")), reverse("events_failures"))

    def test_the_cost_does_not_grow_with_the_number_of_failures(self):
        for i in range(20):
            _fail_a_file(self.kenya, f"kenya/chirps/rainfall/{i}.tif")
        request = self._request()

        from georiva.events.failures import failure_groups

        with CaptureQueriesContext(connection) as queries:
            failure_groups(request)

        # groups, newest per group, introspection, one per type that appears (three),
        # and the collection link's lookup
        self.assertLessEqual(len(queries), 7)

    def test_it_adds_no_menu_entry_and_leaves_the_health_panel_alone(self):
        from wagtail.admin.menu import admin_menu

        urls = [item.url for item in admin_menu.registered_menu_items if getattr(item, "url", None)]
        self.assertFalse([url for url in urls if "/events/failures" in url], urls)


class FailuresPageTests(FailuresTestCase):
    def test_it_lists_every_failure_of_this_organisation(self):
        page = self.client.get(reverse("events_failures"))

        self.assertEqual(page.status_code, 200)
        self.assertEqual(
            sorted(row.type_label for row in page.context["rows"]),
            ["Derivation attempt", "Fetch finished", "File ingestion finished"],
        )
        self.assertContains(page, "bad.tif")
        self.assertNotContains(page, "theirs.tif")
        self.assertNotContains(page, "atlantis")

    def test_it_filters_to_one_group(self):
        _fail_a_file(self.kenya, "kenya/chirps/rainfall/more.tif")
        _fail_a_file(self.kenya, "kenya/chirps/tmax/elsewhere.tif")

        page = self.client.get(self._group("chirps/rainfall").list_url)

        self.assertEqual(len(page.context["rows"]), 2)
        self.assertContains(page, "more.tif")
        self.assertNotContains(page, "elsewhere.tif")
        self.assertEqual(page.context["group_label"], "chirps/rainfall")

    def test_it_pages_through_long_lists(self):
        for i in range(60):
            _fail_a_file(self.kenya, f"kenya/chirps/rainfall/{i}.tif")

        first = self.client.get(reverse("events_failures"))
        second = self.client.get(reverse("events_failures") + "?page=2")

        self.assertEqual(len(first.context["rows"]), 50)
        self.assertEqual(len(second.context["rows"]), 13)


class BackfillTests(FailuresTestCase):
    """Migration 0004 fills the key on failures recorded before the column existed."""

    def test_past_failures_get_their_key_and_successes_are_left_alone(self):
        import importlib

        from django.apps import apps

        from georiva.sources.models import FetchRunFinished

        backfill = importlib.import_module("georiva.events.migrations.0004_eventindex_failure_group_key").backfill

        def unkeyed(outcome, outcome_class):
            event = FetchRunFinished.objects.create(
                organisation=self.kenya,
                correlation_id=uuid.uuid4(),
                actor="system:test",
                data_feed=self.feed,
                feed_name="CHIRPS",
                catalog_slug="chirps",
                outcome=outcome,
            )
            EventIndex.objects.create(
                event_id=event.pk,
                event_type="georivasources.fetchrunfinished",
                correlation_id=event.correlation_id,
                occurred_at=event.occurred_at,
                organisation=self.kenya,
                outcome_class=outcome_class,
            )
            return event.pk

        failed, completed = unkeyed("failed", "failed"), unkeyed("completed", "ok")
        with connection.schema_editor() as editor:
            backfill(apps, editor)

        self.assertEqual(EventIndex.objects.get(pk=failed).failure_group_key, f"feed:{self.feed.pk}")
        self.assertEqual(EventIndex.objects.get(pk=completed).failure_group_key, "")
        self.assertEqual(self._group("CHIRPS").failed, 2)
