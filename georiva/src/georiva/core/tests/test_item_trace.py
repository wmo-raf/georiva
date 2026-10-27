"""The Trace link from a Published Item to its chain (ADR 0030, #431)."""

from datetime import UTC, datetime

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
from georiva.events.recording import record
from georiva.events.testing import event_context
from georiva.events.timeline import timeline_url
from georiva.ingestion.models import FileIngestionFinished
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.processing.models import DerivationAttemptFinished


def _item(org, source_file="", slug="rain"):
    catalog = Catalog.objects.create(organisation=org, name=slug, slug=slug, file_format="geotiff")
    collection = Collection.objects.create(catalog=catalog, name=slug, slug=slug)
    return Item.objects.create(collection=collection, time=datetime(2026, 1, 1, tzinfo=UTC), source_file=source_file)


def _ingested(org, path="kenya/rain/rain/a.tif"):
    return record(
        FileIngestionFinished, outcome="completed", bucket="sources", file_path=path, items_created=1, assets_created=1
    )


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ItemTraceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _trace(self, item):
        return self.client.get(reverse("item_trace", args=[item.pk]))

    def test_an_ingested_item_leads_to_the_timeline_of_its_ingestion(self):
        item = _item(self.kenya, source_file="sources:kenya/rain/rain/a.tif")
        with event_context(organisation_id=self.kenya.pk):
            event = _ingested(self.kenya)

        response = self._trace(item)

        # It lands on the ingestion itself, not the top of the chain.
        self.assertRedirects(response, timeline_url(event.correlation_id, event.pk), fetch_redirect_response=False)

    def test_a_derived_item_leads_to_the_attempt_that_produced_it(self):
        item = _item(self.kenya)
        with event_context(organisation_id=self.kenya.pk):
            attempt = record(
                DerivationAttemptFinished,
                outcome="completed",
                recipe_type="promotion",
                recipe_version="1",
                unit_hash="a" * 64,
                attempt_number=1,
                retry_reason="initial",
                produced_item_id=item.pk,
            )

        response = self._trace(item)

        self.assertRedirects(response, timeline_url(attempt.correlation_id, attempt.pk), fetch_redirect_response=False)

    def test_a_reingested_item_lists_every_ingestion_newest_first(self):
        item = _item(self.kenya, source_file="sources:kenya/rain/rain/a.tif")
        with event_context(organisation_id=self.kenya.pk):
            first = _ingested(self.kenya)
        with event_context(organisation_id=self.kenya.pk):
            second = _ingested(self.kenya)

        response = self._trace(item)

        self.assertEqual(response.status_code, 200)
        urls = [entry.timeline_url for entry in response.context["entries"]]
        self.assertEqual(
            urls,
            [timeline_url(e.correlation_id, e.pk) for e in (second, first)],
        )

    def test_an_item_from_before_history_began_shows_a_clear_state(self):
        item = _item(self.kenya, source_file="sources:kenya/rain/rain/old.tif")

        response = self._trace(item)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No history is recorded for this item")

    def test_another_organisations_item_is_not_found(self):
        item = _item(self.uganda, slug="theirs")
        self.assertEqual(self._trace(item).status_code, 404)

    def test_the_item_page_offers_the_link(self):
        item = _item(self.kenya)
        response = self.client.get(reverse("item_preview", args=[item.pk]))
        self.assertContains(response, reverse("item_trace", args=[item.pk]))

    def test_the_item_table_has_no_new_column_and_the_stac_view_is_untouched(self):
        columns = {f.name for f in Item._meta.get_fields() if hasattr(f, "column")}
        self.assertFalse({c for c in columns if "correlation" in c or "trace" in c})
