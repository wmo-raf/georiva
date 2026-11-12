"""
A Harvest feed is created and edited in the admin like any pull feed, and
saving it is where it is checked against the Upstream it names (ADR 0042 §2).

Driven through the admin's own add and edit views. The Upstream is the fake.
"""

import json
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django_celery_beat.models import PeriodicTask
from wagtail.test.utils.form_data import inline_formset, nested_form_data

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.ingestion.models import PushFeed
from georiva.organisations.testing import dial_org, make_organisation
from georiva.sources.models import DataFeed
from georiva.sources.registry import data_feed_viewset_registry

from .support import UPSTREAM_URL, HarvestFeedMixin

User = get_user_model()


def form_url(action, **kwargs):
    return reverse(data_feed_viewset_registry.get("harvestfeed").get_url_name(action), kwargs=kwargs)


class HarvestFeedAdminMixin:
    """An operator signed in to the organisation's admin, and an Upstream that answers."""

    def setUp(self):
        super().setUp()
        self.upstream = FakeUpstream("chirps-daily", title="CHIRPS daily rainfall")
        patcher = mock.patch.object(HarvestFeed, "upstream_client", lambda feed: self.upstream)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client.force_login(User.objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client)

    def make_catalog(self, slug="chirps", organisation=None):
        return Catalog.objects.create(
            organisation=organisation or make_organisation(),
            name=slug.upper(),
            slug=slug,
            file_format=Catalog.FileFormat.GEOTIFF,
        )

    def make_variable(self, collection, slug="rain"):
        unit, _ = Unit.objects.get_or_create(name="Millimetre", symbol="mm")
        return Variable.objects.create(
            collection=collection, slug=slug, name=slug.title(), unit=unit, value_min=0, value_max=100
        )

    def posted(self, mappings=(), **fields):
        """A filled-in form. `mappings` are the rows of the Variable mapping, as
        `{"asset_key": …, "variable": pk}` (with `id` for a row already saved)."""
        data = {
            "name": "CHIRPS upstream",
            "is_active": "on",
            "interval_minutes": "720",
            "upstream_url": UPSTREAM_URL,
            "upstream_collection_id": "chirps-daily",
            "window_start": "",
            "window_end": "",
            "asset_hosts": "",
            "variable_mappings": inline_formset(list(mappings), initial=sum(1 for row in mappings if row.get("id"))),
        }
        data.update(fields)
        return nested_form_data(data)

    def errors(self, response):
        """Every error the page's form reported, its mapping rows' included, as one string."""
        form = response.context["form"]
        found = [str(form.errors)]
        found += [str(row.errors) for row in form.formsets["variable_mappings"].forms]
        return " ".join(found)


class CreateHarvestFeedTests(HarvestFeedAdminMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.catalog = self.make_catalog()

    def create(self, **fields):
        return self.client.post(form_url("add"), self.posted(catalog=self.catalog.pk, **fields))

    def test_the_form_offers_what_a_harvest_feed_is_set_up_with(self):
        response = self.client.get(form_url("add"))

        self.assertEqual(response.status_code, 200)
        for field in ("upstream_url", "upstream_collection_id", "window_start", "window_end", "asset_hosts"):
            self.assertIn(field, response.context["form"].fields)
        self.assertIn("variable_mappings", response.context["form"].formsets)

    def test_an_operator_creates_a_feed_over_a_reachable_upstream(self):
        response = self.create(window_start="2026-01-01 00:00", asset_hosts="Bucket.Elsewhere.example")

        feed = HarvestFeed.objects.get()
        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": feed.pk}))
        self.assertEqual((feed.catalog, feed.upstream_url), (self.catalog, UPSTREAM_URL))
        self.assertEqual(feed.upstream_collection_id, "chirps-daily")
        self.assertEqual(feed.window_start.year, 2026)
        self.assertIsNone(feed.window_end)
        self.assertEqual(feed.allowed_asset_hosts, {"stac.upstream.example", "bucket.elsewhere.example"})

    def test_a_bucket_is_pinned_to_the_public_host_its_files_are_read_at(self):
        self.create(pinned_buckets='{"DEAfrica-Input": "DEAfrica-Input.S3.af-south-1.amazonaws.com."}')

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.pinned_buckets, {"deafrica-input": "deafrica-input.s3.af-south-1.amazonaws.com"})
        self.assertIn("deafrica-input.s3.af-south-1.amazonaws.com", feed.allowed_asset_hosts)

    def test_a_feed_saved_with_no_pins_has_none(self):
        self.create()

        self.assertEqual(HarvestFeed.objects.get().pinned_buckets, {})

    def test_a_pin_that_is_not_a_bucket_and_a_host_name_is_refused(self):
        refused = (
            '{"bucket": "https://bucket.s3.amazonaws.com/data"}',
            '{"bucket": "georiva-minio:9000"}',
            '{"s3://bucket": "bucket.s3.amazonaws.com"}',
            '{"bucket": ""}',
            '{"bucket": 3}',
            '["bucket"]',
        )
        for pins in refused:
            with self.subTest(pins=pins):
                response = self.create(pinned_buckets=pins)

                self.assertIn("pinned_buckets", response.context["form"].errors)
                self.assertFalse(HarvestFeed.objects.exists())

    def test_the_allowlist_defaults_to_the_catalogs_own_host(self):
        self.create()

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.asset_hosts, [])
        self.assertEqual(feed.allowed_asset_hosts, {"stac.upstream.example"})

    def test_a_saved_feed_has_its_collection_ready_for_variables(self):
        self.create()

        collection = HarvestFeed.objects.get().collection
        self.assertEqual((collection.catalog, collection.slug), (self.catalog, "chirps-daily"))

    def test_a_saved_feed_is_scheduled_at_its_cadence(self):
        self.create()

        feed = HarvestFeed.objects.get()
        periodic = PeriodicTask.objects.get(args=json.dumps([feed.pk]))
        self.assertEqual((periodic.task, periodic.interval.every), ("georiva.harvest.tasks.run_harvest", 720))

    def test_an_upstream_that_cannot_be_read_anonymously_is_refused_with_why(self):
        self.upstream.unreachable = "upstream catalog answered 401 for https://stac.upstream.example/api/collections"

        response = self.create()

        self.assertFalse(HarvestFeed.objects.exists())
        errors = self.errors(response)
        self.assertIn("could not be read without credentials", errors)
        self.assertIn("answered 401", errors)

    def test_an_upstream_without_the_collection_is_refused(self):
        response = self.create(upstream_collection_id="era5-land")

        self.assertFalse(HarvestFeed.objects.exists())
        errors = self.errors(response)
        self.assertIn("has no collection", errors)
        self.assertIn("chirps-daily", errors)

    def test_a_catalog_that_has_a_pull_feed_cannot_be_given_a_harvest_feed(self):
        DataFeed.objects.create(name="Existing loader", catalog=self.catalog)

        response = self.create()

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("A catalog has at most one feed", self.errors(response))
        self.assertIn("Existing loader", self.errors(response))

    def test_a_catalog_that_has_a_push_feed_cannot_be_given_a_harvest_feed(self):
        PushFeed.objects.create(catalog=self.catalog, name="Surface", valid_time_format="YYYYMMDD")

        response = self.create()

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("A catalog has at most one feed", self.errors(response))

    def test_another_organisations_catalog_cannot_be_harvested_into(self):
        theirs = self.make_catalog("theirs", organisation=make_organisation("other-org"))

        self.client.post(form_url("add"), self.posted(catalog=theirs.pk))

        self.assertFalse(HarvestFeed.objects.exists())

    def test_an_asset_host_must_be_a_host_name(self):
        response = self.create(asset_hosts="https://bucket.elsewhere.example/data")

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("is not a host name", self.errors(response))

    def test_an_asset_host_is_a_whole_name_not_a_pattern(self):
        response = self.create(asset_hosts="*.elsewhere.example")

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("is not a host name", self.errors(response))

    def test_a_window_that_ends_before_it_starts_is_refused(self):
        response = self.create(window_start="2026-03-01 00:00", window_end="2026-01-01 00:00")

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("ends before it starts", self.errors(response))

    def test_a_feed_with_no_collection_yet_cannot_be_given_a_mapping(self):
        elsewhere = Collection.objects.create(catalog=self.make_catalog("other"), name="Elsewhere", slug="elsewhere")
        stranger = self.make_variable(elsewhere)

        response = self.create(variable_mappings=inline_formset([{"asset_key": "rainfall", "variable": stranger.pk}]))

        self.assertFalse(HarvestFeed.objects.exists())
        self.assertIn("has no collection yet", self.errors(response))

    def test_a_catalog_curated_beforehand_is_mapped_as_the_feed_is_created(self):
        curated = Collection.objects.create(catalog=self.catalog, name="CHIRPS daily", slug="chirps-daily")
        rain = self.make_variable(curated)

        self.create(variable_mappings=inline_formset([{"asset_key": "rainfall", "variable": rain.pk}]))

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.collection, curated)
        self.assertEqual(feed.mapped_variables(curated), {"rainfall": rain})


class EditHarvestFeedTests(HarvestFeedAdminMixin, HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.collection = self.feed.ensure_collection()
        self.rain = self.make_variable(self.collection)

    def edit(self, mappings=(), **fields):
        return self.client.post(form_url("edit", pk=self.feed.pk), self.posted(mappings, **fields))

    def test_an_operator_maps_an_upstream_asset_key_to_a_curated_variable(self):
        response = self.edit([{"asset_key": "rainfall", "variable": self.rain.pk}])

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.assertEqual(self.feed.mapped_variables(self.collection), {"rainfall": self.rain})

    def test_the_mapping_offers_only_the_feeds_own_collections_variables(self):
        elsewhere = Collection.objects.create(catalog=self.feed.catalog, name="Elsewhere", slug="elsewhere")
        stranger = self.make_variable(elsewhere, slug="wind")

        response = self.client.get(form_url("edit", pk=self.feed.pk))

        offered = response.context["form"].formsets["variable_mappings"].empty_form.fields["variable"].queryset
        self.assertIn(self.rain, offered)
        self.assertNotIn(stranger, offered)

    def test_a_variable_of_another_collection_cannot_be_mapped(self):
        elsewhere = Collection.objects.create(catalog=self.feed.catalog, name="Elsewhere", slug="elsewhere")
        stranger = self.make_variable(elsewhere, slug="wind")

        response = self.edit([{"asset_key": "wind", "variable": stranger.pk}])

        self.assertEqual(response.status_code, 200)
        self.assertFalse(VariableMapping.objects.exists())

    def test_one_asset_key_is_mapped_once(self):
        snow = self.make_variable(self.collection, slug="snow")

        response = self.edit(
            [{"asset_key": "rainfall", "variable": self.rain.pk}, {"asset_key": "rainfall", "variable": snow.pk}]
        )

        self.assertIn("duplicate", self.errors(response))
        self.assertFalse(VariableMapping.objects.exists())

    def test_one_variable_takes_one_asset_key(self):
        response = self.edit(
            [{"asset_key": "rainfall", "variable": self.rain.pk}, {"asset_key": "precip", "variable": self.rain.pk}]
        )

        self.assertIn("duplicate", self.errors(response))
        self.assertFalse(VariableMapping.objects.exists())

    def test_a_mapping_row_is_removed(self):
        mapping = VariableMapping.objects.create(feed=self.feed, asset_key="rainfall", variable=self.rain)

        self.edit([{"id": mapping.pk, "asset_key": "rainfall", "variable": self.rain.pk, "DELETE": "on"}])

        self.assertFalse(VariableMapping.objects.exists())

    def test_the_allowlist_is_extended(self):
        self.edit(asset_hosts="bucket.elsewhere.example, cdn.elsewhere.example")

        self.feed.refresh_from_db()
        self.assertEqual(
            self.feed.allowed_asset_hosts,
            {"stac.upstream.example", "bucket.elsewhere.example", "cdn.elsewhere.example"},
        )

    def test_a_feed_whose_upstream_is_down_can_still_be_paused(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"

        self.edit(is_active="")

        self.feed.refresh_from_db()
        self.assertFalse(self.feed.is_active)

    def test_pointing_a_feed_at_an_unreachable_upstream_is_refused(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"

        response = self.edit(upstream_url="https://stac.moved.example/api")

        self.assertIn("timed out", self.errors(response))
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.upstream_url, UPSTREAM_URL)

    def test_the_catalog_of_a_saved_feed_does_not_move(self):
        other = self.make_catalog("other")

        self.edit(catalog=other.pk)

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.catalog.slug, "chirps")

    def test_the_dashboards_edit_leads_to_the_form_that_has_the_mapping(self):
        response = self.client.get(reverse("data_feed_edit", kwargs={"pk": self.feed.pk}))

        self.assertRedirects(response, form_url("edit", pk=self.feed.pk))
