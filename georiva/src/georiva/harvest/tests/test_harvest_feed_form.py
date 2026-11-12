"""
A Harvest feed is edited in the admin like any pull feed. It is not created
there: the Harvest Feed Wizard creates it, and what the wizard found — its
Upstream, where its files are — is not retyped on this form (ADR 0042 §2).

Driven through the admin's own add and edit views. The Upstream is the fake.
"""

from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from wagtail.test.utils.form_data import inline_formset, nested_form_data

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, UpstreamShape, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.organisations.testing import dial_org, make_organisation
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
            "window_start": "",
            "window_end": "",
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


class HarvestFeedsAreCreatedByTheWizardTests(HarvestFeedAdminMixin, TestCase):
    def test_the_admin_form_does_not_create_a_feed(self):
        catalog = self.make_catalog()

        shown = self.client.get(form_url("add"))
        posted = self.client.post(form_url("add"), self.posted(catalog=catalog.pk))

        self.assertRedirects(shown, reverse("harvest_wizard_step1"))
        self.assertRedirects(posted, reverse("harvest_wizard_step1"))
        self.assertFalse(HarvestFeed.objects.exists())

    def test_adding_a_pull_feed_of_this_kind_leads_to_the_wizard(self):
        response = self.client.get(reverse("data_feed_add_select"))

        self.assertContains(response, reverse("harvest_wizard_step1"))
        self.assertNotContains(response, form_url("add"))


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

    def test_the_upstream_and_where_its_files_are_cannot_be_changed_here(self):
        self.feed.pinned_buckets = {"a-bucket": "a-bucket.s3.eu-west-1.amazonaws.com"}
        self.feed.save()

        response = self.edit(
            upstream_url="https://another.example/api",
            upstream_collection_id="era5",
            asset_hosts="evil.example",
            pinned_buckets='{"georiva-assets": "georiva-minio"}',
        )

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.feed.refresh_from_db()
        self.assertEqual((self.feed.upstream_url, self.feed.upstream_collection_id), (UPSTREAM_URL, "chirps-daily"))
        self.assertEqual(self.feed.asset_hosts, [])
        self.assertEqual(self.feed.pinned_buckets, {"a-bucket": "a-bucket.s3.eu-west-1.amazonaws.com"})
        fields = self.client.get(form_url("edit", pk=self.feed.pk)).context["form"].fields
        for fixed in ("upstream_url", "upstream_collection_id", "asset_hosts", "pinned_buckets"):
            self.assertNotIn(fixed, fields)

    def test_the_mapping_offers_the_asset_keys_the_survey_saw(self):
        self.feed.surveyed_assets = [{"key": "rainfall", "title": "Monthly rainfall"}, {"key": "count", "title": ""}]
        self.feed.save()
        VariableMapping.objects.create(feed=self.feed, asset_key="gone-upstream", variable=self.rain)

        formset = self.client.get(form_url("edit", pk=self.feed.pk)).context["form"].formsets["variable_mappings"]

        offered = [value for value, _label in formset.empty_form.fields["asset_key"].widget.choices]
        self.assertEqual(offered, ["", "rainfall", "count", "gone-upstream"])
        labels = dict(formset.empty_form.fields["asset_key"].widget.choices)
        self.assertEqual(labels["rainfall"], "rainfall — Monthly rainfall")

    def test_a_feed_nothing_was_surveyed_for_has_its_asset_keys_typed(self):
        formset = self.client.get(form_url("edit", pk=self.feed.pk)).context["form"].formsets["variable_mappings"]

        self.assertFalse(hasattr(formset.empty_form.fields["asset_key"].widget, "choices"))

    def test_a_feed_with_a_fixed_window_is_not_offered_a_rolling_one(self):
        fields = self.client.get(form_url("edit", pk=self.feed.pk)).context["form"].fields

        self.assertIn("window_start", fields)
        self.assertNotIn("rolling_window_days", fields)

    def test_a_window_that_ends_before_it_starts_is_refused(self):
        response = self.edit(window_start="2026-03-01 00:00", window_end="2026-01-01 00:00")

        self.assertIn("ends before it starts", self.errors(response))

    def test_a_feed_whose_upstream_is_down_can_still_be_paused(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"

        self.edit(is_active="")

        self.feed.refresh_from_db()
        self.assertFalse(self.feed.is_active)

    def test_the_catalog_of_a_saved_feed_does_not_move(self):
        other = self.make_catalog("other")

        self.edit(catalog=other.pk)

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.catalog.slug, "chirps")

    def test_the_dashboards_edit_leads_to_the_form_that_has_the_mapping(self):
        response = self.client.get(reverse("data_feed_edit", kwargs={"pk": self.feed.pk}))

        self.assertRedirects(response, form_url("edit", pk=self.feed.pk))


STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
GFS = "s3://noaa-gfs-bdp-pds/"


class EditStoreFeedTests(HarvestFeedAdminMixin, HarvestFeedMixin, TestCase):
    """A feed over a forecast Upstream store, as the wizard saves one (ADR 0044 §1, §7)."""

    def make_feed(self, organisation=None, **fields):
        return super().make_feed(
            organisation,
            upstream_shape=UpstreamShape.STORE,
            store_href=STORE,
            rolling_window_days=3,
            pinned_chunk_locations={GFS: "us-east-1"},
            surveyed_assets=[{"key": "temperature_2m", "title": "2 metre temperature"}, {"key": "wind_u_10m"}],
            **fields,
        )

    def setUp(self):
        super().setUp()
        self.collection = self.feed.ensure_collection()
        self.temperature = self.make_variable(self.collection, slug="temperature_2m")

    def edit(self, mappings=(), **fields):
        fields.setdefault("rolling_window_days", "3")
        return self.client.post(form_url("edit", pk=self.feed.pk), self.posted(mappings, **fields))

    def form(self):
        return self.client.get(form_url("edit", pk=self.feed.pk)).context["form"]

    def test_the_rolling_window_and_the_cadence_can_be_changed(self):
        response = self.edit(rolling_window_days="7", interval_minutes="360")

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.feed.refresh_from_db()
        self.assertEqual((self.feed.rolling_window_days, self.feed.interval_minutes), (7, 360))

    def test_it_has_no_fixed_window_to_edit(self):
        fields = self.form().fields

        self.assertIn("rolling_window_days", fields)
        self.assertNotIn("window_start", fields)
        self.assertNotIn("window_end", fields)

        self.edit(window_start="2026-01-01 00:00", window_end="2026-02-01 00:00")
        self.feed.refresh_from_db()
        self.assertEqual((self.feed.window_start, self.feed.window_end), (None, None))

    def test_a_rolling_window_cannot_be_emptied_or_be_no_days_at_all(self):
        for days in ("", "0", "5000"):
            with self.subTest(days=days):
                response = self.edit(rolling_window_days=days)

                self.assertEqual(response.status_code, 200)
                self.feed.refresh_from_db()
                self.assertEqual(self.feed.rolling_window_days, 3)

    def test_its_shape_upstream_store_and_where_its_data_is_read_from_cannot_be_changed_here(self):
        response = self.edit(
            upstream_shape=UpstreamShape.ITEMS,
            upstream_url="https://another.example/catalog.json",
            upstream_collection_id="era5",
            store_href="https://evil.example/store",
            pinned_chunk_locations='{"s3://georiva-assets/": ""}',
        )

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.feed.refresh_from_db()
        self.assertEqual((self.feed.upstream_shape, self.feed.store_href), (UpstreamShape.STORE, STORE))
        self.assertEqual((self.feed.upstream_url, self.feed.upstream_collection_id), (UPSTREAM_URL, "chirps-daily"))
        self.assertEqual(self.feed.pinned_chunk_locations, {GFS: "us-east-1"})
        fields = self.form().fields
        fixed = ("upstream_shape", "upstream_url", "upstream_collection_id", "store_href", "pinned_chunk_locations")
        for name in fixed:
            self.assertNotIn(name, fields)

    def test_the_shape_is_shown_and_is_not_a_field(self):
        page = self.client.get(form_url("edit", pk=self.feed.pk))

        self.assertContains(page, "One data store, laid out a whole map per time step")

    def test_the_mapping_offers_the_array_paths_the_survey_saw(self):
        row = self.form().formsets["variable_mappings"].empty_form.fields["asset_key"]

        self.assertEqual([value for value, _label in row.widget.choices], ["", "temperature_2m", "wind_u_10m"])
        self.assertEqual(dict(row.widget.choices)["temperature_2m"], "temperature_2m — 2 metre temperature")
        self.assertEqual(str(row.label), "Array in the store")

    def test_an_array_is_mapped_by_its_path(self):
        self.edit([{"asset_key": "temperature_2m", "variable": self.temperature.pk}])

        self.assertEqual(self.feed.mapped_variables(self.collection), {"temperature_2m": self.temperature})

    def test_an_array_the_survey_did_not_offer_cannot_be_mapped_by_posting_its_path(self):
        response = self.edit([{"asset_key": "pressure_level/temperature", "variable": self.temperature.pk}])

        self.assertEqual(response.status_code, 200)
        self.assertIn("not an array the upstream store offers", self.errors(response))
        self.assertFalse(VariableMapping.objects.exists())

    def test_the_feeds_page_says_what_it_points_at(self):
        page = self.client.get(reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))

        self.assertContains(page, STORE)
        self.assertContains(page, "Rolling: runs from the last 3 days")
        self.assertContains(page, GFS)
