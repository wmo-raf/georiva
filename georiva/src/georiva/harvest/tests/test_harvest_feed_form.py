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
from georiva.harvest.models import HarvestFeed, VariableMapping
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
