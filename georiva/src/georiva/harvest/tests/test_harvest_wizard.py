"""
The Harvest Feed Wizard: a Harvest feed from an Upstream's address and nothing
else an operator must already know (ADR 0042 §2, §9).

Driven through the wizard's own pages, as an operator signed in to an
organisation's admin. The only fake is the Upstream.
"""

from datetime import UTC, datetime
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django_celery_beat.models import PeriodicTask

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.harvest.upstream import AssetProbe, UpstreamAsset, UpstreamCollection
from georiva.ingestion.models import PushFeed
from georiva.organisations.testing import dial_org, make_organisation
from georiva.sources.models import FetchRun

from .support import HarvestFeedMixin

User = get_user_model()

ADDRESS = "https://stac.upstream.example/api"
COG = "image/tiff; application=geotiff; profile=cloud-optimized"
BUCKET = "deafrica-input-datasets"
BUCKET_HOST = "deafrica-input-datasets.s3.af-south-1.amazonaws.com"


def month(n: int) -> datetime:
    return datetime(2026, n, 15, tzinfo=UTC)


def step(n: int) -> str:
    return reverse(f"harvest_wizard_step{n}")


class WizardMixin:
    """An operator, and an Upstream shaped like Digital Earth Africa's CHIRPS:
    one image a month, named by `s3://`, with no statistics."""

    def setUp(self):
        super().setUp()
        self.organisation = make_organisation()
        self.upstream = FakeUpstream("rainfall_chirps_monthly", title="CHIRPS monthly rainfall")
        self.upstream.regions[BUCKET] = "af-south-1"
        for n in (1, 2, 3):
            self.add_month(n)
        self.upstream.probes[self.https_href(1)] = AssetProbe(
            readable=True, tiled=True, overviews=True, crs="EPSG:4326", value_min=0.4, value_max=287.3
        )
        for method in ("survey_client", "upstream_client"):
            patcher = mock.patch.object(HarvestFeed, method, lambda feed: self.upstream)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.run_now = self.enterContext(mock.patch.object(HarvestFeed, "run_now"))
        self.mm, _ = Unit.objects.get_or_create(name="Millimetre", symbol="mm")

        self.client.force_login(User.objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client)

    def add_month(self, n, key="rainfall", href=None, **asset):
        fields = {"media_type": COG, "title": "Monthly rainfall", "unit": "mm", "nodata": -9999.0, **asset}
        href = href or f"s3://{BUCKET}/rainfall_chirps_monthly/chirps-v2.0_2026.{n:02d}.tif"
        self.upstream.add_item(month(n), {key: UpstreamAsset(href=href, **fields)}, id=f"{key}-{n}")

    @staticmethod
    def https_href(n):
        return f"https://{BUCKET_HOST}/rainfall_chirps_monthly/chirps-v2.0_2026.{n:02d}.tif"

    # The steps, each as an operator takes it.

    def give_address(self, address=ADDRESS):
        return self.client.post(step(1), {"upstream_url": address})

    def pick_collection(self, collection_id="rainfall_chirps_monthly"):
        return self.client.post(step(2), {"collection_id": collection_id})

    def choose_images(self, **changes):
        data = {"use_0": "on", "name_0": "Rainfall", "unit_0": self.mm.pk, "low_0": "0", "high_0": "300"}
        data["allow_0"] = "on"
        data.update(changes)
        return self.client.post(step(4), {name: value for name, value in data.items() if value is not None})

    def save(self, **changes):
        data = {
            "feed_name": "CHIRPS from Digital Earth Africa",
            "catalog_mode": "create",
            "new_catalog_name": "CHIRPS (remote)",
            "new_catalog_slug": "dea-chirps",
            "window_start": "2024-01-01",
            "window_end": "",
            "interval_minutes": "1440",
        }
        data.update(changes)
        with self.captureOnCommitCallbacks(execute=True):
            return self.client.post(step(5), data)

    def reach_step(self, n):
        """Take every step before step `n`."""
        taken = [self.give_address, self.pick_collection, lambda: self.client.get(step(3)), self.choose_images]
        for take in taken[: n - 1]:
            take()

    def errors(self, response):
        return " ".join(str(message) for message in response.context["messages"])


class WizardSetsUpAFeedTests(WizardMixin, TestCase):
    def test_an_operator_with_only_an_address_reaches_a_saved_feed(self):
        self.assertRedirects(self.give_address(), step(2))
        self.assertContains(self.client.get(step(2)), "CHIRPS monthly rainfall")
        self.assertRedirects(self.pick_collection(), step(3))
        self.assertContains(self.client.get(step(3)), "GeoRiva can show")
        self.assertContains(self.client.get(step(4)), BUCKET_HOST)
        self.assertRedirects(self.choose_images(), step(5))
        response = self.save()

        feed = HarvestFeed.objects.get()
        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": feed.pk}))
        self.assertEqual((feed.name, feed.upstream_url), ("CHIRPS from Digital Earth Africa", ADDRESS))
        self.assertEqual(feed.upstream_collection_id, "rainfall_chirps_monthly")
        self.assertEqual((feed.window_start, feed.window_end), (datetime(2024, 1, 1, tzinfo=UTC), None))
        self.assertEqual(feed.interval_minutes, 1440)
        self.assertEqual(feed.pinned_buckets, {BUCKET: BUCKET_HOST})
        self.assertEqual(feed.surveyed_assets, [{"key": "rainfall", "title": "Monthly rainfall"}])

        catalog = feed.catalog
        self.assertEqual((catalog.organisation, catalog.slug), (self.organisation, "dea-chirps"))
        self.assertEqual(catalog.name, "CHIRPS (remote)")
        collection = feed.collection
        self.assertEqual((collection.catalog, collection.slug), (catalog, "rainfall_chirps_monthly"))
        variable = Variable.objects.get(collection=collection)
        self.assertEqual((variable.slug, variable.name, variable.unit), ("rainfall", "Rainfall", self.mm))
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 300.0))
        self.assertEqual(feed.mapped_variables(collection), {"rainfall": variable})

    def test_the_first_harvest_starts_as_the_feed_is_saved_and_it_is_scheduled(self):
        self.reach_step(5)
        self.save()

        self.run_now.assert_called_once_with()
        feed = HarvestFeed.objects.get()
        self.assertTrue(PeriodicTask.objects.filter(args=f"[{feed.pk}]").exists())

    def test_the_feed_it_saves_harvests(self):
        self.reach_step(5)
        self.save()
        feed = HarvestFeed.objects.get()

        from georiva.harvest.harvester import harvest

        result = harvest(feed, upstream=self.upstream)

        self.assertEqual((result.indexed, result.rejected), (3, 0))
        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED)

    def test_the_operator_is_shown_names_units_and_a_range_they_did_not_type(self):
        self.reach_step(4)

        (row,) = self.client.get(step(4)).context["rows"]

        self.assertEqual((row["name"], row["unit_id"]), ("Monthly rainfall", self.mm.pk))
        # From the Sample item's file, rounded outward to bounds worth colouring by.
        self.assertEqual((row["low"], row["high"]), (0.0, 300.0))
        self.assertTrue(row["use"])

    def test_no_page_uses_a_word_an_operator_must_look_up(self):
        pages = [self.client.get(step(1))]
        self.give_address()
        pages.append(self.client.get(step(2)))
        self.pick_collection()
        pages += [self.client.get(step(3)), self.client.get(step(4))]
        self.choose_images()
        pages.append(self.client.get(step(5)))

        for number, page in enumerate(pages, start=1):
            html = page.content.decode()
            body = html[html.index("hwiz-progress") :]
            for jargon in ("STAC", "S3", "s3://", "COG", "bucket", "allowlist", "asset"):
                # The example address on step 1 is a real catalog's, and spells its own path.
                self.assertNotIn(jargon, body.replace("explorer.digitalearth.africa/stac", ""), f"step {number}")

    def test_a_catalog_whose_collections_are_known_by_id_alone_says_so(self):
        self.upstream.collections_unreachable = "upstream catalog did not answer JSON for …/collections"
        self.give_address()

        response = self.client.get(step(2))

        self.assertContains(response, "rainfall_chirps_monthly")
        self.assertContains(response, "without titles")

    def test_an_existing_unclaimed_catalog_can_be_harvested_into(self):
        catalog = Catalog.objects.create(
            organisation=self.organisation, name="Rainfall", slug="rainfall", file_format=Catalog.FileFormat.GEOTIFF
        )
        self.reach_step(5)

        self.assertContains(self.client.get(step(5)), "Rainfall (rainfall)")
        self.save(catalog_mode="select", catalog_id=str(catalog.pk))

        self.assertEqual(HarvestFeed.objects.get().catalog, catalog)
        self.assertEqual(Catalog.objects.count(), 1)

    def test_a_variable_curated_beforehand_is_never_overwritten(self):
        catalog = Catalog.objects.create(
            organisation=self.organisation, name="Rainfall", slug="rainfall", file_format=Catalog.FileFormat.GEOTIFF
        )
        collection = Collection.objects.create(catalog=catalog, name="Tuned", slug="rainfall_chirps_monthly")
        tuned = Variable.objects.create(
            collection=collection, slug="rainfall", name="Hand-tuned rain", unit=self.mm, value_min=0, value_max=50
        )
        self.reach_step(5)

        self.save(catalog_mode="select", catalog_id=str(catalog.pk))

        tuned.refresh_from_db()
        self.assertEqual((tuned.name, tuned.value_max), ("Hand-tuned rain", 50.0))
        self.assertEqual(VariableMapping.objects.get().variable, tuned)
        self.assertEqual(Collection.objects.get().name, "Tuned")


class WizardStopsWhereItMustTests(WizardMixin, TestCase):
    def test_an_address_that_is_not_one_is_refused(self):
        for address in ("", "ftp://files.example/catalog", "not an address"):
            with self.subTest(address=address):
                response = self.give_address(address)

                self.assertEqual(response.status_code, 200)
                self.assertIn("web address", self.errors(response))

    def test_a_catalog_that_cannot_be_read_without_an_account_is_refused_with_why(self):
        self.upstream.unreachable = "upstream catalog answered 401 for https://stac.upstream.example/api"

        response = self.give_address()

        self.assertEqual(response.status_code, 200)
        self.assertIn("answered 401", self.errors(response))
        self.assertIn("without an account", self.errors(response))

    def test_a_catalog_of_plain_files_is_refused_in_plain_words(self):
        self.upstream.is_api = False

        response = self.give_address()

        self.assertIn("cannot be searched by time", self.errors(response))

    def test_a_collection_the_catalog_does_not_have_cannot_be_picked(self):
        self.give_address()

        response = self.pick_collection("era5-land")

        self.assertIn("choose a collection", self.errors(response))

    def test_a_collection_that_cannot_be_harvested_ends_at_step_three_with_its_reason(self):
        # Scenes: each item covers another place.
        self.upstream.items = []
        for n in (1, 2):
            scene = {"red": UpstreamAsset(href=f"https://stac.upstream.example/{n}.tif", media_type=COG)}
            self.upstream.add_item(month(n), scene, bbox=[30.0 + n, -5.0, 31.0 + n, -4.0])
        self.give_address()
        self.pick_collection()

        found = self.client.get(step(3))

        self.assertContains(found, "GeoRiva cannot show")
        self.assertContains(found, "split across several items")
        self.assertNotContains(found, step(4))
        # And there is no way round it.
        self.assertRedirects(self.client.get(step(4)), step(3))
        self.assertRedirects(self.choose_images(), step(3))
        self.assertRedirects(self.client.get(step(5)), step(4), fetch_redirect_response=False)
        self.assertFalse(HarvestFeed.objects.exists())
        self.assertFalse(Catalog.objects.exists())

    def test_a_data_store_ends_at_step_three_too(self):
        self.upstream.is_api = False
        self.upstream.is_collection_document = True
        self.upstream.items = []
        self.upstream.collections = [
            UpstreamCollection(
                id="noaa-gfs-forecast",
                title="NOAA GFS forecast",
                assets={
                    "icechunk": UpstreamAsset(
                        href="https://b.example/gfs.icechunk", media_type="application/x-icechunk"
                    )
                },
            )
        ]
        self.give_address("https://stac.upstream.example/noaa-gfs-forecast/collection.json")
        self.pick_collection("noaa-gfs-forecast")

        self.assertContains(self.client.get(step(3)), "one data store")

    def test_a_step_cannot_be_reached_before_the_ones_it_depends_on(self):
        self.assertRedirects(self.client.get(step(2)), step(1))
        self.assertRedirects(self.client.get(step(3)), step(1))
        self.assertRedirects(self.client.get(step(4)), step(1))
        self.assertRedirects(self.client.get(step(5)), step(1))

    def test_a_new_address_forgets_what_was_found_at_the_last(self):
        self.reach_step(5)

        self.give_address("https://another.example/api")

        self.assertRedirects(self.client.get(step(5)), step(1))


class WizardConfirmsWhereFilesAreTests(WizardMixin, TestCase):
    def test_the_operator_is_asked_in_one_plain_sentence(self):
        self.reach_step(4)

        response = self.client.get(step(4))

        self.assertContains(response, f"keeps its files at <strong>{BUCKET_HOST}</strong>")
        self.assertContains(response, "Allow this feed to read from there?")

    def test_declining_the_place_a_chosen_images_files_are_in_does_not_go_on(self):
        self.reach_step(4)

        response = self.choose_images(allow_0=None)

        self.assertEqual(response.status_code, 200)
        self.assertIn(BUCKET_HOST, self.errors(response))
        self.assertRedirects(self.client.get(step(5)), step(4))

    def test_files_on_the_catalogs_own_host_need_no_confirmation(self):
        self.upstream.items = []
        for n in (1, 2):
            self.add_month(n, href=f"https://stac.upstream.example/chirps/{n}.tif")
        self.reach_step(4)

        self.assertEqual(self.client.get(step(4)).context["locations"], [])
        self.assertRedirects(self.choose_images(allow_0=None), step(5))
        self.save()

        feed = HarvestFeed.objects.get()
        self.assertEqual((feed.pinned_buckets, feed.asset_hosts), ({}, []))

    def test_only_the_places_of_the_images_chosen_are_pinned(self):
        self.upstream.items = []
        self.upstream.add_item(
            month(1),
            {
                "rainfall": UpstreamAsset(href=f"s3://{BUCKET}/1.tif", media_type=COG, unit="mm"),
                "anomaly": UpstreamAsset(href="https://data.elsewhere.example/1-anom.tif", media_type=COG, unit="mm"),
            },
        )
        self.reach_step(4)
        hosts = [location["host"] for location in self.client.get(step(4)).context["locations"]]
        self.assertEqual(sorted(hosts), sorted([BUCKET_HOST, "data.elsewhere.example"]))

        # Both places agreed to, one image chosen.
        self.choose_images(allow_1="on")
        self.save()

        feed = HarvestFeed.objects.get()
        self.assertEqual((feed.pinned_buckets, feed.asset_hosts), ({BUCKET: BUCKET_HOST}, []))
        self.assertEqual(VariableMapping.objects.count(), 1)

    def test_an_image_whose_file_cannot_be_read_is_shown_and_cannot_be_chosen(self):
        self.upstream.items = []
        self.upstream.add_item(
            month(1),
            {
                "rainfall": UpstreamAsset(href=f"s3://{BUCKET}/1.tif", media_type=COG, unit="mm"),
                "anomaly": UpstreamAsset(href=f"s3://{BUCKET}/1-anom.tif", media_type=COG, unit="mm"),
            },
        )
        broken = f"https://{BUCKET_HOST}/1-anom.tif"
        self.upstream.probes[broken] = AssetProbe(readable=False, problem="HTTP response code: 404")
        self.reach_step(4)

        self.assertContains(self.client.get(step(4)), "This one cannot be shown")
        self.choose_images(use_1="on", name_1="Anomaly", unit_1=self.mm.pk, low_1="-1", high_1="1")
        self.save()

        self.assertEqual([m.asset_key for m in VariableMapping.objects.all()], ["rainfall"])

    def test_a_file_that_will_be_slow_to_draw_says_so(self):
        self.upstream.probes[self.https_href(1)] = AssetProbe(readable=True, tiled=False, overviews=False)
        self.reach_step(4)

        response = self.client.get(step(4))

        self.assertContains(response, "slow to draw")
        (row,) = response.context["rows"]
        self.assertEqual((row["low"], row["high"]), (None, None))


class WizardChecksWhatIsChosenTests(WizardMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.reach_step(4)

    def refused(self, **changes):
        response = self.choose_images(**changes)
        self.assertEqual(response.status_code, 200)
        return self.errors(response)

    def test_nothing_chosen_is_refused(self):
        self.assertIn("at least one", self.refused(use_0=None))

    def test_an_image_needs_a_unit(self):
        self.assertIn("unit", self.refused(unit_0=""))
        self.assertIn("unit", self.refused(unit_0="999999"))

    def test_an_image_needs_a_range_the_right_way_round(self):
        for low, high in (("300", "0"), ("5", "5"), ("", "300"), ("nan", "300"), ("0", "inf")):
            with self.subTest(low=low, high=high):
                self.assertIn("lowest", self.refused(low_0=low, high_0=high))

    def test_what_was_typed_is_still_there_after_a_refusal(self):
        response = self.choose_images(name_0="Rain, monthly", high_0="")

        (row,) = response.context["rows"]
        self.assertEqual((row["name"], row["low"], row["high"]), ("Rain, monthly", "0", ""))

    def test_an_empty_name_falls_back_to_the_upstreams(self):
        self.choose_images(name_0="  ")
        self.save()

        self.assertEqual(Variable.objects.get().name, "Monthly rainfall")


class WizardSavesTogetherOrNotAtAllTests(WizardMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.reach_step(5)

    def assert_nothing_saved(self, catalogs=0):
        self.assertFalse(HarvestFeed.objects.exists())
        self.assertFalse(Variable.objects.exists())
        self.assertFalse(Collection.objects.exists())
        self.assertEqual(Catalog.objects.count(), catalogs)
        self.run_now.assert_not_called()

    def test_a_catalog_short_name_already_taken_saves_nothing(self):
        Catalog.objects.create(
            organisation=self.organisation, name="Taken", slug="dea-chirps", file_format=Catalog.FileFormat.GEOTIFF
        )

        response = self.save()

        self.assertIn("already exists", self.errors(response))
        self.assert_nothing_saved(catalogs=1)

    def test_a_failure_part_way_leaves_nothing_behind(self):
        with mock.patch.object(HarvestFeed, "ensure_collection", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self.save()

        self.assert_nothing_saved()

    def test_a_catalog_that_has_a_feed_cannot_be_harvested_into(self):
        claimed = Catalog.objects.create(
            organisation=self.organisation, name="Pushed", slug="pushed", file_format=Catalog.FileFormat.GEOTIFF
        )
        PushFeed.objects.create(catalog=claimed, name="A push feed", valid_time_format="YYYYMMDD")

        response = self.save(catalog_mode="select", catalog_id=str(claimed.pk))

        self.assertNotContains(response, "Pushed (pushed)")
        self.assertIn("already has a feed", self.errors(response))
        self.assert_nothing_saved(catalogs=1)

    def test_another_organisations_catalog_is_neither_offered_nor_taken(self):
        theirs = Catalog.objects.create(
            organisation=make_organisation("elsewhere"),
            name="Theirs",
            slug="theirs",
            file_format=Catalog.FileFormat.GEOTIFF,
        )

        offered = self.client.get(step(5))
        response = self.save(catalog_mode="select", catalog_id=str(theirs.pk))

        self.assertNotContains(offered, "Theirs")
        self.assertIn("choose a catalog", self.errors(response))
        self.assert_nothing_saved(catalogs=1)

    def test_the_feed_is_the_organisations_the_operator_is_working_in(self):
        self.save()

        self.assertEqual(HarvestFeed.objects.get().catalog.organisation, self.organisation)

    def test_a_feed_needs_a_name_dates_that_are_dates_and_a_cadence_on_offer(self):
        for changes, said in (
            ({"feed_name": ""}, "name"),
            ({"window_start": "yesterday"}, "dates"),
            ({"window_start": "2026-03-01", "window_end": "2026-01-01"}, "before the first"),
            ({"interval_minutes": "1"}, "how often"),
            ({"new_catalog_name": ""}, "name for the new catalog"),
        ):
            with self.subTest(changes=changes):
                response = self.save(**changes)

                self.assertIn(said, self.errors(response))
                self.assert_nothing_saved()

    def test_a_window_left_empty_is_everything_and_still_open(self):
        self.save(window_start="")

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.window, (None, None))


class RecheckUpstreamTests(HarvestFeedMixin, TestCase):
    """A feed that exists, whose Upstream has since moved its files."""

    def setUp(self):
        super().setUp()
        self.upstream.regions[BUCKET] = "af-south-1"
        self.variable = self.map_variable("rainfall")
        self.upstream.add_item(month(1), {"rainfall": UpstreamAsset(href=f"s3://{BUCKET}/1.tif", media_type=COG)})
        for method in ("survey_client", "upstream_client"):
            patcher = mock.patch.object(HarvestFeed, method, lambda feed: self.upstream)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client.force_login(User.objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client)
        self.url = reverse("harvest_recheck_upstream", kwargs={"pk": self.feed.pk})

    def test_a_place_the_feed_does_not_read_from_yet_is_put_to_the_operator(self):
        response = self.client.get(self.url)

        self.assertContains(response, f"now keeps files at <strong>{BUCKET_HOST}</strong>")
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.pinned_buckets, {}, "looking is not agreeing")

    def test_it_is_pinned_only_on_their_yes(self):
        self.client.post(self.url, {})
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.pinned_buckets, {})

        response = self.client.post(self.url, {"allow": f"{BUCKET}|{BUCKET_HOST}"})

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.pinned_buckets, {BUCKET: BUCKET_HOST})
        self.assertEqual(self.harvest().rejected, 0)

    def test_a_place_the_survey_did_not_propose_cannot_be_pinned_by_posting_it(self):
        self.client.post(self.url, {"allow": ["georiva-assets|georiva-minio", "|169.254.169.254"]})

        self.feed.refresh_from_db()
        self.assertEqual((self.feed.pinned_buckets, self.feed.asset_hosts), ({}, []))

    def test_a_place_already_pinned_is_not_asked_about_again(self):
        self.feed.pinned_buckets = {BUCKET: BUCKET_HOST}
        self.feed.save()

        response = self.client.get(self.url)

        self.assertContains(response, "nothing to confirm")
        self.assertEqual(response.context["new"], [])

    def test_the_asset_keys_the_mapping_offers_are_refreshed(self):
        self.client.post(self.url, {})

        self.feed.refresh_from_db()
        self.assertEqual(self.feed.surveyed_assets, [{"key": "rainfall", "title": ""}])

    def test_an_upstream_that_no_longer_fits_changes_nothing(self):
        self.upstream.items = []

        response = self.client.get(self.url)
        self.client.post(self.url, {"allow": f"{BUCKET}|{BUCKET_HOST}"})

        self.assertContains(response, "no longer fits")
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.pinned_buckets, {})

    def test_an_unreachable_upstream_sends_the_operator_back_with_why(self):
        self.upstream.unreachable = "upstream catalog unreachable: timed out"

        response = self.client.get(self.url, follow=True)

        self.assertRedirects(response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        self.assertIn("timed out", " ".join(str(m) for m in response.context["messages"]))

    def test_another_organisations_feed_cannot_be_rechecked(self):
        theirs = self.make_feed(organisation=make_organisation("elsewhere"))

        response = self.client.get(reverse("harvest_recheck_upstream", kwargs={"pk": theirs.pk}))

        self.assertEqual(response.status_code, 404)

    def test_the_feeds_page_and_a_run_with_rejected_assets_lead_here(self):
        self.harvest()
        run = FetchRun.objects.get()
        self.assertEqual(run.harvest_rejected, 1)

        dashboard = self.client.get(reverse("data_feed_detail", kwargs={"pk": self.feed.pk}))
        run_url = reverse("data_feed_fetch_run_detail", kwargs={"feed_pk": self.feed.pk, "run_pk": run.pk})
        run_page = self.client.get(run_url)

        self.assertContains(dashboard, self.url)
        self.assertContains(run_page, self.url)
        self.assertContains(run_page, "re-check it")
