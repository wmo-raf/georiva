"""
"Re-check upstream" on a feed over an Upstream store (ADR 0044 §1, §8, §11).

The survey is run again against the store, and what changed is put to the
operator: arrays newly seen, places its data has moved to, a new address for
the store, or a verdict that turned into a refusal. Nothing the Upstream says
is acted on without their yes (ADR 0042 §9).

Driven through the re-check page as an operator; the only fake is the Upstream.
"""

import json
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from django_redis import get_redis_connection

from georiva.core.machine_plane.remote_assets import PUBLISHED_FIELD, REMOTE_ASSETS_KEY_PREFIX, variable_key
from georiva.core.models import Asset, Catalog, Item
from georiva.harvest import background_survey
from georiva.harvest.models import HarvestFeed
from georiva.ingestion.models import RunIngestion
from georiva.organisations.testing import dial_org
from georiva.sources.models import FetchRun
from georiva.sources.registry import data_feed_viewset_registry

from .support import RecheckPages
from .test_store_harvest import RUN_STEP_GRID, STORE, STORE_TYPE, WHOLE_FIELD, StoreFeedMixin

User = get_user_model()

GFS_BUCKET = "s3://noaa-gfs-bdp-pds/"
MOVED_BUCKET = "s3://noaa-gfs-moved/"
MOVED_STORE = STORE.replace("v0.1.0", "v0.2.0")
LEVELLED = ("init_time", "lead_time", "pressure_level", "latitude", "longitude")


class StoreRecheckMixin(RecheckPages, StoreFeedMixin):
    """A store feed set up with `temperature_2m` and `wind_u_10m` offered, its
    data pinned in GFS's own bucket, and two steps of one run harvested."""

    arrays = {
        "temperature_2m": (RUN_STEP_GRID, WHOLE_FIELD),
        "wind_u_10m": (RUN_STEP_GRID, WHOLE_FIELD),
    }
    places = (GFS_BUCKET,)

    def setUp(self):
        super().setUp()
        self.upstream.regions.update({"noaa-gfs-bdp-pds": "us-east-1", "noaa-gfs-moved": "us-east-1"})
        self.publish_store()
        HarvestFeed.objects.filter(pk=self.feed.pk).update(
            pinned_chunk_locations={GFS_BUCKET: "us-east-1"},
            surveyed_assets=[{"key": "temperature_2m", "title": ""}, {"key": "wind_u_10m", "title": ""}],
        )
        self.feed.refresh_from_db()
        self.upstream.expected_steps[self.run] = 2
        self.hold("temperature_2m", 0, 3)
        self.harvest()

        for method in ("survey_client", "upstream_client"):
            patcher = mock.patch.object(HarvestFeed, method, lambda feed: self.upstream)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.run_now = self.enterContext(mock.patch.object(HarvestFeed, "run_now"))
        self.start_running_surveys_inline()
        self.client.force_login(User.objects.create_superuser("op", "op@test.com", "pw"))
        dial_org(self.client, self.organisation.slug)
        self.url = reverse("harvest_recheck_upstream", kwargs={"pk": self.feed.pk})

    def publish_store(self, href=STORE, arrays=None, places=None, codecs=None):
        """The Upstream publishes its store — again, perhaps changed."""
        self.upstream.become_store(
            href,
            self.dimensions,
            arrays or self.arrays,
            media_type=STORE_TYPE,
            chunk_locations=self.places if places is None else places,
            codecs=codecs,
        )
        # The old address still answers: a version bump leaves the old repository in place.
        self.upstream.stores.setdefault(STORE, {path: ("bytes", "zstd") for path in self.arrays})

    def configuration(self):
        feed = HarvestFeed.objects.get(pk=self.feed.pk)
        return (
            feed.store_href,
            feed.pinned_chunk_locations,
            feed.surveyed_assets,
            feed.asset_hosts,
            feed.pinned_buckets,
            feed.upstream_refusal,
        )

    def rows(self):
        return list(Asset.objects.order_by("pk").values_list("pk", "href", "store_array", "modified"))


class NewlySeenArraysTests(StoreRecheckMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.publish_store(
            arrays={
                **self.arrays,
                "precipitation_surface": (RUN_STEP_GRID, WHOLE_FIELD, {"title": "Precipitation"}),
                "pressure_level/temperature": (LEVELLED, (1, 1, 1, 721, 1440)),
            }
        )

    def test_newly_seen_arrays_are_shown(self):
        page = self.look()

        self.assertEqual([array.path for array in page.context["new_arrays"]], ["precipitation_surface"])
        self.assertContains(page, "Precipitation")

    def test_arrays_that_still_cannot_be_mapped_keep_their_reason(self):
        page = self.look()

        unoffered = {array.path: array.reason for array in page.context["unoffered"]}
        self.assertIn("pressure_level/temperature", unoffered)
        self.assertTrue(unoffered["pressure_level/temperature"])
        self.assertContains(page, unoffered["pressure_level/temperature"])

    def test_on_their_yes_the_mapping_offers_them(self):
        self.answer({"offer_arrays": "1"})

        feed = HarvestFeed.objects.get(pk=self.feed.pk)
        self.assertEqual(
            sorted(key for key, _label in feed.surveyed_asset_keys),
            ["precipitation_surface", "temperature_2m", "wind_u_10m"],
        )
        edit_url = data_feed_viewset_registry.get("harvestfeed").get_url_name("edit")
        edit = self.client.get(reverse(edit_url, kwargs={"pk": feed.pk}))
        self.assertContains(edit, 'value="precipitation_surface"')


class NewChunkLocationTests(StoreRecheckMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.publish_store(places=(MOVED_BUCKET,))

    def test_it_is_proposed_and_the_page_says_maps_fail_until_it_is_allowed(self):
        page = self.look()

        self.assertEqual([place["declared"] for place in page.context["new_places"]], [MOVED_BUCKET])
        self.assertContains(page, "<strong>noaa-gfs-moved</strong>")
        self.assertContains(page, "cannot be drawn until you allow")
        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations, {GFS_BUCKET: "us-east-1"})

    def test_it_is_pinned_only_on_their_yes_with_its_region(self):
        self.answer({"allow_place": MOVED_BUCKET})

        pins = HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations
        self.assertEqual(pins, {GFS_BUCKET: "us-east-1", MOVED_BUCKET: "us-east-1"})

    def test_the_old_pin_is_removed_only_if_they_agree(self):
        page = self.look()
        self.assertEqual([place["declared"] for place in page.context["gone_places"]], [GFS_BUCKET])

        self.answer({"allow_place": MOVED_BUCKET})
        self.assertIn(GFS_BUCKET, HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations)

        self.answer({"forget_place": GFS_BUCKET})
        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations, {MOVED_BUCKET: "us-east-1"})

    def test_a_place_the_store_does_not_declare_cannot_be_pinned_by_posting_it(self):
        self.answer({"allow_place": ["s3://somewhere-else/", "http://169.254.169.254/"]})

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations, {GFS_BUCKET: "us-east-1"})


class NewStoreAddressTests(StoreRecheckMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.publish_store(href=MOVED_STORE)

    def test_it_is_proposed(self):
        page = self.look()

        self.assertEqual(page.context["moved_to"], MOVED_STORE)
        self.assertContains(page, "v0.2.0.icechunk")
        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).store_href, STORE)

    def test_on_their_yes_the_feed_adopts_it(self):
        self.answer({"adopt_store": MOVED_STORE})

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).store_href, MOVED_STORE)

    def test_an_address_the_survey_did_not_find_cannot_be_adopted_by_posting_it(self):
        self.answer({"adopt_store": "https://169.254.169.254/latest.icechunk"})

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).store_href, STORE)

    def test_the_next_harvest_leaves_every_row_in_the_window_pointing_at_it(self):
        # The run is closed: a scheduled harvest would not ask about it again.
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.CLOSED)

        self.answer({"adopt_store": MOVED_STORE})
        self.feed.refresh_from_db()
        self.harvest()

        self.assertEqual(set(Asset.objects.values_list("href", flat=True)), {MOVED_STORE})
        self.assertEqual(Item.objects.count(), 2, "rewritten where they are, not removed and indexed again")

    def test_a_store_on_a_new_host_pins_that_host_with_the_address(self):
        elsewhere = "https://stores.example.org/gfs/v0.2.0.icechunk"
        self.publish_store(href=elsewhere)

        self.answer({"adopt_store": elsewhere})

        feed = HarvestFeed.objects.get(pk=self.feed.pk)
        self.assertEqual(feed.store_href, elsewhere)
        self.assertIn("stores.example.org", feed.allowed_asset_hosts)


class ChangedVerdictTests(StoreRecheckMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.before = self.rows()
        self.publish_store(codecs={"temperature_2m": ("no-such-codec",)})

    def test_the_reason_is_shown_and_no_row_changes(self):
        page = self.look()
        self.answer({"adopt_store": STORE, "offer_arrays": "1"})

        self.assertContains(page, "no-such-codec")
        self.assertFalse(page.context["fits"])
        self.assertEqual(self.rows(), self.before)
        self.assertTrue(HarvestFeed.objects.filter(pk=self.feed.pk).exists())

    def test_harvests_fail_with_that_reason_until_it_is_resolved(self):
        self.look()
        self.feed.refresh_from_db()

        self.harvest(recheck=True)

        failed = FetchRun.objects.latest("pk")
        self.assertEqual(failed.status, FetchRun.Status.FAILED)
        self.assertIn("no-such-codec", failed.error_message)
        self.assertEqual(self.rows(), self.before)

        self.publish_store()
        self.look()
        self.answer({})
        self.feed.refresh_from_db()
        self.harvest()

        self.assertEqual(FetchRun.objects.latest("pk").status, FetchRun.Status.COMPLETED)

    def test_an_unreachable_upstream_records_no_refusal(self):
        self.upstream.unreachable = "timed out"

        self.look()

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).upstream_refusal, "")

    def test_a_store_that_does_not_answer_in_time_records_no_refusal_and_says_to_try_again(self):
        self.publish_store()
        self.upstream.store_times_out = True

        response = self.look()

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).upstream_refusal, "")
        said = " ".join(str(message) for message in response.context["messages"])
        self.assertIn("took too long to answer", said)
        self.assertIn("try again", said)


class RecheckWaitsOffTheRequestTests(StoreRecheckMixin, TestCase):
    """The survey behind a re-check runs off the request, and saving acts on
    the survey the operator saw without reading the Upstream again (#695)."""

    def setUp(self):
        super().setUp()
        self.publish_store(places=(MOVED_BUCKET,))

    def reads(self):
        return (self.upstream.roots_read, len(self.upstream.stores_opened), len(self.upstream.regions_asked))

    def test_opening_it_returns_at_once_and_says_what_it_is_waiting_on(self):
        self.hold_surveys = True

        opened = self.client.get(self.url)
        waiting = self.client.get(opened["Location"])

        self.assertEqual(opened.status_code, 302)
        self.assertEqual(self.reads(), (0, 0, 0), "nothing is read in the request")
        self.assertContains(waiting, "GeoRiva is reading the catalog")
        self.assertContains(waiting, "Waiting for GeoRiva to start the check")
        self.assertNotContains(waiting, 'name="allow_place"')

        for args in self.surveys_started:
            background_survey.run(*args)

        found = self.client.get(opened["Location"])
        self.assertEqual([place["declared"] for place in found.context["new_places"]], [MOVED_BUCKET])

    def test_the_wait_looks_again_on_its_own_without_a_script_and_stops_once_answered(self):
        self.hold_surveys = True
        waiting = self.client.get(self.client.get(self.url)["Location"])
        self.assertContains(waiting, '<meta http-equiv="refresh"')

        self.hold_surveys = False
        self.assertNotContains(self.look(), 'http-equiv="refresh"')

    def test_reloading_the_wait_starts_no_second_survey_and_opening_it_again_does(self):
        self.hold_surveys = True
        opened = self.client.get(self.url)

        for _ in range(3):
            self.client.get(opened["Location"])
        self.assertEqual(len(self.surveys_started), 1)

        self.client.get(self.url)
        self.assertEqual(len(self.surveys_started), 2, "a re-check always reads afresh")

    def test_saving_acts_on_the_survey_shown_without_reading_the_upstream_again(self):
        shown = self.look()
        before = self.reads()
        # Changed since the page was shown: what is saved is what the operator saw.
        self.publish_store(places=(GFS_BUCKET,))

        self.client.post(shown.redirect_chain[-1][0], {"allow_place": MOVED_BUCKET})

        self.assertEqual(self.reads(), before)
        pins = HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations
        self.assertEqual(pins, {GFS_BUCKET: "us-east-1", MOVED_BUCKET: "us-east-1"})

    def test_a_survey_no_longer_kept_is_checked_again_not_saved_blind(self):
        shown = self.look()
        address = shown.redirect_chain[-1][0]
        token = address.rpartition("check=")[2]
        cache.delete(background_survey._key(token))
        before = self.configuration()

        response = self.client.post(address, {"allow_place": MOVED_BUCKET}, follow=True)

        self.assertEqual(self.configuration(), before)
        self.assertIn("check again", " ".join(str(message) for message in response.context["messages"]))

    def test_a_check_this_session_did_not_open_cannot_be_saved(self):
        self.look()
        elsewhere = background_survey.start(self.feed.upstream_url, self.feed.upstream_collection_id)
        before = self.configuration()

        self.client.post(f"{self.url}?check={elsewhere}", {"allow_place": MOVED_BUCKET})

        self.assertEqual(self.configuration(), before)

    def test_a_check_opened_for_one_feed_cannot_be_saved_on_another(self):
        shown = self.look()
        token = shown.redirect_chain[-1][0].rpartition("check=")[2]
        other = HarvestFeed.objects.create(
            name="Another feed",
            catalog=Catalog.objects.create(
                organisation=self.feed.catalog.organisation,
                name="GFS too",
                slug="gfs-too",
                file_format=Catalog.FileFormat.GEOTIFF,
            ),
            upstream_url=self.feed.upstream_url,
            upstream_collection_id=self.feed.upstream_collection_id,
            upstream_shape=self.feed.upstream_shape,
            store_href=STORE,
            pinned_chunk_locations={GFS_BUCKET: "us-east-1"},
        )
        url = reverse("harvest_recheck_upstream", kwargs={"pk": other.pk})

        refused = self.client.post(f"{url}?check={token}", {"allow_place": MOVED_BUCKET}, follow=True)

        self.assertEqual(HarvestFeed.objects.get(pk=other.pk).pinned_chunk_locations, {GFS_BUCKET: "us-east-1"})
        self.assertIn("not started here", " ".join(str(message) for message in refused.context["messages"]))

    def test_a_saved_check_cannot_be_saved_again(self):
        shown = self.look()
        address = shown.redirect_chain[-1][0]
        self.client.post(address, {"allow_place": MOVED_BUCKET})
        # Since then, the place was taken away again.
        HarvestFeed.objects.filter(pk=self.feed.pk).update(pinned_chunk_locations={GFS_BUCKET: "us-east-1"})

        self.client.post(address, {"allow_place": MOVED_BUCKET})

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations, {GFS_BUCKET: "us-east-1"})

    def test_an_older_check_left_open_in_another_tab_is_closed_by_a_save(self):
        older = self.look().redirect_chain[-1][0]
        self.answer({"allow_place": MOVED_BUCKET})
        HarvestFeed.objects.filter(pk=self.feed.pk).update(pinned_chunk_locations={GFS_BUCKET: "us-east-1"})

        self.client.post(older, {"allow_place": MOVED_BUCKET})

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).pinned_chunk_locations, {GFS_BUCKET: "us-east-1"})

    def test_a_check_whose_kept_answer_cannot_be_read_is_checked_again_not_an_error(self):
        shown = self.look()
        token = shown.redirect_chain[-1][0].rpartition("check=")[2]

        with mock.patch.object(background_survey.cache, "get", side_effect=ValueError("unpickling failed")):
            page = self.client.get(shown.redirect_chain[-1][0])

        self.assertEqual(page.status_code, 200)
        self.assertContains(page, "Try again")
        self.assertNotContains(page, token and 'name="allow_place"')

    def test_a_survey_that_never_answers_is_given_up_on_with_a_way_to_try_again(self):
        self.hold_surveys = True
        opened = self.client.get(self.url)

        with mock.patch.object(background_survey, "GIVE_UP_SECONDS", -1):
            page = self.client.get(opened["Location"])

        self.assertIn("took too long", " ".join(str(message) for message in page.context["messages"]))
        self.assertContains(page, "Try again")
        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).upstream_refusal, "")


class StaleRefusalTests(StoreRecheckMixin, TestCase):
    def test_reloading_an_older_check_does_not_record_its_refusal_again(self):
        self.publish_store(codecs={"temperature_2m": ("no-such-codec",)})
        older = self.look().redirect_chain[-1][0]
        self.publish_store()
        self.answer({})
        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).upstream_refusal, "")

        self.client.get(older)

        self.assertEqual(HarvestFeed.objects.get(pk=self.feed.pk).upstream_refusal, "")


class DecliningEverythingTests(StoreRecheckMixin, TestCase):
    def test_declining_every_proposal_changes_nothing(self):
        self.publish_store(
            href=MOVED_STORE,
            arrays={**self.arrays, "precipitation_surface": (RUN_STEP_GRID, WHOLE_FIELD)},
            places=(MOVED_BUCKET,),
        )
        before, rows = self.configuration(), self.rows()

        page = self.look()
        self.answer({})

        self.assertTrue(page.context["moved_to"] and page.context["new_places"] and page.context["new_arrays"])
        self.assertEqual(self.configuration(), before)
        self.assertEqual(self.rows(), rows)

    def test_saving_starts_the_full_pass_over_every_run_in_the_window(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.answer({})

        self.run_now.assert_called_once_with()


class ConfirmedChangesReachTitilerTests(StoreRecheckMixin, TestCase):
    def setUp(self):
        self.redis = get_redis_connection("default")
        self._sweep_redis()
        self.addCleanup(self._sweep_redis)
        super().setUp()

    def _sweep_redis(self):
        keys = list(self.redis.scan_iter(match=f"{REMOTE_ASSETS_KEY_PREFIX}:*"))
        if keys:
            self.redis.delete(*keys)

    def published(self) -> list[dict]:
        fields = {k.decode(): v.decode() for k, v in self.redis.hgetall(variable_key(self.temperature)).items()}
        fields.pop(PUBLISHED_FIELD, None)
        return [json.loads(value) for value in fields.values()]

    def test_a_confirmed_pin_is_published_at_once(self):
        self.publish_store(places=(MOVED_BUCKET,))

        self.answer({"allow_place": MOVED_BUCKET, "forget_place": GFS_BUCKET})

        self.assertEqual([entry["chunk_locations"] for entry in self.published()], [{MOVED_BUCKET: "us-east-1"}] * 2)

    def test_an_adopted_address_is_published_once_the_harvest_has_rewritten_the_rows(self):
        self.publish_store(href=MOVED_STORE)

        self.answer({"adopt_store": MOVED_STORE})
        self.feed.refresh_from_db()
        self.harvest()

        self.assertEqual({entry["store"] for entry in self.published()}, {MOVED_STORE})
