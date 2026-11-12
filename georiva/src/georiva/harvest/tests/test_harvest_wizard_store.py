"""
The Harvest Feed Wizard, from an accepting verdict on a map-shaped Upstream
store to a saved Harvest feed (ADR 0044 §1, §7, §8, §11, §12).

Driven through the wizard's own pages, as an operator signed in to an
organisation's admin. The only fake is the Upstream — here one that is a store.
"""

from datetime import timedelta
from unittest import mock

from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Asset, Catalog, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, UpstreamShape, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.harvest.upstream import StoreField
from georiva.organisations.testing import make_organisation
from georiva.sources.models import FetchRun

from .test_harvest_wizard import WizardMixin, step, wizard_copy

STATIC = "https://stac.upstream.example/catalog.json"
STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
STORE_HOST = "dynamical-noaa-gfs.s3.us-west-2.amazonaws.com"
GFS = "s3://noaa-gfs-bdp-pds/"
ECMWF_S3 = "s3://ecmwf-forecasts/"
ECMWF_GS = "gs://ecmwf-open-data/"
FORECAST = {"init_time": None, "lead_time": 209, "latitude": 721, "longitude": 1440, "pressure_level": 13}
RUN_STEP_GRID = ("init_time", "lead_time", "latitude", "longitude")
WHOLE_FIELD = (1, 1, 721, 1440)
ABOUT = "Temperature of the air near the ground."
#: The words that are a store's makers' or a cloud's, never an operator's.
JARGON = ("Zarr", "zarr", "Icechunk", "icechunk", "chunk", "codec", "STAC", "S3", "s3://", "gs://", "bucket")


class StoreWizardMixin(WizardMixin):
    """An Upstream like stac.dynamical.org's GFS: plain files, no API, one
    virtual forecast store whose data is the producer's own files."""

    places = (GFS,)

    def setUp(self):
        super().setUp()
        self.upstream = FakeUpstream("noaa-gfs-forecast-virtual", title="NOAA GFS forecast")
        self.upstream.is_api = False
        self.upstream.regions.update({"noaa-gfs-bdp-pds": "us-east-1", "ecmwf-forecasts": "eu-central-1"})
        # Seeded with core, as every unit an operator is offered is.
        self.celsius = Unit.objects.get(symbol="°C")
        self.arrays = {
            "temperature_2m": (
                RUN_STEP_GRID,
                WHOLE_FIELD,
                {"title": "2 metre temperature", "unit": "°C", "description": ABOUT},
            ),
            "wind_u_10m": (RUN_STEP_GRID, WHOLE_FIELD),
            "pressure_level/temperature": (
                ("init_time", "lead_time", "pressure_level", "latitude", "longitude"),
                (1, 1, 1, 721, 1440),
                {"title": "Temperature aloft"},
            ),
            "init_time": (("init_time",), (1024,)),
        }
        self.upstream.field_ranges["temperature_2m"] = (-38.2, 41.7)
        self.store()

    def store(self, href=STORE, dimensions=FORECAST, **kwargs):
        self.upstream.become_store(href, dimensions, self.arrays, chunk_locations=self.places, **kwargs)
        self.upstream.field_places = {path: self.places[0] for path in self.arrays} if self.places else {}

    # The steps, each as an operator takes it.

    def reach_fields(self):
        self.assertRedirects(self.give_address(STATIC), step(2))
        self.assertRedirects(self.pick_collection("noaa-gfs-forecast-virtual"), step(3))
        return self.client.get(step(4))

    def tick(self, *indexes, places=(0,), store=True, **typed):
        """Tick the fields at `indexes` and allow the places at `places`."""
        data = {f"use_{index}": "on" for index in indexes}
        data.update({f"allow_{index}": "on" for index in places})
        if store:
            data["allow_store"] = "on"
        data.update(typed)
        return self.client.post(step(4), data)

    def choose_temperature(self, places=(0,), **changes):
        """Both halves of step 4: tick the field, then accept what was read of it."""
        typed = {"name_0": "Temperature", "unit_0": self.celsius.pk, "low_0": "-40", "high_0": "50", "about_0": ""}
        typed.update(changes)
        self.tick(0, places=places)
        return self.tick(0, places=places, **typed)

    def save_store(self, **changes):
        data = {"window_start": "", "rolling_window_days": "3", "feed_name": "GFS from dynamical.org"}
        data.update(changes)
        return self.save(new_catalog_name="GFS (remote)", new_catalog_slug="gfs", **data)

    def assert_nothing_saved(self):
        self.assertFalse(HarvestFeed.objects.exists())
        self.assertFalse(Catalog.objects.exists())
        self.assertFalse(Collection.objects.exists())
        self.assertFalse(Variable.objects.exists())


class WizardSetsUpAStoreFeedTests(StoreWizardMixin, TestCase):
    def test_an_operator_with_only_an_address_reaches_a_saved_feed_for_a_store(self):
        self.assertRedirects(self.give_address(STATIC), step(2))
        self.assertRedirects(self.pick_collection("noaa-gfs-forecast-virtual"), step(3))
        verdict = self.client.get(step(3))
        self.assertContains(verdict, "GeoRiva can draw maps from “NOAA GFS forecast”")
        self.assertContains(verdict, "It holds 3 kinds of map.")
        self.assertContains(verdict, "GeoRiva can show 2 of them for now.")
        self.assertContains(verdict, step(4))

        self.assertContains(self.client.get(step(4)), "2 metre temperature")
        self.assertEqual(self.tick(0).status_code, 200)
        self.assertRedirects(self.choose_temperature(), step(5))
        self.assertContains(self.client.get(step(5)), "Temperature")
        response = self.save_store()

        feed = HarvestFeed.objects.get()
        self.assertRedirects(response, f"/admin/data-feeds/{feed.pk}/", fetch_redirect_response=False)
        self.assertEqual(feed.upstream_url, STATIC)
        self.assertEqual(feed.upstream_collection_id, "noaa-gfs-forecast-virtual")
        self.assertEqual((feed.upstream_shape, feed.is_store), (UpstreamShape.STORE, True))
        self.assertEqual(feed.store_href, STORE)
        self.assertEqual(feed.catalog.organisation, self.organisation)
        self.assertEqual(feed.catalog.file_format, Catalog.FileFormat.ZARR)
        collection = feed.collection
        variable = Variable.objects.get(collection=collection)
        self.assertEqual((variable.name, variable.unit, variable.slug), ("Temperature", self.celsius, "temperature_2m"))
        self.assertEqual((variable.value_min, variable.value_max), (-40.0, 50.0))
        self.assertEqual(feed.mapped_variables(collection), {"temperature_2m": variable})

    def test_the_mapping_key_is_the_arrays_full_path_in_the_store(self):
        self.arrays = {"surface/temperature_2m": self.arrays["temperature_2m"]}
        self.store()
        self.upstream.field_places = {}
        self.reach_fields()
        self.choose_temperature()

        self.save_store()

        self.assertEqual(VariableMapping.objects.get().asset_key, "surface/temperature_2m")

    def test_a_forecast_stores_feed_keeps_three_days_of_runs_unless_told_otherwise(self):
        self.reach_fields()
        self.choose_temperature()

        page = self.client.get(step(5))
        self.save_store()

        self.assertEqual(page.context["form"]["rolling_window_days"], "3")
        self.assertContains(page, 'name="rolling_window_days"')
        self.assertNotContains(page, 'name="window_start"')
        feed = HarvestFeed.objects.get()
        self.assertEqual((feed.rolling_window_days, feed.window_start, feed.window_end), (3, None, None))
        start, end = feed.window
        self.assertIsNone(end)
        self.assertAlmostEqual(start, timezone.now() - timedelta(days=3), delta=timedelta(minutes=1))

    def test_how_many_days_to_keep_is_the_operators_to_say_within_reason(self):
        self.reach_fields()
        self.choose_temperature()

        for days in ("0", "-1", "a week", "", "5000"):
            with self.subTest(days=days):
                self.assertIn("how many days", self.errors(self.save_store(rolling_window_days=days)))
                self.assert_nothing_saved()

        self.save_store(rolling_window_days="10")
        self.assertEqual(HarvestFeed.objects.get().rolling_window_days, 10)

    def test_an_analysis_stores_feed_is_saved_with_the_fixed_window(self):
        self.arrays = {"precipitation": (("time", "latitude", "longitude"), (1, 721, 1440), {"unit": "°C"})}
        self.store(dimensions={"time": None, "latitude": 721, "longitude": 1440})
        self.reach_fields()
        self.choose_temperature()

        page = self.client.get(step(5))
        self.save_store(window_start="2024-01-01", rolling_window_days="9")

        self.assertContains(page, 'name="window_start"')
        self.assertNotContains(page, 'name="rolling_window_days"')
        feed = HarvestFeed.objects.get()
        self.assertTrue(feed.is_store)
        self.assertIsNone(feed.rolling_window_days)
        self.assertEqual(feed.window_start.isoformat(), "2024-01-01T00:00:00+00:00")
        self.assertEqual(feed.window, (feed.window_start, None))

    def test_the_first_harvest_starts_as_the_feed_is_saved_and_the_operator_is_told(self):
        self.reach_fields()
        self.choose_temperature()

        response = self.save_store()

        self.run_now.assert_called_once_with()
        page = self.client.get(response.url)
        self.assertContains(page, "is now listing what the data store holds")
        self.assertContains(page, "maps cannot be drawn yet")

    def test_the_feed_it_saves_harvests(self):
        self.reach_fields()
        self.choose_temperature()
        self.save_store()
        feed = HarvestFeed.objects.get()
        run = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
        self.upstream.hold("temperature_2m", run + timedelta(hours=3), run=run)
        self.upstream.hold("wind_u_10m", run, run=run)  # offered, and not chosen

        from georiva.harvest.harvester import harvest

        result = harvest(feed, upstream=self.upstream)

        self.assertEqual((result.indexed, result.rejected), (1, 0))
        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED)
        asset = Asset.objects.get()
        self.assertEqual((asset.href, asset.store_array), (STORE, "temperature_2m"))
        self.assertEqual((asset.item.reference_time, asset.item.time), (run, run + timedelta(hours=3)))

    def test_the_feed_remembers_the_arrays_the_survey_offered(self):
        self.reach_fields()
        self.choose_temperature()

        self.save_store()

        self.assertEqual(
            HarvestFeed.objects.get().surveyed_assets,
            [{"key": "temperature_2m", "title": "2 metre temperature"}, {"key": "wind_u_10m", "title": ""}],
        )

    def test_no_page_uses_a_word_an_operator_must_look_up(self):
        self.places = (ECMWF_S3, ECMWF_GS)
        self.store()
        # Its data is in the Google place, asked about first; the Amazon one is declined, so the review speaks of it.
        self.upstream.field_places = {"temperature_2m": ECMWF_GS}
        pages = [self.reach_fields(), self.tick(0, places=(0,))]
        self.choose_temperature(places=(0,))
        pages.append(self.client.get(step(5)))

        for number, page in enumerate(pages):
            body = wizard_copy(page)
            for jargon in JARGON:
                self.assertNotIn(jargon, body, f"page {number}")


class WizardOffersAStoresFieldsTests(StoreWizardMixin, TestCase):
    def test_plain_fields_are_offered_and_the_rest_listed_with_why_not(self):
        page = self.reach_fields()

        self.assertEqual([row["path"] for row in page.context["rows"]], ["temperature_2m", "wind_u_10m"])
        self.assertEqual([array["path"] for array in page.context["unoffered"]], ["pressure_level/temperature"])
        self.assertContains(page, "Temperature aloft")
        self.assertContains(page, "It has levels.")
        self.assertNotContains(page, 'name="use_2"')

    def test_a_field_that_is_not_offered_cannot_be_chosen_by_posting_it(self):
        self.reach_fields()

        response = self.tick(2)

        self.assertIn("at least one", self.errors(response))
        self.assertEqual(self.upstream.samples_asked, [])
        self.assertRedirects(self.client.get(step(5)), step(4))

    def test_nothing_is_ticked_until_the_operator_ticks_it(self):
        page = self.reach_fields()

        self.assertFalse(any(row["use"] for row in page.context["rows"]))

    def test_a_store_with_hundreds_of_fields_is_a_list_to_search_not_one_long_form(self):
        self.arrays = {f"field_{n:03d}": (RUN_STEP_GRID, WHOLE_FIELD) for n in range(274)}
        self.store()

        page = self.reach_fields()

        self.assertContains(page, 'id="hwiz-search"')
        self.assertEqual(len(page.context["rows"]), 274)
        self.assertNotContains(page, 'name="unit_')
        self.assertNotContains(page, 'name="low_')

        chosen = self.tick(7, 200)
        self.assertEqual([row["path"] for row in chosen.context["details"]], ["field_007", "field_200"])

    def test_a_chosen_fields_name_unit_description_and_range_are_prefilled(self):
        self.reach_fields()

        page = self.tick(0)

        (row,) = page.context["details"]
        self.assertEqual((row["name"], row["unit_id"]), ("2 metre temperature", self.celsius.pk))
        self.assertEqual(row["about"], ABOUT)
        self.assertEqual((row["low"], row["high"]), (-40.0, 50.0), "from one map that was read, rounded outward")
        self.assertIn("read one map", self.errors(page))
        self.assertRedirects(self.client.get(step(5)), step(4), msg_prefix="shown before it is taken")

    def test_the_map_read_is_one_of_each_chosen_field_with_which_axis_is_which(self):
        self.reach_fields()

        self.tick(0, 1)

        ((href, fields, places),) = self.upstream.samples_asked
        run_step_grid = ("run", "step", "y", "x")
        self.assertEqual(href, STORE)
        self.assertEqual(fields, [StoreField("temperature_2m", run_step_grid), StoreField("wind_u_10m", run_step_grid)])
        self.assertEqual(places, {GFS: "us-east-1"})

    def test_what_the_operator_types_is_what_is_saved(self):
        self.reach_fields()
        self.choose_temperature(name_0="Air temperature", low_0="-60", high_0="60", about_0="Two metres up.")

        self.save_store()

        variable = Variable.objects.get()
        self.assertEqual((variable.name, variable.description), ("Air temperature", "Two metres up."))
        self.assertEqual((variable.value_min, variable.value_max), (-60.0, 60.0))

    def test_a_chosen_field_needs_a_unit_and_a_range_the_right_way_round(self):
        self.reach_fields()

        for changes, said in (({"unit_0": ""}, "Choose a unit"), ({"low_0": "50", "high_0": "-40"}, "lowest first")):
            with self.subTest(changes=changes):
                response = self.choose_temperature(**changes)

                self.assertIn(said, self.errors(response))
                self.assertRedirects(self.client.get(step(5)), step(4))

    def test_a_field_is_read_once_however_many_times_the_step_is_posted(self):
        self.reach_fields()

        self.choose_temperature()
        self.choose_temperature(unit_0="")

        self.assertEqual(len(self.upstream.samples_asked), 1)

    def test_coming_back_to_the_step_shows_what_was_chosen(self):
        self.reach_fields()
        self.choose_temperature(name_0="Air temperature")

        page = self.client.get(step(4))

        self.assertEqual([row["name"] for row in page.context["details"]], ["Air temperature"])
        self.assertTrue(page.context["places"][0]["allowed"])
        self.assertTrue(page.context["own"]["allowed"])


class WizardStopsAtAReadThatFailsTests(StoreWizardMixin, TestCase):
    def test_a_failed_read_stops_the_step_with_a_message_and_creates_nothing(self):
        self.upstream.field_problems["temperature_2m"] = "it did not answer within 60 seconds"
        self.reach_fields()

        response = self.choose_temperature()

        self.assertEqual(response.status_code, 200)
        self.assertIn("“2 metre temperature” could not be read", self.errors(response))
        self.assertIn("did not answer within 60 seconds", self.errors(response))
        self.assertEqual(response.context["details"], [])
        self.assertRedirects(self.client.get(step(5)), step(4))
        self.assertRedirects(self.save_store(), step(4), fetch_redirect_response=False)
        self.assert_nothing_saved()

    def test_a_store_with_no_closed_run_in_reach_says_so(self):
        self.upstream.field_problems["temperature_2m"] = "none of its newest 8 forecasts has arrived in full yet"
        self.reach_fields()

        response = self.tick(0)

        self.assertIn("none of its newest 8 forecasts has arrived in full yet", self.errors(response))

    def test_a_read_that_failed_is_tried_again_the_next_time(self):
        self.upstream.field_problems["temperature_2m"] = "it did not answer within 60 seconds"
        self.reach_fields()
        self.tick(0)
        del self.upstream.field_problems["temperature_2m"]

        self.assertRedirects(self.choose_temperature(), step(5))


class WizardConfirmsWhereAStoresDataIsTests(StoreWizardMixin, TestCase):
    """ECMWF AIFS Single as it was found: two places declared, and every field
    sampled kept in the Google one (ADR 0044 §11)."""

    places = (ECMWF_S3, ECMWF_GS)
    #: Declared places are put in order: the Google one is asked about first.
    GOOGLE, AMAZON = 0, 1

    def setUp(self):
        super().setUp()
        self.upstream.field_places = {path: ECMWF_GS for path in self.arrays}

    def test_the_stores_own_place_and_each_place_its_data_lives_are_asked_one_by_one(self):
        page = self.reach_fields()

        self.assertContains(page, f"This collection is kept at <strong>{STORE_HOST}</strong>")
        self.assertContains(page, "<strong>ecmwf-open-data</strong>, on Google&#x27;s cloud storage")
        self.assertContains(page, "<strong>ecmwf-forecasts</strong>, on Amazon&#x27;s cloud storage")
        self.assertContains(page, "Allow this feed to read from there?", count=3)
        self.assertFalse(any(place["allowed"] for place in page.context["places"]), "nothing is allowed unasked")

    def test_each_confirmed_place_is_saved_as_a_pinned_location(self):
        self.reach_fields()
        self.choose_temperature(places=(self.GOOGLE, self.AMAZON))

        self.save_store()

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.pinned_chunk_locations, {ECMWF_GS: "", ECMWF_S3: "eu-central-1"})
        self.assertEqual(feed.asset_hosts, [STORE_HOST])
        self.assertEqual(feed.pinned_buckets, {})

    def test_a_google_place_is_saved_as_declared_untranslated(self):
        self.reach_fields()
        self.choose_temperature(places=(self.GOOGLE,))

        self.save_store()

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.pinned_chunk_locations, {ECMWF_GS: ""})
        self.assertNotIn("storage.googleapis.com", " ".join(feed.allowed_asset_hosts))

    def test_declining_the_stores_own_place_does_not_go_on_and_nothing_is_read(self):
        self.reach_fields()

        response = self.tick(0, places=(self.GOOGLE,), store=False)

        self.assertEqual(response.status_code, 200)
        self.assertIn(STORE_HOST, self.errors(response))
        self.assertEqual(self.upstream.samples_asked, [])
        self.assertRedirects(self.client.get(step(5)), step(4))
        self.assertRedirects(self.save_store(), step(4), fetch_redirect_response=False)
        self.assert_nothing_saved()

    def test_a_store_on_the_catalogs_own_host_needs_no_confirmation_of_its_own(self):
        own = "https://stac.upstream.example/stores/aifs.icechunk"
        self.store(href=own)
        self.upstream.field_places = {path: ECMWF_GS for path in self.arrays}
        page = self.reach_fields()

        self.assertNotContains(page, 'name="allow_store"')
        self.assertRedirects(self.choose_temperature(places=(self.GOOGLE,), store=False), step(5))
        self.save_store()

        feed = HarvestFeed.objects.get()
        self.assertEqual((feed.store_href, feed.asset_hosts), (own, []))

    def test_the_read_follows_the_confirmed_places_only(self):
        self.reach_fields()

        self.tick(0, places=(self.GOOGLE,))

        ((_href, _fields, places),) = self.upstream.samples_asked
        self.assertEqual(places, {ECMWF_GS: ""})

    def test_a_declined_place_no_read_needed_is_allowed_and_the_review_says_what_it_costs(self):
        self.reach_fields()

        self.assertRedirects(self.choose_temperature(places=(self.GOOGLE,)), step(5))
        review = self.client.get(step(5))
        self.save_store()

        self.assertContains(review, "This feed will not read from <strong>ecmwf-forecasts</strong>")
        self.assertContains(review, "will not draw")
        self.assertContains(review, "<strong>ecmwf-open-data</strong>")
        self.assertEqual(HarvestFeed.objects.get().pinned_chunk_locations, {ECMWF_GS: ""})

    def test_the_review_says_nothing_of_declined_places_when_every_place_was_allowed(self):
        self.reach_fields()
        self.choose_temperature(places=(self.GOOGLE, self.AMAZON))

        self.assertNotContains(self.client.get(step(5)), "will not draw")

    def test_declining_the_place_a_chosen_field_needs_stops_the_step_naming_it_and_saves_nothing(self):
        self.reach_fields()

        response = self.choose_temperature(places=(self.AMAZON,))

        self.assertEqual(response.status_code, 200)
        said = self.errors(response)
        self.assertIn("“2 metre temperature”", said)
        self.assertIn("“ecmwf-open-data”", said)
        self.assertIn("have not allowed", said)
        self.assertNotIn("gs://", said)
        self.assertRedirects(self.client.get(step(5)), step(4))
        self.assertRedirects(self.save_store(), step(4), fetch_redirect_response=False)
        self.assert_nothing_saved()

        both = self.choose_temperature(places=(self.GOOGLE, self.AMAZON))
        self.assertRedirects(both, step(5), msg_prefix="confirmed")

    def test_a_field_left_out_takes_its_place_with_it(self):
        self.upstream.field_places["wind_u_10m"] = ECMWF_S3
        self.reach_fields()

        self.tick(0, 1, places=(self.GOOGLE,))
        response = self.choose_temperature(places=(self.GOOGLE,))

        self.assertRedirects(response, step(5), msg_prefix="the field that needed the other place was dropped")

    def test_taking_a_place_away_reads_again_without_it(self):
        self.reach_fields()
        self.choose_temperature(places=(self.GOOGLE, self.AMAZON))

        self.assertRedirects(self.choose_temperature(places=(self.GOOGLE,)), step(5))
        response = self.choose_temperature(places=(self.AMAZON,))

        self.assertIn("“ecmwf-open-data”", self.errors(response))
        _href, _fields, places = self.upstream.samples_asked[-1]
        self.assertEqual(places, {ECMWF_S3: "eu-central-1"})

    def test_data_kept_somewhere_the_collection_never_mentioned_is_not_read_and_says_so(self):
        self.upstream.field_places["temperature_2m"] = "s3://somewhere-else/"
        self.reach_fields()

        response = self.tick(0, places=(self.GOOGLE, self.AMAZON))

        self.assertIn("somewhere this collection does not mention", self.errors(response))

    def test_a_place_that_cannot_be_found_is_shown_and_cannot_be_allowed(self):
        del self.upstream.regions["ecmwf-forecasts"]
        self.upstream.field_places["temperature_2m"] = ECMWF_S3
        page = self.reach_fields()

        self.assertContains(page, "which GeoRiva could not find")
        self.assertNotContains(page, f'name="allow_{self.AMAZON}"')

        response = self.tick(0, places=(self.GOOGLE, self.AMAZON))
        ((_href, _fields, places),) = self.upstream.samples_asked
        self.assertEqual(places, {ECMWF_GS: ""}, "posting its box does not allow it")
        self.assertIn("could not find", self.errors(response))

    def test_a_store_declaring_a_kind_of_place_georiva_does_not_read_ends_at_the_verdict(self):
        self.places = (GFS, "az://forecasts/")
        self.store()
        self.give_address(STATIC)
        self.pick_collection("noaa-gfs-forecast-virtual")

        verdict = self.client.get(step(3))

        self.assertContains(verdict, "GeoRiva cannot show “NOAA GFS forecast”")
        self.assertContains(verdict, "Microsoft Azure storage")
        self.assertNotContains(verdict, step(4))
        self.assertRedirects(self.client.get(step(4)), step(3))
        self.assertEqual(self.upstream.stores_opened, [])


class WizardSavesAStoreFeedTogetherOrNotAtAllTests(StoreWizardMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.reach_fields()
        self.choose_temperature()

    def test_a_failure_part_way_leaves_nothing_behind(self):
        with mock.patch.object(VariableMapping.objects, "create", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self.save_store()

        self.assert_nothing_saved()

    def test_a_catalog_short_name_already_taken_saves_nothing(self):
        Catalog.objects.create(
            organisation=self.organisation, name="Taken", slug="gfs", file_format=Catalog.FileFormat.GEOTIFF
        )

        response = self.save_store()

        self.assertIn("already exists", self.errors(response))
        self.assertFalse(HarvestFeed.objects.exists())
        self.assertFalse(Variable.objects.exists())

    def test_a_variable_curated_beforehand_is_never_overwritten(self):
        catalog = Catalog.objects.create(
            organisation=self.organisation, name="GFS", slug="mine", file_format=Catalog.FileFormat.ZARR
        )
        collection = Collection.objects.create(catalog=catalog, name="GFS", slug="noaa-gfs-forecast-virtual")
        tuned = Variable.objects.create(
            collection=collection,
            slug="temperature_2m",
            name="Hand-tuned",
            unit=self.celsius,
            value_min=-5,
            value_max=5,
        )

        self.save_store(catalog_mode="select", catalog_id=str(catalog.pk))

        tuned.refresh_from_db()
        self.assertEqual((tuned.name, tuned.value_min, tuned.value_max), ("Hand-tuned", -5.0, 5.0))
        self.assertEqual(HarvestFeed.objects.get().mapped_variables(collection), {"temperature_2m": tuned})

    def test_another_organisations_catalog_is_neither_offered_nor_taken(self):
        theirs = Catalog.objects.create(
            organisation=make_organisation("elsewhere"),
            name="Theirs",
            slug="theirs",
            file_format=Catalog.FileFormat.ZARR,
        )

        offered = self.client.get(step(5))
        response = self.save_store(catalog_mode="select", catalog_id=str(theirs.pk))

        self.assertNotContains(offered, "Theirs")
        self.assertIn("choose a catalog", self.errors(response))
        self.assertFalse(HarvestFeed.objects.exists())

    def test_the_feed_and_everything_made_with_it_is_the_active_organisations(self):
        self.save_store()

        feed = HarvestFeed.objects.get()
        self.assertEqual(feed.catalog.organisation, self.organisation)
        self.assertEqual(Variable.objects.get().collection.catalog, feed.catalog)

    def test_choosing_another_collection_forgets_what_was_read_and_confirmed(self):
        self.pick_collection("noaa-gfs-forecast-virtual")

        page = self.client.get(step(4))

        self.assertEqual(page.context["details"], [])
        self.assertFalse(page.context["places"][0]["allowed"])
        self.assertRedirects(self.client.get(step(5)), step(4))
