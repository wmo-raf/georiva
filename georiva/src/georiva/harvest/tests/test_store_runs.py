"""
A store harvest opens and closes model runs (ADR 0026, ADR 0044 §6).

A harvest writes no `FileIngestion`, so it keeps the run record itself: a run
opens on its first Held step, closes once the store holds every step it expects
of it for every mapped Variable, and is not asked about again once closed —
until an operator's own run re-checks the whole window.
"""

from datetime import timedelta
from unittest import mock

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Collection, Item
from georiva.harvest.models import HarvestFeed
from georiva.ingestion.models import RunIngestion
from georiva.ingestion.run_closers import close_quiet_runs
from georiva.organisations.testing import dial_org

from .test_store_harvest import StoreFeedMixin, hours

#: How many steps the fake store expects of a run, in the tests below.
STEPS = 3


class StoreRunsMixin(StoreFeedMixin):
    def setUp(self):
        super().setUp()
        self.upstream.steps_per_run = STEPS

    def hold_run(self, path, run=None):
        """Every step the store expects of `run`, held for `path`."""
        self.hold(path, *(3 * n for n in range(STEPS)), run=run)

    def record(self, run=None):
        return RunIngestion.objects.filter(collection=self.feed.collection, reference_time=run or self.run).first()


class StoreRunRecordTests(StoreRunsMixin, TestCase):
    def test_a_run_opens_on_its_first_held_step(self):
        self.hold("temperature_2m", 0)

        self.harvest()

        record = self.record()
        self.assertEqual(record.status, RunIngestion.Status.OPEN)
        self.assertEqual((record.file_count, record.expected_file_count), (1, STEPS))

    def test_a_run_the_store_holds_no_step_of_has_no_record(self):
        self.harvest()

        self.assertFalse(RunIngestion.objects.exists())

    def test_a_run_whose_every_expected_step_is_held_closes_on_the_new_signal(self):
        self.hold_run("temperature_2m")

        self.harvest()

        record = self.record()
        self.assertEqual(record.status, RunIngestion.Status.CLOSED)
        self.assertEqual(record.closed_by, RunIngestion.Closer.STORE_HELD)
        self.assertEqual(record.revision, 0)

    def test_steps_arriving_out_of_order_close_the_run_once_the_last_lands(self):
        self.hold("temperature_2m", 0, 6)
        self.harvest()
        self.assertTrue(self.record().is_open)

        self.hold("temperature_2m", 3)
        self.harvest()

        self.assertTrue(self.record().is_complete)

    def test_the_stores_own_expectation_of_a_run_bounds_it(self):
        later = self.run + hours(6)
        self.upstream.expected_steps[later] = 1
        self.hold("temperature_2m", 0, run=later)

        self.harvest()

        self.assertTrue(self.record(later).is_complete)
        self.assertEqual(self.record(later).expected_file_count, 1)

    def test_a_run_complete_for_one_mapped_variable_and_not_another_stays_open(self):
        self.map_variable("wind_u_10m", "wind-u-10m")
        self.hold_run("temperature_2m")
        self.hold("wind_u_10m", 0, 3)

        self.harvest()

        self.assertTrue(self.record().is_open)

        self.hold("wind_u_10m", 6)
        self.harvest()

        self.assertTrue(self.record().is_complete)

    def test_the_quiet_period_does_not_close_a_run_with_holes(self):
        self.hold("temperature_2m", 0)
        self.harvest()

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(hours=12)):
            close_quiet_runs(minutes=1)

        self.assertTrue(self.record().is_open)

    def test_a_run_aged_out_of_the_window_has_no_record_left(self):
        newer = self.run + hours(12)
        self.hold_run("temperature_2m")
        self.hold("temperature_2m", 0, run=newer)
        self.harvest()

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=3, hours=1)):
            self.harvest()

        self.assertIsNone(self.record())
        self.assertIsNotNone(self.record(newer))

    def test_a_failed_harvest_leaves_the_run_records_as_they_were(self):
        self.hold("temperature_2m", 0)
        self.harvest()
        self.upstream.store_unreachable = "connection reset"

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=4)):
            self.harvest()

        self.assertTrue(self.record().is_open)


class ClosedRunNotAskedTests(StoreRunsMixin, TestCase):
    def test_a_harvest_does_not_ask_the_store_about_a_closed_run(self):
        newer = self.run + hours(6)
        self.hold_run("temperature_2m")
        self.hold("temperature_2m", 0, run=newer)
        self.harvest()

        self.harvest()

        self.assertEqual(self.upstream.runs_asked[-1], {newer})

    def test_the_rows_of_a_closed_run_are_kept_though_it_was_not_asked(self):
        self.hold_run("temperature_2m")
        self.harvest()

        result = self.harvest()

        self.assertEqual(result.removed, 0)
        self.assertEqual(Item.objects.filter(reference_time=self.run).count(), STEPS)

    def test_a_closed_run_is_still_aged_out_of_the_window(self):
        self.hold_run("temperature_2m")
        self.harvest()

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=4)):
            self.harvest()

        self.assertFalse(Item.objects.exists())
        self.assertIsNone(self.record())

    def test_a_step_gone_from_a_closed_run_is_not_found_by_a_scheduled_harvest(self):
        self.hold_run("temperature_2m")
        self.harvest()
        self.upstream.release("temperature_2m", self.run + hours(3), run=self.run)

        self.harvest()

        self.assertTrue(self.record().is_complete)

    def test_a_full_recheck_reopens_a_closed_run_a_step_has_gone_from(self):
        self.hold_run("temperature_2m")
        self.harvest()
        self.upstream.release("temperature_2m", self.run + hours(3), run=self.run)

        self.harvest(recheck=True)

        record = self.record()
        self.assertTrue(record.is_open)
        self.assertEqual(record.revision, 1)
        self.assertEqual(record.file_count, STEPS - 1)
        self.assertFalse(Item.objects.filter(time=self.run + hours(3)).exists())

    def test_a_reopened_run_closes_again_once_the_store_holds_it_whole(self):
        self.hold_run("temperature_2m")
        self.harvest()
        self.upstream.release("temperature_2m", self.run + hours(3), run=self.run)
        self.harvest(recheck=True)

        self.hold("temperature_2m", 3)
        self.harvest()

        record = self.record()
        self.assertTrue(record.is_complete)
        self.assertEqual(record.revision, 1)

    def test_run_now_rechecks_the_whole_window(self):
        self.hold_run("temperature_2m")
        self.harvest()

        with mock.patch.object(HarvestFeed, "upstream_client", return_value=self.upstream):
            self.feed.run_now(async_run=False)

        self.assertEqual(self.upstream.runs_asked[-1], {self.run})


class WidenedMappingTests(StoreRunsMixin, TestCase):
    def test_a_newly_mapped_variable_is_completed_for_closed_runs_on_the_next_harvest(self):
        self.hold_run("temperature_2m")
        self.hold_run("wind_u_10m")
        self.harvest()
        self.assertTrue(self.record().is_complete)

        wind = self.map_variable("wind_u_10m", "wind-u-10m")
        self.harvest()

        self.assertEqual(self.steps(wind), [(self.run, 3 * n) for n in range(STEPS)])
        record = self.record()
        self.assertTrue(record.is_complete)
        self.assertEqual(record.revision, 0)

    def test_a_closed_run_the_new_variable_is_not_whole_for_reopens(self):
        self.hold_run("temperature_2m")
        self.hold("wind_u_10m", 0)
        self.harvest()

        self.map_variable("wind_u_10m", "wind-u-10m")
        self.harvest()

        record = self.record()
        self.assertTrue(record.is_open)
        self.assertEqual(record.revision, 1)


class StoreRunApiTests(StoreRunsMixin, TestCase):
    def setUp(self):
        super().setUp()
        dial_org(self.client, self.organisation.slug)
        collection = self.feed.ensure_collection()
        Collection.objects.filter(pk=collection.pk).update(visibility=Collection.Visibility.PUBLIC)
        self.newer = self.run + hours(6)

    def get(self, **params):
        response = self.client.get(reverse("run-list", args=["gfs", self.feed.collection.slug]), params)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()

    def test_a_run_with_some_steps_held_is_open_and_not_latest_complete(self):
        self.hold("temperature_2m", 0, 6)

        self.harvest()

        body = self.get()
        self.assertIsNone(body["latest_complete"])
        (run,) = body["runs"]
        self.assertEqual((run["status"], run["complete"]), ("open", False))

    def test_the_newest_run_held_whole_is_latest_complete(self):
        older = self.run - hours(6)
        self.hold_run("temperature_2m", run=older)
        self.hold_run("temperature_2m")
        self.hold("temperature_2m", 0, run=self.newer)

        self.harvest()

        body = self.get()
        self.assertEqual(body["latest_complete"]["reference_time"], self.run.isoformat())
        self.assertEqual(body["latest_complete"]["closed_by"], "store_held")
        self.assertEqual([run["complete"] for run in body["runs"]], [False, True, True])

    def test_valid_times_and_one_run_answer_as_for_any_collection(self):
        self.hold_run("temperature_2m")
        self.hold("temperature_2m", 0, 6, run=self.newer)
        self.harvest()

        body = self.get(valid_times=1, reference_time=self.newer.isoformat())

        (run,) = body["runs"]
        self.assertEqual(run["reference_time"], self.newer.isoformat())
        self.assertEqual([*map(_parsed, run["valid_times"])], [self.newer, self.newer + hours(6)])
        self.assertEqual(
            [*map(_parsed, body["latest_complete"]["valid_times"])], [self.run + hours(3 * n) for n in range(STEPS)]
        )


def _parsed(value):
    from django.utils.dateparse import parse_datetime

    return parse_datetime(value)
