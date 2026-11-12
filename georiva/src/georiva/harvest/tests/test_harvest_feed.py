"""
A Harvest feed is a pull feed that rides the shared scheduling, retry and queue
machinery with its own task (ADR 0042 §4, §11).
"""

import json
from datetime import timedelta
from unittest import mock

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Asset, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.tasks import run_harvest
from georiva.harvest.upstream import StacApiClient
from georiva.ingestion.models import LoaderJob
from georiva.sources.acquisition_recovery import RETRY_QUEUED, dispatch_due_retries, dispatch_retry
from georiva.sources.models import DataFeed, FetchRun

from .support import UPSTREAM_URL, HarvestFeedMixin, day, rain_href

HARVEST_TASK = "georiva.harvest.tasks.run_harvest"


class HarvestFeedConfigurationTests(HarvestFeedMixin, TestCase):
    def test_it_is_a_data_feed_and_its_catalogs_one_feed(self):
        self.assertEqual(DataFeed.objects.get(), self.feed)
        self.assertEqual(self.feed.catalog.data_feed.get_real_instance(), self.feed)

    def test_the_allowlist_defaults_to_the_catalogs_own_host(self):
        self.assertEqual(self.feed.allowed_asset_hosts, {"stac.upstream.example"})

    def test_the_operator_extends_the_allowlist(self):
        self.feed.asset_hosts = ["Bucket.Elsewhere.example"]

        self.assertEqual(self.feed.allowed_asset_hosts, {"stac.upstream.example", "bucket.elsewhere.example"})

    def test_its_own_client_reads_its_upstream_catalog(self):
        client = self.feed.upstream_client()

        self.assertIsInstance(client, StacApiClient)
        self.assertEqual(client.url, UPSTREAM_URL)

    def test_ensuring_the_collection_twice_gives_the_same_one(self):
        first = self.feed.ensure_collection()

        self.assertEqual(self.feed.ensure_collection(), first)
        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(self.feed.collection_links.count(), 1)

    def test_a_mapping_must_name_a_variable_of_the_feeds_collection(self):
        self.feed.ensure_collection()
        elsewhere = Collection.objects.create(catalog=self.feed.catalog, name="Elsewhere", slug="elsewhere")
        unit, _ = Unit.objects.get_or_create(name="Millimetre", symbol="mm")
        stranger = Variable.objects.create(
            collection=elsewhere, slug="rain", name="Rain", unit=unit, value_min=0, value_max=1
        )

        with self.assertRaises(ValidationError) as raised:
            VariableMapping(feed=self.feed, asset_key="rainfall", variable=stranger).full_clean()
        self.assertIn("variable", raised.exception.message_dict)


class HarvestTaskTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        patcher = mock.patch.object(HarvestFeed, "upstream_client", lambda feed: self.upstream)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_the_harvest_task_runs_on_the_default_queue(self):
        app.loader.import_default_modules()

        self.assertEqual(app.tasks[HARVEST_TASK].queue, "georiva-default")

    def test_the_task_harvests_the_feed(self):
        outcome = run_harvest(self.feed.pk)

        self.assertEqual((outcome["status"], outcome["indexed"]), ("success", 1))
        self.assertEqual(Asset.objects.get().href, rain_href(1))
        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED)

    def test_a_paused_feeds_schedule_harvests_nothing(self):
        HarvestFeed.objects.filter(pk=self.feed.pk).update(is_active=False)

        self.assertIsNone(run_harvest(self.feed.pk))
        self.assertFalse(FetchRun.objects.exists())

    def test_a_feed_that_is_gone_harvests_nothing(self):
        self.assertIsNone(run_harvest(self.feed.pk + 1000))

    def test_the_feeds_schedule_fires_the_harvest_task_at_its_cadence(self):
        self.feed.interval_minutes = 720
        self.feed.save()

        periodic = PeriodicTask.objects.get(args=json.dumps([self.feed.pk]))
        self.assertEqual((periodic.task, periodic.enabled), (HARVEST_TASK, True))
        self.assertEqual((periodic.interval.every, periodic.interval.period), (720, "minutes"))

    def test_run_now_enqueues_the_harvest_task_and_no_loader_job(self):
        with mock.patch.object(run_harvest, "delay") as delay:
            self.feed.run_now()

        delay.assert_called_once_with(self.feed.pk, scheduled=False)
        self.assertFalse(LoaderJob.objects.exists())

    def test_run_now_synchronously_harvests(self):
        (result,) = self.feed.run_now(async_run=False)

        self.assertEqual(result.indexed, 1)


class HarvestRetryTests(HarvestFeedMixin, TestCase):
    """A stopped harvest is retried by the same sweep as any run, through its own task."""

    def setUp(self):
        super().setUp()
        self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        patcher = mock.patch.object(HarvestFeed, "upstream_client", lambda feed: self.upstream)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.upstream.unreachable = "upstream catalog unreachable: timed out"
        self.harvest()
        self.failed = FetchRun.objects.get()
        self.upstream.unreachable = None

    def test_a_due_retry_is_dispatched_to_the_harvest_task_not_a_loader_job(self):
        with mock.patch.object(run_harvest, "delay") as delay:
            dispatched = dispatch_due_retries(now=timezone.now() + timedelta(hours=1))

        self.assertEqual(dispatched, 1)
        delay.assert_called_once_with(self.feed.pk, resume_of_run_id=self.failed.pk)
        self.assertFalse(LoaderJob.objects.exists())

    def test_the_retry_harvests_as_a_run_linked_to_the_one_it_retries(self):
        run_harvest(self.feed.pk, resume_of_run_id=self.failed.pk)

        retry = FetchRun.objects.exclude(pk=self.failed.pk).get()
        self.assertEqual((retry.resumed_from, retry.status, retry.harvest_indexed), (self.failed, "completed", 1))
        self.assertEqual(retry.correlation_id, self.failed.correlation_id)

    def test_a_retry_dispatched_twice_harvests_once(self):
        run_harvest(self.feed.pk, resume_of_run_id=self.failed.pk)

        self.assertIsNone(run_harvest(self.feed.pk, resume_of_run_id=self.failed.pk))
        self.assertEqual(FetchRun.objects.count(), 2)

    def test_a_run_already_retried_is_not_dispatched_again(self):
        run_harvest(self.feed.pk, resume_of_run_id=self.failed.pk)

        with mock.patch.object(run_harvest, "delay") as delay:
            outcome = dispatch_retry(self.failed)

        self.assertNotEqual(outcome, RETRY_QUEUED)
        delay.assert_not_called()

    def test_a_paused_feed_is_still_retried(self):
        HarvestFeed.objects.filter(pk=self.feed.pk).update(is_active=False)

        run_harvest(self.feed.pk, resume_of_run_id=self.failed.pk)

        self.assertEqual(FetchRun.objects.count(), 2)
