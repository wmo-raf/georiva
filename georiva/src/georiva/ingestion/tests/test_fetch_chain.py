"""A fetch and the ingestion of its files form one chain (ADR 0030, #425)."""

from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.events.context import current_context
from georiva.events.testing import event_context
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.models import FileIngestion, FileIngestionFinished
from georiva.ingestion.unprocessed import UnprocessedFile
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import make_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.sources.loader import Loader
from georiva.sources.models import DataFeed, FetchedFile, FetchRun, FetchRunFinished

KEY = "kenya/chirps/rainfall/rain.tif"


def _feed(organisation):
    catalog = Catalog.objects.create(organisation=organisation, name="CHIRPS", slug="chirps", file_format="geotiff")
    collection = Collection.objects.create(catalog=catalog, name="Rainfall", slug="rainfall")
    return DataFeed.objects.create(name="CHIRPS feed", catalog=catalog), collection


def _notify(key, **kwargs):
    """Handle a Bucket notification, returning the context the dispatch ran under."""
    seen = []
    with (
        patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
        patch("georiva.ingestion.consumer.process_incoming_file") as task,
    ):
        task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
        _handle_event({"s3": {"bucket": {"name": "georiva-sources"}, "object": {"key": key}}})
    return seen


class FetchRunEventTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.feed, self.collection = _feed(self.kenya)

    def test_a_completed_run_records_one_finished_event_in_its_own_chain(self):
        with event_context(actor="celery", organisation_id=self.kenya.pk) as ctx:
            run = FetchRun.objects.create(data_feed=self.feed, files_requested=3)
        run.mark_completed(files_fetched=2, files_skipped=1)

        event = FetchRunFinished.objects.get()
        self.assertEqual(event.outcome, "completed")
        self.assertEqual((event.files_requested, event.files_fetched, event.files_skipped), (3, 2, 1))
        self.assertEqual(event.correlation_id, ctx.correlation_id)
        self.assertEqual(event.organisation_id, self.kenya.pk)
        self.assertEqual(event.feed_name, "CHIRPS feed")

    def test_a_failed_run_records_a_failed_event(self):
        run = FetchRun.objects.create(data_feed=self.feed)
        run.mark_failed("source refused us")

        event = FetchRunFinished.objects.get()
        self.assertEqual((event.outcome, event.outcome_class), ("failed", "failed"))
        self.assertIn("source refused us", event.error)

    def test_an_interrupted_run_is_a_failure_and_a_cancelled_one_is_not_an_event(self):
        interrupted = FetchRun.objects.create(data_feed=self.feed)
        interrupted.mark_interrupted("worker died")
        cancelled = FetchRun.objects.create(data_feed=self.feed)
        cancelled.mark_cancelled()

        self.assertEqual(list(FetchRunFinished.objects.values_list("outcome", flat=True)), ["failed"])

    def test_a_sweep_finishing_a_run_speaks_in_the_runs_chain(self):
        with event_context(actor="celery", organisation_id=self.kenya.pk) as ctx:
            run = FetchRun.objects.create(data_feed=self.feed)
        with event_context(actor="sweep:stale_fetch_runs"):
            run.mark_interrupted("worker died")

        event = FetchRunFinished.objects.get()
        self.assertEqual(event.correlation_id, ctx.correlation_id)
        self.assertEqual(event.actor, "sweep:stale_fetch_runs")
        self.assertEqual(event.organisation_id, self.kenya.pk)

    def test_a_resumed_run_stays_in_the_interrupted_runs_chain(self):
        first = FetchRun.objects.create(data_feed=self.feed)
        resumed = FetchRun.objects.create(data_feed=self.feed, resumed_from=first)
        self.assertEqual(resumed.correlation_id, first.correlation_id)


class FetchToIngestionChainTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.feed, self.collection = _feed(self.kenya)

    def _fetched(self, key=KEY, finished=True):
        with event_context(actor="celery", organisation_id=self.kenya.pk):
            run = FetchRun.objects.create(data_feed=self.feed)
        FetchedFile.objects.create(fetch_run=run, file_path=key, status=FetchedFile.Status.STORED)
        if finished:
            run.mark_completed(files_fetched=1)
        return run

    def test_the_notification_for_a_fetched_file_adopts_the_fetchs_chain(self):
        run = self._fetched()

        (context,) = _notify(KEY)

        self.assertEqual(context.correlation_id, run.correlation_id)
        self.assertEqual(context.organisation_id, self.kenya.pk)
        self.assertEqual(context.caused_by, FetchRunFinished.objects.get().pk)

    def test_a_file_that_lands_before_the_fetch_finishes_still_joins_the_chain(self):
        run = self._fetched(finished=False)

        (context,) = _notify(KEY)

        self.assertEqual(context.correlation_id, run.correlation_id)
        self.assertIsNone(context.caused_by)

    def test_the_whole_story_shares_one_correlation(self):
        run = self._fetched()
        (context,) = _notify(KEY)

        with event_context(correlation_id=context.correlation_id, organisation_id=self.kenya.pk):
            FileIngestion.acquire(BucketType.SOURCES, KEY, "w")
            FileIngestion.mark_completed(BucketType.SOURCES, KEY, items_created=1)

        correlations = {
            FetchRunFinished.objects.get().correlation_id,
            FileIngestionFinished.objects.get().correlation_id,
        }
        self.assertEqual(correlations, {run.correlation_id})

    def test_a_file_dropped_into_the_bucket_starts_its_own_chain(self):
        run = self._fetched()

        (context,) = _notify("kenya/chirps/rainfall/dropped.tif")

        self.assertNotEqual(context.correlation_id, run.correlation_id)
        self.assertIsNone(context.caused_by)
        self.assertEqual(context.actor, "system:bucket")

    def test_the_most_recent_fetch_of_a_key_wins(self):
        self._fetched()
        second = self._fetched()

        (context,) = _notify(KEY)

        self.assertEqual(context.correlation_id, second.correlation_id)

    def _planned_in_a_later_run(self, key=KEY, **fields):
        with event_context(actor="celery", organisation_id=self.kenya.pk):
            later = FetchRun.objects.create(data_feed=self.feed)
        return FetchedFile.objects.create(fetch_run=later, file_path=key, **fields)

    def test_a_later_runs_waiting_row_does_not_take_the_chain(self):
        # The whole plan is written up front (#474): a later run holds a Waiting row
        # for the key long before it reaches it, and has stored nothing.
        run = self._fetched()
        self._planned_in_a_later_run()

        (context,) = _notify(KEY)

        self.assertEqual(context.correlation_id, run.correlation_id)

    def test_a_later_runs_not_attempted_row_does_not_take_the_chain(self):
        run = self._fetched()
        self._planned_in_a_later_run(status=FetchedFile.Status.FAILED, error="not attempted: the run stopped (x)")

        (context,) = _notify(KEY)

        self.assertEqual(context.correlation_id, run.correlation_id)

    def test_an_upload_older_than_the_fetch_loses_to_it_despite_a_later_waiting_row(self):
        from types import SimpleNamespace

        from django.utils import timezone

        from georiva.ingestion.chains import _upload_is_newer

        upload = SimpleNamespace(received_at=timezone.now())
        run = self._fetched()
        run.fetched_files.update(started_at=timezone.now(), completed_at=timezone.now())
        self._planned_in_a_later_run()

        self.assertFalse(_upload_is_newer(upload, KEY))

    def test_the_unprocessed_sweep_adopts_the_same_chain(self):
        from georiva.ingestion.tasks import sweep_unprocessed

        run = self._fetched()
        seen = []
        found = [UnprocessedFile(bucket=BucketType.SOURCES, file_path=KEY, reason="untracked")]
        with (
            patch("georiva.ingestion.unprocessed.find_unprocessed", return_value=found),
            patch("georiva.ingestion.tasks.process_incoming_file") as task,
        ):
            task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
            sweep_unprocessed.run()

        (context,) = seen
        self.assertEqual(context.correlation_id, run.correlation_id)
        self.assertEqual(context.actor, "sweep:unprocessed")


class LoaderRecordsFileBeforeStoringTests(TestCase):
    """The consumer finds a file's chain through its FetchedFile, so the record must exist first."""

    def test_the_copy_path_writes_the_record_before_the_object(self):
        kenya = make_organisation("kenya")
        feed, collection = _feed(kenya)
        loader = Loader(data_source=MagicMock(), collection=collection, data_feed=feed)
        loader.data_source.name = "test"
        request = MagicMock(filename="rain.tif", reference_time=None, expected_size=None)
        loader.data_source.generate_requests_for_collection.return_value = [request]
        destination = loader._get_storage_path(request)

        present_at_copy = []

        def copy(source, dest):
            present_at_copy.append(FetchedFile.objects.filter(file_path=dest).exists())

        with (
            patch.object(loader, "_already_exists", return_value=False),
            patch.object(loader, "_find_existing_catalog_path", return_value="kenya/chirps/other/rain.tif"),
            patch.object(loader, "_cleanup_temp"),
            patch.object(loader.fetch_strategy, "connect"),
            patch.object(loader.fetch_strategy, "disconnect"),
            patch.object(Loader, "_tier_bucket", MagicMock(copy=copy), create=True),
        ):
            loader.run()

        self.assertEqual(present_at_copy, [True])
        self.assertEqual(FetchedFile.objects.get().file_path, destination)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class FetchRunHistoryPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_feed, _ = _feed(cls.kenya)
        cls.uganda_feed, _ = _feed(cls.uganda)
        with event_context(organisation_id=cls.kenya.pk):
            cls.kenya_run = FetchRun.objects.create(data_feed=cls.kenya_feed)
            cls.kenya_run.mark_completed(files_fetched=1)
        cls.uganda_run = FetchRun.objects.create(data_feed=cls.uganda_feed)
        cls.uganda_run.mark_failed("x")

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def test_the_fetch_run_page_lists_its_history(self):
        response = self.client.get(
            reverse("data_feed_fetch_run_detail", kwargs={"feed_pk": self.kenya_feed.pk, "run_pk": self.kenya_run.pk})
        )

        self.assertEqual(response.status_code, 200)
        rows = response.context["history"].rows
        self.assertEqual([row.type_label for row in rows], ["Fetch finished"])
        self.assertEqual({row.event.organisation_id for row in rows}, {self.kenya.pk})


class StagingArrivalCauseTests(TestCase):
    """A derivation triggered by a fetched file is caused by that fetch's event."""

    def test_the_attempts_context_carries_the_fetchs_event_as_its_cause(self):
        from georiva.ingestion.tasks import process_staging_file

        kenya = make_organisation("kenya")
        feed, _ = _feed(kenya)
        key = "kenya/chirps/rainfall/f.tif"
        with event_context(actor="celery", organisation_id=kenya.pk):
            run = FetchRun.objects.create(data_feed=feed)
        FetchedFile.objects.create(fetch_run=run, file_path=key, status=FetchedFile.Status.STORED)
        run.mark_completed(files_fetched=1)

        seen = []
        with (
            patch("georiva.ingestion.staging_consumer.register_staging_file", return_value=MagicMock()),
            patch("georiva.processing.invocation.staging_item_trigger", return_value={}),
            patch(
                "georiva.sources.derivation_invocation.dispatch_for_input",
                side_effect=lambda trigger, **kw: seen.append(current_context()),
            ),
        ):
            from georiva.events.context import use_context
            from georiva.ingestion.chains import context_for_arrival

            with use_context(context_for_arrival(key, actor="system:bucket")):
                process_staging_file.apply(kwargs={"bucket": BucketType.STAGING, "key": key})

        (context,) = seen
        self.assertEqual(context.caused_by, FetchRunFinished.objects.get().pk)
        self.assertEqual(context.correlation_id, run.correlation_id)
