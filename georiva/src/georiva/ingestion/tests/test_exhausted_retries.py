"""A file whose retries are spent is failed, not left pending where nothing can lock it."""

from datetime import timedelta

from django.test import TestCase
from django.utils import timezone as dj_timezone

from georiva.core.storage import BucketType
from georiva.events.models import SweepIntervened
from georiva.ingestion.models import FileIngestion, FileIngestionFinished
from georiva.ingestion.tests.test_sweep_events import _sweep
from georiva.organisations.testing import make_organisation

KEY = "kenya/chirps/rainfall/rain.tif"
MAX = FileIngestion.MAX_RETRIES


class ExhaustedRetriesTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.log, _ = FileIngestion.register(BucketType.SOURCES, KEY)

    def _row(self, **fields):
        FileIngestion.objects.filter(pk=self.log.pk).update(**fields)
        self.log.refresh_from_db()
        return self.log

    def _stale_processing(self, retry_count):
        return self._row(
            status=FileIngestion.Status.PROCESSING,
            locked_by="w",
            locked_at=dj_timezone.now() - timedelta(hours=2),
            retry_count=retry_count,
        )

    # -- a lock that went stale -------------------------------------------------

    def test_a_stale_lock_with_retries_left_goes_back_to_pending(self):
        self._stale_processing(MAX - 1)

        self.assertEqual(FileIngestion.reset_stale_locks(), 1)

        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.PENDING)

    def test_a_stale_lock_with_no_retries_left_is_failed(self):
        self._stale_processing(MAX)

        self.assertEqual(FileIngestion.reset_stale_locks(), 1)

        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.FAILED)
        self.assertEqual(self.log.locked_by, "")
        self.assertIsNone(self.log.locked_at)
        self.assertEqual(self.log.error, f"Gave up after {MAX} attempts without finishing")

    def test_a_file_failed_that_way_is_permanently_failed(self):
        self._stale_processing(MAX)

        FileIngestion.reset_stale_locks()

        self.assertEqual(list(FileIngestion.get_permanently_failed()), [self.log])
        self.assertEqual(list(FileIngestion.get_retryable()), [])

    def test_the_failure_is_recorded_as_the_sweep_in_the_files_chain(self):
        self._stale_processing(MAX)

        FileIngestion.reset_stale_locks(sweep="unprocessed")

        event = FileIngestionFinished.objects.get()
        self.assertEqual(event.outcome, FileIngestionFinished.Outcome.FAILED)
        self.assertEqual(event.actor, "sweep:unprocessed")
        self.assertEqual(event.retry_count, MAX)
        self.assertEqual(event.organisation_id, self.kenya.pk)
        # Nothing was revived, so no intervention says so.
        self.assertFalse(SweepIntervened.objects.exists())

    def test_a_lock_still_within_its_timeout_is_left_alone(self):
        self._row(status=FileIngestion.Status.PROCESSING, locked_by="w", locked_at=dj_timezone.now(), retry_count=MAX)

        self.assertEqual(FileIngestion.reset_stale_locks(), 0)

        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.PROCESSING)

    # -- a row left pending -----------------------------------------------------

    def test_a_pending_file_with_no_retries_left_is_failed(self):
        self._row(retry_count=MAX)

        self.assertEqual(FileIngestion.fail_exhausted_pending(), 1)

        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.FAILED)
        self.assertEqual(self.log.error, f"Gave up after {MAX} attempts without finishing")
        self.assertEqual(FileIngestionFinished.objects.get().outcome, FileIngestionFinished.Outcome.FAILED)

    def test_a_pending_file_with_retries_left_is_left_alone(self):
        self._row(retry_count=MAX - 1)

        self.assertEqual(FileIngestion.fail_exhausted_pending(), 0)

        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.PENDING)
        self.assertFalse(FileIngestionFinished.objects.exists())

    def test_files_in_other_states_are_left_alone(self):
        for status in (FileIngestion.Status.COMPLETED, FileIngestion.Status.PROCESSING):
            self._row(status=status, retry_count=MAX, locked_at=dj_timezone.now())

            self.assertEqual(FileIngestion.fail_exhausted_pending(), 0)

            self.log.refresh_from_db()
            self.assertEqual(self.log.status, status)

    def test_failing_twice_records_one_event(self):
        self._row(retry_count=MAX)

        FileIngestion.fail_exhausted_pending()
        FileIngestion.fail_exhausted_pending()

        self.assertEqual(FileIngestionFinished.objects.count(), 1)

    # -- the sweep --------------------------------------------------------------

    def test_the_sweep_fails_a_stuck_pending_file_instead_of_redispatching_it(self):
        self._row(retry_count=MAX, updated_at=dj_timezone.now() - FileIngestion.PENDING_TIMEOUT * 2)

        dispatched = _sweep()

        self.assertEqual(dispatched, [])
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.FAILED)

    def test_the_sweep_fails_a_stale_lock_with_no_retries_left_in_one_pass(self):
        self._stale_processing(MAX)

        dispatched = _sweep()

        self.assertEqual(dispatched, [])
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, FileIngestion.Status.FAILED)

    # -- after it has failed ----------------------------------------------------

    def test_a_fresh_upload_of_the_same_file_can_be_ingested_again(self):
        self._row(retry_count=MAX)
        FileIngestion.fail_exhausted_pending()

        self.assertTrue(FileIngestion.reset_for_reingest(BucketType.SOURCES, KEY))

        self.assertTrue(FileIngestion.acquire(BucketType.SOURCES, KEY, "w2"))
