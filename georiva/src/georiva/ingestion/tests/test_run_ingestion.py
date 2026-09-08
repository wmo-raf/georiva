"""One model run arriving into one collection (ADR 0026).

What is pinned here is the reopen contract and the three closers' differing
strength. The run boundary is only useful to a consumer if "closed" means the
same thing every time it is read, and the three arrival routes do not have
equally good evidence: only the DataFeed route can name the expected set before
any of it arrives. So the tests are written around the two properties that make
a weak closer safe — a later arrival always reopens, and the revision only ever
goes up.
"""

from datetime import UTC, datetime, timedelta
from itertools import count

from django.test import TestCase
from django.utils import timezone as dj_timezone

from georiva.core.models import Catalog, Collection
from georiva.ingestion.domain_signals import run_ingestion_closed, run_ingestion_reopened
from georiva.ingestion.models import FileIngestion, RunIngestion, UploadedFile, UploadSession
from georiva.organisations.testing import make_organisation
from georiva.sources.models import DataFeed, FetchedFile, FetchRun

REF = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
STAMP = "20260902T1200"

_catalog_seq = count()


def _catalog():
    return Catalog.objects.create(
        organisation=make_organisation(),
        name="Weather Models",
        slug=f"weather-models-{next(_catalog_seq)}",
        file_format="grib2",
    )


def _collection(slug="ifs-surface", is_forecast=True, catalog=None):
    return Collection.objects.create(
        catalog=catalog or _catalog(),
        name="Surface",
        slug=slug,
        is_forecast=is_forecast,
    )


def _key(collection, step, reference_time=REF):
    """The key the Loader would build: catalog prefix, collection slug, and the
    reference time in the GR-- filename prefix."""
    stamp = reference_time.strftime("%Y%m%dT%H%M")
    return f"{collection.catalog.storage_prefix}/{collection.slug}/GR--{stamp}--step{step}.grib2"


def _complete_file(collection, step, reference_time=REF):
    """Register a file, attach the collection, and mark it completed —
    the same order the pipeline writes them in."""
    path = _key(collection, step, reference_time)
    log, _ = FileIngestion.register(bucket="sources", file_path=path, reference_time=reference_time)
    log.collections.set([collection])
    FileIngestion.mark_completed(bucket="sources", file_path=path)
    return path


class RecordFileTests(TestCase):
    def setUp(self):
        self.collection = _collection()

    def test_first_completed_file_opens_the_run(self):
        _complete_file(self.collection, 0)

        run = RunIngestion.objects.get(collection=self.collection, reference_time=REF)
        self.assertEqual(run.status, RunIngestion.Status.OPEN)
        self.assertEqual(run.revision, 0)
        self.assertEqual(run.file_count, 1)
        self.assertIsNotNone(run.last_file_at)

    def test_further_files_join_the_same_run(self):
        _complete_file(self.collection, 0)
        _complete_file(self.collection, 6)

        self.assertEqual(RunIngestion.objects.count(), 1)
        run = RunIngestion.objects.get()
        self.assertEqual(run.file_count, 2)

    def test_a_second_reference_time_is_a_second_run(self):
        _complete_file(self.collection, 0)
        _complete_file(self.collection, 0, reference_time=REF + timedelta(hours=12))

        self.assertEqual(RunIngestion.objects.count(), 2)

    def test_non_forecast_collections_get_no_run(self):
        collection = _collection(slug="rainfall", is_forecast=False)
        _complete_file(collection, 0)

        self.assertEqual(RunIngestion.objects.count(), 0)

    def test_a_file_with_no_reference_time_opens_nothing(self):
        path = f"{self.collection.catalog.storage_prefix}/{self.collection.slug}/ndvi.tif"
        log, _ = FileIngestion.register(bucket="sources", file_path=path)
        log.collections.set([self.collection])
        FileIngestion.mark_completed(bucket="sources", file_path=path)

        self.assertEqual(RunIngestion.objects.count(), 0)

    def test_file_count_is_recomputed_not_incremented(self):
        # Re-ingesting the same file must not inflate the count: it is one file
        # arriving twice, not two files.
        path = _complete_file(self.collection, 0)
        FileIngestion.mark_completed(bucket="sources", file_path=path)

        self.assertEqual(RunIngestion.objects.get().file_count, 1)


class ReopenTests(TestCase):
    def setUp(self):
        self.collection = _collection()

    def _closed_run(self):
        _complete_file(self.collection, 0)
        run = RunIngestion.objects.get()
        run.close(RunIngestion.Closer.QUIET_PERIOD)
        run.refresh_from_db()
        return run

    def test_a_later_file_reopens_a_closed_run(self):
        self._closed_run()
        _complete_file(self.collection, 6)

        run = RunIngestion.objects.get()
        self.assertEqual(run.status, RunIngestion.Status.OPEN)
        self.assertEqual(run.revision, 1)
        self.assertIsNone(run.closed_at)
        self.assertEqual(run.closed_by, "")

    def test_revision_only_goes_up(self):
        self._closed_run()
        for step in (6, 12, 18):
            _complete_file(self.collection, step)
            run = RunIngestion.objects.get()
            run.close(RunIngestion.Closer.QUIET_PERIOD)

        self.assertEqual(RunIngestion.objects.get().revision, 3)

    def test_version_orders_by_run_first_and_republish_second(self):
        run = self._closed_run()
        older = RunIngestion.objects.create(
            collection=_collection(slug="other"),
            reference_time=REF - timedelta(hours=12),
            revision=99,
        )
        # A backfilled older run must never outrank a newer one, however many
        # times it has been republished.
        self.assertLess(older.version, run.version)

        run.revision = 1
        self.assertEqual(run.version, int(REF.timestamp()) * 100 + 1)


class SignalTests(TestCase):
    def setUp(self):
        self.collection = _collection()
        self.closed = []
        self.reopened = []
        run_ingestion_closed.connect(self._on_closed, dispatch_uid="test-closed")
        run_ingestion_reopened.connect(self._on_reopened, dispatch_uid="test-reopened")
        self.addCleanup(run_ingestion_closed.disconnect, dispatch_uid="test-closed")
        self.addCleanup(run_ingestion_reopened.disconnect, dispatch_uid="test-reopened")

    def _on_closed(self, sender, run, **kwargs):
        self.closed.append(run)

    def _on_reopened(self, sender, run, **kwargs):
        self.reopened.append(run)

    def test_close_emits_once(self):
        _complete_file(self.collection, 0)
        run = RunIngestion.objects.get()

        self.assertTrue(run.close(RunIngestion.Closer.QUIET_PERIOD))
        self.assertFalse(run.close(RunIngestion.Closer.QUIET_PERIOD))
        self.assertEqual(len(self.closed), 1)

    def test_reopen_emits(self):
        _complete_file(self.collection, 0)
        RunIngestion.objects.get().close(RunIngestion.Closer.QUIET_PERIOD)
        _complete_file(self.collection, 6)

        self.assertEqual(len(self.reopened), 1)
        self.assertEqual(self.reopened[0].revision, 1)


class DeclaredSetCloserTests(TestCase):
    """The DataFeed route — the only closer that cannot close early."""

    def setUp(self):
        self.collection = _collection()
        self.feed = DataFeed.objects.create(name="ECMWF IFS", catalog=self.collection.catalog)
        self.fetch_run = FetchRun.objects.create(data_feed=self.feed)

    def _declare(self, steps, status=FetchedFile.Status.STORED):
        for step in steps:
            FetchedFile.objects.create(
                fetch_run=self.fetch_run,
                file_path=_key(self.collection, step),
                status=status,
            )

    def test_run_closes_when_the_last_declared_file_arrives(self):
        self._declare([0, 6, 12])

        _complete_file(self.collection, 0)
        _complete_file(self.collection, 6)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

        _complete_file(self.collection, 12)
        run = RunIngestion.objects.get()
        self.assertEqual(run.status, RunIngestion.Status.CLOSED)
        self.assertEqual(run.closed_by, RunIngestion.Closer.DECLARED_SET)
        self.assertEqual(run.expected_file_count, 3)

    def test_a_failed_declared_file_is_not_waited_for(self):
        # It will never arrive, so counting it would hold the run open forever.
        self._declare([0, 6])
        self._declare([12], status=FetchedFile.Status.FAILED)

        _complete_file(self.collection, 0)
        _complete_file(self.collection, 6)

        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.CLOSED)

    def test_a_skipped_declared_file_still_counts(self):
        # Skipped means the bytes were already in the bucket, so the file exists
        # and its FileIngestion is expected.
        self._declare([0])
        self._declare([6], status=FetchedFile.Status.SKIPPED)

        _complete_file(self.collection, 0)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

        _complete_file(self.collection, 6)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.CLOSED)

    def test_another_reference_times_declaration_is_not_this_runs(self):
        self._declare([0, 6])
        FetchedFile.objects.create(
            fetch_run=self.fetch_run,
            file_path=(
                f"{self.collection.catalog.storage_prefix}/{self.collection.slug}/GR--20260903T0000--step0.grib2"
            ),
            status=FetchedFile.Status.STORED,
        )

        _complete_file(self.collection, 0)
        _complete_file(self.collection, 6)

        self.assertEqual(
            RunIngestion.objects.get(reference_time=REF).status,
            RunIngestion.Status.CLOSED,
        )

    def test_another_collections_declaration_is_not_this_runs(self):
        other = Collection.objects.create(
            catalog=self.collection.catalog, name="Pressure", slug="ifs-pl", is_forecast=True
        )
        self._declare([0])
        FetchedFile.objects.create(
            fetch_run=self.fetch_run,
            file_path=_key(other, 6),
            status=FetchedFile.Status.STORED,
        )

        _complete_file(self.collection, 0)

        self.assertEqual(
            RunIngestion.objects.get(collection=self.collection).status,
            RunIngestion.Status.CLOSED,
        )

    def test_no_declaration_means_no_close(self):
        _complete_file(self.collection, 0)

        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)


class UploadBatchCloserTests(TestCase):
    """The manual-upload route — closes on a batch, which is not the run."""

    def setUp(self):
        self.collection = _collection()
        self.session = UploadSession.objects.create(catalog=self.collection.catalog)

    def _upload(self, step, status=UploadedFile.Status.STORED):
        return UploadedFile.objects.create(
            session=self.session,
            original_filename=f"step{step}.grib2",
            file_path=_key(self.collection, step),
            status=status,
        )

    def test_an_active_session_holds_the_run_open(self):
        self._upload(0)
        self._upload(6, status=UploadedFile.Status.PENDING)
        _complete_file(self.collection, 0)

        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

    def test_a_completed_session_whose_files_have_all_ingested_closes_the_run(self):
        self._upload(0)
        self._upload(6)
        self.session.status = UploadSession.Status.COMPLETED
        self.session.save(update_fields=["status"])

        _complete_file(self.collection, 0)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

        _complete_file(self.collection, 6)
        run = RunIngestion.objects.get()
        self.assertEqual(run.status, RunIngestion.Status.CLOSED)
        self.assertEqual(run.closed_by, RunIngestion.Closer.UPLOAD_SESSION)

    def test_tomorrows_upload_reopens_yesterdays_closed_run(self):
        # The case the reopen exists for: 20 of 68 steps today, 48 tomorrow.
        # Each batch completes; neither means "run complete".
        self._upload(0)
        self.session.status = UploadSession.Status.COMPLETED
        self.session.save(update_fields=["status"])
        _complete_file(self.collection, 0)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.CLOSED)

        later = UploadSession.objects.create(catalog=self.collection.catalog, status=UploadSession.Status.COMPLETED)
        UploadedFile.objects.create(
            session=later,
            original_filename="step6.grib2",
            file_path=_key(self.collection, 6),
            status=UploadedFile.Status.STORED,
        )
        _complete_file(self.collection, 6)

        run = RunIngestion.objects.get()
        self.assertEqual(run.status, RunIngestion.Status.CLOSED)
        self.assertEqual(run.revision, 1)


class QuietPeriodCloserTests(TestCase):
    """The safety net under the other two."""

    def setUp(self):
        self.collection = _collection()

    def _age(self, minutes):
        RunIngestion.objects.update(last_file_at=dj_timezone.now() - timedelta(minutes=minutes))

    def test_a_recently_fed_run_stays_open(self):
        from georiva.ingestion.run_closers import close_quiet_runs

        _complete_file(self.collection, 0)

        self.assertEqual(close_quiet_runs(minutes=20), 0)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

    def test_a_quiet_run_closes(self):
        from georiva.ingestion.run_closers import close_quiet_runs

        _complete_file(self.collection, 0)
        self._age(30)

        self.assertEqual(close_quiet_runs(minutes=20), 1)
        run = RunIngestion.objects.get()
        self.assertEqual(run.status, RunIngestion.Status.CLOSED)
        self.assertEqual(run.closed_by, RunIngestion.Closer.QUIET_PERIOD)

    def test_a_file_still_in_flight_for_the_catalog_holds_the_run_open(self):
        from georiva.ingestion.run_closers import close_quiet_runs

        _complete_file(self.collection, 0)
        self._age(30)
        # A PENDING row has no collections yet — resolution has not run — which
        # is exactly why the in-flight test is catalog-wide.
        FileIngestion.register(
            bucket="incoming",
            file_path=f"{self.collection.catalog.storage_prefix}/GR--{STAMP}--step6.grib2",
            reference_time=REF,
        )

        self.assertEqual(close_quiet_runs(minutes=20), 0)
        self.assertEqual(RunIngestion.objects.get().status, RunIngestion.Status.OPEN)

    def test_an_already_closed_run_is_left_alone(self):
        from georiva.ingestion.run_closers import close_quiet_runs

        _complete_file(self.collection, 0)
        RunIngestion.objects.get().close(RunIngestion.Closer.DECLARED_SET)
        self._age(30)

        self.assertEqual(close_quiet_runs(minutes=20), 0)
        self.assertEqual(RunIngestion.objects.get().closed_by, RunIngestion.Closer.DECLARED_SET)


class BookkeepingNeverFailsIngestionTests(TestCase):
    def test_a_broken_closer_does_not_fail_the_file(self):
        from unittest import mock

        collection = _collection()
        with mock.patch(
            "georiva.ingestion.run_closers.close_if_declared_set_complete",
            side_effect=RuntimeError("boom"),
        ):
            _complete_file(collection, 0)

        # The file is still completed; only the run bookkeeping was lost.
        self.assertEqual(
            FileIngestion.objects.get().status,
            FileIngestion.Status.COMPLETED,
        )


class LatestClosedTests(TestCase):
    def test_latest_closed_returns_the_newest_closed_run(self):
        collection = _collection()
        old = RunIngestion.objects.create(
            collection=collection,
            reference_time=REF - timedelta(hours=12),
            status=RunIngestion.Status.CLOSED,
        )
        RunIngestion.objects.create(
            collection=collection,
            reference_time=REF,
            status=RunIngestion.Status.OPEN,
        )

        self.assertEqual(RunIngestion.latest_closed(collection), old)
