"""The discipline a derived artefact is rebuilt under (ADR 0027).

``virtual_zarr`` debugged this machinery once already — the double-dispatch race
of #398, and the recycled-claim window that claiming alone does not close — and
a second builder was about to reimplement it from the same starting point. What
is pinned here is the part that was expensive to learn:

- a build is claimed in the same conditional UPDATE that decides to dispatch it,
  so a record queued behind a backlog is not dispatched again by the next sweep;
- the claim identifies the *dispatch*, not the worker, so a copy whose claim was
  recycled while it waited stands down instead of running beside its
  replacement;
- an expired lock is the only crash recovery there is, and it has to cover the
  queued window as well as the build.

The last class validates the base against ``VirtualZarrManifest`` by inspection
— the shape was extracted from it and it is deliberately *not* migrated onto it
(ADR 0027), so nothing but a test keeps the two from drifting apart.
"""

from datetime import timedelta
from unittest.mock import MagicMock

from django.db import connection, models
from django.test import TestCase
from django.utils import timezone

from georiva.core.build_discipline import (
    BuildAttemptLog,
    BuildDisciplinedModel,
    build_attempt,
    dispatch_build,
    stand_down,
    sweep_builds,
)
from georiva.virtual_zarr.models import VirtualZarrManifest


class Buildable(BuildDisciplinedModel):
    """A concrete subclass standing in for a real one.

    Defined in a test module, so the app registry only ever sees it during a
    test run and ``makemigrations`` never does.
    """

    label = models.CharField(max_length=50, blank=True, default="")

    class Meta:
        app_label = "georivacore"


class BuildableLog(BuildAttemptLog):
    TARGET_FIELD = "target"

    target = models.ForeignKey(Buildable, on_delete=models.CASCADE, related_name="attempts")
    mode = models.CharField(max_length=20, blank=True, default="")

    class Meta(BuildAttemptLog.Meta):
        app_label = "georivacore"


class BuildDisciplineTestCase(TestCase):
    """Creates the two tables for the fixture models above.

    DDL is transactional in Postgres and this runs inside the class-level
    atomic block, so the tables go away with it whatever the tests do.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with connection.schema_editor() as editor:
            editor.create_model(Buildable)
            editor.create_model(BuildableLog)

    def make(self, status=Buildable.Status.PENDING, **kwargs):
        return Buildable.objects.create(status=status, **kwargs)

    def reread(self, record):
        return Buildable.objects.get(pk=record.pk)


class ClaimTests(BuildDisciplineTestCase):
    def test_a_claim_moves_the_record_to_building_and_records_the_token(self):
        record = self.make()

        self.assertTrue(Buildable.claim_for_build(record.pk, "sweep-abc123"))

        record = self.reread(record)
        self.assertEqual(record.status, Buildable.Status.BUILDING)
        self.assertEqual(record.locked_by, "sweep-abc123")
        self.assertIsNotNone(record.locked_at)

    def test_only_one_of_two_racing_claims_wins(self):
        record = self.make()

        first = Buildable.claim_for_build(record.pk, "sweep-aaa")
        second = Buildable.claim_for_build(record.pk, "sweep-bbb")

        self.assertEqual([first, second], [True, False])
        self.assertEqual(self.reread(record).locked_by, "sweep-aaa")

    def test_stale_and_failed_are_claimable_but_ready_is_not(self):
        for status in (Buildable.Status.PENDING, Buildable.Status.STALE, Buildable.Status.FAILED):
            with self.subTest(status=status):
                self.assertTrue(Buildable.claim_for_build(self.make(status).pk, "c"))

        for status in (Buildable.Status.READY, Buildable.Status.NO_DATA):
            with self.subTest(status=status):
                self.assertFalse(Buildable.claim_for_build(self.make(status).pk, "c"))

    def test_force_claims_a_record_that_does_not_need_building(self):
        record = self.make(Buildable.Status.READY)

        self.assertTrue(Buildable.claim_for_build(record.pk, "operator-1", force=True))
        self.assertEqual(self.reread(record).status, Buildable.Status.BUILDING)

    def test_force_still_refuses_a_lock_someone_is_holding(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="worker-1")

        self.assertFalse(Buildable.claim_for_build(record.pk, "operator-1", force=True))
        self.assertEqual(self.reread(record).locked_by, "worker-1")

    def test_force_takes_over_a_lock_that_has_expired(self):
        expired = timezone.now() - Buildable.LOCK_TIMEOUT - timedelta(minutes=1)
        record = self.make(Buildable.Status.BUILDING, locked_at=expired, locked_by="dead-worker")

        self.assertTrue(Buildable.claim_for_build(record.pk, "operator-1", force=True))

    def test_a_claim_clears_the_previous_error(self):
        record = self.make(Buildable.Status.FAILED, error="last time it exploded")

        Buildable.claim_for_build(record.pk, "c")

        self.assertEqual(self.reread(record).error, "")


class LockRecoveryTests(BuildDisciplineTestCase):
    def test_an_expired_lock_returns_the_record_to_pending(self):
        expired = timezone.now() - Buildable.LOCK_TIMEOUT - timedelta(seconds=1)
        record = self.make(Buildable.Status.BUILDING, locked_at=expired, locked_by="dead")

        self.assertEqual(Buildable.reset_stale_locks(), 1)

        record = self.reread(record)
        self.assertEqual(record.status, Buildable.Status.PENDING)
        self.assertEqual(record.locked_by, "")
        self.assertIsNone(record.locked_at)

    def test_a_live_lock_is_left_alone(self):
        self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="alive")

        self.assertEqual(Buildable.reset_stale_locks(), 0)

    def test_refreshing_restarts_the_clock_without_taking_the_lock(self):
        old = timezone.now() - timedelta(minutes=20)
        record = self.make(Buildable.Status.BUILDING, locked_at=old, locked_by="claim-1")

        record.refresh_build_lock("celery-task-9")

        record = self.reread(record)
        self.assertGreater(record.locked_at, old)
        self.assertEqual(record.locked_by, "celery-task-9")

    def test_holds_claim_is_true_for_the_token_on_the_row(self):
        record = self.make(Buildable.Status.BUILDING, locked_by="claim-1")

        self.assertTrue(record.holds_claim("claim-1"))
        self.assertFalse(record.holds_claim("claim-2"))

    def test_a_blank_claim_predates_claims_and_is_trusted(self):
        record = self.make(Buildable.Status.BUILDING, locked_by="claim-1")

        self.assertTrue(record.holds_claim(""))


class TransitionTests(BuildDisciplineTestCase):
    def test_ready_clears_the_lock_and_stamps_the_build(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="c", error="old")

        record.mark_ready(input_fingerprint="run-42", label="kenya")

        record = self.reread(record)
        self.assertEqual(record.status, Buildable.Status.READY)
        self.assertEqual(record.input_fingerprint, "run-42")
        self.assertEqual(record.label, "kenya")
        self.assertEqual(record.locked_by, "")
        self.assertEqual(record.error, "")
        self.assertIsNotNone(record.built_at)

    def test_failure_keeps_the_message_and_releases_the_lock(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="c")

        record.mark_failed("boom")

        record = self.reread(record)
        self.assertEqual(record.status, Buildable.Status.FAILED)
        self.assertEqual(record.error, "boom")
        self.assertIsNone(record.locked_at)

    def test_a_long_error_is_truncated_rather_than_refused(self):
        record = self.make()

        record.mark_failed("x" * 5000)

        self.assertEqual(len(self.reread(record).error), 2000)

    def test_stale_applies_to_ready_and_no_data_only(self):
        for status in (Buildable.Status.READY, Buildable.Status.NO_DATA):
            with self.subTest(status=status):
                record = self.make(status)
                record.mark_stale()
                self.assertEqual(self.reread(record).status, Buildable.Status.STALE)

        for status in (Buildable.Status.PENDING, Buildable.Status.BUILDING, Buildable.Status.FAILED):
            with self.subTest(status=status):
                record = self.make(status)
                record.mark_stale()
                self.assertEqual(self.reread(record).status, status)

    def test_marking_a_building_record_stale_would_drop_a_live_claim(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="claim-1")

        record.mark_stale()

        self.assertEqual(self.reread(record).locked_by, "claim-1")

    def test_no_data_is_not_buildable_so_the_sweep_stops_retrying(self):
        record = self.make()

        record.mark_no_data()

        self.assertNotIn(self.reread(record).pk, Buildable.get_buildable().values_list("pk", flat=True))


class QueueRebuildTests(BuildDisciplineTestCase):
    def test_an_operator_can_requeue_a_ready_record(self):
        record = self.make(Buildable.Status.READY)

        self.assertTrue(record.queue_rebuild())
        self.assertEqual(self.reread(record).status, Buildable.Status.PENDING)

    def test_a_record_a_worker_is_holding_is_left_to_it(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=timezone.now(), locked_by="worker")

        self.assertFalse(record.queue_rebuild())
        self.assertEqual(self.reread(record).status, Buildable.Status.BUILDING)

    def test_a_stuck_record_is_reset_like_the_sweep_would(self):
        expired = timezone.now() - Buildable.LOCK_TIMEOUT - timedelta(minutes=5)
        record = self.make(Buildable.Status.BUILDING, locked_at=expired, locked_by="dead")

        self.assertTrue(record.queue_rebuild())
        self.assertEqual(self.reread(record).status, Buildable.Status.PENDING)

    def test_a_building_record_with_no_lock_stamp_is_stuck_not_live(self):
        record = self.make(Buildable.Status.BUILDING, locked_at=None, locked_by="ghost")

        self.assertTrue(record.queue_rebuild())


class FingerprintTests(BuildDisciplineTestCase):
    def test_a_ready_record_over_unchanged_inputs_has_nothing_to_do(self):
        record = self.make(Buildable.Status.READY, input_fingerprint="run-42")

        self.assertTrue(record.is_up_to_date("run-42"))

    def test_changed_inputs_are_not_up_to_date(self):
        record = self.make(Buildable.Status.READY, input_fingerprint="run-42")

        self.assertFalse(record.is_up_to_date("run-43"))

    def test_a_matching_fingerprint_on_a_record_that_is_not_ready_still_builds(self):
        record = self.make(Buildable.Status.STALE, input_fingerprint="run-42")

        self.assertFalse(record.is_up_to_date("run-42"))

    def test_an_unfingerprintable_input_always_builds(self):
        record = self.make(Buildable.Status.READY, input_fingerprint="")

        self.assertFalse(record.is_up_to_date(""))


class DispatchTests(BuildDisciplineTestCase):
    def test_dispatch_claims_before_queueing(self):
        record = self.make()
        task = MagicMock()

        self.assertTrue(dispatch_build(Buildable, record.pk, task, claimed_by="sweep"))

        record = self.reread(record)
        self.assertEqual(record.status, Buildable.Status.BUILDING)
        task.delay.assert_called_once_with(record.pk, record.locked_by)

    def test_the_claim_travels_with_the_task(self):
        record = self.make()
        task = MagicMock()

        dispatch_build(Buildable, record.pk, task)

        (_, claim), _ = task.delay.call_args
        self.assertEqual(claim, self.reread(record).locked_by)
        self.assertTrue(claim.startswith("dispatch-"))

    def test_a_record_already_in_flight_is_not_queued_again(self):
        """The #398 regression: a build waiting behind a backlog is BUILDING,
        not PENDING, so the next sweep passes over it."""
        record = self.make()
        task = MagicMock()
        dispatch_build(Buildable, record.pk, task, claimed_by="sweep")

        self.assertFalse(dispatch_build(Buildable, record.pk, task, claimed_by="sweep"))
        self.assertEqual(task.delay.call_count, 1)

    def test_two_dispatches_of_one_record_never_share_a_claim(self):
        first, second = self.make(), self.make()
        task = MagicMock()

        dispatch_build(Buildable, first.pk, task)
        dispatch_build(Buildable, second.pk, task)

        self.assertNotEqual(self.reread(first).locked_by, self.reread(second).locked_by)

    def test_dispatch_never_names_a_queue(self):
        """ADR 0025: the queue a task declares is the routing, and a caller that
        overrides it silently wins. ``apply_async`` is not reached from here."""
        record = self.make()
        task = MagicMock()

        dispatch_build(Buildable, record.pk, task)

        task.apply_async.assert_not_called()


class StandDownTests(BuildDisciplineTestCase):
    def test_a_copy_holding_the_current_claim_proceeds(self):
        record = self.make(Buildable.Status.BUILDING, locked_by="claim-1")

        self.assertFalse(stand_down(record, "claim-1"))

    def test_a_copy_whose_claim_was_recycled_stands_down(self):
        """The window claiming alone does not close: a queue wait longer than
        LOCK_TIMEOUT frees the row, a later sweep dispatches a replacement, and
        both copies are live."""
        record = self.make(Buildable.Status.BUILDING, locked_by="claim-2")

        self.assertTrue(stand_down(record, "claim-1"))


class SweepTests(BuildDisciplineTestCase):
    def test_the_sweep_recovers_locks_before_looking_for_work(self):
        expired = timezone.now() - Buildable.LOCK_TIMEOUT - timedelta(minutes=1)
        crashed = self.make(Buildable.Status.BUILDING, locked_at=expired, locked_by="dead")
        task = MagicMock()

        result = sweep_builds(Buildable, task)

        self.assertEqual(result.reset, 1)
        self.assertEqual(result.dispatched, 1)
        self.assertEqual(self.reread(crashed).status, Buildable.Status.BUILDING)

    def test_the_sweep_dispatches_every_buildable_record_once(self):
        pending = self.make(Buildable.Status.PENDING)
        stale = self.make(Buildable.Status.STALE)
        failed = self.make(Buildable.Status.FAILED)
        self.make(Buildable.Status.READY)
        self.make(Buildable.Status.NO_DATA)
        task = MagicMock()

        result = sweep_builds(Buildable, task)

        self.assertEqual(result.dispatched, 3)
        dispatched = {call.args[0] for call in task.delay.call_args_list}
        self.assertEqual(dispatched, {pending.pk, stale.pk, failed.pk})

    def test_a_second_sweep_over_a_backlog_dispatches_nothing(self):
        for _ in range(3):
            self.make()
        task = MagicMock()
        sweep_builds(Buildable, task)

        self.assertEqual(sweep_builds(Buildable, task).dispatched, 0)


class BuildLogTests(BuildDisciplineTestCase):
    def test_an_attempt_that_succeeds_is_recorded_with_its_facts(self):
        record = self.make()

        with build_attempt(BuildableLog, record) as facts:
            facts["mode"] = "rebuild"

        log = BuildableLog.objects.get()
        self.assertEqual(log.outcome, BuildableLog.Outcome.SUCCESS)
        self.assertEqual(log.mode, "rebuild")
        self.assertEqual(log.target, record)

    def test_a_failed_attempt_keeps_what_it_had_learned_before_it_died(self):
        record = self.make()

        with self.assertRaises(ValueError):
            with build_attempt(BuildableLog, record) as facts:
                facts["mode"] = "append"
                raise ValueError("the grid moved")

        log = BuildableLog.objects.get()
        self.assertEqual(log.outcome, BuildableLog.Outcome.FAILURE)
        self.assertEqual(log.mode, "append")
        self.assertEqual(log.error, "the grid moved")

    def test_the_exception_propagates_so_the_caller_decides_what_failed_means(self):
        record = self.make()

        with self.assertRaises(ValueError):
            with build_attempt(BuildableLog, record):
                raise ValueError("boom")

        self.assertEqual(self.reread(record).status, Buildable.Status.PENDING)

    def test_a_retention_pass_is_logged_under_its_own_kind(self):
        record = self.make()

        with build_attempt(BuildableLog, record, kind=BuildableLog.Kind.GC):
            pass

        self.assertEqual(BuildableLog.objects.get().kind, BuildableLog.Kind.GC)

    def test_duration_is_the_span_of_the_attempt(self):
        record = self.make()
        started = timezone.now() - timedelta(seconds=30)

        log = BuildableLog.record(record, BuildableLog.Kind.BUILD, BuildableLog.Outcome.SUCCESS, started)

        self.assertGreaterEqual(log.duration, timedelta(seconds=30))

    def test_rows_older_than_retention_are_pruned(self):
        record = self.make()
        old = timezone.now() - BuildableLog.RETENTION - timedelta(days=1)
        BuildableLog.record(record, BuildableLog.Kind.BUILD, BuildableLog.Outcome.SUCCESS, old)
        BuildableLog.objects.update(started_at=old)
        BuildableLog.record(record, BuildableLog.Kind.BUILD, BuildableLog.Outcome.SUCCESS, timezone.now())

        self.assertEqual(BuildableLog.prune_expired(), 1)
        self.assertEqual(BuildableLog.objects.count(), 1)


class ShapedForVirtualZarrTests(TestCase):
    """The base was extracted from ``VirtualZarrManifest`` and deliberately does
    not replace it (ADR 0027): migrating a live model onto a new base is a
    separate decision with a migration attached, and the second grain has to
    exist first. So the fit is validated by inspection — which is only worth
    anything if something notices when the two drift.
    """

    def test_the_state_machine_is_the_same_six_states(self):
        self.assertEqual(
            {choice.value for choice in VirtualZarrManifest.Status},
            {choice.value for choice in BuildDisciplinedModel.Status},
        )

    def test_the_same_three_states_are_buildable(self):
        self.assertEqual(
            [status.value for status in VirtualZarrManifest.BUILDABLE_STATUSES],
            [status.value for status in BuildDisciplinedModel.BUILDABLE_STATUSES],
        )

    def test_the_lock_window_agrees(self):
        self.assertEqual(VirtualZarrManifest.LOCK_TIMEOUT, BuildDisciplinedModel.LOCK_TIMEOUT)

    def test_every_field_the_base_holds_exists_on_the_manifest(self):
        base_fields = {f.name for f in BuildDisciplinedModel._meta.get_fields()}
        manifest_fields = {f.name for f in VirtualZarrManifest._meta.get_fields()}

        # ``watermark`` is the manifest's input fingerprint under an older name
        # and a datetime type — the one field that would be renamed by a
        # migration onto the base.
        self.assertEqual(base_fields - manifest_fields, {"input_fingerprint"})

    def test_the_discipline_the_base_promises_is_callable_on_the_manifest(self):
        for name in (
            "claim_for_build",
            "reset_stale_locks",
            "refresh_build_lock",
            "queue_rebuild",
            "get_buildable",
            "mark_ready",
            "mark_failed",
            "mark_stale",
            "mark_no_data",
        ):
            with self.subTest(method=name):
                self.assertTrue(callable(getattr(VirtualZarrManifest, name, None)))
