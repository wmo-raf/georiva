"""
DerivationRun retry story (issue #209).

`acquire()` is the single claim site, so it is where the retry bookkeeping
lives: every transition into RUNNING bumps `attempts` and records why the run
(re)fired (`last_retry_reason` / `last_retry_at`). These tests drive `acquire`
directly — the highest seam — plus the synchronous `dispatch=False` paths that
thread a reason down to it, so no Celery broker is needed.
"""
from datetime import datetime, timedelta, timezone

from georiva.processing.models import DerivationRun
from georiva.processing.recipe import BaseRecipe, OutputItem, ResolvedInput, unit_hash
from georiva.processing.registry import RecipeRegistry

from django.test import TestCase
from django.utils import timezone as dj_timezone


class _Asset:
    """Minimal asset-like object carrying just a checksum (for input hashing)."""

    def __init__(self, checksum):
        self.checksum = checksum


class _CompletingRecipe(BaseRecipe):
    """A recipe that runs inline to completion with no assets/links — enough for
    the engine to acquire, transform, and mark the run completed, so a
    `dispatch=False` re-dispatch actually flows through `acquire` and records its
    reason. Its single input is optional so readiness passes with no items."""

    type = "retry_fake"
    version = "1"

    def enumerate_units(self, selector):
        return [{"n": 1}]

    def candidate_units(self, trigger):
        return [{"n": 1}]

    def resolve_inputs(self, unit):
        return {"src": ResolvedInput("src", required=False, items=[], assets=[_Asset("v1")])}

    def outputs(self, unit):
        from georiva.core.models import Collection

        return OutputItem(
            collection=Collection.objects.get(slug="retry-out"),
            time=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )

    def transform(self, unit, resolved):
        return []


class _InlineRecipeMixin:
    """Register `_CompletingRecipe` and give it an output Collection to write."""

    def setUp(self):
        super().setUp()
        from georiva.core.models import Catalog, Collection

        catalog = Catalog.objects.create(name="Retry", slug="retry", file_format="geotiff")
        Collection.objects.create(catalog=catalog, slug="retry-out", name="retry-out")

        self._saved = dict(RecipeRegistry._recipes)
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes[_CompletingRecipe.type] = _CompletingRecipe

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)
        super().tearDown()


def _acquire(**overrides):
    kwargs = dict(
        recipe_type="climatology",
        recipe_version="1",
        unit_key={"period": "1991-2020"},
        unit_hash="a" * 64,
    )
    kwargs.update(overrides)
    return DerivationRun.acquire(**kwargs)


class AcquireRetryTrackingTests(TestCase):
    def test_a_fresh_acquire_is_attempt_one_with_the_initial_reason(self):
        run = _acquire()

        self.assertEqual(run.attempts, 1)
        self.assertEqual(run.last_retry_reason, DerivationRun.RetryReason.INITIAL)

    def test_reacquiring_after_a_terminal_status_counts_and_restamps_the_reason(self):
        run = _acquire()
        run.mark_failed("boom")  # release the lock into a terminal status

        again = _acquire(reason=DerivationRun.RetryReason.MANUAL_RERUN)

        self.assertEqual(again.attempts, 2)
        self.assertEqual(again.last_retry_reason, DerivationRun.RetryReason.MANUAL_RERUN)
        self.assertIsNotNone(again.last_retry_at)

    def test_a_refused_live_lock_does_not_count_as_an_attempt(self):
        first = _acquire()  # takes the lock, leaves it RUNNING (not released)

        refused = _acquire(reason=DerivationRun.RetryReason.CELERY_RETRY)

        self.assertIsNone(refused)
        first.refresh_from_db()
        self.assertEqual(first.attempts, 1)
        self.assertEqual(first.last_retry_reason, DerivationRun.RetryReason.INITIAL)


class ReclaimReasonTests(_InlineRecipeMixin, TestCase):
    def _stale_running(self, unit):
        return DerivationRun.objects.create(
            recipe_type=_CompletingRecipe.type, recipe_version="1",
            unit_key=unit, unit_hash=unit_hash(unit),
            status=DerivationRun.Status.RUNNING,
            locked_by="dead-worker",
            locked_at=dj_timezone.now() - (DerivationRun.LOCK_TIMEOUT + timedelta(minutes=1)),
        )

    def test_reclaiming_a_stale_running_unit_records_the_reclaim_reason(self):
        from georiva.processing.invocation import reclaim_stale_running

        self._stale_running({"n": 1})

        reclaim_stale_running(dispatch=False)

        run = DerivationRun.objects.get(unit_hash=unit_hash({"n": 1}))
        self.assertEqual(run.last_retry_reason, DerivationRun.RetryReason.STALE_RUNNING_RECLAIM)
        self.assertEqual(run.attempts, 1)


class InputStaleReasonTests(_InlineRecipeMixin, TestCase):
    def test_recomputing_an_input_stale_unit_records_the_input_stale_reason(self):
        from georiva.processing.invocation import sweep_stale_units

        # A completed run whose recorded input_hash no longer matches its inputs.
        DerivationRun.objects.create(
            recipe_type=_CompletingRecipe.type, recipe_version="1",
            unit_key={"n": 1}, unit_hash=unit_hash({"n": 1}),
            input_hash="STALE", status=DerivationRun.Status.COMPLETED,
        )

        sweep_stale_units(dispatch=False)

        run = DerivationRun.objects.get(unit_hash=unit_hash({"n": 1}))
        self.assertEqual(run.last_retry_reason, DerivationRun.RetryReason.INPUT_STALE)


class CeleryRetryReasonTests(TestCase):
    """A Celery auto-retry re-runs the same task; from the operator's view the
    most recent trigger is the retry itself, so it overrides whatever reason the
    original dispatch carried."""

    def test_a_celery_auto_retry_overrides_the_dispatch_reason(self):
        from unittest.mock import MagicMock, patch

        from georiva.processing.tasks import run_unit_task

        with (
            patch("georiva.processing.registry.recipe_registry.get", return_value=MagicMock()),
            patch("georiva.processing.engine.run_unit") as run_unit,
        ):
            run_unit.return_value = MagicMock(status="failed", item_id=None)
            # retries=1 → this is a Celery-driven retry, not the first attempt.
            run_unit_task.apply(
                kwargs={"recipe_type": "x", "unit": {"n": 1}, "reason": "input_stale"},
                retries=1,
            )

        self.assertEqual(
            run_unit.call_args.kwargs["reason"], DerivationRun.RetryReason.CELERY_RETRY,
        )
