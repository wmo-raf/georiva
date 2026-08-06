"""
Not-ready resurrection (ADR-0020).

A unit whose required input was absent parks its DerivationRun as NOT_READY.
`resurrect_not_ready_units` is the revival primitive both recovery paths share:
the periodic sweep (all parked runs) and the completion wake-up (scoped by
origin). It is readiness-gated — a unit whose inputs still don't resolve stays
parked instead of churning the queue.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.processing.invocation import resurrect_not_ready_units
from georiva.processing.models import DerivationRun
from georiva.processing.recipe import BaseRecipe, OutputItem, ResolvedInput, unit_hash
from georiva.processing.registry import RecipeRegistry


class _Asset:
    def __init__(self, checksum):
        self.checksum = checksum


class _WaitingRecipe(BaseRecipe):
    """A recipe whose single required input's presence is toggled per-test via
    the class flag, standing in for 'the baseline normal now exists'."""

    type = "waiting_fake"
    version = "1"
    input_present = False

    def enumerate_units(self, selector):
        return [{"n": 1}]

    def resolve_inputs(self, unit):
        items = [object()] if type(self).input_present else []
        return {
            "baseline": ResolvedInput(
                "baseline", required=True, items=items, assets=[_Asset("v1")]
            )
        }

    def outputs(self, unit):
        return OutputItem(collection=None, time=None)

    def transform(self, unit, resolved):
        return []


class _RegistryMixin:
    def setUp(self):
        super().setUp()
        self._saved = dict(RecipeRegistry._recipes)
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes[_WaitingRecipe.type] = _WaitingRecipe
        _WaitingRecipe.input_present = False

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)
        super().tearDown()


def _parked(unit, *, recipe_type=_WaitingRecipe.type, origin=""):
    return DerivationRun.objects.create(
        recipe_type=recipe_type, recipe_version="1",
        unit_key=unit, unit_hash=unit_hash(unit),
        status=DerivationRun.Status.NOT_READY, origin=origin,
    )


class ResurrectNotReadyTests(_RegistryMixin, TestCase):
    def test_a_still_unready_unit_stays_parked(self):
        _parked({"n": 1})

        with patch("georiva.processing.tasks.run_unit_task") as task:
            revived = resurrect_not_ready_units()

        self.assertEqual(revived, 0)
        self.assertEqual(task.delay.call_count, 0)

    def test_a_now_ready_unit_is_redispatched_with_the_sweep_reason(self):
        _parked({"n": 1})
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            revived = resurrect_not_ready_units()

        self.assertEqual(revived, 1)
        self.assertEqual(task.delay.call_count, 1)
        kwargs = task.delay.call_args.kwargs
        self.assertEqual(kwargs["unit"], {"n": 1})
        self.assertEqual(kwargs["reason"], DerivationRun.RetryReason.NOT_READY_SWEEP)

    def test_origins_scopes_the_scan_to_the_named_products(self):
        _parked({"n": 1}, origin="derived_product:1")
        _parked({"n": 2}, origin="derived_product:2")
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            revived = resurrect_not_ready_units(origins=["derived_product:2"])

        self.assertEqual(revived, 1)
        self.assertEqual(task.delay.call_args.kwargs["unit"], {"n": 2})

    def test_an_empty_origins_list_revives_nothing(self):
        _parked({"n": 1})
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            self.assertEqual(resurrect_not_ready_units(origins=[]), 0)
        self.assertEqual(task.delay.call_count, 0)

    def test_an_unknown_recipe_is_skipped_not_fatal(self):
        _parked({"n": 1}, recipe_type="gone")
        _parked({"n": 2})
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            revived = resurrect_not_ready_units()

        self.assertEqual(revived, 1)
        self.assertEqual(task.delay.call_args.kwargs["unit"], {"n": 2})

    def test_a_wakeup_reason_override_is_threaded_to_dispatch(self):
        _parked({"n": 1})
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            resurrect_not_ready_units(
                reason=DerivationRun.RetryReason.INPUT_ARRIVED,
            )

        self.assertEqual(
            task.delay.call_args.kwargs["reason"],
            DerivationRun.RetryReason.INPUT_ARRIVED,
        )

    def test_terminal_and_running_runs_are_never_touched(self):
        for status in (
            DerivationRun.Status.COMPLETED, DerivationRun.Status.FAILED,
            DerivationRun.Status.RUNNING, DerivationRun.Status.PENDING,
        ):
            DerivationRun.objects.create(
                recipe_type=_WaitingRecipe.type, recipe_version="1",
                unit_key={"s": str(status)}, unit_hash=unit_hash({"s": str(status)}),
                status=status,
            )
        _WaitingRecipe.input_present = True

        with patch("georiva.processing.tasks.run_unit_task") as task:
            self.assertEqual(resurrect_not_ready_units(), 0)
        self.assertEqual(task.delay.call_count, 0)
