"""
Origin threading through the engine (ADR-0008, issue #147).

The invocation layer stamps each DerivationRun with an opaque `origin` (the
product identity) by passing it through run/run_unit to DerivationRun.acquire.
The engine never interprets it — these tests assert it is stored, threaded, and
not clobbered by engine-internal re-runs. The product-driven dispatcher that
*builds* the origin lives in the sources layer (tested separately).
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.processing.engine import run, run_unit
from georiva.processing.models import DerivationRun
from georiva.processing.recipe import BaseRecipe, OutputItem, ResolvedInput


class _NotReadyRecipe(BaseRecipe):
    """A recipe whose required input is absent, so run_unit acquires the run
    (stamping origin) then stops at the readiness gate — no Item is produced."""

    type = "origin_fake"
    version = "1"

    def enumerate_units(self, selector):
        return [{"k": 1}]

    def resolve_inputs(self, unit):
        return {"src": ResolvedInput("src", required=True, items=[], assets=[])}

    def outputs(self, unit):
        from datetime import datetime, timezone
        return OutputItem(collection=None, time=datetime(2020, 1, 1, tzinfo=timezone.utc))

    def transform(self, unit, resolved):
        return []


class _TwoUnitRecipe(BaseRecipe):
    type = "two_unit_fake"
    version = "1"

    def enumerate_units(self, selector):
        return [{"n": 1}, {"n": 2}]

    def resolve_inputs(self, unit):
        return {}

    def outputs(self, unit):
        return OutputItem(collection=None, time=None)

    def transform(self, unit, resolved):
        return []


class RunOriginThreadingTests(TestCase):
    def test_run_threads_origin_to_each_dispatched_unit_task(self):
        with patch("georiva.processing.tasks.run_unit_task") as task:
            run(_TwoUnitRecipe(), {}, dispatch=True, origin="derived_product:9")

        self.assertEqual(task.delay.call_count, 2)
        origins = {c.kwargs["origin"] for c in task.delay.call_args_list}
        self.assertEqual(origins, {"derived_product:9"})


class RunUnitOriginTests(TestCase):
    def test_run_unit_stamps_origin_on_the_derivation_run(self):
        run_unit(_NotReadyRecipe(), {"k": 1}, origin="derived_product:7")

        runrec = DerivationRun.objects.get(recipe_type="origin_fake")
        self.assertEqual(runrec.origin, "derived_product:7")

    def test_re_running_without_origin_keeps_the_existing_stamp(self):
        recipe = _NotReadyRecipe()
        run_unit(recipe, {"k": 1}, origin="derived_product:7")

        # An engine-internal re-dispatch (sweep / invalidation) carries no origin.
        run_unit(recipe, {"k": 1})

        runrec = DerivationRun.objects.get(recipe_type="origin_fake")
        self.assertEqual(runrec.origin, "derived_product:7")
