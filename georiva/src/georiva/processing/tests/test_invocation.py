"""
Invocation surface — event-driven, scheduled/backfill, and the periodic
backfill sweep all funnel through the one ``run(recipe, selector)`` primitive.
Tests drive each path and assert the right units are (re)computed, with the
per-unit compute (``run_unit_task``) mocked.

See issue #125 and docs/adr/0005-generic-derivation-engine.md.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.processing.engine import run
from georiva.processing.recipe import BaseRecipe, OutputItem
from georiva.processing.registry import RecipeRegistry


class _TriggerRecipe(BaseRecipe):
    """
    A recipe whose ``candidate_units`` maps a narrow trigger to just the units
    it feeds, while ``enumerate_units`` would return a *wide* set. Proves that
    ``run()`` enumerates via ``candidate_units`` (so streaming stays narrow).
    """

    type = "trigger_fake"
    version = "1"

    def enumerate_units(self, selector):
        # Deliberately wide — run() must NOT use this for a trigger.
        return [{"n": i} for i in range(5)]

    def candidate_units(self, trigger):
        if trigger.get("relevant"):
            return [{"n": 1}, {"n": 2}]
        return []

    def resolve_inputs(self, unit):
        return {}

    def outputs(self, unit):
        return OutputItem(collection=None, time=None)

    def transform(self, unit, resolved):
        return []


class RunUsesCandidateUnitsTests(TestCase):
    def test_run_dispatches_candidate_units_not_enumerate(self):
        with patch("georiva.processing.tasks.run_unit_task") as task:
            run(_TriggerRecipe(), {"relevant": True}, dispatch=True)

        # candidate_units → 2 units (not enumerate_units' 5).
        self.assertEqual(task.delay.call_count, 2)
        dispatched = {c.kwargs["unit"]["n"] for c in task.delay.call_args_list}
        self.assertEqual(dispatched, {1, 2})

    def test_irrelevant_trigger_dispatches_nothing(self):
        with patch("georiva.processing.tasks.run_unit_task") as task:
            run(_TriggerRecipe(), {"relevant": False}, dispatch=True)

        self.assertEqual(task.delay.call_count, 0)


class _RangeRecipe(BaseRecipe):
    """A recipe that expands a wide range selector into one unit per year —
    the scheduled/backfill shape. Uses the *default* ``candidate_units`` (which
    delegates to ``enumerate_units``), so streaming and backfill share one path."""

    type = "range_fake"
    version = "1"

    def enumerate_units(self, selector):
        start, end = selector["years"]
        return [{"year": y} for y in range(start, end + 1)]

    def resolve_inputs(self, unit):
        return {}

    def outputs(self, unit):
        return OutputItem(collection=None, time=None)

    def transform(self, unit, resolved):
        return []


class ScheduledBackfillTests(TestCase):
    def test_run_over_range_fans_out_one_task_per_unit(self):
        with patch("georiva.processing.tasks.run_unit_task") as task:
            run(_RangeRecipe(), {"years": [2011, 2013]}, dispatch=True)

        # One per-unit task per year in the range (2011, 2012, 2013).
        self.assertEqual(task.delay.call_count, 3)
        years = {c.kwargs["unit"]["year"] for c in task.delay.call_args_list}
        self.assertEqual(years, {2011, 2012, 2013})


class _RegistryIsolationMixin:
    """Register fake recipes for the duration of a test, then restore."""

    fake_recipes: list = []

    def setUp(self):
        super().setUp()
        self._saved = dict(RecipeRegistry._recipes)
        RecipeRegistry._recipes.clear()
        for cls in self.fake_recipes:
            RecipeRegistry._recipes[cls.type] = cls

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)
        super().tearDown()


class _RelevantRecipe(_TriggerRecipe):
    type = "fake_relevant"

    def candidate_units(self, trigger):
        return [{"n": 1}] if trigger.get("kind") == "match" else []


class _OtherRecipe(_TriggerRecipe):
    type = "fake_other"

    def candidate_units(self, trigger):
        return []


class DispatchForTriggerTests(_RegistryIsolationMixin, TestCase):
    fake_recipes = [_RelevantRecipe, _OtherRecipe]

    def test_only_recipes_that_claim_the_trigger_dispatch(self):
        from georiva.processing.invocation import dispatch_for_trigger

        with patch("georiva.processing.tasks.run_unit_task") as task:
            dispatch_for_trigger({"kind": "match"})

        dispatched = [c.kwargs["recipe_type"] for c in task.delay.call_args_list]
        self.assertEqual(dispatched, ["fake_relevant"])

    def test_trigger_no_recipe_claims_dispatches_nothing(self):
        from georiva.processing.invocation import dispatch_for_trigger

        with patch("georiva.processing.tasks.run_unit_task") as task:
            dispatch_for_trigger({"kind": "nomatch"})

        self.assertEqual(task.delay.call_count, 0)


class _StagingFixture(TestCase):
    def setUp(self):
        from datetime import datetime, timezone

        from georiva.core.models import Catalog
        from georiva.staging.models import (
            StagingAsset,
            StagingCollection,
            StagingItem,
        )

        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="tas", name="tas"
        )
        self.sitem = StagingItem.objects.create(
            collection=self.scol,
            datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )
        StagingAsset.objects.create(
            item=self.sitem, href="cmip6/tas/f.tif", roles=["source"],
            format="geotiff", checksum="abc123",
        )


class PromotionCandidateUnitsTests(_StagingFixture):
    """Promotion is the naturally event-driven recipe: an arriving staging
    item maps 1:1 to its promotion unit; a wide selector still enumerates."""

    def test_staging_trigger_maps_to_single_unit(self):
        from datetime import datetime, timezone

        from georiva.processing.recipes.promotion import PromotionRecipe
        from georiva.staging.models import StagingItem

        # A second staging item that must NOT be triggered by the first's event.
        StagingItem.objects.create(
            collection=self.scol,
            datetime=datetime(2020, 1, 2, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10,
        )

        units = list(
            PromotionRecipe().candidate_units({"staging_item_id": self.sitem.pk})
        )
        self.assertEqual(units, [{"staging_item_id": self.sitem.pk}])

    def test_selector_without_trigger_still_enumerates(self):
        from georiva.processing.recipes.promotion import PromotionRecipe

        units = list(
            PromotionRecipe().candidate_units({"collection_slug": "tas"})
        )
        self.assertIn({"staging_item_id": self.sitem.pk}, units)

    def test_ignores_published_item_trigger(self):
        # Promotion consumes Staging inputs only. A completion-chaining trigger
        # for a Published item (carrying a collection_slug) must NOT make it
        # fire for same-named staging items.
        from georiva.processing.recipes.promotion import PromotionRecipe

        units = list(PromotionRecipe().candidate_units(
            {"published_item_id": 99, "collection_slug": "tas"}
        ))
        self.assertEqual(units, [])


class CompletionChainingTests(_RegistryIsolationMixin, TestCase):
    """When a unit completes producing a Published item, that item is itself an
    input — the streaming task chains a downstream trigger so internal
    intermediates flow to their consumers."""

    fake_recipes = [_RelevantRecipe]

    def _published_item(self):
        from datetime import datetime, timezone

        from georiva.core.models import Catalog, Collection, Item

        catalog = Catalog.objects.create(
            name="C", slug="c", file_format="geotiff"
        )
        col = Collection.objects.create(catalog=catalog, slug="anom", name="anom")
        return Item.objects.create(
            collection=col, time=datetime(2020, 1, 1, tzinfo=timezone.utc)
        )

    def test_completed_unit_chains_downstream_trigger(self):
        from georiva.processing.engine import UnitResult
        from georiva.processing.tasks import run_unit_task

        item = self._published_item()

        with (
            patch(
                "georiva.processing.engine.run_unit",
                return_value=UnitResult(status="completed", item_id=item.pk),
            ),
            patch(
                "georiva.processing.invocation.dispatch_for_trigger"
            ) as dispatch,
        ):
            run_unit_task.apply(
                kwargs={"recipe_type": "fake_relevant", "unit": {"n": 1}}
            )

        dispatch.assert_called_once()
        trigger = dispatch.call_args.args[0]
        self.assertEqual(trigger["published_item_id"], item.pk)
        self.assertEqual(trigger["collection_slug"], "anom")

    def test_not_ready_unit_does_not_chain(self):
        from georiva.processing.engine import UnitResult
        from georiva.processing.tasks import run_unit_task

        with (
            patch(
                "georiva.processing.engine.run_unit",
                return_value=UnitResult(status="not_ready"),
            ),
            patch(
                "georiva.processing.invocation.dispatch_for_trigger"
            ) as dispatch,
        ):
            run_unit_task.apply(
                kwargs={"recipe_type": "fake_relevant", "unit": {"n": 1}}
            )

        dispatch.assert_not_called()


class _Asset:
    """A minimal asset-like object carrying just a checksum (for hashing)."""

    def __init__(self, checksum):
        self.checksum = checksum


class _SweepRecipe(BaseRecipe):
    """A recipe whose single input's checksum the test controls, to drive the
    sweep's recorded-vs-current input_hash comparison."""

    type = "sweep_fake"
    version = "1"
    checksum = "v1"

    def enumerate_units(self, selector):
        return []

    def candidate_units(self, trigger):
        return []

    def resolve_inputs(self, unit):
        from georiva.processing.recipe import ResolvedInput

        return {
            "src": ResolvedInput(
                "src", required=True, items=[], assets=[_Asset(self.checksum)]
            )
        }

    def outputs(self, unit):
        return OutputItem(collection=None, time=None)

    def transform(self, unit, resolved):
        return []


class SweepStalenessTests(_RegistryIsolationMixin, TestCase):
    fake_recipes = [_SweepRecipe]

    def _completed_run(self, recorded_hash):
        from georiva.processing.models import DerivationRun
        from georiva.processing.recipe import unit_hash

        unit = {"n": 1}
        return DerivationRun.objects.create(
            recipe_type="sweep_fake", recipe_version="1",
            unit_key=unit, unit_hash=unit_hash(unit),
            input_hash=recorded_hash, status=DerivationRun.Status.COMPLETED,
        )

    def _current_hash(self):
        from georiva.processing.recipe import compute_input_hash

        recipe = _SweepRecipe()
        return compute_input_hash(recipe.resolve_inputs({"n": 1}), recipe.version)

    def test_stale_unit_is_recomputed_without_an_event(self):
        from georiva.processing.tasks import sweep_derivations

        # Recorded hash differs from current → the input changed.
        self._completed_run(recorded_hash="STALE")

        with patch("georiva.processing.tasks.run_unit_task") as task:
            sweep_derivations.apply()

        task.delay.assert_called_once()
        self.assertEqual(task.delay.call_args.kwargs["recipe_type"], "sweep_fake")
        self.assertEqual(task.delay.call_args.kwargs["unit"], {"n": 1})

    def test_current_unit_is_not_recomputed(self):
        from georiva.processing.tasks import sweep_derivations

        # Recorded hash equals current → nothing to do.
        self._completed_run(recorded_hash=self._current_hash())

        with patch("georiva.processing.tasks.run_unit_task") as task:
            sweep_derivations.apply()

        task.delay.assert_not_called()

    def test_sweep_propagates_through_intermediates(self):
        """A stale unit's recompute also invalidates items derived from its
        output — in one pass, before the intermediate has recomputed."""
        from datetime import datetime, timezone

        from georiva.core.models import Catalog, Collection, Item
        from georiva.processing.models import DerivationRun
        from georiva.processing.recipe import unit_hash
        from georiva.processing.tasks import sweep_derivations
        from georiva.staging.models import DerivationLink

        t = datetime(2020, 1, 1, tzinfo=timezone.utc)
        catalog = Catalog.objects.create(name="C2", slug="c2", file_format="geotiff")

        # B is the stale unit's product (sweep_fake, recorded ≠ current).
        bcol = Collection.objects.create(catalog=catalog, slug="b2", name="b2")
        b = Item.objects.create(collection=bcol, time=t)
        b_unit = {"n": 1}
        DerivationRun.objects.create(
            recipe_type="sweep_fake", recipe_version="1",
            unit_key=b_unit, unit_hash=unit_hash(b_unit),
            input_hash="STALE", status=DerivationRun.Status.COMPLETED,
            produced_item=b,
        )

        # C is derived from B (a further product). Its own input (B) hasn't
        # changed yet, so only the forward walk reaches it.
        ccol = Collection.objects.create(catalog=catalog, slug="c2c", name="c2c")
        c = Item.objects.create(collection=ccol, time=t)
        c_unit = {"id": "C"}
        DerivationRun.objects.create(
            recipe_type="recipe_c", recipe_version="1",
            unit_key=c_unit, unit_hash=unit_hash(c_unit),
            input_hash="hc", status=DerivationRun.Status.COMPLETED,
            produced_item=c,
        )
        DerivationLink.objects.create(
            derived_item=c, source_published_item=b,
            recipe_id="recipe_c", recipe_version="1", input_hash="hc",
        )

        with patch("georiva.processing.tasks.run_unit_task") as task:
            sweep_derivations.apply()

        dispatched = {
            (call.kwargs["recipe_type"], tuple(sorted(call.kwargs["unit"].items())))
            for call in task.delay.call_args_list
        }
        self.assertIn(("sweep_fake", (("n", 1),)), dispatched)   # the stale unit
        self.assertIn(("recipe_c", (("id", "C"),)), dispatched)  # propagated downstream


class ForwardInvalidationTests(TestCase):
    """A changed input invalidates its derived items transitively — walking
    DerivationLink forward through internal intermediates (A → B → C)."""

    def setUp(self):
        from datetime import datetime, timezone

        from georiva.core.models import Catalog, Collection, Item
        from georiva.processing.models import DerivationRun
        from georiva.processing.recipe import unit_hash
        from georiva.staging.models import (
            DerivationLink,
            StagingCollection,
            StagingItem,
        )

        t = datetime(2020, 1, 1, tzinfo=timezone.utc)
        catalog = Catalog.objects.create(name="C", slug="c", file_format="geotiff")

        # A: staging input.
        scol = StagingCollection.objects.create(catalog=catalog, slug="a", name="a")
        self.A = StagingItem.objects.create(collection=scol, datetime=t)

        # B: internal intermediate derived from A.
        bcol = Collection.objects.create(
            catalog=catalog, slug="b", name="b",
            visibility=Collection.Visibility.INTERNAL,
        )
        self.B = Item.objects.create(collection=bcol, time=t)
        DerivationRun.objects.create(
            recipe_type="recipe_b", recipe_version="1",
            unit_key={"id": "B"}, unit_hash=unit_hash({"id": "B"}),
            status=DerivationRun.Status.COMPLETED, produced_item=self.B,
        )
        DerivationLink.objects.create(
            derived_item=self.B, source_staging_item=self.A,
            recipe_id="recipe_b", recipe_version="1", input_hash="hb",
        )

        # C: product derived from B.
        ccol = Collection.objects.create(catalog=catalog, slug="cc", name="cc")
        self.C = Item.objects.create(collection=ccol, time=t)
        DerivationRun.objects.create(
            recipe_type="recipe_c", recipe_version="1",
            unit_key={"id": "C"}, unit_hash=unit_hash({"id": "C"}),
            status=DerivationRun.Status.COMPLETED, produced_item=self.C,
        )
        DerivationLink.objects.create(
            derived_item=self.C, source_published_item=self.B,
            recipe_id="recipe_c", recipe_version="1", input_hash="hc",
        )

    def test_changed_input_recomputes_downstream_transitively(self):
        from georiva.processing.invocation import invalidate_downstream

        with patch("georiva.processing.tasks.run_unit_task") as task:
            invalidate_downstream(self.A)

        dispatched = {
            (c.kwargs["recipe_type"], c.kwargs["unit"]["id"])
            for c in task.delay.call_args_list
        }
        self.assertEqual(dispatched, {("recipe_b", "B"), ("recipe_c", "C")})

    def test_invalidating_intermediate_recomputes_only_below_it(self):
        from georiva.processing.invocation import invalidate_downstream

        # Changing B (not A) should recompute C only.
        with patch("georiva.processing.tasks.run_unit_task") as task:
            invalidate_downstream(self.B)

        dispatched = {
            (c.kwargs["recipe_type"], c.kwargs["unit"]["id"])
            for c in task.delay.call_args_list
        }
        self.assertEqual(dispatched, {("recipe_c", "C")})


class ClimatologyCandidateUnitsTests(TestCase):
    """Climatology is scheduled/manual, not event-driven: it needs period
    config an arriving input can't supply, so it ignores bare event triggers
    (and never crashes when fanned out across recipes for an input event)."""

    def test_ignores_event_trigger(self):
        from georiva.processing.recipes.climatology import ClimatologyRecipe

        units = list(
            ClimatologyRecipe().candidate_units({"staging_item_id": 5})
        )
        self.assertEqual(units, [])

    def test_full_selector_still_enumerates(self):
        from georiva.processing.recipes.climatology import ClimatologyRecipe

        selector = {
            "source_collection": "tas", "variable": "tas",
            "periods": [[2011, 2040]], "seasons": ["annual"],
            "quantities": ["value"],
        }
        units = list(ClimatologyRecipe().candidate_units(selector))
        self.assertEqual(len(units), 1)
