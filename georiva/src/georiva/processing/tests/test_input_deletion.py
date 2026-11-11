"""
A derived item follows its inputs out (ADR 0041, issue #495).

Two seams:

- **Deleting an input** (a StagingItem or a published Item) re-dispatches, once
  the delete commits, every unit whose produced item was derived from it — with
  the ``input_deleted`` reason. The lineage is read *before* the cascade removes
  the ``DerivationLink`` rows that name it.
- **The engine** re-runs such a unit like any other. Ready on the remaining
  inputs → it recomputes. Not ready → the item it produced earlier is retracted
  (deleted; its files follow via ADR 0032), and the run parks as ``not_ready``.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import numpy as np
from django.db import transaction
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.events.testing import event_context
from georiva.organisations.testing import make_organisation
from georiva.processing.engine import run_unit
from georiva.processing.models import DerivationAttemptFinished, DerivationRun
from georiva.processing.recipe import BaseRecipe, OutputAsset, OutputItem, ResolvedInput
from georiva.processing.recipes.promotion import PromotionRecipe
from georiva.processing.registry import RecipeRegistry
from georiva.staging.models import DerivationLink, StagingAsset, StagingCollection, StagingItem

_TIME = datetime(2020, 1, 1, tzinfo=UTC)


def _mock_writer():
    w = MagicMock()
    w.bucket.save.side_effect = lambda path, data: path
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    return w


class _SumRecipe(BaseRecipe):
    """Sums every StagingItem of one staging collection into a single published
    item — a many-input unit that tolerates losing one of its inputs."""

    type = "sum_fake"
    version = "1"

    def __init__(self, *, staging_collection=None, output_collection=None, output_variable=None):
        self.scol = staging_collection
        self.out_col = output_collection
        self.out_var = output_variable

    def enumerate_units(self, selector):
        return [{"sum": "all"}]

    def resolve_inputs(self, unit):
        items = list(StagingItem.objects.filter(collection=self.scol).prefetch_related("assets"))
        assets = [a for it in items for a in it.assets.all()]
        return {"days": ResolvedInput("days", required=True, items=items, assets=assets)}

    def outputs(self, unit):
        return OutputItem(collection=self.out_col, time=_TIME, bounds=[0, 0, 1, 1], width=2, height=2)

    def transform(self, unit, resolved):
        n = float(len(resolved["days"].items))
        return [
            OutputAsset(
                variable=self.out_var,
                array=np.full((2, 2), n, dtype="float32"),
                bounds=[0, 0, 1, 1],
                width=2,
                height=2,
            )
        ]


class _Fixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.unit_c = Unit.objects.create(name="Celsius", symbol="C")
        self.pub_col = Collection.objects.create(catalog=self.catalog, slug="tas", name="tas")
        self.variable = self._variable(self.pub_col, "tas")
        self.scol = StagingCollection.objects.create(catalog=self.catalog, slug="tas", name="tas")
        self.sitem = self._staging_item(_TIME, "abc")

        self.sum_col = Collection.objects.create(catalog=self.catalog, slug="tas-sum", name="tas-sum")
        self.sum_var = self._variable(self.sum_col, "tas-sum")

        self._saved = dict(RecipeRegistry._recipes)

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)

    def _variable(self, collection, slug):
        return Variable.objects.create(
            collection=collection, slug=slug, name=slug, unit=self.unit_c, value_min=0, value_max=50
        )

    def _staging_item(self, when, checksum):
        si = StagingItem.objects.create(
            collection=self.scol, datetime=when, bounds=[0, 0, 1, 1], crs="EPSG:4326", width=10, height=10
        )
        StagingAsset.objects.create(
            item=si,
            href=f"cmip6/tas/{checksum}.tif",
            roles=["source"],
            format="geotiff",
            checksum=checksum,
            variable=self.variable,
        )
        return si

    def _promote(self, staging_item):
        data = np.full((10, 10), 5.0, dtype="float32")
        with patch.object(PromotionRecipe, "read_raster", return_value=(data, [0, 0, 1, 1], "EPSG:4326", 10, 10)):
            return run_unit(PromotionRecipe(), {"staging_item_id": staging_item.pk}, writer=_mock_writer())

    def _sum_recipe(self):
        return _SumRecipe(staging_collection=self.scol, output_collection=self.sum_col, output_variable=self.sum_var)

    def _sum(self, **kwargs):
        return run_unit(self._sum_recipe(), {"sum": "all"}, writer=_mock_writer(), **kwargs)


class DeletingAnInputRedispatchesItsDerivedUnitsTests(_Fixture):
    def _delete(self, obj):
        with patch("georiva.processing.tasks.run_unit_task") as task:
            with self.captureOnCommitCallbacks(execute=True):
                obj.delete()
        return task.delay

    def test_deleting_a_staging_input_redispatches_the_unit_with_input_deleted(self):
        self._promote(self.sitem)
        unit = {"staging_item_id": self.sitem.pk}

        delay = self._delete(self.sitem)

        delay.assert_called_once_with(recipe_type="promotion", unit=unit, reason="input_deleted")

    def test_deleting_a_published_input_redispatches_the_units_derived_from_it(self):
        promoted = Item.objects.get(pk=self._promote(self.sitem).item_id)
        # A further unit consumed the promoted item.
        downstream = Item.objects.create(collection=self.sum_col, time=_TIME)
        DerivationLink.objects.create(
            derived_item=downstream,
            source_published_item=promoted,
            recipe_id="sum_fake",
            recipe_version="1",
            input_hash="h",
        )
        run_rec = DerivationRun.objects.create(
            recipe_type="sum_fake",
            recipe_version="1",
            unit_key={"sum": "all"},
            unit_hash="u",
            status=DerivationRun.Status.COMPLETED,
            produced_item=downstream,
        )

        delay = self._delete(promoted)

        delay.assert_called_once_with(recipe_type="sum_fake", unit=run_rec.unit_key, reason="input_deleted")

    def test_one_unit_fed_by_several_deleted_inputs_is_dispatched_once(self):
        self._staging_item(datetime(2020, 1, 2, tzinfo=UTC), "def")
        self._sum()

        with patch("georiva.processing.tasks.run_unit_task") as task:
            with self.captureOnCommitCallbacks(execute=True):
                StagingItem.objects.filter(collection=self.scol).delete()

        task.delay.assert_called_once_with(recipe_type="sum_fake", unit={"sum": "all"}, reason="input_deleted")

    def test_an_input_with_no_dependents_dispatches_nothing(self):
        delay = self._delete(self.sitem)

        delay.assert_not_called()

    def test_a_rolled_back_delete_dispatches_nothing(self):
        self._promote(self.sitem)
        sitem_pk = self.sitem.pk

        with patch("georiva.processing.tasks.run_unit_task") as task:
            with self.captureOnCommitCallbacks(execute=True):
                try:
                    with transaction.atomic():
                        self.sitem.delete()
                        raise RuntimeError("roll back")
                except RuntimeError:
                    pass

        task.delay.assert_not_called()
        self.assertTrue(DerivationLink.objects.filter(source_staging_item_id=sitem_pk).exists())

    def test_a_derived_item_deleted_with_its_input_is_not_redispatched(self):
        promoted = Item.objects.get(pk=self._promote(self.sitem).item_id)

        with patch("georiva.processing.tasks.run_unit_task") as task:
            with self.captureOnCommitCallbacks(execute=True):
                with transaction.atomic():
                    self.sitem.delete()
                    promoted.delete()

        task.delay.assert_not_called()


class TheEngineRetractsWhatIsNoLongerReadyTests(_Fixture):
    def test_a_unit_whose_only_input_is_gone_retracts_its_item(self):
        result = self._promote(self.sitem)
        unit = {"staging_item_id": self.sitem.pk}
        StagingItem.objects.filter(pk=self.sitem.pk).delete()

        with event_context():
            outcome = run_unit(PromotionRecipe(), unit, reason="input_deleted")

        self.assertEqual(outcome.status, "retracted")
        self.assertFalse(Item.objects.filter(pk=result.item_id).exists())
        run_rec = DerivationRun.objects.get(recipe_type="promotion")
        self.assertEqual(run_rec.status, DerivationRun.Status.NOT_READY)
        self.assertIsNone(run_rec.produced_item_id)
        self.assertEqual(run_rec.last_retry_reason, "input_deleted")

        attempt = DerivationAttemptFinished.objects.order_by("-occurred_at").first()
        self.assertEqual(attempt.outcome, "retracted")
        self.assertEqual(attempt.produced_item_id, result.item_id)  # names what was withdrawn

    def test_a_unit_that_tolerates_the_loss_recomputes_on_what_remains(self):
        second = self._staging_item(datetime(2020, 1, 2, tzinfo=UTC), "def")
        first_run = self._sum()
        StagingItem.objects.filter(pk=second.pk).delete()

        outcome = self._sum(reason="input_deleted")

        self.assertEqual(outcome.status, "completed")
        self.assertEqual(outcome.item_id, first_run.item_id)  # overwritten in place
        self.assertNotEqual(outcome.input_hash, first_run.input_hash)
        links = DerivationLink.objects.filter(derived_item_id=outcome.item_id)
        self.assertEqual([link.source_staging_item_id for link in links], [self.sitem.pk])

    def test_a_unit_never_produced_parks_as_not_ready(self):
        unit = {"staging_item_id": self.sitem.pk + 999}

        outcome = run_unit(PromotionRecipe(), unit)

        self.assertEqual(outcome.status, "not_ready")
        self.assertEqual(DerivationRun.objects.get().status, DerivationRun.Status.NOT_READY)


class RetractionCascadesForwardTests(_Fixture):
    """Retracting an intermediate is itself the deletion of an input, so the
    units built on it are re-dispatched in turn."""

    def test_deleting_a_source_retracts_its_promotion_then_reaches_the_next_unit(self):
        RecipeRegistry._recipes["promotion"] = PromotionRecipe
        promoted = Item.objects.get(pk=self._promote(self.sitem).item_id)
        downstream = Item.objects.create(collection=self.sum_col, time=_TIME)
        DerivationLink.objects.create(
            derived_item=downstream,
            source_published_item=promoted,
            recipe_id="sum_fake",
            recipe_version="1",
            input_hash="h",
        )
        DerivationRun.objects.create(
            recipe_type="sum_fake",
            recipe_version="1",
            unit_key={"sum": "all"},
            unit_hash="u",
            status=DerivationRun.Status.COMPLETED,
            produced_item=downstream,
        )

        dispatched = []

        def inline(recipe_type, unit, reason):
            dispatched.append(recipe_type)
            if recipe_type == "promotion":
                run_unit(RecipeRegistry.get(recipe_type), unit, reason=reason)

        with patch("georiva.processing.tasks.run_unit_task") as task:
            task.delay.side_effect = inline
            with self.captureOnCommitCallbacks(execute=True):
                self.sitem.delete()

        self.assertFalse(Item.objects.filter(pk=promoted.pk).exists())
        self.assertEqual(dispatched, ["promotion", "sum_fake"])


class RetractionNeedsChangedInputsTests(_Fixture):
    def test_a_unit_whose_inputs_are_unchanged_is_not_retracted_by_its_readiness(self):
        # A custom readiness (a minimum count, a time window) can turn false with
        # nothing deleted. The item it published stays: only lost inputs retract.
        first = self._sum()
        recipe = self._sum_recipe()
        recipe.readiness = lambda unit, resolved: False

        outcome = run_unit(recipe, {"sum": "all"}, writer=_mock_writer())

        self.assertEqual(outcome.status, "skipped")
        self.assertTrue(Item.objects.filter(pk=first.item_id).exists())


class TheSweepCatchesARedispatchThatLostTheLockTests(_Fixture):
    """A unit RUNNING when its input is deleted refuses the re-dispatch's lock;
    the running attempt then fails on the vanished input. The stale sweep, which
    looks only at completed runs, would never see it again."""

    def test_a_failed_run_whose_item_lost_its_inputs_is_retracted_by_the_sweep(self):
        from georiva.processing.invocation import sweep_stale_units

        RecipeRegistry._recipes["promotion"] = PromotionRecipe
        promoted_id = self._promote(self.sitem).item_id
        with patch("georiva.processing.tasks.run_unit_task"):
            with self.captureOnCommitCallbacks(execute=True):
                self.sitem.delete()
        DerivationRun.objects.filter(recipe_type="promotion").update(status=DerivationRun.Status.FAILED)

        swept = sweep_stale_units(dispatch=False)

        self.assertEqual(swept, 1)
        self.assertFalse(Item.objects.filter(pk=promoted_id).exists())
        run_rec = DerivationRun.objects.get(recipe_type="promotion")
        self.assertEqual((run_rec.status, run_rec.last_retry_reason), ("not_ready", "input_deleted"))

    def test_a_failed_run_whose_inputs_still_resolve_is_left_to_its_retries(self):
        from georiva.processing.invocation import sweep_stale_units

        self._promote(self.sitem)
        DerivationRun.objects.filter(recipe_type="promotion").update(status=DerivationRun.Status.FAILED)

        with patch("georiva.processing.tasks.run_unit_task") as task:
            swept = sweep_stale_units()

        self.assertEqual(swept, 0)
        task.delay.assert_not_called()
