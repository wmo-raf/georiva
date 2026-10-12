"""Derivation sweep passes record their interventions and keep the chain (ADR 0030, #428)."""

from datetime import timedelta

from django.test import TestCase
from django.utils import timezone as dj_timezone

from georiva.events.models import SweepIntervened
from georiva.events.testing import event_context
from georiva.organisations.testing import make_organisation
from georiva.processing.invocation import reclaim_stale_running, resurrect_not_ready_units, sweep_stale_units
from georiva.processing.models import DerivationAttemptFinished, DerivationRun
from georiva.processing.recipe import unit_hash
from georiva.processing.registry import RecipeRegistry

from .test_attempt_events import UNIT, _Recipe


class DerivationSweepTests(TestCase):
    def setUp(self):
        from georiva.core.models import Catalog, Collection

        self.kenya = make_organisation("kenya")
        catalog = Catalog.objects.create(organisation=self.kenya, name="Out", slug="out", file_format="geotiff")
        Collection.objects.create(catalog=catalog, slug="attempt-out", name="out")
        self._saved = dict(RecipeRegistry._recipes)
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes[_Recipe.type] = _Recipe
        self.recipe = _Recipe()

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)

    def _run_with_history(self, status, **overrides):
        """A unit with a prior attempt event in a known chain, then forced into ``status``."""
        from georiva.processing.engine import run_unit

        with event_context(organisation_id=self.kenya.pk) as ctx:
            run_unit(self.recipe, UNIT, origin="derived_product:1")
        DerivationRun.objects.update(status=status, **overrides)
        return ctx, DerivationRun.objects.get()

    def test_input_stale_recompute_is_recorded_and_the_next_attempt_stays_in_the_chain(self):
        ctx, run = self._run_with_history(DerivationRun.Status.COMPLETED, input_hash="STALE")

        self.assertEqual(sweep_stale_units(dispatch=False, sweep="derivations"), 1)

        intervention = SweepIntervened.objects.get()
        self.assertEqual((intervention.reason, intervention.target_kind), ("inputs_changed", "derivation_run"))
        self.assertEqual(intervention.target_id, run.pk)
        self.assertEqual(intervention.correlation_id, ctx.correlation_id)
        self.assertEqual(intervention.organisation_id, self.kenya.pk)
        self.assertEqual(intervention.actor, "sweep:derivations")
        latest = DerivationAttemptFinished.objects.order_by("-occurred_at").first()
        self.assertEqual(latest.correlation_id, ctx.correlation_id)
        self.assertEqual(latest.actor, "sweep:derivations")
        self.assertEqual(latest.attempt_number, 2)

    def test_a_stale_running_reclaim_is_recorded(self):
        ctx, _ = self._run_with_history(
            DerivationRun.Status.RUNNING,
            locked_at=dj_timezone.now() - DerivationRun.LOCK_TIMEOUT - timedelta(minutes=1),
        )

        self.assertEqual(reclaim_stale_running(dispatch=False, sweep="derivations"), 1)

        intervention = SweepIntervened.objects.get()
        self.assertEqual(intervention.reason, "stale_running_reclaimed")
        self.assertEqual(intervention.correlation_id, ctx.correlation_id)

    def test_a_not_ready_revival_is_recorded(self):
        ctx, _ = self._run_with_history(DerivationRun.Status.NOT_READY)

        self.assertEqual(resurrect_not_ready_units(dispatch=False, sweep="derivations"), 1)

        self.assertEqual(SweepIntervened.objects.get().reason, "not_ready_revived")

    def test_a_run_with_no_prior_event_starts_a_fresh_chain(self):
        DerivationRun.objects.create(
            recipe_type=_Recipe.type,
            recipe_version="3",
            unit_key=UNIT,
            unit_hash=unit_hash(UNIT),
            status=DerivationRun.Status.COMPLETED,
            input_hash="STALE",
        )

        sweep_stale_units(dispatch=False, sweep="derivations")

        intervention = SweepIntervened.objects.get()
        self.assertEqual(intervention.actor, "sweep:derivations")
        self.assertFalse(
            DerivationAttemptFinished.objects.filter(correlation_id=intervention.correlation_id)
            .exclude(actor="sweep:derivations")
            .exists()
        )

    def test_the_wakeup_path_is_not_a_sweep_and_writes_no_intervention(self):
        self._run_with_history(DerivationRun.Status.NOT_READY)

        resurrect_not_ready_units(dispatch=False)

        self.assertFalse(SweepIntervened.objects.exists())

    def test_a_pass_with_nothing_to_do_writes_nothing(self):
        sweep_stale_units(dispatch=False, sweep="derivations")
        reclaim_stale_running(dispatch=False, sweep="derivations")
        resurrect_not_ready_units(dispatch=False, sweep="derivations")
        self.assertFalse(SweepIntervened.objects.exists())
