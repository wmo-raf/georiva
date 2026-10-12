"""Derivation attempt history as Domain events (ADR 0030, #427)."""

import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection, ProductPublished
from georiva.events.context import current_context, new_context
from georiva.events.tasks import context_to_headers
from georiva.events.testing import event_context
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import make_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.processing.engine import run_unit
from georiva.processing.models import DerivationAttemptFinished, DerivationRun
from georiva.processing.recipe import BaseRecipe, OutputItem, ResolvedInput, unit_hash
from georiva.processing.registry import RecipeRegistry
from georiva.processing.signals import unit_completed


class _Asset:
    def __init__(self, checksum):
        self.checksum = checksum


class _Recipe(BaseRecipe):
    type = "attempt_fake"
    version = "3"
    checksum = "v1"
    fail = False
    ready = True

    def enumerate_units(self, selector):
        return [{"n": 1}]

    def candidate_units(self, trigger):
        return [{"n": 1}]

    def resolve_inputs(self, unit):
        return {"src": ResolvedInput("src", required=False, items=[], assets=[_Asset(self.checksum)])}

    def readiness(self, unit, resolved):
        return self.ready

    def outputs(self, unit):
        return OutputItem(
            collection=Collection.objects.get(slug="attempt-out"),
            time=datetime(2020, 1, 1, tzinfo=UTC),
        )

    def transform(self, unit, resolved):
        if self.fail:
            raise RuntimeError("transform exploded")
        return []


UNIT = {"n": 1}


class AttemptEventTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        catalog = Catalog.objects.create(organisation=self.kenya, name="Out", slug="out", file_format="geotiff")
        self.collection = Collection.objects.create(
            catalog=catalog, slug="attempt-out", name="out", visibility=Collection.Visibility.INTERNAL
        )
        self.recipe = _Recipe()
        self.recipe.checksum, self.recipe.fail, self.recipe.ready = "v1", False, True
        self._saved = dict(RecipeRegistry._recipes)
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes[_Recipe.type] = _Recipe

    def tearDown(self):
        RecipeRegistry._recipes.clear()
        RecipeRegistry._recipes.update(self._saved)

    def _run(self, **kwargs):
        return run_unit(self.recipe, UNIT, writer=MagicMock(), origin="derived_product:9", **kwargs)

    def test_a_completed_attempt_snapshots_the_unit_and_publishes_the_product(self):
        with event_context(organisation_id=self.kenya.pk) as ctx:
            result = self._run()

        attempt = DerivationAttemptFinished.objects.get()
        self.assertEqual(result.event_id, attempt.pk)
        self.assertEqual(attempt.outcome, "completed")
        self.assertEqual((attempt.recipe_type, attempt.recipe_version), ("attempt_fake", "3"))
        self.assertEqual(attempt.attempt_number, 1)
        self.assertEqual(attempt.retry_reason, "initial")
        self.assertEqual(attempt.unit_key, UNIT)
        self.assertEqual(attempt.origin, "derived_product:9")
        self.assertEqual(attempt.produced_item_id, result.item_id)
        self.assertTrue(attempt.input_hash)

        published = ProductPublished.objects.get()
        self.assertEqual(published.origin, "derived")
        self.assertEqual(published.visibility, "internal")
        self.assertEqual(published.produced_item_id, result.item_id)
        self.assertEqual(published.caused_by_id, attempt.pk)
        self.assertEqual({attempt.correlation_id, published.correlation_id}, {ctx.correlation_id})
        self.assertEqual({attempt.organisation_id, published.organisation_id}, {self.kenya.pk})

    def test_each_outcome_yields_one_event(self):
        with event_context():
            self._run()  # completed
            self._run()  # inputs unchanged → skipped
            self.recipe.checksum, self.recipe.ready = "v2", False
            self._run()  # changed but not ready
            self.recipe.ready, self.recipe.fail = True, True
            with self.assertRaises(RuntimeError):
                self._run()  # failed

        outcomes = list(DerivationAttemptFinished.objects.order_by("occurred_at").values_list("outcome", flat=True))
        self.assertEqual(outcomes, ["completed", "skipped", "not_ready", "failed"])

    def test_a_retry_writes_a_second_event_and_leaves_the_first_untouched(self):
        self.recipe.fail = True
        with event_context():
            with self.assertRaises(RuntimeError):
                self._run()
            first = DerivationAttemptFinished.objects.get()
            self.recipe.fail = False
            self._run(reason="celery_retry")

        first.refresh_from_db()
        self.assertEqual((first.outcome, first.attempt_number, first.error), ("failed", 1, "transform exploded"))
        second = DerivationAttemptFinished.objects.exclude(pk=first.pk).get()
        self.assertEqual((second.outcome, second.attempt_number, second.retry_reason), ("completed", 2, "celery_retry"))
        self.assertEqual(DerivationRun.objects.get().attempts, 2)  # the row still holds only the latest

    def test_a_worker_refused_the_lock_writes_no_event(self):
        DerivationRun.acquire(
            recipe_type=_Recipe.type,
            recipe_version="3",
            unit_key=UNIT,
            unit_hash=unit_hash(UNIT),
        )
        with event_context():
            result = self._run()

        self.assertEqual(result.status, "locked")
        self.assertFalse(DerivationAttemptFinished.objects.exists())

    def test_the_attempt_takes_the_correlation_of_what_triggered_it(self):
        from georiva.processing.tasks import run_unit_task

        trigger = uuid.uuid4()
        headers = context_to_headers(
            new_context(actor="system:bucket", organisation_id=self.kenya.pk, caused_by=trigger)
        )
        with patch("georiva.ingestion.asset_writer.AssetWriter", MagicMock()):
            run_unit_task.apply(kwargs={"recipe_type": _Recipe.type, "unit": UNIT}, headers=headers)

        attempt = DerivationAttemptFinished.objects.get()
        self.assertEqual(attempt.correlation_id, uuid.UUID(headers["georiva_correlation_id"]))
        self.assertEqual(attempt.caused_by_id, trigger)
        self.assertEqual(attempt.actor, "system:bucket")
        self.assertEqual(attempt.organisation_id, self.kenya.pk)

    def test_unit_completed_fires_after_commit_carrying_the_event_and_continues_the_chain(self):
        from georiva.processing.tasks import run_unit_task

        received, downstream = [], []
        receiver = lambda sender, item, event=None, **kw: received.append(event)  # noqa: E731
        unit_completed.connect(receiver, dispatch_uid="attempt-test")
        self.addCleanup(unit_completed.disconnect, dispatch_uid="attempt-test")

        with (
            patch("georiva.ingestion.asset_writer.AssetWriter", MagicMock()),
            patch(
                "georiva.processing.invocation.dispatch_for_trigger",
                side_effect=lambda trigger, **kw: downstream.append(current_context()) or [],
            ),
            event_context(organisation_id=self.kenya.pk) as ctx,
            self.captureOnCommitCallbacks(execute=False) as callbacks,
        ):
            run_unit_task.apply(kwargs={"recipe_type": _Recipe.type, "unit": UNIT})
            self.assertEqual(received, [])  # not yet: waiting for the commit

        for callback in callbacks:
            callback()
        attempt = DerivationAttemptFinished.objects.get()
        self.assertEqual(received, [attempt])
        self.assertEqual(downstream[0].caused_by, attempt.pk)
        self.assertEqual(downstream[0].correlation_id, ctx.correlation_id)

    def test_the_engine_imports_nothing_from_the_feed_layer_or_organisations(self):
        source = (Path(__file__).resolve().parents[1] / "engine.py").read_text()
        self.assertIsNone(re.search(r"^\s*(from|import)\s+georiva\.(sources|organisations)\b", source, re.M))


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class UnitHistoryPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from georiva.sources.derivation_invocation import product_origin
        from georiva.sources.models import DataFeed, DerivedProduct

        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.products = {}
        for org in (cls.kenya, cls.uganda):
            catalog = Catalog.objects.create(organisation=org, name="C", slug="c", file_format="geotiff")
            feed = DataFeed.objects.create(name=f"{org.slug} feed", catalog=catalog)
            product = DerivedProduct.objects.create(data_feed=feed, definition_key="k", recipe_type="promotion")
            cls.products[org.slug] = product
            with event_context(organisation_id=org.pk):
                run = DerivationRun.acquire(
                    recipe_type="promotion",
                    recipe_version="1",
                    unit_key={"u": org.slug},
                    unit_hash=org.slug.ljust(64, "0"),
                    origin=product_origin(product),
                )
                run.mark_failed("no product was produced")

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def test_a_failed_unit_with_no_item_is_in_its_organisations_history_and_no_one_elses(self):
        response = self.client.get(reverse("derived_product_runs", args=[self.products["kenya"].pk]))

        self.assertEqual(response.status_code, 200)
        rows = response.context["history"].rows
        self.assertEqual([row.event.outcome for row in rows], ["failed"])
        self.assertEqual({row.event.organisation_id for row in rows}, {self.kenya.pk})
        self.assertEqual(
            self.client.get(reverse("derived_product_runs", args=[self.products["uganda"].pk])).status_code, 404
        )


class FeedLayerContextTests(TestCase):
    """The feed layer resolves the organisation; the acting user is kept."""

    def test_an_operators_rerun_keeps_the_user_and_takes_the_products_organisation(self):
        from georiva.sources.derivation_invocation import _product_context
        from georiva.sources.models import DataFeed, DerivedProduct

        org = make_organisation("kenya")
        catalog = Catalog.objects.create(organisation=org, name="C", slug="c", file_format="geotiff")
        product = DerivedProduct.objects.create(
            data_feed=DataFeed.objects.create(name="f", catalog=catalog), definition_key="k", recipe_type="promotion"
        )

        with event_context(actor="user:5") as outer, _product_context(product):
            inner = current_context()

        self.assertEqual(inner.actor, "user:5")
        self.assertEqual(inner.correlation_id, outer.correlation_id)
        self.assertEqual(inner.organisation_id, org.pk)
