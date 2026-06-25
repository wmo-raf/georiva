"""
Scheduled-product beat loop (ADR-0008, issue #148).

A single beat iterates the enabled DerivedProducts whose trigger_mode is
scheduled and dispatches each due one via the product-driven invocation path
(run_product_now). Event-driven and manual products are never fired here, and a
product that ran recently is skipped until its interval elapses.
"""
from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog
from georiva.sources.derivation_invocation import dispatch_due_scheduled_products
from georiva.sources.models import DataFeed, DerivedProduct


def _definition(trigger_mode="scheduled", **overrides):
    kwargs = dict(
        key="anomaly",
        recipe_type="climatology",
        label="Rainfall anomaly",
        description="",
        config_schema=(),
        inputs=(InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode=trigger_mode,
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


class DispatchDueScheduledProductsTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(
            name="Feed", catalog=self.catalog, interval_minutes=60,
        )

    def _product(self, **overrides):
        return DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key="anomaly",
            recipe_type="climatology",
            is_enabled=overrides.get("is_enabled", True),
            interval_minutes=overrides.get("interval_minutes"),
            last_run_at=overrides.get("last_run_at"),
        )

    def test_beat_task_invokes_the_dispatch_seam(self):
        from georiva.sources.tasks import sweep_scheduled_products

        with patch(
            "georiva.sources.derivation_invocation.dispatch_due_scheduled_products"
        ) as seam:
            sweep_scheduled_products.apply()

        seam.assert_called_once()

    def test_dispatches_an_enabled_scheduled_due_product(self):
        product = self._product()  # never run -> due

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()

        run_now.assert_called_once()
        self.assertEqual(run_now.call_args.args[0], product)

    def test_event_driven_product_is_not_fired_by_the_beat(self):
        self._product()

        with (
            patch.object(DataFeed, "get_derived_products",
                         return_value=[_definition(trigger_mode="event")]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()

        run_now.assert_not_called()

    def test_manual_product_is_not_fired_by_the_beat(self):
        self._product()

        with (
            patch.object(DataFeed, "get_derived_products",
                         return_value=[_definition(trigger_mode="manual")]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()

        run_now.assert_not_called()

    def test_disabled_scheduled_product_is_not_fired(self):
        self._product(is_enabled=False)

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()

        run_now.assert_not_called()

    def test_dispatch_stamps_last_run_at_so_it_does_not_refire(self):
        product = self._product()  # due

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()
            product.refresh_from_db()
            self.assertIsNotNone(product.last_run_at)

            # A second beat right after must not re-fire (now within interval).
            run_now.reset_mock()
            dispatch_due_scheduled_products()

        run_now.assert_not_called()

    def test_product_that_ran_within_its_interval_is_not_yet_due(self):
        # interval falls back to the feed's 60 min; ran 10 min ago -> not due.
        self._product(last_run_at=timezone.now() - timedelta(minutes=10))

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            dispatch_due_scheduled_products()

        run_now.assert_not_called()
