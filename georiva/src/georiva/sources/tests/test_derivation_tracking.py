"""
Product run-tracking aggregate (ADR-0008, issue #149).

product_status joins a DerivedProduct to its DerivationRuns by the opaque
`origin` key and summarises them — the seam the tracking view renders. The UI
knows about products; the engine still does not (it only stored the origin).
"""
from datetime import datetime, timezone
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.processing.models import DerivationRun
from georiva.sources.derivation_invocation import product_origin
from georiva.sources.derivation_tracking import product_runs, product_status
from georiva.sources.models import DataFeed, DerivedProduct, DerivedProductInput
from georiva.staging.models import StagingAsset, StagingCollection, StagingItem

User = get_user_model()


def _anomaly_definition():
    return DerivedProductDefinition(
        key="anomaly", recipe_type="climatology", label="Rainfall anomaly",
        description="", config_schema=(),
        inputs=(InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode="scheduled",
    )


def _run(origin, status, *, completed_at=None, unit):
    from georiva.processing.recipe import unit_hash
    return DerivationRun.objects.create(
        recipe_type="climatology", recipe_version="1",
        unit_key=unit, unit_hash=unit_hash(unit),
        status=status, origin=origin, completed_at=completed_at,
    )


class ProductStatusTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def _origin(self):
        return product_origin(self.product)

    def test_product_with_no_runs_is_idle(self):
        status = product_status(self.product)

        self.assertEqual(status.status, "idle")
        self.assertEqual(status.total, 0)

    def test_a_running_unit_makes_the_product_running(self):
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        _run(self._origin(), DerivationRun.Status.RUNNING, unit={"n": 2})

        self.assertEqual(product_status(self.product).status, "running")

    def test_a_failed_unit_with_none_running_makes_the_product_failed(self):
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        _run(self._origin(), DerivationRun.Status.FAILED, unit={"n": 2})

        self.assertEqual(product_status(self.product).status, "failed")

    def test_running_takes_precedence_over_failed(self):
        _run(self._origin(), DerivationRun.Status.FAILED, unit={"n": 1})
        _run(self._origin(), DerivationRun.Status.RUNNING, unit={"n": 2})

        self.assertEqual(product_status(self.product).status, "running")

    def test_only_completed_runs_report_completed_with_last_completed_at(self):
        earlier = datetime(2026, 1, 1, tzinfo=timezone.utc)
        latest = datetime(2026, 3, 1, tzinfo=timezone.utc)
        _run(self._origin(), DerivationRun.Status.COMPLETED, completed_at=earlier, unit={"n": 1})
        _run(self._origin(), DerivationRun.Status.COMPLETED, completed_at=latest, unit={"n": 2})

        status = product_status(self.product)
        self.assertEqual(status.status, "completed")
        self.assertEqual(status.last_completed_at, latest)

    def test_counts_tally_per_status(self):
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 2})
        _run(self._origin(), DerivationRun.Status.FAILED, unit={"n": 3})

        status = product_status(self.product)
        self.assertEqual(status.total, 3)
        self.assertEqual(status.counts.get("completed"), 2)
        self.assertEqual(status.counts.get("failed"), 1)

    def test_another_products_runs_do_not_bleed_in(self):
        other = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="normals", recipe_type="climatology",
        )
        # The other product is FAILED; ours has only a COMPLETED run.
        _run(product_origin(other), DerivationRun.Status.FAILED, unit={"n": 9})
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})

        status = product_status(self.product)
        self.assertEqual(status.status, "completed")
        self.assertEqual(status.total, 1)


class TrackingViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_track", "t@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def test_tracking_page_lists_product_with_its_status(self):
        response = self.client.get(reverse("derived_product_tracking"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "anomaly")   # the product is listed
        self.assertContains(response, "idle")      # no runs yet

    def test_orphaned_product_shows_an_orphan_badge_linking_to_the_feed(self):
        # The base feed declares no products, so this row is an orphan.
        response = self.client.get(reverse("derived_product_tracking"))

        self.assertContains(response, "Orphaned")
        self.assertContains(
            response, reverse("data_feed_detail", kwargs={"pk": self.feed.pk})
        )

    def test_toggle_pauses_a_product_without_deleting_it(self):
        self.assertTrue(self.product.is_enabled)

        self.client.post(reverse("derived_product_tracking"), {
            "action": "toggle", "product_pk": self.product.pk,
        })

        self.product.refresh_from_db()
        self.assertFalse(self.product.is_enabled)
        # Config row survives — pausing is not deletion.
        self.assertTrue(DerivedProduct.objects.filter(pk=self.product.pk).exists())

    def _add_rainfall_staging(self):
        # Readiness now resolves through the product's binding rows by collection
        # identity (ADR-0010 §5), so link the staging collection to its core
        # Collection and pin the product's 'value' input to it.
        core = Collection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall"
        )
        scol = StagingCollection.objects.create(
            catalog=self.catalog, slug="rainfall", name="Rainfall", collection=core
        )
        si = StagingItem.objects.create(
            collection=scol, datetime=datetime(2020, 1, 1, tzinfo=timezone.utc),
            bounds=[0, 0, 1, 1], crs="EPSG:4326", width=4, height=4,
        )
        StagingAsset.objects.create(
            item=si, href="chirps/rainfall/f.tif", roles=["source"],
            format="geotiff", checksum="r1",
        )
        DerivedProductInput.objects.update_or_create(
            product=self.product, role="value",
            defaults={
                "tier": "staging", "required": True, "source_key": "rainfall",
                "collection": core,
            },
        )

    def test_blocked_product_shows_its_blocking_reason(self):
        # The anomaly's required 'value' input (rainfall) is empty -> blocked.
        with patch.object(DataFeed, "get_derived_products", return_value=[_anomaly_definition()]):
            response = self.client.get(reverse("derived_product_tracking"))

        self.assertContains(response, "value empty")

    def test_run_now_triggers_a_ready_product(self):
        self._add_rainfall_staging()  # required input now present -> ready

        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_anomaly_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            self.client.post(reverse("derived_product_tracking"), {
                "action": "run_now", "product_pk": self.product.pk,
            })

        run_now.assert_called_once()
        self.assertEqual(run_now.call_args.args[0], self.product)

    def test_run_now_refuses_a_blocked_product_and_shows_the_reason(self):
        # No rainfall staging -> blocked.
        with (
            patch.object(DataFeed, "get_derived_products", return_value=[_anomaly_definition()]),
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            response = self.client.post(reverse("derived_product_tracking"), {
                "action": "run_now", "product_pk": self.product.pk,
            }, follow=True)

        run_now.assert_not_called()
        self.assertContains(response, "value empty")


def _touch_modified(run, when):
    """Pin a run's `modified` (auto_now) deterministically for ordering tests."""
    DerivationRun.objects.filter(pk=run.pk).update(modified=when)


class ProductRunsTests(TestCase):
    """product_runs is the per-product run list the drill-down renders (issue
    #211): the product's DerivationRuns joined by origin, most-recent first."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def _origin(self):
        return product_origin(self.product)

    def test_returns_the_products_runs_most_recently_modified_first(self):
        older = _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        newer = _run(self._origin(), DerivationRun.Status.FAILED, unit={"n": 2})
        _touch_modified(older, datetime(2026, 1, 1, tzinfo=timezone.utc))
        _touch_modified(newer, datetime(2026, 6, 1, tzinfo=timezone.utc))

        runs = list(product_runs(self.product))

        self.assertEqual([r.pk for r in runs], [newer.pk, older.pk])

    def test_excludes_runs_belonging_to_another_product(self):
        other = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="normals", recipe_type="climatology",
        )
        mine = _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        _run(product_origin(other), DerivationRun.Status.FAILED, unit={"n": 9})

        runs = list(product_runs(self.product))

        self.assertEqual([r.pk for r in runs], [mine.pk])

    def test_status_filter_narrows_to_a_single_status(self):
        _run(self._origin(), DerivationRun.Status.COMPLETED, unit={"n": 1})
        failed = _run(self._origin(), DerivationRun.Status.FAILED, unit={"n": 2})

        runs = list(product_runs(self.product, status=DerivationRun.Status.FAILED))

        self.assertEqual([r.pk for r in runs], [failed.pk])


class RunListViewTests(TestCase):
    """The run-list drill-down page (issue #211): a thin view over product_runs,
    reached from the Derived Products dashboard."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_runs", "r@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def _url(self):
        return reverse("derived_product_runs", kwargs={"product_pk": self.product.pk})

    def _failed_run(self):
        run = _run(product_origin(self.product), DerivationRun.Status.FAILED, unit={"n": 1})
        DerivationRun.objects.filter(pk=run.pk).update(
            error="boom: the transform blew up", attempts=3,
        )
        return run

    def test_lists_a_products_runs_with_status_unit_attempts_and_error(self):
        self._failed_run()

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "failed")                     # status
        self.assertContains(response, "climatology")                # unit recipe type
        self.assertContains(response, "boom: the transform blew up")  # error snippet
        self.assertContains(response, "3")                          # attempts

    def test_status_filter_querystring_narrows_the_list(self):
        from georiva.processing.recipe import unit_hash

        completed = _run(product_origin(self.product), DerivationRun.Status.COMPLETED, unit={"n": 1})
        failed = _run(product_origin(self.product), DerivationRun.Status.FAILED, unit={"n": 2})
        completed_tag = unit_hash({"n": 1})[:8]
        failed_tag = unit_hash({"n": 2})[:8]

        # Default (no filter) shows both.
        both = self.client.get(self._url())
        self.assertContains(both, completed_tag)
        self.assertContains(both, failed_tag)

        # ?status=failed shows only the failed run.
        only_failed = self.client.get(self._url(), {"status": DerivationRun.Status.FAILED})
        self.assertContains(only_failed, failed_tag)
        self.assertNotContains(only_failed, completed_tag)

    def test_run_list_is_paginated(self):
        for i in range(26):
            _run(product_origin(self.product), DerivationRun.Status.COMPLETED, unit={"n": i})

        first = self.client.get(self._url())
        self.assertEqual(first.context["page"].paginator.num_pages, 2)
        self.assertEqual(len(first.context["rows"]), 25)

        second = self.client.get(self._url(), {"page": 2})
        self.assertEqual(len(second.context["rows"]), 1)

    def test_dashboard_row_links_to_the_run_list(self):
        response = self.client.get(reverse("derived_product_tracking"))

        self.assertContains(response, self._url())

    def test_run_list_breadcrumbs_back_to_the_dashboard(self):
        response = self.client.get(self._url())

        self.assertContains(response, reverse("derived_product_tracking"))
