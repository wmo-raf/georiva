"""
Product run-tracking aggregate (ADR-0008, issue #149).

product_status joins a DerivedProduct to its DerivationRuns by the opaque
`origin` key and summarises them — the seam the tracking view renders. The UI
knows about products; the engine still does not (it only stored the origin).
"""
from datetime import datetime, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog
from georiva.processing.models import DerivationRun
from georiva.sources.derivation_invocation import product_origin
from georiva.sources.derivation_tracking import product_status
from georiva.sources.models import DataFeed, DerivedProduct

User = get_user_model()


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

    def test_toggle_pauses_a_product_without_deleting_it(self):
        self.assertTrue(self.product.is_enabled)

        self.client.post(reverse("derived_product_tracking"), {
            "action": "toggle", "product_pk": self.product.pk,
        })

        self.product.refresh_from_db()
        self.assertFalse(self.product.is_enabled)
        # Config row survives — pausing is not deletion.
        self.assertTrue(DerivedProduct.objects.filter(pk=self.product.pk).exists())
