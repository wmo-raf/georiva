"""``sweep_missing_boundary_stats`` — the recovery path for a dropped run (#402).

The absence of ``BoundaryZonalStats`` rows for a COG asset is the signal, so
each test builds a collection with a COG asset and asks which assets the sweep
re-dispatches.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase
from django.utils import timezone

from georiva.analysis.zonal_stats import tasks
from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.core.models import Asset, Item
from georiva.organisations.testing import make_org_tree, make_organisation

DELAY = "georiva.analysis.zonal_stats.tasks.compute_boundary_zonal_stats.delay"


class SweepMissingBoundaryStatsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.boundary = AdminBoundary.objects.create(
            name_0="Testland",
            gid_0="TST",
            level=1,
            geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10))),
        )
        cls.tree = make_org_tree(make_organisation("kenya"))
        cls.collection = cls.tree["collection"]
        cls.collection.boundary_stats_levels = [1]
        cls.collection.save()
        # The tree's own item sits in March; a hypertable cannot move a row
        # across chunks, so replace it rather than re-time it.
        cls.tree["item"].delete()
        cls.item = Item.objects.create(collection=cls.collection, time=timezone.now() - timedelta(days=1))
        cls.asset = Asset.objects.create(
            item=cls.item, variable=cls.tree["variable"], href="x.tif", format=Asset.Format.COG
        )
        cls.age(cls.asset, minutes=60)

    @staticmethod
    def age(asset, *, minutes):
        Asset.objects.filter(pk=asset.pk).update(created=timezone.now() - timedelta(minutes=minutes))

    def sweep(self):
        with mock.patch(DELAY) as delay:
            tasks.sweep_missing_boundary_stats()
        return [call.args[0] for call in delay.call_args_list]

    def give_stats(self):
        BoundaryZonalStats.objects.create(
            item=self.item, variable=self.tree["variable"], boundary=self.boundary, time=self.item.time, mean=1.0
        )

    def test_an_asset_with_no_rows_is_redispatched(self):
        self.assertEqual(self.sweep(), [self.asset.pk])

    def test_an_asset_with_rows_is_left_alone(self):
        self.give_stats()
        self.assertEqual(self.sweep(), [])

    def test_a_just_saved_asset_is_given_time_for_its_own_run(self):
        self.age(self.asset, minutes=1)
        self.assertEqual(self.sweep(), [])

    def test_history_beyond_the_window_is_not_scanned(self):
        self.item.delete()
        old = Item.objects.create(collection=self.collection, time=timezone.now() - timedelta(days=30))
        old_asset = Asset.objects.create(
            item=old, variable=self.tree["variable"], href="o.tif", format=Asset.Format.COG
        )
        self.age(old_asset, minutes=60)
        self.assertEqual(self.sweep(), [])

    def test_a_collection_without_levels_is_skipped(self):
        self.collection.boundary_stats_levels = []
        self.collection.save()
        self.assertEqual(self.sweep(), [])

    def test_levels_with_no_boundaries_loaded_are_skipped(self):
        """Otherwise every pass would re-dispatch a run that can write nothing."""
        AdminBoundary.objects.all().delete()
        self.assertEqual(self.sweep(), [])

    def test_a_non_cog_asset_is_ignored(self):
        Asset.objects.filter(pk=self.asset.pk).update(format=Asset.Format.PNG)
        self.assertEqual(self.sweep(), [])

    def test_a_past_forecast_the_pruner_would_delete_is_not_recomputed(self):
        self.collection.is_forecast = True
        self.collection.retain_past_forecasts = False
        self.collection.save()
        self.assertEqual(self.sweep(), [])

    def test_a_superseded_run_the_pruner_would_delete_is_not_recomputed(self):
        self.collection.is_forecast = True
        self.collection.retain_past_forecasts = True
        self.collection.retain_latest_run_only = True
        self.collection.save()
        Item.objects.filter(pk=self.item.pk).update(reference_time=datetime(2026, 1, 1, tzinfo=UTC))
        newer = Item.objects.create(
            collection=self.collection,
            time=timezone.now() + timedelta(days=1),
            reference_time=datetime(2026, 1, 2, tzinfo=UTC),
        )
        newer_asset = Asset.objects.create(
            item=newer, variable=self.tree["variable"], href="y.tif", format=Asset.Format.COG
        )
        self.age(newer_asset, minutes=60)
        self.assertEqual(self.sweep(), [newer_asset.pk])

    def test_an_inactive_collection_is_skipped(self):
        self.collection.is_active = False
        self.collection.save()
        self.assertEqual(self.sweep(), [])

    def test_one_failed_enqueue_does_not_stop_the_pass(self):
        with mock.patch(DELAY, side_effect=RuntimeError("broker down")):
            tasks.sweep_missing_boundary_stats()  # must not raise
