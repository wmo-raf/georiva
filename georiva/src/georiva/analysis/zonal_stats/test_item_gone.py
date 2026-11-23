"""The zonal-stats task finishes quietly when its Item is deleted while it runs (#703).

``BoundaryZonalStats.item`` is a real foreign key since Item became a plain
table (ADR 0047). Retention or a re-ingest can delete the Item between dispatch
and write; the rows then fail at commit, and the task treats that as "the Item
is gone": nothing written, no exception, no retry.

A TestCase never commits, so the tests make the constraints immediate: the
write then fails inside the task's transaction instead of at its end, and takes
the same way out.
"""

from unittest import mock

from adminboundarymanager.models import AdminBoundary
from celery.exceptions import Retry
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.db import IntegrityError, connection
from django.test import TestCase

from georiva.analysis.zonal_stats import tasks
from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.core.models import Asset, Item
from georiva.organisations.testing import make_org_tree, make_organisation

SERVICE = "georiva.analysis.zonal_stats.service"


class ItemDeletedMidTaskTests(TestCase):
    def setUp(self):
        with connection.cursor() as cursor:
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        self.boundary = AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=1, geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10)))
        )
        tree = make_org_tree(make_organisation("kenya"))
        tree["collection"].boundary_stats_levels = [1]
        tree["collection"].save()
        Asset.objects.filter(pk=tree["asset"].pk).update(format=Asset.Format.COG)
        self.asset, self.item = tree["asset"], tree["item"]

    def run_task(self, *, delete_item):
        def read_band(location):
            if delete_item:
                Item.objects.filter(pk=self.item.pk).delete()
            return mock.Mock()

        stats = [{"boundary_id": self.boundary.pk, "mean": 1.0}]
        with (
            mock.patch(f"{SERVICE}.read_band", side_effect=read_band),
            mock.patch(f"{SERVICE}.compute_stats_from_band", return_value=stats),
            mock.patch.object(tasks.compute_boundary_zonal_stats, "retry", side_effect=Retry()) as retry,
        ):
            tasks.compute_boundary_zonal_stats(self.asset.pk)
        return retry

    def test_an_item_deleted_before_the_write_ends_the_task_without_rows_or_retry(self):
        with self.assertLogs("georiva.analysis.zonal_stats.tasks", level="INFO") as logs:
            retry = self.run_task(delete_item=True)

        self.assertFalse(retry.called)
        self.assertFalse(BoundaryZonalStats.objects.exists())
        self.assertTrue(any("is gone" in line for line in logs.output))

    def test_an_item_still_there_writes_its_rows(self):
        self.run_task(delete_item=False)
        self.assertEqual(BoundaryZonalStats.objects.filter(item=self.item).count(), 1)

    def test_an_integrity_error_with_the_item_still_there_is_raised(self):
        with mock.patch(f"{SERVICE}.persist_stats", side_effect=IntegrityError("something else")):
            with self.assertRaises(IntegrityError):
                self.run_task(delete_item=False)
