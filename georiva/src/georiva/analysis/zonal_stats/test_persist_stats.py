"""``persist_stats`` upserts one (item, variable)'s rows, and stamps when it did (#717)."""

from datetime import timedelta

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase

from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.analysis.zonal_stats.service import persist_stats
from georiva.organisations.testing import make_org_tree, make_organisation


class PersistStatsTests(TestCase):
    """``persist_stats`` upserts: a recompute rewrites a row, and says when (#717)."""

    def setUp(self):
        self.boundary = AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=1, geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10)))
        )
        tree = make_org_tree(make_organisation("kenya"))
        self.item, self.variable = tree["item"], tree["variable"]

    def persist(self, mean, **kwargs):
        persist_stats(self.item, self.variable, [{"boundary_id": self.boundary.pk, "mean": mean}], **kwargs)
        return BoundaryZonalStats.objects.get(boundary=self.boundary)

    def test_a_recompute_over_an_existing_row_advances_its_modified(self):
        for overwrite in (False, True):
            with self.subTest(overwrite=overwrite):
                first = self.persist(1.0)
                BoundaryZonalStats.objects.filter(pk=first.pk).update(modified=first.modified - timedelta(days=1))
                before = BoundaryZonalStats.objects.get(pk=first.pk).modified

                again = self.persist(2.0, overwrite=overwrite)

                self.assertEqual((again.pk, again.mean), (first.pk, 2.0))
                self.assertGreater(again.modified, before)
                BoundaryZonalStats.objects.all().delete()
