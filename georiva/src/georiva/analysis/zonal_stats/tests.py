"""Proof that a zonal-stats vector tile shows exactly one organisation's rows.

Martin runs the ``georiva_boundary_stats`` function against the database with no
Django in the loop and no usable Host — so whatever scoping this feature has
lives in the function's WHERE clause and nowhere else. These tests install the
real function and call it the way Martin does.

The fixture is two organisations holding statistics for the *same boundary*
under the *same catalog, collection and variable slugs*, differing only in the
statistics. That is the arrangement in which every weaker filter looks correct:
drop the org join and both organisations' numbers land in one tile, or one
organisation's land in the other's. Since the two fixtures differ only in their
values, two hosts receiving *identical* tile bytes is itself the leak — which is
what the assertions read.
"""
import json
from datetime import datetime, timezone

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.db import InternalError, connection, transaction
from django.test import TestCase

from adminboundarymanager.models import AdminBoundary

from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.organisations.testing import (
    SHARED_TREE_SLUG as SHARED_SLUG,
    make_org_tree,
    make_organisation,
)

VALID_TIME = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def _whole_world_boundary():
    """One admin boundary covering the tile the tests request (z=0, x=0, y=0)."""
    return AdminBoundary.objects.create(
        name_0="Testland", gid_0="TST", level=1,
        geom=MultiPolygon(Polygon.from_bbox((-170, -80, 170, 80))),
    )


class BoundaryStatsTileScopingTests(TestCase):
    """``georiva_boundary_stats`` as Martin calls it: SQL in, MVT bytes out."""

    @classmethod
    def setUpTestData(cls):
        call_command("create_martin_function", verbosity=0)
        cls.boundary = _whole_world_boundary()
        cls._build_stats("kenya", mean=1.0)
        cls._build_stats("uganda", mean=2.0)

    @classmethod
    def _build_stats(cls, org_slug, *, mean):
        """One organisation's statistics for the shared boundary.

        The tree comes from the shared factory, so both organisations get the
        same catalog/collection/variable slugs and differ only in their owner —
        which is what the org join has to be able to tell apart.
        """
        tree = make_org_tree(make_organisation(org_slug), name=org_slug)
        BoundaryZonalStats.objects.create(
            item=tree["item"], variable=tree["variable"], boundary=cls.boundary,
            time=VALID_TIME, mean=mean, min=mean, max=mean, sum=mean, std=0, count=1,
        )

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _tile(**params):
        """The MVT bytes Martin would return for ``params``.

        Deliberately the encoded tile rather than the rows behind it: bytes are
        what a browser receives, and an empty tile — the answer to every query
        that names nothing — is only observable here.
        """
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT georiva_boundary_stats(0, 0, 0, %s::json)",
                [json.dumps({k: str(v) for k, v in params.items()})],
            )
            return bytes(cursor.fetchone()[0])

    def _params(self, **overrides):
        params = {
            "org": "kenya",
            "catalog": SHARED_SLUG,
            "collection": SHARED_SLUG,
            "variable": SHARED_SLUG,
            "admin_level": 1,
            "time": VALID_TIME.isoformat(),
        }
        params.update(overrides)
        return params

    def test_a_complete_triple_renders_a_tile(self):
        self.assertTrue(self._tile(**self._params()))

    def test_each_organisation_gets_a_tile_of_its_own(self):
        """Same boundary, same slugs — so identical bytes would be the leak."""
        kenya = self._tile(**self._params(org="kenya"))
        uganda = self._tile(**self._params(org="uganda"))
        self.assertTrue(kenya)
        self.assertTrue(uganda)
        self.assertNotEqual(kenya, uganda)

    def test_an_unknown_organisation_yields_an_empty_tile(self):
        """Not an error: a tile must not reveal whether a neighbour exists."""
        self.assertEqual(self._tile(**self._params(org="nowhere")), b"")

    def test_a_catalog_belonging_to_another_organisation_yields_an_empty_tile(self):
        lonely = make_organisation("lonely")
        self.assertEqual(self._tile(**self._params(org=lonely.slug)), b"")

    def test_a_collection_that_does_not_exist_yields_an_empty_tile(self):
        self.assertEqual(self._tile(**self._params(collection="nope")), b"")

    def test_the_triple_is_required(self):
        """A blank parameter is an operator error, and says so rather than
        answering with an empty tile that looks like "no data here"."""
        for missing in ("org", "catalog", "collection", "variable"):
            with self.subTest(missing=missing):
                # Each failure aborts its transaction; the savepoint keeps the
                # next subtest able to query at all.
                with self.assertRaises(InternalError), transaction.atomic():
                    self._tile(**self._params(**{missing: ""}))
