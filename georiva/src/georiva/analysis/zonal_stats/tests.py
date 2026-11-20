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
import struct
from datetime import UTC, datetime

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.core.management import call_command
from django.db import InternalError, connection, transaction
from django.test import TestCase

from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.core.models import Unit
from georiva.organisations.testing import (
    SHARED_TREE_SLUG as SHARED_SLUG,
)
from georiva.organisations.testing import (
    make_org_tree,
    make_organisation,
)

VALID_TIME = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)


def _whole_world_boundary():
    """One admin boundary covering the tile the tests request (z=0, x=0, y=0)."""
    return AdminBoundary.objects.create(
        name_0="Testland",
        gid_0="TST",
        level=1,
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
            item=tree["item"],
            variable=tree["variable"],
            boundary=cls.boundary,
            time=VALID_TIME,
            mean=mean,
            min=mean,
            max=mean,
            sum=mean,
            std=0,
            count=1,
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


# -- reading a tile back -----------------------------------------------------
#
# Just enough of the Mapbox Vector Tile protobuf to read feature properties: a
# tile holds layers (3); a layer holds features (2), keys (3) and values (4); a
# feature's tags (2) are packed key/value index pairs. A value is a string (1),
# float (2), double (3), int (4), uint (5) or zigzag sint (6).


def _varint(buf, pos):
    shift = result = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def _fields(buf):
    """(field number, value) for each field of one message."""
    pos = 0
    while pos < len(buf):
        key, pos = _varint(buf, pos)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _varint(buf, pos)
        elif wire == 1:
            value, pos = buf[pos : pos + 8], pos + 8
        elif wire == 2:
            length, pos = _varint(buf, pos)
            value, pos = buf[pos : pos + length], pos + length
        elif wire == 5:
            value, pos = buf[pos : pos + 4], pos + 4
        else:  # pragma: no cover - ST_AsMVT emits no other wire type
            raise ValueError(f"wire type {wire}")
        yield number, value


def _value(buf):
    for number, value in _fields(buf):
        if number == 1:
            return value.decode()
        if number == 2:
            return struct.unpack("<f", value)[0]
        if number == 3:
            return struct.unpack("<d", value)[0]
        if number == 6:
            return (value >> 1) ^ -(value & 1)
        return value
    return None


def _features(tile):
    """Each feature's properties, as a dict, across every layer of ``tile``."""
    features = []
    for number, layer in _fields(tile):
        if number != 3:
            continue
        keys, values, tagged = [], [], []
        for field, value in _fields(layer):
            if field == 3:
                keys.append(value.decode())
            elif field == 4:
                values.append(_value(value))
            elif field == 2:
                tags = next((v for f, v in _fields(value) if f == 2), b"")
                indices, pos = [], 0
                while pos < len(tags):
                    index, pos = _varint(tags, pos)
                    indices.append(index)
                tagged.append(indices)
        for indices in tagged:
            features.append({keys[k]: values[v] for k, v in zip(indices[::2], indices[1::2], strict=True)})
    return features


def _unit(symbol):
    return Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})[0]


class BoundaryStatsServedUnitTests(TestCase):
    """The tile shows statistics in the Variable's served unit (ADR 0046).

    The rows stay as produced — in the source unit — and the function converts
    them on the way out with the Variable it already joins: ``mean``, ``min``
    and ``max`` as ``x·scale + offset``, ``sum`` as ``sum·scale + offset·count``
    (every cell gains the offset), ``std`` as ``std·scale`` (a spread has no
    offset).
    """

    @classmethod
    def setUpTestData(cls):
        call_command("create_martin_function", verbosity=0)
        boundary = _whole_world_boundary()
        tree = make_org_tree(make_organisation("ghana"), name="ghana")
        cls.variable = tree["variable"]
        # The tree's asset is Kelvin: say so, past the lock (ADR 0046 §8).
        cls.variable.unit = _unit("degC")
        cls.variable.correct_source_unit(_unit("K"))
        cls.variable.save()
        cls.row = BoundaryZonalStats.objects.create(
            item=tree["item"],
            variable=cls.variable,
            boundary=boundary,
            time=VALID_TIME,
            mean=290.5,
            min=280.25,
            max=300.75,
            sum=1162.0,
            std=4.5,
            count=4,
        )

    def _served(self):
        tile = BoundaryStatsTileScopingTests._tile(
            org="ghana",
            catalog=SHARED_SLUG,
            collection=SHARED_SLUG,
            variable=SHARED_SLUG,
            admin_level=1,
            time=VALID_TIME.isoformat(),
        )
        (feature,) = _features(tile)
        return feature

    def assertServed(self, feature, expected):
        for stat, value in expected.items():
            with self.subTest(stat=stat):
                self.assertAlmostEqual(feature[stat], value, places=6)

    def test_a_kelvin_source_is_served_in_celsius(self):
        self.assertServed(
            self._served(),
            {
                "mean": 17.35,
                "min": 7.1,
                "max": 27.6,
                "sum": 69.4,  # 1162 K over 4 cells: each cell loses 273.15
                "std": 4.5,
                "count": 4,
            },
        )

    def test_changing_the_served_unit_changes_the_tile_not_the_rows(self):
        self.variable.unit = _unit("degF")
        self.variable.save()

        self.assertServed(
            self._served(),
            {
                "mean": 63.23,
                "min": 44.78,
                "max": 81.68,
                "sum": 252.92,  # four cells of 63.23 °F
                "std": 8.1,
            },
        )
        stored = BoundaryZonalStats.objects.get(pk=self.row.pk)
        self.assertEqual(
            (stored.mean, stored.min, stored.max, stored.sum, stored.std),
            (290.5, 280.25, 300.75, 1162.0, 4.5),
        )

    def test_a_variable_served_in_its_source_unit_is_unchanged(self):
        self.variable.unit = self.variable.source_unit
        self.variable.save()

        self.assertServed(
            self._served(),
            {"mean": 290.5, "min": 280.25, "max": 300.75, "sum": 1162.0, "std": 4.5, "count": 4},
        )
