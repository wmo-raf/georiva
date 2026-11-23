"""Item is a plain table with a primary key, and the foreign keys to it are real (#703, ADR 0047).

Item was a TimescaleDB hypertable. Its reads name a collection, not a time
window, so the hypertable bought nothing — while planning every Item query
across hundreds of chunks cost close to a second, and a hypertable's unique keys
must include ``time``, which left Item without a primary key and every foreign
key to it unconstrained. Orphans accumulated behind those.

These tests pin the new shape, so re-adding ``TimescaleModel`` to Item fails CI.
"""

from datetime import UTC, datetime

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.db import IntegrityError, connection, transaction
from django.test import TestCase

from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.core.models import Asset, Item
from georiva.organisations.testing import make_org_tree, make_organisation
from georiva.processing.models import DerivationRun
from georiva.staging.models import DerivationLink, StagingAsset, StagingCollection, StagingItem

#: Every foreign key this change constrains: (model, field) — five to Item, one to StagingItem.
CONSTRAINED = [
    (Asset, "item"),
    (DerivationLink, "derived_item"),
    (DerivationLink, "source_published_item"),
    (DerivationRun, "produced_item"),
    (BoundaryZonalStats, "item"),
    (StagingAsset, "item"),
]


class ItemIsAPlainTableTests(TestCase):
    def test_only_zonal_stats_is_a_hypertable(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT hypertable_name FROM timescaledb_information.hypertables")
            hypertables = {row[0] for row in cursor.fetchall()}
        self.assertEqual(hypertables, {BoundaryZonalStats._meta.db_table})

    def test_item_has_a_primary_key_on_id(self):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.attname
                FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
                WHERE c.conrelid = %s::regclass AND c.contype = 'p'
                """,
                [Item._meta.db_table],
            )
            self.assertEqual([row[0] for row in cursor.fetchall()], ["id"])


class ConstraintsRejectAMissingParentTests(TestCase):
    """Each constrained foreign key rejects a row naming a parent that does not exist.

    Django's foreign keys are ``DEFERRABLE INITIALLY DEFERRED``: Postgres checks
    them when the transaction commits, which a TestCase never does. So the
    tests read that deferral from the catalog, and make the check itself
    immediate to see it reject — together, a rejection at commit.
    """

    def setUp(self):
        tree = make_org_tree(make_organisation("kenya"))
        item, variable = tree["item"], tree["variable"]
        source = Item.objects.create(collection=tree["collection"], time=datetime(2026, 3, 2, tzinfo=UTC))
        boundary = AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=1, geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10)))
        )
        staging_collection = StagingCollection.objects.create(catalog=tree["catalog"], slug="raw", name="Raw")
        link = DerivationLink.objects.create(
            derived_item=item, source_published_item=source, recipe_id="r", recipe_version="1", input_hash="h"
        )
        self.rows = {
            Asset: tree["asset"],
            DerivationLink: link,
            DerivationRun: DerivationRun.objects.create(
                recipe_type="r", recipe_version="1", unit_hash="u", produced_item=item
            ),
            BoundaryZonalStats: BoundaryZonalStats.objects.create(
                time=item.time, item=item, variable=variable, boundary=boundary, mean=1.0
            ),
            StagingAsset: StagingAsset.objects.create(
                item=StagingItem.objects.create(collection=staging_collection), href="raw.tif", roles=["source"]
            ),
        }

    def test_each_constraint_is_checked_at_commit(self):
        for model, field_name in CONSTRAINED:
            field = model._meta.get_field(field_name)
            with self.subTest(f"{model.__name__}.{field_name}"), connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT c.condeferrable, c.condeferred
                    FROM pg_constraint c
                    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
                    WHERE c.conrelid = %s::regclass AND c.contype = 'f'
                      AND c.confrelid = %s::regclass AND a.attname = %s
                    """,
                    [model._meta.db_table, field.related_model._meta.db_table, field.column],
                )
                self.assertEqual(cursor.fetchall(), [(True, True)])

    def test_each_constraint_rejects_a_missing_parent(self):
        with connection.cursor() as cursor:
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        for model, field_name in CONSTRAINED:
            column = model._meta.get_field(field_name).column
            row = self.rows[model]
            with self.subTest(f"{model.__name__}.{field_name}"):
                with self.assertRaises(IntegrityError), transaction.atomic():
                    model.objects.filter(pk=row.pk).update(**{column: 10**12})
