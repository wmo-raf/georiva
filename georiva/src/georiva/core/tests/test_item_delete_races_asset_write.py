"""An Item deleted while an Asset is being recorded for it (#704).

Django deletes an Item by first collecting its Assets, then deleting them, then
the Item. An Asset committed between the collecting and the Item's delete would
name no Item, and fails the deleter at commit. So the deleters lock the Items
before collecting, and wait out any Asset write holding the row.

Two connections are needed, hence real transactions and a thread.
"""

import threading
from datetime import UTC, datetime, timedelta
from unittest import mock

from django.db import connection, transaction
from django.test import TransactionTestCase

from georiva.core import retention
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.models import Organisation

NOW = datetime(2026, 11, 23, 12, tzinfo=UTC)


class RetentionWaitsOutAnAssetBeingRecordedTests(TransactionTestCase):
    def _fixture_teardown(self):
        # No flush: it would take the migrations' data (the Wagtail Site, the
        # central organisation) with it, and a --keepdb run on this database
        # could not migrate again. The test removes what it made instead.
        pass

    def setUp(self):
        # Cleanups run last first: the catalog goes, then the unit, then the patch.
        # Deleting an Asset dispatches its object's deletion on commit (ADR 0032).
        patcher = mock.patch("georiva.core.storage.asset_deletion.dispatch")
        patcher.start()
        self.addCleanup(patcher.stop)
        unit, made = Unit.objects.get_or_create(name="Kelvin", defaults={"symbol": "K"})
        if made:
            self.addCleanup(Unit.objects.filter(pk=unit.pk).delete)
        # The central organisation the migrations bootstrap: one made here
        # would bring a Site and a page tree to clean up.
        central = Organisation.objects.get(site__is_default_site=True)
        catalog = Catalog.objects.create(organisation=central, name="Cat", slug="race-704", file_format="grib")
        self.addCleanup(Catalog.objects.filter(pk=catalog.pk).delete)
        self.collection = Collection.objects.create(
            catalog=catalog, slug="fc", name="fc", is_forecast=True, retain_past_forecasts=False
        )
        self.variable = Variable.objects.create(
            collection=self.collection, slug="t2m", name="t2m", source_unit=unit, unit=unit
        )
        self.item = Item.objects.create(
            collection=self.collection,
            time=NOW - timedelta(hours=1),
            bounds=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=1,
            height=1,
        )

    def test_the_prune_takes_the_asset_recorded_while_it_waited(self):
        locked, release = threading.Event(), threading.Event()

        def record_an_asset():
            try:
                with transaction.atomic():
                    # What AssetMaterializer.record_cog does: lock the Item, record.
                    Item.objects.select_for_update().filter(pk=self.item.pk).exists()
                    Asset.objects.create(
                        item_id=self.item.pk, variable=self.variable, format=Asset.Format.COG, href="k.tif"
                    )
                    locked.set()
                    release.wait(10)
            finally:
                connection.close()

        errors = []

        def prune():
            try:
                retention.prune(self.collection, NOW)
            except Exception as exc:
                errors.append(exc)
            finally:
                connection.close()

        writer = threading.Thread(target=record_an_asset)
        writer.start()
        self.assertTrue(locked.wait(10))
        pruner = threading.Thread(target=prune)
        pruner.start()
        pruner.join(1)  # it blocks on the writer's lock
        release.set()
        writer.join(10)
        pruner.join(10)

        self.assertEqual(errors, [])
        self.assertFalse(Item.objects.filter(pk=self.item.pk).exists())
        self.assertFalse(Asset.objects.filter(href="k.tif").exists())
