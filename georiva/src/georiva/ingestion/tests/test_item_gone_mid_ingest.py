"""An Item deleted while its file is still being ingested (#704).

Ingestion writes the Item and each variable's Asset in separate transactions,
so forecast retention or a sibling file's empty-Item cleanup can delete the
Item between them. The timestamp then ends with no Item: no Asset rows, no
objects left behind, and the variables it never reached are not failures.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.handlers.context import IngestionContext
from georiva.ingestion.handlers.ingestion_handler import IngestionHandler
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.organisations.testing import make_organisation

BOUNDS = (10.0, -5.0, 20.0, 5.0)


class ItemGoneFixture(TestCase):
    def setUp(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Cat", slug="cat", file_format="geotiff"
        )
        self.collection = Collection.objects.create(catalog=catalog, slug="col", name="col")
        unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        for slug in ("precip", "temp", "wind"):
            Variable.objects.create(collection=self.collection, slug=slug, name=slug, source_unit=unit, unit=unit)
        self.ts = datetime(2024, 5, 1, tzinfo=UTC)

    def _item(self, **kwargs):
        return Item.objects.create(
            collection=self.collection,
            time=self.ts,
            bounds=list(BOUNDS),
            crs="EPSG:4326",
            width=4,
            height=4,
            **kwargs,
        )


class AnItemDeletedBetweenVariablesTests(ItemGoneFixture):
    def setUp(self):
        super().setUp()
        self.writer = MagicMock(spec=["write_cog", "bucket"])
        self.writer.write_cog.side_effect = lambda arr, path, *a, **k: path
        self.writer.bucket.size.return_value = 10

        extractor = MagicMock()
        extractor.get_metadata.return_value = {"width": 4, "height": 4, "bounds": BOUNDS, "crs": "EPSG:4326"}
        self.extracted = []

        def extract(variable, *args, **kwargs):
            self.extracted.append(variable.slug)
            if len(self.extracted) == 2:
                # Retention prunes the Item while the second variable is read.
                Item.objects.filter(collection=self.collection).delete()
            return np.ones((4, 4), dtype="float32")

        extractor.extract.side_effect = extract
        clipper = MagicMock(is_active=False)
        ctx = IngestionContext(
            plugin=None, clipper=clipper, writer=self.writer, extractor=extractor, origin_bucket="sources"
        )
        self.handler = IngestionHandler(ctx)

    def _process(self):
        return self.handler.process_timestamp(
            collection=self.collection,
            local_path=Path("/tmp/file.tif"),
            timestamp=self.ts,
            source_file="sources:file.tif",
        )

    def test_the_timestamp_ends_with_no_item_no_assets_and_no_failures(self):
        item, assets, _, failed = self._process()

        self.assertIsNone(item)
        self.assertEqual(assets, [])
        self.assertEqual(failed, [])
        self.assertFalse(Asset.objects.filter(variable__collection=self.collection).exists())

    def test_the_object_written_for_the_gone_item_is_removed_and_no_more_are_read(self):
        self._process()

        second_href = self.writer.write_cog.call_args_list[1].args[1]
        self.writer.bucket.delete.assert_called_once_with(second_href)
        self.assertEqual(self.extracted, ["precip", "temp"])


class TheEmptyItemCleanupTests(ItemGoneFixture):
    def test_an_item_a_sibling_file_has_given_an_asset_is_kept(self):
        item = self._item()
        Asset.objects.create(
            item=item,
            variable=self.collection.variables.get(slug="precip"),
            format=Asset.Format.COG,
            href="org/cat/col/precip/a.tif",
        )

        ItemHandler().delete_orphan(item)

        self.assertTrue(Item.objects.filter(pk=item.pk).exists())

    def test_an_item_with_no_asset_is_deleted(self):
        item = self._item()

        ItemHandler().delete_orphan(item)

        self.assertFalse(Item.objects.filter(pk=item.pk).exists())
