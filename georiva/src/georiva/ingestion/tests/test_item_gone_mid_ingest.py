"""An Item deleted while its file is still being ingested (#704).

Ingestion writes the Item and each variable's Asset in separate transactions,
so forecast retention or a sibling file's empty-Item cleanup can delete the
Item between them. The timestamp then ends with no Item: no Asset rows, no
objects left behind, and the variables it never reached are not failures.
"""

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
from django.core.files.storage import FileSystemStorage
from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.core.storage.manager import Bucket, BucketType
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.handlers.context import IngestionContext
from georiva.ingestion.handlers.ingestion_handler import IngestionHandler
from georiva.ingestion.handlers.item_handler import ItemHandler
from georiva.ingestion.materialization import AssetMaterializer, ItemGone
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


class IngestionFixture(ItemGoneFixture):
    """A handler over a mocked writer and extractor; ``on_extract(n)`` runs as
    the n-th variable is read."""

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
            self.on_extract(len(self.extracted))
            return np.ones((4, 4), dtype="float32")

        extractor.extract.side_effect = extract
        clipper = MagicMock(is_active=False)
        ctx = IngestionContext(
            plugin=None, clipper=clipper, writer=self.writer, extractor=extractor, origin_bucket="sources"
        )
        self.handler = IngestionHandler(ctx)

    def on_extract(self, n):
        pass

    def _process(self):
        return self.handler.process_timestamp(
            collection=self.collection,
            local_path=Path("/tmp/file.tif"),
            timestamp=self.ts,
            source_file="sources:file.tif",
        )


class AnItemRetentionPrunedBetweenVariablesTests(IngestionFixture):
    def setUp(self):
        super().setUp()
        # A forecast whose past valid times retention prunes: self.ts is long past.
        Collection.objects.filter(pk=self.collection.pk).update(is_forecast=True, retain_past_forecasts=False)
        self.collection.refresh_from_db()

    def on_extract(self, n):
        if n == 2:
            # Retention prunes the Item while the second variable is read.
            Item.objects.filter(collection=self.collection).delete()

    def test_the_timestamp_ends_with_no_item_no_assets_and_no_failures(self):
        item, assets, _, failed = self._process()

        self.assertIsNone(item)
        self.assertEqual(assets, [])
        self.assertEqual(failed, [])
        self.assertFalse(Asset.objects.filter(variable__collection=self.collection).exists())

    def test_the_object_written_for_the_gone_item_is_removed_and_no_more_are_read(self):
        self._process()

        second_href = self.writer.write_cog.call_args_list[1].args[1]
        self.writer.bucket.delete_if_version.assert_called_once_with(
            second_href, self.writer.bucket.version_of.return_value
        )
        self.assertEqual(self.extracted, ["precip", "temp"])


class AnItemASiblingFileCleanedUpTests(IngestionFixture):
    """Another file for the same timestamp failed every variable and deleted
    the Item as empty while this one read its first variable (#769). The
    timestamp is not one retention removes, so this file's data still lands."""

    def on_extract(self, n):
        if n == 1:
            ItemHandler().delete_orphan(Item.objects.get(collection=self.collection))

    def test_the_item_is_made_again_and_holds_every_variable(self):
        item, assets, _, failed = self._process()

        self.assertIsNotNone(item)
        self.assertEqual(failed, [])
        self.assertEqual(
            sorted(Asset.objects.filter(item=item).values_list("variable__slug", flat=True)),
            ["precip", "temp", "wind"],
        )
        self.assertEqual(len(assets), 3)

    def test_the_new_item_is_counted_in_its_collection(self):
        self._process()

        self.collection.refresh_from_db()
        self.assertEqual(self.collection.item_count, 1)


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


class TheObjectWrittenForAGoneItemTests(ItemGoneFixture):
    """What ``record_cog`` removes when its Item is gone: its own write, and
    not a write landed under the same key since — a re-ingest of the same
    timestamp names its COG as this one did (#769)."""

    def setUp(self):
        super().setUp()
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        self.bucket = Bucket(BucketType.ASSETS, "georiva-assets")
        self.bucket._storage = FileSystemStorage(location=root)
        self.materializer = AssetMaterializer(AssetWriter(self.bucket))
        self.item = self._item()
        self.variable = self.collection.variables.get(slug="precip")

    def _write(self):
        return self.materializer.write_cog(
            item=self.item,
            variable=self.variable,
            data=np.ones((4, 4), dtype="float32"),
            bounds=BOUNDS,
            crs="EPSG:4326",
            timestamp=self.ts,
        )

    def _record(self, written):
        with self.assertRaises(ItemGone):
            self.materializer.record_cog(item=self.item, variable=self.variable, written=written, timestamp=self.ts)

    def test_its_own_write_is_removed(self):
        written = self._write()
        Item.objects.filter(pk=self.item.pk).delete()

        self._record(written)

        self.assertFalse(self.bucket.exists(written.href))

    def test_a_write_landed_under_the_same_key_since_is_kept(self):
        written = self._write()
        Item.objects.filter(pk=self.item.pk).delete()
        self.bucket.delete(written.href)
        self.bucket.save(written.href, b"the re-ingest's COG")

        self._record(written)

        self.assertEqual(self.bucket.read_bytes(written.href), b"the re-ingest's COG")
