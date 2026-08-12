"""
rematerialize_derived_assets — replays the shared materialization over
existing derived items (the backfill for pre-materializer history).
"""

from datetime import datetime, timezone
from io import StringIO
from unittest.mock import MagicMock, patch

import numpy as np
from django.core.management import call_command
from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import make_organisation


def _mock_writer_cls():
    writer = MagicMock()
    writer.write_cog.side_effect = lambda arr, path, *a, **k: path
    cls = MagicMock(return_value=writer)
    return cls, writer


class RematerializeDerivedAssetsTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="CHIRPS",
            slug="chirps",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            slug="precip-anomaly",
            name="Precip anomaly",
        )
        unit, _ = Unit.objects.get_or_create(
            name="Millimetre",
            defaults={"symbol": "mm"},
        )
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precip",
            name="Precipitation",
            unit=unit,
            value_min=-150,
            value_max=150,
        )
        self.item = Item.objects.create(
            collection=self.collection,
            time=datetime(2024, 5, 1, tzinfo=timezone.utc),
            bounds=[10, -5, 20, 5],
            crs="EPSG:4326",
            width=10,
            height=10,
            properties={"derivation": {"recipe": "chirps-anomaly"}},
        )
        self.cog = Asset.objects.create(
            item=self.item,
            variable=self.variable,
            format=Asset.Format.COG,
            roles=["data"],
            href="k/old.tif",
        )
        # A non-derived item in the same collection must be left alone.
        self.plain_item = Item.objects.create(
            collection=self.collection,
            time=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )

    def _run(self, *args, **kwargs):
        data = np.full((10, 10), -12.0, dtype="float32")
        writer_cls, writer = _mock_writer_cls()
        out = StringIO()
        with (
            patch(
                "georiva.processing.management.commands.rematerialize_derived_assets._read_cog",
                return_value=(data, [10, -5, 20, 5], "EPSG:4326"),
            ),
            patch("georiva.ingestion.asset_writer.AssetWriter", writer_cls),
        ):
            call_command(
                "rematerialize_derived_assets",
                *args,
                stdout=out,
                stderr=out,
                **kwargs,
            )
        return out.getvalue(), writer

    def test_backfills_assets_and_collection_extent(self):
        output, writer = self._run()

        writer.write_cog.assert_called_once()
        # No stored visual: textures are derived on demand (ADR 0021).
        self.assertFalse(self.item.assets.filter(format=Asset.Format.PNG).exists())

        self.collection.refresh_from_db()
        self.assertEqual(self.collection.bounds, [10, -5, 20, 5])
        # The observed-range report is the operator's display-range evidence.
        self.assertIn("[-12.00, -12.00]", output)
        self.assertIn("[-150.0, 150.0]", output)

        # No assets sprouted on the non-derived item.
        self.assertEqual(self.plain_item.assets.count(), 0)

    def test_dry_run_writes_nothing_but_reports(self):
        output, writer = self._run("--dry-run")

        writer.write_cog.assert_not_called()
        self.collection.refresh_from_db()
        self.assertIsNone(self.collection.bounds)
        self.assertIn("would rematerialize", output)
        self.assertIn("[-12.00, -12.00]", output)
