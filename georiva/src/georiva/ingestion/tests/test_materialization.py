"""
AssetMaterializer tests — the shared encode → clip → write → record → extent
seam that both ingestion and the derivation engine call (issue: derived PNGs
stretched on the item map because the derived flow skipped this housekeeping).

Mirrors processing/tests/test_engine.py: mock the writer, assert on records.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.materialization import AssetMaterializer
from georiva.organisations.testing import make_organisation


def _mock_writer():
    w = MagicMock()
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    w.write_png.side_effect = lambda rgba, path, *a, **k: path
    w.write_metadata.side_effect = lambda meta, path: path
    return w


class _StubClipper:
    """Duck-typed BoundaryClipper: masks the left half of the grid."""

    is_active = True
    apply_mask = True

    def apply_geometry_mask(self, data, bounds, nodata=np.nan):
        out = data.copy()
        out[:, : out.shape[1] // 2] = nodata
        return out

    def apply_rgba_mask(self, rgba, bounds):
        out = rgba.copy()
        out[:, : out.shape[1] // 2, 3] = 0
        return out


class MaterializerFixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="Cat", slug="cat", file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog, slug="col", name="col",
        )
        self.unit, _ = Unit.objects.get_or_create(
            name="Millimetre", defaults={"symbol": "mm"},
        )
        self.variable = Variable.objects.create(
            collection=self.collection, slug="precip", name="Precipitation",
            unit=self.unit, value_min=0, value_max=300,
        )
        self.ts = datetime(2024, 5, 1, tzinfo=timezone.utc)
        self.item = Item.objects.create(
            collection=self.collection, time=self.ts,
            bounds=[10, -5, 20, 5], crs="EPSG:4326", width=10, height=10,
        )
        self.writer = _mock_writer()
        self.materializer = AssetMaterializer(self.writer)
        self.data = np.full((10, 10), 42.0, dtype="float32")

    def _materialize(self, **kwargs):
        defaults = dict(
            item=self.item, variable=self.variable, data=self.data,
            bounds=[10, -5, 20, 5], crs="EPSG:4326", timestamp=self.ts,
        )
        defaults.update(kwargs)
        return self.materializer.materialize_variable(**defaults)


class MaterializeVariableTests(MaterializerFixture):
    def test_writes_cog_png_json_and_records_assets(self):
        assets = self._materialize()

        self.assertEqual(len(assets), 2)
        cog = self.item.assets.get(format=Asset.Format.COG)
        self.assertEqual(cog.roles, ["data"])
        self.assertEqual(cog.width, 10)
        self.assertEqual(cog.stats_min, 42.0)

        png = self.item.assets.get(format=Asset.Format.PNG)
        self.assertEqual(png.roles, ["visual"])
        self.assertEqual(
            png.extra_fields["imageUnscale"], [0, 300],
        )
        self.assertEqual(png.extra_fields["scale"], "linear")

        self.writer.write_cog.assert_called_once()
        self.writer.write_png.assert_called_once()
        self.writer.write_metadata.assert_called_once()

    def test_expands_collection_extent(self):
        self.assertIsNone(self.collection.bounds)
        self._materialize()
        self.collection.refresh_from_db()
        self.assertEqual(self.collection.bounds, [10, -5, 20, 5])
        self.assertEqual(self.collection.time_start, self.ts)
        self.assertEqual(self.collection.time_end, self.ts)

    def test_normalizes_0_360_bounds(self):
        self._materialize(bounds=[190, -5, 200, 5])
        self.collection.refresh_from_db()
        self.assertEqual(self.collection.bounds, [-170, -5, -160, 5])
        # The COG is georeferenced with the normalized bounds too.
        args, kwargs = self.writer.write_cog.call_args
        written_bounds = kwargs.get("bounds") or args[2]
        self.assertEqual(list(written_bounds), [-170, -5, -160, 5])

    def test_geometry_mask_applied_to_data_and_alpha(self):
        self._materialize(clipper=_StubClipper())

        cog = self.item.assets.get(format=Asset.Format.COG)
        # Left half masked to NaN → stats still from the surviving half.
        self.assertEqual(cog.stats_min, 42.0)
        written = self.writer.write_cog.call_args[0][0]
        self.assertTrue(np.isnan(written[:, :5]).all())
        self.assertTrue((written[:, 5:] == 42.0).all())

        rgba = self.writer.write_png.call_args[0][0]
        self.assertTrue((rgba[:, :5, 3] == 0).all())
        self.assertTrue((rgba[:, 5:, 3] == 255).all())

    def test_png_failure_is_nonfatal_and_cog_survives(self):
        # An inverted range makes the encoder raise; the COG must still land.
        bad = Variable.objects.create(
            collection=self.collection, slug="broken", name="Broken",
            unit=self.unit, value_min=10, value_max=10,
        )
        assets = self._materialize(variable=bad)
        self.assertEqual([a.format for a in assets], [Asset.Format.COG])
        self.assertFalse(
            self.item.assets.filter(format=Asset.Format.PNG, variable=bad).exists()
        )

    def test_precomputed_rgba_is_used_verbatim(self):
        rgba = np.zeros((10, 10, 4), dtype=np.uint8)
        rgba[..., 3] = 255
        self._materialize(rgba=rgba)
        written = self.writer.write_png.call_args[0][0]
        self.assertTrue((written == rgba).all())


class ClipArrayTests(MaterializerFixture):
    def test_clip_array_crops_to_window(self):
        clipper = MagicMock()
        clipper.is_active = True
        clipper.compute_window.return_value = {
            "x_off": 2, "y_off": 2, "width": 4, "height": 4,
            "bounds": (12, -3, 16, 1), "resolution": (1, 1),
        }
        data, bounds = self.materializer.clip_array(
            self.data, [10, -5, 20, 5], clipper,
        )
        self.assertEqual(data.shape, (4, 4))
        self.assertEqual(list(bounds), [12, -3, 16, 1])

    def test_clip_array_no_intersection_keeps_full_grid(self):
        clipper = MagicMock()
        clipper.is_active = True
        clipper.compute_window.side_effect = ValueError("no intersection")
        data, bounds = self.materializer.clip_array(
            self.data, [10, -5, 20, 5], clipper,
        )
        self.assertEqual(data.shape, (10, 10))
        self.assertEqual(list(bounds), [10, -5, 20, 5])
