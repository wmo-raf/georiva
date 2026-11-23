"""
AssetMaterializer tests — the shared clip → write → record → extent seam that
both ingestion and the derivation engine call. Visual textures are derived on
demand by Titiler (ADR 0021), so no PNG is written or recorded here.

Mirrors processing/tests/test_engine.py: mock the writer, assert on records.
"""

from datetime import UTC, datetime
from unittest import mock
from unittest.mock import MagicMock

import numpy as np
from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.db import connection
from django.test import TestCase
from wagtail import hooks

from georiva.analysis.zonal_stats import tasks as zonal_tasks
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.ingestion.materialization import AssetMaterializer, ItemGone
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.models import VirtualZarrManifest


def _mock_writer():
    w = MagicMock(spec=["write_cog", "bucket"])
    w.write_cog.side_effect = lambda arr, path, *a, **k: path
    return w


class _StubClipper:
    """Duck-typed BoundaryClipper: masks the left half of the grid."""

    is_active = True
    apply_mask = True

    def apply_geometry_mask(self, data, bounds, nodata=np.nan):
        out = data.copy()
        out[:, : out.shape[1] // 2] = nodata
        return out


class MaterializerFixture(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="Cat",
            slug="cat",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            slug="col",
            name="col",
        )
        self.unit, _ = Unit.objects.get_or_create(
            name="Millimetre",
            defaults={"symbol": "mm"},
        )
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precip",
            name="Precipitation",
            source_unit=self.unit,
            unit=self.unit,
            value_min=0,
            value_max=300,
        )
        self.ts = datetime(2024, 5, 1, tzinfo=UTC)
        self.item = Item.objects.create(
            collection=self.collection,
            time=self.ts,
            bounds=[10, -5, 20, 5],
            crs="EPSG:4326",
            width=10,
            height=10,
        )
        self.writer = _mock_writer()
        self.materializer = AssetMaterializer(self.writer)
        self.data = np.full((10, 10), 42.0, dtype="float32")

    def _materialize(self, **kwargs):
        defaults = dict(
            item=self.item,
            variable=self.variable,
            data=self.data,
            bounds=[10, -5, 20, 5],
            crs="EPSG:4326",
            timestamp=self.ts,
        )
        defaults.update(kwargs)
        return self.materializer.materialize_variable(**defaults)


class MaterializeVariableTests(MaterializerFixture):
    def test_writes_cog_and_records_asset(self):
        assets = self._materialize()

        self.assertEqual(len(assets), 1)
        cog = self.item.assets.get(format=Asset.Format.COG)
        self.assertEqual(cog.roles, ["data"])
        self.assertEqual(cog.width, 10)
        self.assertEqual(cog.stats_min, 42.0)

        # No stored visual: textures are derived on demand (ADR 0021).
        self.assertFalse(self.item.assets.filter(format=Asset.Format.PNG).exists())

        # The COG is the only object written — no JSON sidecar (ADR 0024).
        # The spec'd writer mock raises if any other method is touched.
        self.writer.write_cog.assert_called_once()

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

    def test_geometry_mask_applied_to_data(self):
        self._materialize(clipper=_StubClipper())

        cog = self.item.assets.get(format=Asset.Format.COG)
        # Left half masked to NaN → stats still from the surviving half.
        self.assertEqual(cog.stats_min, 42.0)
        written = self.writer.write_cog.call_args[0][0]
        self.assertTrue(np.isnan(written[:, :5]).all())
        self.assertTrue((written[:, 5:] == 42.0).all())

    def test_an_all_nodata_raster_records_no_stats_rather_than_nan(self):
        self._materialize(data=np.full((10, 10), np.nan, dtype="float32"))

        cog = self.item.assets.get(format=Asset.Format.COG)
        self.assertIsNone(cog.stats_min)
        self.assertIsNone(cog.stats_max)
        self.assertIsNone(cog.stats_mean)
        self.assertIsNone(cog.stats_std)

    def test_an_all_infinite_raster_records_no_stats_either(self):
        self._materialize(data=np.full((10, 10), np.inf, dtype="float32"))

        self.assertIsNone(self.item.assets.get(format=Asset.Format.COG).stats_mean)


class AnExplicitNodataIsKeptAsGivenTests(MaterializerFixture):
    """A caller that says what its array's nodata is — a materialization of a
    harvested file (#725) — has it kept, its dtype with it, and its every band."""

    def written(self):
        args, kwargs = self.writer.write_cog.call_args
        return args[0], kwargs["nodata"]

    def test_the_writer_is_given_the_nodata_and_dtype_as_they_are(self):
        data = np.full((10, 10), 7, dtype="int16")

        (asset,) = self._materialize(data=data, nodata=-9999)

        written, nodata = self.written()
        self.assertEqual((written.dtype, nodata), (np.int16, -9999))
        self.assertEqual(asset.extra_fields["nodata"], -9999)

    def test_no_nodata_is_none_rather_than_one_derived_from_the_dtype(self):
        self._materialize(data=np.full((10, 10), 255, dtype="uint8"), nodata=None)

        self.assertIsNone(self.written()[1])

    def test_statistics_leave_out_the_nodata_value(self):
        data = np.full((10, 10), 7, dtype="int16")
        data[0] = -9999

        (asset,) = self._materialize(data=data, nodata=-9999)

        self.assertEqual((asset.stats_min, asset.stats_max), (7, 7))

    def test_a_mask_over_integers_with_no_nodata_declares_a_fill_it_can_hold(self):
        data = np.full((10, 10), 7, dtype="uint8")

        (asset,) = self._materialize(data=data, nodata=None, clipper=_StubClipper())

        written, nodata = self.written()
        self.assertEqual(written.dtype, np.uint8)
        self.assertEqual(nodata, 255)
        self.assertTrue((written[:, :5] == 255).all())
        self.assertEqual((asset.stats_min, asset.stats_max), (7, 7))

    def test_every_band_is_written_and_counted_and_band_one_has_the_statistics(self):
        data = np.stack([np.full((10, 10), 1.0, dtype="float32"), np.full((10, 10), 9.0, dtype="float32")])

        (asset,) = self._materialize(data=data, nodata=None)

        self.assertEqual(self.written()[0].shape, (2, 10, 10))
        self.assertEqual((asset.bands, asset.width, asset.height), (2, 10, 10))
        self.assertEqual(asset.stats_max, 1.0)

    def test_a_nan_nodata_is_recorded_as_none(self):
        """NaN is not JSON; an ingested asset's NaN nodata reads None the same way."""
        (asset,) = self._materialize(nodata=np.nan)

        self.assertIsNone(asset.extra_fields["nodata"])


class ABandedArrayIsClippedBandByBandTests(MaterializerFixture):
    def test_clip_array_crops_every_band(self):
        clipper = MagicMock()
        clipper.is_active = True
        window = {"x_off": 2, "y_off": 2, "width": 4, "height": 3, "bounds": (12, -3, 16, 0)}
        clipper.compute_window.return_value = window

        data, _ = self.materializer.clip_array(np.zeros((3, 10, 10)), [10, -5, 20, 5], clipper)

        self.assertEqual(data.shape, (3, 3, 4))

    def test_the_geometry_mask_covers_every_band(self):
        from georiva.ingestion.clipper import BoundaryClipper

        inside = np.zeros((10, 10), dtype=bool)
        inside[:, 5:] = True
        clipper = BoundaryClipper(apply_mask=True)
        with (
            mock.patch.object(BoundaryClipper, "shapely_geom", new_callable=mock.PropertyMock, return_value=object()),
            mock.patch.object(clipper, "create_mask", return_value=inside),
        ):
            masked = clipper.apply_geometry_mask(np.ones((2, 10, 10), dtype="int16"), (10, -5, 20, 5), nodata=-1)

        self.assertTrue((masked[:, :, :5] == -1).all())
        self.assertTrue((masked[:, :, 5:] == 1).all())


class ClipArrayTests(MaterializerFixture):
    def test_clip_array_crops_to_window(self):
        clipper = MagicMock()
        clipper.is_active = True
        clipper.compute_window.return_value = {
            "x_off": 2,
            "y_off": 2,
            "width": 4,
            "height": 4,
            "bounds": (12, -3, 16, 1),
            "resolution": (1, 1),
        }
        data, bounds = self.materializer.clip_array(
            self.data,
            [10, -5, 20, 5],
            clipper,
        )
        self.assertEqual(data.shape, (4, 4))
        self.assertEqual(list(bounds), [12, -3, 16, 1])

    def test_clip_array_no_intersection_keeps_full_grid(self):
        clipper = MagicMock()
        clipper.is_active = True
        clipper.compute_window.side_effect = ValueError("no intersection")
        data, bounds = self.materializer.clip_array(
            self.data,
            [10, -5, 20, 5],
            clipper,
        )
        self.assertEqual(data.shape, (10, 10))
        self.assertEqual(list(bounds), [10, -5, 20, 5])


class AfterSaveAssetHookTests(MaterializerFixture):
    """Every ``GEORIVA_AFTER_SAVE_ASSET`` hook runs for a saved asset (#715)."""

    def _register(self, fn):
        self.enterContext(hooks.register_temporarily(GEORIVA_AFTER_SAVE_ASSET, fn))

    def test_every_registered_hook_runs(self):
        first, second = mock.Mock(return_value=None), mock.Mock(return_value=None)
        self._register(first)
        self._register(second)

        (asset,) = self._materialize()

        first.assert_called_once_with(asset)
        second.assert_called_once_with(asset)

    def test_a_hook_that_raises_is_logged_and_the_next_still_runs(self):
        after = mock.Mock(return_value=None)
        self._register(mock.Mock(side_effect=RuntimeError("boom")))
        self._register(after)

        with self.assertLogs("georiva.ingestion.materialization", level="WARNING") as logged:
            (asset,) = self._materialize()

        after.assert_called_once_with(asset)
        self.assertIn("boom", "\n".join(logged.output))
        self.assertTrue(Asset.objects.filter(pk=asset.pk).exists())

    def test_a_hook_whose_query_fails_inside_a_transaction_leaves_the_next_able_to_query(self):
        # A derivation materializes inside transaction.atomic(), as every test here runs.
        def broken(asset):
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM no_such_table_715")

        seen = []
        self._register(broken)
        self._register(lambda asset: seen.append(Asset.objects.filter(pk=asset.pk).exists()))

        with self.assertLogs("georiva.ingestion.materialization", level="WARNING"):
            self._materialize()

        self.assertEqual(seen, [True])
        self.assertTrue(Asset.objects.exists())

    def test_a_saved_cog_both_marks_its_manifest_stale_and_enqueues_its_zonal_statistics(self):
        AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=1, geom=MultiPolygon(Polygon.from_bbox((10, -5, 20, 5)))
        )
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])
        self.item.collection.refresh_from_db()
        manifest = VirtualZarrManifest.objects.create(variable=self.variable, status=VirtualZarrManifest.Status.READY)

        with (
            mock.patch.object(zonal_tasks.compute_boundary_zonal_stats, "delay") as delay,
            self.captureOnCommitCallbacks(execute=True),
        ):
            (asset,) = self._materialize()
            # Not before the asset's row commits: a worker would not find it.
            delay.assert_not_called()

        delay.assert_called_once_with(asset.pk)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.STALE)

    def test_no_zonal_statistics_are_enqueued_where_no_boundaries_are_loaded(self):
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])
        self.item.collection.refresh_from_db()

        with (
            mock.patch.object(zonal_tasks.compute_boundary_zonal_stats, "delay") as delay,
            self.captureOnCommitCallbacks(execute=True),
        ):
            self._materialize()

        delay.assert_not_called()


class AnItemGoneBeforeItsAssetIsRecordedTests(MaterializerFixture):
    """Retention or a sibling file may delete the Item between its creation and
    an asset's write (#704): no row is recorded and the object is removed."""

    def setUp(self):
        super().setUp()
        self.writer = MagicMock(spec=["write_cog", "bucket"])
        self.writer.write_cog.side_effect = lambda arr, path, *a, **k: path
        self.materializer = AssetMaterializer(self.writer)

    def test_no_asset_is_recorded_and_the_written_object_is_removed(self):
        Item.objects.filter(pk=self.item.pk).delete()

        with self.assertRaises(ItemGone):
            self._materialize()

        self.assertFalse(Asset.objects.filter(item_id=self.item.pk).exists())
        (href,) = self.writer.write_cog.call_args.args[1:2]
        self.writer.bucket.delete_if_version.assert_called_once_with(href, self.writer.bucket.version_of.return_value)
