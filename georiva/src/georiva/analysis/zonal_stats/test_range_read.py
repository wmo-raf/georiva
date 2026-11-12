"""Zonal statistics open an asset by range rather than reading it whole (ADR 0043 §7).

The task used to pull every COG's bytes into memory before decoding it. It now
opens the asset where the location seam says it lives, local or remote, and
the numbers it persists for a local COG are the ones it always computed.
"""

import os

import numpy as np
import rasterio
from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from georiva.analysis.zonal_stats import tasks
from georiva.analysis.zonal_stats.models import BoundaryZonalStats
from georiva.analysis.zonal_stats.service import compute_stats_from_array
from georiva.core.models import Asset
from georiva.core.storage import Bucket
from georiva.core.tests.test_asset_deletion import AssetsBucketTestCase
from georiva.organisations.testing import make_org_tree, make_organisation

KEY = "kenya/c/c/v/2026/03/01/rain.tif"
NODATA = -9999.0


def _write_cog(path):
    """A 40×40 grid over (-20, -20, 20, 20) with a nodata hole, as GTiff bytes too."""
    data = np.arange(1600, dtype="float32").reshape(40, 40)
    data[0:5, 0:5] = NODATA
    profile = dict(
        driver="GTiff",
        width=40,
        height=40,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-20, 20, 1, 1),
        nodata=NODATA,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)
    with open(path, "rb") as f:
        return f.read()


def _stats_the_old_way(cog_bytes, boundaries):
    """What the task computed when it read the whole COG into memory."""
    with MemoryFile(cog_bytes) as memfile, memfile.open() as src:
        data = src.read(1).astype(np.float32)
        data[data == src.nodata] = np.nan
        return compute_stats_from_array(data, src.transform, src.crs.to_wkt(), boundaries)


class RangeReadZonalStatsTests(AssetsBucketTestCase):
    def setUp(self):
        super().setUp()
        self.boundary = AdminBoundary.objects.create(
            name_0="Testland",
            gid_0="TST",
            level=1,
            geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10)), Polygon.from_bbox((-20, 15, -15, 20))),
        )
        tree = make_org_tree(make_organisation("kenya"))
        tree["collection"].boundary_stats_levels = [1]
        tree["collection"].save()
        Asset.objects.filter(pk=tree["asset"].pk).update(format=Asset.Format.COG, href=KEY)
        self.asset = tree["asset"]
        self.cog_bytes = _write_cog(self.bucket.storage.path(KEY))

    def test_a_local_cog_gives_the_same_statistics_without_being_read_whole(self):
        from unittest import mock

        manager = mock.Mock(assets=self.bucket)
        with (
            mock.patch("georiva.core.storage.asset_location.storage", manager),
            mock.patch.object(Bucket, "read_bytes", side_effect=AssertionError("read whole")),
        ):
            tasks.compute_boundary_zonal_stats(self.asset.pk)

        expected = _stats_the_old_way(self.cog_bytes, [self.boundary])[0]
        row = BoundaryZonalStats.objects.get(boundary=self.boundary)
        for field in ("mean", "min", "max", "std", "count"):
            with self.subTest(field=field):
                self.assertAlmostEqual(getattr(row, field), expected[field], places=4)
        self.assertGreater(row.count, 0)
