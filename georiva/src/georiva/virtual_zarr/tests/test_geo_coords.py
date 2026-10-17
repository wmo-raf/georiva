"""``_assign_geo_coords`` reads the grid from the virtual dataset, not over GDAL (#406).

The virtual-TIFF parser already carries the GeoTIFF georeferencing tags on the
variable, so the builder needs no second, GDAL-mediated read of a sample COG —
the read whose vsicurl negative cache could poison a worker process.
"""

import os
import tempfile

import numpy as np
import rasterio
import xarray as xr
from django.test import SimpleTestCase
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import LocalStore
from rasterio.transform import from_origin
from virtual_tiff import VirtualTIFF
from virtualizarr import open_virtual_dataset

from georiva.virtual_zarr.virtual_zarr import VirtualZarrBuilder


def _write_geotiff(path, transform, *, height=8, width=12, point=False):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    ) as dst:
        dst.write(np.zeros((height, width), dtype="float32"), 1)
        if point:
            dst.update_tags(AREA_OR_POINT="Point")


def _virtual_stack(path, times=2):
    """What ``_build_virtual_dataset`` hands over: per-COG datasets concatenated on time."""
    registry = ObjectStoreRegistry({"file://": LocalStore(prefix="/")})
    vds = open_virtual_dataset(url=f"file://{path}", registry=registry, parser=VirtualTIFF())
    return xr.concat([vds.expand_dims(time=[i]) for i in range(times)], dim="time")


class AssignGeoCoordsTests(SimpleTestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "t.tif")

    def assign(self, ds):
        return VirtualZarrBuilder._assign_geo_coords(ds)

    def test_coords_match_what_rasterio_reads(self):
        transform = from_origin(30.0, 10.0, 0.25, 0.5)
        _write_geotiff(self.path, transform)

        out = self.assign(_virtual_stack(self.path))

        with rasterio.open(self.path) as src:
            lons = src.transform.c + src.transform.a * (np.arange(src.width) + 0.5)
            lats = src.transform.f + src.transform.e * (np.arange(src.height) + 0.5)
        np.testing.assert_allclose(out["lon"].values, lons)
        np.testing.assert_allclose(out["lat"].values, lats)
        self.assertEqual(out["lat"].dims, ("lat",))
        self.assertEqual(out["lon"].dims, ("lon",))

    def test_a_pixel_is_point_grid_gets_the_same_shift_gdal_applies(self):
        _write_geotiff(self.path, from_origin(30.0, 10.0, 0.25, 0.5), point=True)

        out = self.assign(_virtual_stack(self.path))

        with rasterio.open(self.path) as src:
            lons = src.transform.c + src.transform.a * (np.arange(src.width) + 0.5)
            lats = src.transform.f + src.transform.e * (np.arange(src.height) + 0.5)
        np.testing.assert_allclose(out["lon"].values, lons)
        np.testing.assert_allclose(out["lat"].values, lats)

    def test_more_than_one_tiepoint_fails_loudly(self):
        _write_geotiff(self.path, from_origin(30.0, 10.0, 0.25, 0.25))
        ds = _virtual_stack(self.path)
        ds["0"].attrs["model_tiepoint"] = list(ds["0"].attrs["model_tiepoint"]) * 2
        with self.assertRaisesRegex(ValueError, "tiepoint"):
            self.assign(ds)

    def test_a_dataset_without_georeferencing_fails_loudly(self):
        ds = xr.Dataset({"0": (("time", "y", "x"), np.zeros((1, 2, 2)))})
        with self.assertRaisesRegex(ValueError, "georeferenc"):
            self.assign(ds)

    def test_a_rotated_grid_fails_loudly(self):
        """A model_transformation tag describes shear/rotation; lat/lon vectors would be wrong."""
        _write_geotiff(self.path, from_origin(30.0, 10.0, 0.25, 0.25))
        ds = _virtual_stack(self.path)
        ds["0"].attrs["model_transformation"] = [1.0] * 16
        with self.assertRaisesRegex(ValueError, "rotat|transformation"):
            self.assign(ds)
