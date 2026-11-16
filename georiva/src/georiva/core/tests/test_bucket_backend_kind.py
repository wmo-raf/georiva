"""Which kind of backend a Bucket stands on, asked of the real backend classes.

django-storages' S3Storage has a ``location`` too (its key prefix), so a bucket on S3
must not be taken for a local directory: ``open_raster`` would ask S3 for an absolute
path and every zonal-stats or timeseries read of our own COGs would fail.
"""

import tempfile
from unittest.mock import patch

from django.core.files.storage import FileSystemStorage
from django.test import SimpleTestCase
from storages.backends.s3 import S3Storage

from georiva.core.storage.manager import Bucket, BucketType


def _bucket_on(backend) -> Bucket:
    bucket = Bucket(BucketType.ASSETS, "assets")
    bucket._storage = backend
    return bucket


class BucketBackendKindTests(SimpleTestCase):
    def test_a_bucket_on_s3_is_not_local(self):
        bucket = _bucket_on(S3Storage(bucket_name="georiva-assets"))

        self.assertTrue(bucket.is_s3)
        self.assertFalse(bucket.is_local)

    def test_a_bucket_on_a_directory_is_local(self):
        bucket = _bucket_on(FileSystemStorage(location=tempfile.gettempdir()))

        self.assertTrue(bucket.is_local)
        self.assertFalse(bucket.is_s3)

    def test_a_raster_on_s3_is_opened_by_bucket_and_key_not_by_path(self):
        bucket = _bucket_on(S3Storage(bucket_name="georiva-assets"))

        with (
            patch("rasterio.open") as rasterio_open,
            patch("georiva.core.storage.manager.storage.get_zarr_fs") as get_fs,
        ):
            bucket.open_raster("org/cat/col/var/2026/11/16/a.tif")

        rasterio_open.assert_called_once_with(
            "georiva-assets/org/cat/col/var/2026/11/16/a.tif", opener=get_fs.return_value
        )
