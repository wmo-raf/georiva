"""
AssetWriter — what the COG it writes declares. A caller may say its array's
nodata, None included, and have it kept as given, every band of the array
written in its own dtype (#725); one that says nothing has it derived from the
dtype, as before.
"""

import io
import shutil
import tempfile
from pathlib import Path

import numpy as np
import rasterio
from django.test import SimpleTestCase, override_settings

from georiva.ingestion.asset_writer import AssetWriter


class FolderBucket:
    """A bucket that keeps each object in a folder, by key."""

    def __init__(self, root: Path):
        self.root = root

    def put(self, key: str, content: io.BufferedReader) -> tuple[str, str]:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.read())
        return key, f"receipt:{key}"


class AssetWriterTests(SimpleTestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.enterContext(override_settings(GEORIVA_TEMP_DIR=str(self.root)))
        self.writer = AssetWriter(FolderBucket(self.root))

    def written(self, data, key="out/a.tif", **kwargs):
        key, _ = self.writer.write_cog(data, key, (33.0, -5.0, 42.0, 5.0), **kwargs)
        return rasterio.open(self.root / key)

    def test_it_answers_the_receipt_of_its_write(self):
        self.assertEqual(
            self.writer.write_cog(np.ones((16, 16), dtype="float32"), "out/a.tif", (33.0, -5.0, 42.0, 5.0)),
            ("out/a.tif", "receipt:out/a.tif"),
        )

    def test_two_writes_of_the_same_array_differ_in_their_bytes_alone(self):
        # Each write is told apart by its bytes, so an S3 ETag — their MD5 —
        # names one write, and a conditional delete takes no other (#771).
        data = np.arange(16 * 16, dtype="float32").reshape(16, 16)

        with self.written(data, key="out/a.tif", nodata=-1) as a, self.written(data, key="out/b.tif", nodata=-1) as b:
            np.testing.assert_array_equal(a.read(), b.read())
            self.assertEqual(a.profile, b.profile)
        self.assertNotEqual((self.root / "out/a.tif").read_bytes(), (self.root / "out/b.tif").read_bytes())

    def test_every_band_is_written_in_its_own_dtype_with_the_nodata_given(self):
        data = np.arange(3 * 16 * 16, dtype="int16").reshape(3, 16, 16)

        with self.written(data, nodata=-9999) as cog:
            self.assertEqual((cog.count, cog.dtypes[0], cog.nodata), (3, "int16", -9999))
            np.testing.assert_array_equal(cog.read(), data)

    def test_a_nodata_of_none_is_kept_as_none(self):
        data = np.full((16, 16), 255, dtype="uint8")

        with self.written(data, nodata=None) as cog:
            self.assertIsNone(cog.nodata)
            np.testing.assert_array_equal(cog.read(1), data)

    def test_a_float64_array_stays_float64(self):
        data = np.full((16, 16), 1 + 1e-12, dtype="float64")

        with self.written(data, nodata=None) as cog:
            self.assertEqual(cog.dtypes[0], "float64")
            np.testing.assert_array_equal(cog.read(1), data)

    def test_saying_nothing_derives_the_nodata_from_the_dtype(self):
        with self.written(np.full((16, 16), 3, dtype="uint8")) as cog:
            self.assertEqual(cog.nodata, 255)
