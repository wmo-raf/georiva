"""
Unit tests for the hard heterogeneity guard at append (design decision 7).

Each new COG's shape / chunk shape / dtype / codec pipeline must match the
repo's existing array metadata; a mismatch fails the build naming the
offending asset.
"""

from django.test import SimpleTestCase

from georiva.virtual_zarr.compat import (
    ArraySpec,
    HeterogeneousAssetError,
    assert_compatible,
)

BASE = ArraySpec(
    shape=(512, 1024),
    chunks=(256, 256),
    dtype="float32",
    codecs=('{"name":"numcodecs.deflate"}', '{"name":"virtual_tiff.float_pred"}'),
)


class AssertCompatibleTests(SimpleTestCase):
    def test_identical_specs_pass(self):
        assert_compatible(BASE, BASE, source="cog-a.tif")

    def test_shape_mismatch_names_asset(self):
        candidate = ArraySpec(
            shape=(256, 1024), chunks=BASE.chunks,
            dtype=BASE.dtype, codecs=BASE.codecs,
        )
        with self.assertRaises(HeterogeneousAssetError) as ctx:
            assert_compatible(BASE, candidate, source="cog-b.tif")
        self.assertIn("cog-b.tif", str(ctx.exception))
        self.assertIn("shape", str(ctx.exception))

    def test_chunk_mismatch_fails(self):
        candidate = ArraySpec(
            shape=BASE.shape, chunks=(512, 512),
            dtype=BASE.dtype, codecs=BASE.codecs,
        )
        with self.assertRaises(HeterogeneousAssetError) as ctx:
            assert_compatible(BASE, candidate, source="cog-c.tif")
        self.assertIn("chunks", str(ctx.exception))

    def test_dtype_mismatch_fails(self):
        candidate = ArraySpec(
            shape=BASE.shape, chunks=BASE.chunks,
            dtype="int16", codecs=BASE.codecs,
        )
        with self.assertRaises(HeterogeneousAssetError) as ctx:
            assert_compatible(BASE, candidate, source="cog-d.tif")
        self.assertIn("dtype", str(ctx.exception))

    def test_codec_pipeline_mismatch_fails(self):
        candidate = ArraySpec(
            shape=BASE.shape, chunks=BASE.chunks,
            dtype=BASE.dtype, codecs=('{"name":"numcodecs.deflate"}',),
        )
        with self.assertRaises(HeterogeneousAssetError) as ctx:
            assert_compatible(BASE, candidate, source="cog-e.tif")
        self.assertIn("codecs", str(ctx.exception))
