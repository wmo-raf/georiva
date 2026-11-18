"""
Unit tests for choosing which COGs a Variable's virtual Zarr holds (#732).

A repo holds one array signature — spatial shape, chunk shape, dtype, codec
pipeline, one band. The newest single-band asset's signature is the one held;
every asset of another signature, and every multi-band asset, is left out with
a reason rather than failing the build.
"""

from django.test import SimpleTestCase

from georiva.virtual_zarr.compat import (
    ArraySpec,
    LeftOut,
    appends_cleanly,
    left_out_from_json,
    left_out_to_json,
    select,
)

FLOAT = ArraySpec(
    shape=(512, 1024),
    chunks=(256, 256),
    dtype="float32",
    codecs=('{"name":"numcodecs.deflate"}', '{"name":"virtual_tiff.float_pred"}'),
)
INT = ArraySpec(
    shape=FLOAT.shape,
    chunks=FLOAT.chunks,
    dtype="int16",
    codecs=('{"name":"numcodecs.deflate"}',),
)
THREE_BANDS = ArraySpec(shape=FLOAT.shape, chunks=FLOAT.chunks, dtype="uint8", codecs=(), bands=3)


class SelectTests(SimpleTestCase):
    def test_one_signature_keeps_everything(self):
        selection = select([FLOAT, FLOAT, FLOAT])

        self.assertEqual(selection.signature, FLOAT)
        self.assertEqual(selection.kept, (0, 1, 2))
        self.assertEqual(selection.left_out, ())

    def test_the_newest_signature_wins(self):
        # Copies made under #640 were float32; the newest copy is the upstream's int16.
        selection = select([FLOAT, FLOAT, INT])

        self.assertEqual(selection.signature, INT)
        self.assertEqual(selection.kept, (2,))
        (left_out,) = selection.left_out
        self.assertEqual(left_out.count, 2)
        self.assertIn("dtype float32", left_out.reason)
        self.assertIn("int16", left_out.reason)

    def test_a_reason_names_every_field_that_differs(self):
        smaller = ArraySpec(shape=(256, 512), chunks=(128, 128), dtype=FLOAT.dtype, codecs=FLOAT.codecs)

        (left_out,) = select([smaller, FLOAT]).left_out

        self.assertIn("shape 256×512", left_out.reason)
        self.assertIn("chunks 128×128", left_out.reason)
        self.assertNotIn("dtype", left_out.reason)

    def test_a_multi_band_asset_is_left_out_and_never_sets_the_signature(self):
        selection = select([FLOAT, THREE_BANDS])

        self.assertEqual(selection.signature, FLOAT)
        self.assertEqual(selection.kept, (0,))
        (left_out,) = selection.left_out
        self.assertEqual(left_out.count, 1)
        self.assertIn("3 bands", left_out.reason)

    def test_only_multi_band_assets_leave_nothing_to_hold(self):
        selection = select([THREE_BANDS, THREE_BANDS])

        self.assertIsNone(selection.signature)
        self.assertEqual(selection.kept, ())
        self.assertEqual(selection.left_out, (LeftOut(selection.left_out[0].reason, 2),))

    def test_left_out_reasons_are_grouped_most_first(self):
        selection = select([THREE_BANDS, FLOAT, FLOAT, INT])

        self.assertEqual([entry.count for entry in selection.left_out], [2, 1])


class AppendsCleanlyTests(SimpleTestCase):
    def test_new_assets_of_the_committed_signature_append(self):
        self.assertTrue(appends_cleanly(FLOAT, [FLOAT, FLOAT]))

    def test_a_new_signature_does_not_append(self):
        self.assertFalse(appends_cleanly(FLOAT, [FLOAT, INT]))

    def test_a_multi_band_asset_does_not_append(self):
        self.assertFalse(appends_cleanly(FLOAT, [THREE_BANDS]))


class LeftOutRecordTests(SimpleTestCase):
    def test_round_trips_through_json(self):
        entries = (LeftOut("dtype float32, not int16", 2), LeftOut("3 bands", 1))

        self.assertEqual(left_out_from_json(left_out_to_json(entries)), entries)

    def test_reads_nothing_from_a_commit_that_predates_it(self):
        self.assertEqual(left_out_from_json(None), ())
