"""
Opening one Upstream store to see which codecs its arrays name: the read
behind the survey's verdict on a store (ADR 0044 §12).

The repositories here are built in a temporary directory and opened from it.
Nothing reaches a network: where a probe would, the read is stood in for.
"""

import asyncio
import json
import tempfile
import threading
from unittest import mock

import icechunk
import zarr
from django.test import SimpleTestCase
from zarr.core.buffer import default_buffer_prototype

from georiva.harvest import upstream
from georiva.harvest.upstream import StacApiClient, can_decode, codec_names, probe_store, read_store_codecs

STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
GRID = (2, 3, 4, 8)


class CodecNameTests(SimpleTestCase):
    def test_every_codec_is_named_once_in_the_order_the_array_names_them(self):
        named = codec_names(
            [
                {"name": "scale_offset", "configuration": {"offset": -273.15, "scale": 1}},
                {"name": "gribberish", "configuration": {"var": "TMP"}},
                {"name": "scale_offset"},
            ]
        )

        self.assertEqual(named, ("scale_offset", "gribberish"))

    def test_the_codecs_inside_a_sharded_array_are_named_too(self):
        named = codec_names(
            [
                {
                    "name": "sharding_indexed",
                    "configuration": {
                        "codecs": [{"name": "bytes"}, {"name": "blosc"}],
                        "index_codecs": [{"name": "bytes"}, {"name": "crc32c"}],
                    },
                }
            ]
        )

        self.assertEqual(named, ("sharding_indexed", "bytes", "blosc", "crc32c"))

    def test_a_codec_given_as_a_bare_name_is_named(self):
        self.assertEqual(codec_names(["bytes", {"name": "zstd"}, 7, {"configuration": {}}]), ("bytes", "zstd"))

    def test_metadata_that_names_none_names_none(self):
        self.assertEqual(codec_names(None), ())


class CanDecodeTests(SimpleTestCase):
    def test_the_codecs_zarr_brings_can_be_decoded(self):
        for codec in ("bytes", "zstd", "blosc", "sharding_indexed", "scale_offset"):
            with self.subTest(codec=codec):
                self.assertTrue(can_decode(codec))

    def test_the_grib_codec_core_carries_can_be_decoded(self):
        # Nothing here imports gribberish: installed, it registers itself with zarr.
        self.assertTrue(can_decode("gribberish"))

    def test_a_codec_nobody_installed_cannot(self):
        self.assertFalse(can_decode("acme_wavelet"))
        self.assertFalse(can_decode(""))


class ReadStoreCodecsTests(SimpleTestCase):
    """Against a real repository on local disk, shaped like a virtual forecast store."""

    def setUp(self):
        super().setUp()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.storage = icechunk.local_filesystem_storage(folder.name)
        self.repository = icechunk.Repository.create(self.storage)

    def commit(self, build):
        session = self.repository.writable_session("main")
        build(session.store)
        session.commit("build")

    @staticmethod
    def forecast(store):
        from gribberish.zarr import GribberishCodec

        zarr.create_array(store, name="init_time", shape=(2,), chunks=(2,), dtype="i8")
        zarr.create_array(
            store,
            name="temperature_2m",
            shape=GRID,
            chunks=(1, 1, 4, 8),
            dtype="f8",
            serializer=GribberishCodec(var="TMP"),
            compressors=None,
        )
        zarr.open_group(store, mode="a").create_group("pressure_level")
        zarr.create_array(
            store, name="pressure_level/temperature", shape=GRID, chunks=(1, 1, 4, 8), shards=(1, 1, 4, 8), dtype="f4"
        )

    def test_every_array_is_found_by_its_full_path_with_the_codecs_it_names(self):
        self.commit(self.forecast)

        codecs = read_store_codecs(self.storage)

        self.assertEqual(codecs.keys(), {"init_time", "temperature_2m", "pressure_level/temperature"})
        self.assertEqual(codecs["temperature_2m"], ("gribberish",))
        self.assertEqual(codecs["pressure_level/temperature"][0], "sharding_indexed")
        self.assertIn("zstd", codecs["pressure_level/temperature"], "a sharded array's inner codecs count")
        self.assertTrue(all(can_decode(codec) for named in codecs.values() for codec in named))

    def test_an_array_naming_a_codec_nobody_installed_is_read_all_the_same(self):
        # Opening such an array raises; what it names is learnt without opening it.
        def build(store):
            self.forecast(store)
            key = "temperature_2m/zarr.json"
            prototype = default_buffer_prototype()
            metadata = json.loads(asyncio.run(store.get(key, prototype)).to_bytes())
            metadata["codecs"] = [{"name": "acme_wavelet", "configuration": {"level": 3}}]
            asyncio.run(store.set(key, prototype.buffer.from_bytes(json.dumps(metadata).encode())))

        self.commit(build)

        codecs = read_store_codecs(self.storage)

        self.assertEqual(codecs["temperature_2m"], ("acme_wavelet",))
        self.assertFalse(can_decode("acme_wavelet"))

    def test_no_data_is_read_and_no_place_a_virtual_stores_data_lives_in_is_authorised(self):
        self.commit(self.forecast)

        with mock.patch.object(icechunk.Repository, "open", wraps=icechunk.Repository.open) as opened:
            read_store_codecs(self.storage)

        (call,) = opened.call_args_list
        self.assertEqual(call.args, (self.storage,))
        self.assertEqual(call.kwargs, {}, "opened with nothing but where it is")

    def test_an_empty_store_has_no_arrays(self):
        self.commit(lambda store: zarr.open_group(store, mode="a"))

        self.assertEqual(read_store_codecs(self.storage), {})


class ProbeStoreTests(SimpleTestCase):
    """What a probe makes of a store that will not be read. The read is stood in for."""

    def test_a_store_that_opens_is_readable_with_its_arrays_codecs(self):
        with (
            mock.patch.object(upstream, "read_store_codecs", return_value={"t": ("bytes",)}) as read,
            mock.patch.object(icechunk, "http_storage", return_value="storage") as storage,
        ):
            probe = probe_store(STORE)

        self.assertTrue(probe.readable)
        self.assertEqual(probe.codecs, {"t": ("bytes",)})
        read.assert_called_once_with("storage")
        (call,) = storage.call_args_list
        self.assertEqual(call.args, (STORE,))
        self.assertIn("timeout", call.kwargs["opts"], "each request to the store has a limit of its own")

    def test_a_store_that_raises_is_unreadable_in_words_an_operator_may_be_shown(self):
        failure = icechunk.IcechunkError("error fetching ref: chunk container s3://x; zarr codec missing")
        with (
            mock.patch.object(upstream, "read_store_codecs", side_effect=failure),
            mock.patch.object(icechunk, "http_storage"),
            self.assertLogs("georiva.harvest.upstream", level="WARNING") as logged,
        ):
            probe = probe_store(STORE)

        self.assertFalse(probe.readable)
        self.assertEqual(probe.codecs, {})
        for jargon in ("chunk", "zarr", "codec", "Icechunk", "s3://"):
            self.assertNotIn(jargon, probe.problem)
        self.assertIn("chunk container", logged.output[0], "what the library said is kept for whoever reads the log")

    def test_a_store_that_does_not_answer_is_given_up_on(self):
        release = threading.Event()
        self.addCleanup(release.set)
        with (
            mock.patch.object(upstream, "read_store_codecs", side_effect=lambda storage: release.wait(30)),
            mock.patch.object(icechunk, "http_storage"),
        ):
            probe = probe_store(STORE, timeout=0.2)

        self.assertFalse(probe.readable)
        self.assertIn("did not answer", probe.problem)

    def test_the_real_client_probes_the_address_it_is_given(self):
        with mock.patch.object(upstream, "probe_store", return_value="probe") as probed:
            answer = StacApiClient("https://stac.upstream.example/catalog.json").describe_store(STORE)

        self.assertEqual(answer, "probe")
        probed.assert_called_once_with(STORE)
