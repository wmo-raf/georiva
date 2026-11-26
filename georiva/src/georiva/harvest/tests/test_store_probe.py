"""
Opening one Upstream store to see which codecs its arrays name: the read
behind the survey's verdict on a store (ADR 0044 §12).

The repositories here are built in a temporary directory and opened from it.
Nothing reaches beyond this machine: where a probe would, the read is stood in
for, and a plain Zarr store is served from this machine's own loopback.
"""

import asyncio
import functools
import json
import tempfile
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import icechunk
import zarr
from django.test import SimpleTestCase
from zarr.core.buffer import default_buffer_prototype

from georiva.harvest import upstream
from georiva.harvest.upstream import (
    StacApiClient,
    StoreArrayMetadata,
    StoreChunking,
    StoreFormat,
    can_decode,
    codec_names,
    probe_store,
    read_plain_zarr,
    read_store_arrays,
)

STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
GRID = (2, 3, 4, 8)


def codecs_of(storage):
    return {path: array.codecs for path, array in read_store_arrays(storage).items()}


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
            store, name="pressure_level/temperature", shape=GRID, chunks=(1, 1, 4, 8), shards=(2, 1, 4, 8), dtype="f4"
        )

    def test_every_array_is_found_by_its_full_path_with_the_codecs_it_names(self):
        self.commit(self.forecast)

        codecs = codecs_of(self.storage)

        self.assertEqual(codecs.keys(), {"init_time", "temperature_2m", "pressure_level/temperature"})
        self.assertEqual(codecs["temperature_2m"], ("gribberish",))
        self.assertEqual(codecs["pressure_level/temperature"][0], "sharding_indexed")
        self.assertIn("zstd", codecs["pressure_level/temperature"], "a sharded array's inner codecs count")
        self.assertTrue(all(can_decode(codec) for named in codecs.values() for codec in named))

    def test_each_arrays_layout_is_read_from_the_store_itself(self):
        def build(store):
            self.forecast(store)
            zarr.create_array(
                store,
                name="precipitation",
                shape=(2, 105, 40, 80),
                chunks=(1, 105, 10, 10),
                dtype="f4",
                dimension_names=("init_time", "lead_time", "latitude", "longitude"),
            )

        self.commit(build)

        arrays = read_store_arrays(self.storage)

        self.assertEqual(
            arrays["precipitation"].chunking,
            StoreChunking(("init_time", "lead_time", "latitude", "longitude"), (2, 105, 40, 80), (1, 105, 10, 10)),
        )
        self.assertEqual(arrays["temperature_2m"].chunking.chunks, (1, 1, 4, 8))
        self.assertEqual(arrays["temperature_2m"].chunking.dimensions, (), "a store that names no dimensions says so")
        self.assertEqual(
            arrays["pressure_level/temperature"].chunking.chunks,
            (1, 1, 4, 8),
            "a sharded array is read a piece of its shard at a time",
        )

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

        codecs = codecs_of(self.storage)

        self.assertEqual(codecs["temperature_2m"], ("acme_wavelet",))
        self.assertFalse(can_decode("acme_wavelet"))

    def test_no_data_is_read_and_no_place_a_virtual_stores_data_lives_in_is_authorised(self):
        self.commit(self.forecast)

        with mock.patch.object(icechunk.Repository, "open", wraps=icechunk.Repository.open) as opened:
            read_store_arrays(self.storage)

        (call,) = opened.call_args_list
        self.assertEqual(call.args, (self.storage,))
        self.assertEqual(call.kwargs, {}, "opened with nothing but where it is")

    def test_hundreds_of_arrays_are_read_together_and_one_odd_codec_among_them_is_named(self):
        def build(store):
            for n in range(300):
                zarr.create_array(store, name=f"field_{n:03d}", shape=(4, 8), chunks=(4, 8), dtype="f4")
            key = "field_123/zarr.json"
            prototype = default_buffer_prototype()
            metadata = json.loads(asyncio.run(store.get(key, prototype)).to_bytes())
            metadata["codecs"] = [{"name": "acme_wavelet"}]
            asyncio.run(store.set(key, prototype.buffer.from_bytes(json.dumps(metadata).encode())))

        self.commit(build)
        store = icechunk.Repository.open(self.storage).readonly_session("main").store
        in_flight, most = [0], [0]

        class Counting:
            """The store, counting how many metadata documents are asked for at once."""

            async def get(self, key, prototype):
                in_flight[0] += 1
                most[0] = max(most[0], in_flight[0])
                try:
                    await asyncio.sleep(0.001)
                    return await store.get(key, prototype)
                finally:
                    in_flight[0] -= 1

            def list_dir(self, prefix):
                return store.list_dir(prefix)

        codecs = {path: array.codecs for path, array in asyncio.run(upstream._array_metadata(Counting())).items()}

        self.assertEqual(len(codecs), 300)
        self.assertEqual([path for path, named in codecs.items() if "acme_wavelet" in named], ["field_123"])
        self.assertGreater(most[0], 1, "not one after another")
        self.assertLessEqual(most[0], upstream.STORE_CONCURRENCY)

    def test_an_empty_store_has_no_arrays(self):
        self.commit(lambda store: zarr.open_group(store, mode="a"))

        self.assertEqual(codecs_of(self.storage), {})


class ProbeStoreTests(SimpleTestCase):
    """What a probe makes of a store that will not be read. The read is stood in for."""

    def test_a_store_that_opens_is_readable_with_its_arrays_codecs(self):
        with (
            mock.patch.object(
                upstream, "read_store_arrays", return_value={"t": StoreArrayMetadata(("bytes",))}
            ) as read,
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
            mock.patch.object(upstream, "read_store_arrays", side_effect=failure),
            mock.patch.object(icechunk, "http_storage"),
            mock.patch.object(upstream, "_plain_zarr_at", return_value=None),
            self.assertLogs("georiva.harvest.upstream", level="WARNING") as logged,
        ):
            probe = probe_store(STORE)

        self.assertFalse(probe.readable)
        self.assertFalse(probe.timed_out, "a refusal is not a wait")
        self.assertEqual(probe.codecs, {})
        for jargon in ("chunk", "zarr", "codec", "Icechunk", "s3://"):
            self.assertNotIn(jargon, probe.problem)
        self.assertIn("chunk container", logged.output[0], "what the library said is kept for whoever reads the log")

    def test_a_store_that_does_not_answer_is_given_up_on(self):
        release = threading.Event()
        self.addCleanup(release.set)
        with (
            mock.patch.object(upstream, "read_store_arrays", side_effect=lambda storage: release.wait(30)),
            mock.patch.object(icechunk, "http_storage"),
        ):
            probe = probe_store(STORE, timeout=0.2)

        self.assertFalse(probe.readable)
        self.assertTrue(probe.timed_out)
        self.assertIn("did not answer", probe.problem)

    def test_the_real_client_probes_the_address_it_is_given(self):
        with mock.patch.object(upstream, "probe_store", return_value="probe") as probed:
            answer = StacApiClient("https://stac.upstream.example/catalog.json").describe_store(STORE)

        self.assertEqual(answer, "probe")
        probed.assert_called_once_with(STORE)


class PlainZarrProbeTests(SimpleTestCase):
    """A plain Zarr store, not an Icechunk repository: read from its own
    consolidated metadata, served here over this machine's loopback (ADR 0052 §1)."""

    def setUp(self):
        super().setUp()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name
        handler = functools.partial(QuietHandler, directory=self.folder)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.base = f"http://127.0.0.1:{server.server_port}"

    def write(self, name, zarr_format):
        group = zarr.open_group(f"{self.folder}/{name}", mode="w", zarr_format=zarr_format)
        dimensions = ("time", "latitude", "longitude")
        if zarr_format == 2:
            array = group.create_array("t2m", shape=(744, 721, 1440), chunks=(372, 150, 150), dtype="f4")
            array.attrs["_ARRAY_DIMENSIONS"] = list(dimensions)
        else:
            group.create_array(
                "t2m", shape=(744, 721, 1440), chunks=(1, 721, 1440), dtype="f4", dimension_names=dimensions
            )
        zarr.consolidate_metadata(f"{self.folder}/{name}", zarr_format=zarr_format)
        return f"{self.base}/{name}"

    def read(self, href):
        return read_plain_zarr(href, address="127.0.0.1", timeout=5)

    def test_a_version_2_store_is_read_for_how_it_is_divided_up(self):
        arrays = self.read(self.write("era5.zarr", 2))

        self.assertEqual(
            arrays["t2m"].chunking,
            StoreChunking(("time", "latitude", "longitude"), (744, 721, 1440), (372, 150, 150)),
        )

    def test_a_version_3_store_is_read_for_how_it_is_divided_up(self):
        arrays = self.read(self.write("arco.zarr", 3))

        self.assertEqual(arrays["t2m"].chunking.chunks, (1, 721, 1440))
        self.assertEqual(arrays["t2m"].chunking.dimensions, ("time", "latitude", "longitude"))

    def test_an_address_with_neither_is_no_plain_zarr_store(self):
        self.assertIsNone(self.read(f"{self.base}/nothing-here.zarr"))


class ProbeStoreFormatTests(SimpleTestCase):
    """Which kind of store a probe found, and where it may look for a plain Zarr one."""

    def probe(self, href, *, addresses=("93.184.216.34",), plain=None):
        resolved = [(0, 0, 0, "", (address, 443)) for address in addresses]
        with (
            mock.patch.object(upstream, "read_store_arrays", side_effect=icechunk.IcechunkError("no repository")),
            mock.patch.object(icechunk, "http_storage"),
            mock.patch.object(upstream.socket, "getaddrinfo", return_value=resolved),
            mock.patch.object(upstream, "read_plain_zarr", return_value=plain) as read,
            self.assertLogs("georiva.harvest.upstream", level="INFO"),
        ):
            return probe_store(href), read

    def test_a_store_that_is_no_repository_but_says_how_it_is_divided_up_is_plain_zarr(self):
        said = {"t2m": StoreArrayMetadata(("blosc",), StoreChunking(("time",), (744,), (372,)))}

        probe, read = self.probe(STORE, plain=said)

        self.assertTrue(probe.readable)
        self.assertEqual(probe.store_format, StoreFormat.ZARR)
        self.assertEqual(probe.chunking["t2m"].chunks, (372,))
        self.assertEqual(read.call_args.kwargs["address"], "93.184.216.34", "from the address judged, and no other")

    def test_a_store_that_is_neither_is_unreadable_for_the_reason_the_repository_gave(self):
        probe, _read = self.probe(STORE)

        self.assertFalse(probe.readable)
        self.assertEqual(probe.problem, "it did not open for a reader with no account")

    def test_a_host_that_resolves_anywhere_private_is_never_asked_for_plain_zarr(self):
        probe, read = self.probe(STORE, addresses=("93.184.216.34", "10.0.0.7"))

        self.assertFalse(probe.readable)
        read.assert_not_called()

    def test_a_plain_http_address_is_never_asked_for_plain_zarr(self):
        probe, read = self.probe(STORE.replace("https://", "http://"))

        self.assertFalse(probe.readable)
        read.assert_not_called()

    def test_an_icechunk_repository_is_called_icechunk(self):
        with (
            mock.patch.object(upstream, "read_store_arrays", return_value={}),
            mock.patch.object(icechunk, "http_storage"),
        ):
            self.assertEqual(probe_store(STORE).store_format, StoreFormat.ICECHUNK)


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass
