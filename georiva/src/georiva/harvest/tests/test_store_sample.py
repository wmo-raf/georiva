"""
Reading one field of an Upstream store: the proof that it decodes without
credentials, and where a Variable's value range comes from (ADR 0044 §11, §12).

The repositories here are built in a temporary directory and opened from it.
Nothing reaches a network: where a read would, it is stood in for.
"""

import os
import tempfile
import threading
from unittest import mock

import icechunk
import numpy as np
import zarr
from django.test import SimpleTestCase

from georiva.harvest import upstream
from georiva.harvest.asset_hosts import ChunkPlace, chunk_place
from georiva.harvest.upstream import (
    FieldSample,
    StacApiClient,
    StoreField,
    open_repository,
    parse_asset,
    parse_cube,
    read_field_samples,
    sample_store,
)

STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
GFS = "s3://noaa-gfs-bdp-pds/"
ECMWF = "gs://ecmwf-open-data/"
FORECAST = ("run", "step", "y", "x")
#: Three runs of four steps on a 4 × 8 grid.
SHAPE = (3, 4, 4, 8)
TEMPERATURE = StoreField("temperature_2m", FORECAST)


class ChunkPlaceTests(SimpleTestCase):
    """The kinds of place a store's data may be pinned in are a closed list."""

    def test_an_s3_or_a_gs_bucket_is_a_place_and_so_is_a_folder_of_one(self):
        self.assertEqual(chunk_place(GFS), ChunkPlace("s3", "noaa-gfs-bdp-pds"))
        self.assertEqual(chunk_place(ECMWF), ChunkPlace("gs", "ecmwf-open-data"))
        self.assertEqual(chunk_place("gs://ecmwf-open-data/2026/"), ChunkPlace("gs", "ecmwf-open-data"))

    def test_any_other_kind_is_not(self):
        for declared in (
            "az://container/",
            "abfs://container@account.dfs.core.windows.net/",
            "https://data.example/grib/",
            "http://minio.internal:9000/bucket/",
            "file:///srv/grib/",
            "noaa-gfs-bdp-pds",
            "",
        ):
            with self.subTest(declared=declared):
                self.assertIsNone(chunk_place(declared))

    def test_a_prefix_ends_in_a_slash(self):
        # Without one it would match "noaa-gfs-bdp-pds-private" too, and a store's reader refuses it.
        self.assertIsNone(chunk_place("s3://noaa-gfs-bdp-pds"))
        self.assertIsNone(chunk_place("gs://ecmwf-open-data/2026"))

    def test_a_name_that_is_not_a_buckets_is_not(self):
        for declared in (
            "s3://",
            "s3://a/",
            "s3://bucket.with.dots/",
            "s3://Bucket/",
            "gs://bucket name/",
            "s3://b\\c/",
        ):
            with self.subTest(declared=declared):
                self.assertIsNone(chunk_place(declared))


class DeclaredPlacesTests(SimpleTestCase):
    def asset(self, containers):
        return parse_asset({"href": STORE, "icechunk:virtual_chunk_containers": containers})

    def test_each_declared_place_is_read_as_the_collection_spells_it(self):
        asset = self.asset(
            [
                {"url_prefix": GFS, "credentials": {"type": "s3", "anonymous": True}},
                {"url_prefix": ECMWF},
            ]
        )

        self.assertEqual(asset.chunk_locations, (GFS, ECMWF))
        self.assertEqual(asset.chunk_endpoints, ())

    def test_a_place_served_at_an_address_of_its_own_is_marked(self):
        asset = self.asset(
            [
                {"url_prefix": GFS},
                {"url_prefix": "s3://private/", "store": {"type": "s3", "endpoint_url": "http://minio.internal:9000"}},
            ]
        )

        self.assertEqual(asset.chunk_endpoints, ("s3://private/",))


class CubeDescriptionTests(SimpleTestCase):
    def test_a_fields_comment_and_its_grids_reference_system_are_read(self):
        cube = parse_cube(
            {
                "cube:dimensions": {
                    "x": {"type": "spatial", "axis": "x", "reference_system": 3857},
                    "y": {"type": "spatial", "axis": "y", "reference_system": {"type": "ProjectedCRS"}},
                    "latitude": {"type": "spatial", "axis": "y"},
                },
                "cube:variables": {
                    "temperature_2m": {
                        "dimensions": ["y", "x"],
                        "long_name": "2 metre temperature",
                        "comment": "Temperature of the air two metres above the ground.",
                    }
                },
            }
        )

        self.assertEqual(cube.dimensions["x"].reference_system, "EPSG:3857")
        self.assertEqual(cube.dimensions["y"].reference_system, "defined")
        self.assertEqual(cube.dimensions["latitude"].reference_system, "")
        self.assertEqual(
            cube.arrays["temperature_2m"].description, "Temperature of the air two metres above the ground."
        )


class StoreFixture:
    """A real repository on local disk, shaped like a forecast store."""

    def setUp(self):
        super().setUp()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.storage = icechunk.local_filesystem_storage(folder.name)
        self.config = icechunk.RepositoryConfig.default()
        self.config.caching = icechunk.CachingConfig(num_chunk_refs=1_000_000)
        self.repository = None

    def commit(self, build):
        if self.repository is None:
            self.repository = icechunk.Repository.create(self.storage, config=self.config)
            self.repository.save_config()
        session = self.repository.writable_session("main")
        build(session.store)
        session.commit("build")

    @staticmethod
    def forecast(store, name="temperature_2m", shape=SHAPE):
        return zarr.create_array(store, name=name, shape=shape, chunks=(1, 1, *shape[2:]), dtype="f8")

    def read(self, *fields, places=None):
        return read_field_samples(self.storage, list(fields) or [TEMPERATURE], places or {})


class NewestClosedRunTests(StoreFixture, SimpleTestCase):
    def test_the_field_read_is_of_the_newest_run_that_holds_every_step(self):
        def build(store):
            array = self.forecast(store)
            array[0] = 1.0  # an old run, whole
            array[1] = np.linspace(200.0, 300.0, 4 * 4 * 8).reshape(4, 4, 8)  # the newest whole run
            array[2, 0] = 9999.0  # the newest run: allocated, one step of four arrived

        self.commit(build)

        sample = self.read()["temperature_2m"]

        self.assertTrue(sample.readable, sample.problem)
        self.assertGreaterEqual(sample.value_min, 200.0)
        self.assertLessEqual(sample.value_max, 300.0)
        self.assertLess(sample.value_min, sample.value_max)

    def test_a_run_with_a_gap_in_the_middle_is_not_closed(self):
        def build(store):
            array = self.forecast(store)
            array[0] = 5.0
            for step in (0, 1, 3):  # steps land out of order: step 2 is not there yet
                array[2, step] = 9999.0
                array[1, step] = 9999.0

        self.commit(build)

        sample = self.read()["temperature_2m"]

        self.assertEqual((sample.value_min, sample.value_max), (5.0, 5.0))

    def test_a_step_no_run_holds_is_not_expected_of_any(self):
        # An average over the hours before has no first step, in any run.
        def build(store):
            array = self.forecast(store)
            for step in (1, 2, 3):
                array[1, step] = 7.0
            array[2, 1] = 9999.0  # the newest run, still arriving

        self.commit(build)

        sample = self.read()["temperature_2m"]

        self.assertTrue(sample.readable, sample.problem)
        self.assertEqual((sample.value_min, sample.value_max), (7.0, 7.0))

    def test_a_store_with_no_closed_run_in_reach_says_so(self):
        def build(store):
            array = self.forecast(store)
            array[2, 0] = 1.0
            array[1, 1] = 1.0  # between them every step is expected, and neither run has them all

        self.commit(build)

        sample = self.read()["temperature_2m"]

        self.assertFalse(sample.readable)
        self.assertIn("has arrived in full", sample.problem)

    def test_a_store_that_holds_nothing_yet_says_so(self):
        self.commit(self.forecast)

        sample = self.read()["temperature_2m"]

        self.assertFalse(sample.readable)
        self.assertIn("holds any data yet", sample.problem)

    def test_only_the_newest_few_runs_are_asked(self):
        def build(store):
            array = self.forecast(store, shape=(4, 4, 4, 8))
            array[0] = 1.0

        self.commit(build)

        with mock.patch.object(upstream, "RUNS_IN_REACH", 2):
            sample = self.read()["temperature_2m"]

        self.assertFalse(sample.readable, "the one whole run is older than the two that were asked")

    def test_an_analysis_store_is_read_at_its_newest_time_step_that_holds_data(self):
        def build(store):
            array = zarr.create_array(store, name="rain", shape=(5, 4, 8), chunks=(1, 4, 8), dtype="f8")
            array[2] = np.linspace(0.0, 50.0, 32).reshape(4, 8)
            # Steps 3 and 4 are allocated ahead of their data.

        self.commit(build)

        sample = self.read(StoreField("rain", ("time", "y", "x")))["rain"]

        self.assertTrue(sample.readable, sample.problem)
        self.assertLessEqual(sample.value_max, 50.0)

    def test_a_field_of_nothing_but_gaps_has_no_range_and_says_so(self):
        def build(store):
            self.forecast(store)[0] = np.nan

        self.commit(build)

        sample = self.read()["temperature_2m"]

        self.assertFalse(sample.readable)
        self.assertIn("holds no values", sample.problem)

    def test_a_field_that_is_not_laid_out_as_the_collection_says_is_an_answer_not_a_crash(self):
        self.commit(self.forecast)

        sample = self.read(StoreField("temperature_2m", ("time", "y", "x")))["temperature_2m"]

        self.assertFalse(sample.readable)

    def test_one_field_failing_does_not_stop_the_next(self):
        def build(store):
            self.forecast(store)[0] = 3.0

        self.commit(build)

        with self.assertLogs("georiva.harvest.upstream", level="WARNING"):
            samples = self.read(StoreField("no_such_field", FORECAST), TEMPERATURE)

        self.assertFalse(samples["no_such_field"].readable)
        self.assertTrue(samples["temperature_2m"].readable)


class OnlyConfirmedPlacesAreFollowedTests(StoreFixture, SimpleTestCase):
    """A virtual store whose own configuration says where its data lives, and
    would have a reader go there (ADR 0044 §11)."""

    def setUp(self):
        super().setUp()
        data = tempfile.TemporaryDirectory()
        self.addCleanup(data.cleanup)
        self.data_prefix = f"file://{data.name}/"
        self.raw = os.path.join(data.name, "field.bin")
        np.arange(32, dtype="f8").tofile(self.raw)
        # The repository's own word on where its data is: a folder on its own
        # machine, and a bucket at an internal address that wants credentials.
        self.config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(self.data_prefix, icechunk.local_filesystem_store(data.name))
        )
        self.config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                GFS, icechunk.s3_store(endpoint_url="http://minio.internal:9000", region="internal", anonymous=False)
            )
        )

        def build(store):
            zarr.create_array(
                store, name="temperature_2m", shape=SHAPE, chunks=(1, 1, 4, 8), dtype="f8", compressors=None
            )
            for step in range(SHAPE[1]):
                store.set_virtual_ref(f"temperature_2m/c/2/{step}/0/0", f"file://{self.raw}", offset=0, length=256)

        self.commit(build)

    def containers(self, places):
        """Where the repository, opened for `places`, will follow its data: each prefix and how it is read."""
        return open_repository(self.storage, places).config.virtual_chunk_containers

    def test_the_repositorys_own_word_would_be_followed_were_it_trusted(self):
        # What the rule guards against: authorised as it stands, the repository reads its folder.
        trusted = icechunk.Repository.open(
            self.storage,
            authorize_virtual_chunk_access={self.data_prefix: icechunk.credentials.LocalFileSystemAccess},
        )
        array = zarr.open_array(trusted.readonly_session("main").store, path="temperature_2m", mode="r")

        self.assertEqual(float(array[2, 0].sum()), float(np.arange(32).sum()))

    def test_data_somewhere_nobody_confirmed_fails_the_sample_read(self):
        with self.assertLogs("georiva.harvest.upstream", level="WARNING"):
            sample = self.read()["temperature_2m"]

        self.assertFalse(sample.readable)
        self.assertTrue(sample.elsewhere.startswith(self.data_prefix), sample.elsewhere)

    def test_confirming_another_place_does_not_open_the_one_the_data_is_in(self):
        with self.assertLogs("georiva.harvest.upstream", level="WARNING"):
            sample = self.read(places={GFS: "us-east-1", ECMWF: ""})["temperature_2m"]

        self.assertFalse(sample.readable)
        self.assertTrue(sample.elsewhere.startswith(self.data_prefix))

    def test_what_it_says_of_the_failure_is_in_an_operators_words(self):
        with self.assertLogs("georiva.harvest.upstream", level="WARNING") as logged:
            sample = self.read()["temperature_2m"]

        for jargon in ("chunk", "container", "icechunk", "zarr"):
            self.assertNotIn(jargon, sample.problem)
        self.assertIn("virtual chunk container", logged.output[0], "the library's words are kept for the log")

    def test_the_settings_a_store_is_read_with_come_from_the_confirmed_places_alone(self):
        containers = self.containers({GFS: "us-east-1", ECMWF: ""})

        self.assertEqual(containers.keys(), {GFS, ECMWF}, "the repository's own folder is not among them")
        said = str(containers[GFS].store)
        self.assertIn('region: "us-east-1"', said)
        self.assertIn("endpoint_url: None", said, "the provider's public endpoint, not the repository's")
        self.assertIn("anonymous: True", said)
        self.assertNotIn("minio.internal", said)

    def test_with_nothing_confirmed_the_repositorys_own_places_are_dropped_not_kept(self):
        self.assertEqual(self.containers({}), {})

    def test_a_place_of_a_kind_that_is_never_pinned_is_never_followed(self):
        self.assertEqual(self.containers({self.data_prefix: "", "https://data.example/": ""}), {})

    def test_what_the_repository_says_of_its_own_caching_is_kept(self):
        # Not where anything is read from, and what makes a large store quick to open.
        opened = open_repository(self.storage, {GFS: "us-east-1"})

        self.assertEqual(opened.config.caching.num_chunk_refs, 1_000_000)


class SampleStoreTests(SimpleTestCase):
    """What a sample makes of a store that will not be read. The read is stood in for."""

    def test_each_field_is_read_from_the_address_it_is_given_with_the_places_confirmed(self):
        def read(storage, fields, places, *, into):
            into["temperature_2m"] = FieldSample(readable=True, value_min=1.0, value_max=2.0)

        with (
            mock.patch.object(upstream, "read_field_samples", side_effect=read) as reading,
            mock.patch.object(icechunk, "http_storage", return_value="storage") as storage,
        ):
            samples = sample_store(STORE, [TEMPERATURE], {GFS: "us-east-1"})

        self.assertEqual(samples, {"temperature_2m": FieldSample(readable=True, value_min=1.0, value_max=2.0)})
        self.assertEqual(reading.call_args.args, ("storage", [TEMPERATURE], {GFS: "us-east-1"}))
        self.assertEqual(storage.call_args.args, (STORE,))
        self.assertIn("timeout", storage.call_args.kwargs["opts"])

    def test_a_store_that_does_not_open_fails_every_field_in_words_an_operator_may_be_shown(self):
        failure = icechunk.IcechunkError("error fetching ref: chunk container s3://x")
        with (
            mock.patch.object(upstream, "read_field_samples", side_effect=failure),
            mock.patch.object(icechunk, "http_storage"),
            self.assertLogs("georiva.harvest.upstream", level="WARNING"),
        ):
            samples = sample_store(STORE, [TEMPERATURE, StoreField("wind_u_10m", FORECAST)], {})

        self.assertEqual(samples.keys(), {"temperature_2m", "wind_u_10m"})
        for sample in samples.values():
            self.assertFalse(sample.readable)
            for jargon in ("chunk", "s3://", "Icechunk"):
                self.assertNotIn(jargon, sample.problem)

    def test_a_store_that_stops_answering_keeps_what_was_read_and_gives_up_on_the_rest(self):
        release = threading.Event()
        self.addCleanup(release.set)

        def read(storage, fields, places, *, into):
            into["temperature_2m"] = FieldSample(readable=True, value_min=1.0, value_max=2.0)
            release.wait(30)

        with (
            mock.patch.object(upstream, "read_field_samples", side_effect=read),
            mock.patch.object(icechunk, "http_storage"),
        ):
            samples = sample_store(STORE, [TEMPERATURE, StoreField("wind_u_10m", FORECAST)], {}, timeout=0.2)

        self.assertTrue(samples["temperature_2m"].readable)
        self.assertFalse(samples["wind_u_10m"].readable)
        self.assertIn("did not answer", samples["wind_u_10m"].problem)

    def test_the_real_client_samples_the_store_it_is_given(self):
        with mock.patch.object(upstream, "sample_store", return_value="samples") as sampled:
            answer = StacApiClient("https://stac.upstream.example/catalog.json").sample_store(
                STORE, [TEMPERATURE], {GFS: "us-east-1"}
            )

        self.assertEqual(answer, "samples")
        sampled.assert_called_once_with(STORE, [TEMPERATURE], {GFS: "us-east-1"})
