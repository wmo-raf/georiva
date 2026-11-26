"""
Which steps of an Upstream store are Held in a window (ADR 0044 §6, §7).

The repositories here are built in a temporary directory and opened from it,
with nowhere authorised to read data: a Held step is asked by key. Nothing
reaches a network.
"""

import tempfile
from datetime import UTC, datetime, timedelta
from unittest import mock

import icechunk
import numpy as np
import xarray
import zarr
from django.test import SimpleTestCase

from georiva.core.time_statistic import Declared
from georiva.harvest import upstream
from georiva.harvest.upstream import (
    HeldStep,
    StoreField,
    UpstreamUnreachable,
    list_held_steps,
    read_held_steps,
)

STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
FORECAST = ("run", "step", "y", "x")
RUN_STEP_GRID = ("init_time", "lead_time", "latitude", "longitude")
TEMPERATURE = StoreField("temperature_2m", FORECAST)
WIND = StoreField("wind_u_10m", FORECAST)
#: An average over the hours before each step: never held at a run's first step.
CLOUD = StoreField("total_cloud_cover_atmosphere", FORECAST)
EVERYTHING = (None, None)
RUN_TOTAL = Declared("sum", "run_start")

#: Three runs, six hours apart, each of four steps three hours apart, on a 4 × 8 grid.
RUNS = [datetime(2026, 3, 1, tzinfo=UTC) + timedelta(hours=6 * n) for n in range(3)]
LEADS = [timedelta(hours=3 * n) for n in range(4)]
GRID = (4, 8)
SHAPE = (len(RUNS), len(LEADS), *GRID)


def seconds(when: datetime) -> int:
    return int(when.timestamp())


class StoreFixture:
    """A real repository on local disk, laid out as xarray writes a forecast store."""

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
    def coordinates(store, runs=RUNS, leads=LEADS):
        """Allocated whole, ahead of any data, as a store's coordinates are."""
        init = zarr.create_array(
            store,
            name="init_time",
            shape=(len(runs),),
            dtype="i8",
            dimension_names=("init_time",),
            attributes={"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"},
        )
        init[:] = [seconds(run) for run in runs]
        lead = zarr.create_array(
            store,
            name="lead_time",
            shape=(len(leads),),
            dtype="i8",
            dimension_names=("lead_time",),
            attributes={"units": "seconds"},
        )
        lead[:] = [int(step.total_seconds()) for step in leads]

    @staticmethod
    def forecast(store, name="temperature_2m", shape=SHAPE, names=RUN_STEP_GRID, chunks=None):
        return zarr.create_array(
            store,
            name=name,
            shape=shape,
            chunks=chunks or (1, 1, *shape[2:]),
            dtype="f8",
            dimension_names=names,
        )

    def held(self, *fields, window=EVERYTHING):
        return read_held_steps(self.storage, list(fields) or [TEMPERATURE], window)

    def steps(self, *fields, window=EVERYTHING):
        """`(run index, step index)` of each Held step, in the order given."""
        return [
            (RUNS.index(step.reference_time), LEADS.index(step.time - step.reference_time))
            for step in self.held(*fields, window=window)
        ]


class HeldStepsOfAForecastTests(StoreFixture, SimpleTestCase):
    def test_a_step_is_listed_only_where_the_store_holds_data(self):
        def build(store):
            self.coordinates(store)
            array = self.forecast(store)
            array[0] = 1.0  # a whole run
            # The newest run: every step is in the coordinates; two have arrived, out of order.
            array[2, 0] = 1.0
            array[2, 2] = 1.0

        self.commit(build)

        self.assertEqual(self.steps(), [(0, 0), (0, 1), (0, 2), (0, 3), (2, 0), (2, 2)])

    def test_a_step_carries_its_run_and_its_valid_time(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[1, 3] = 1.0

        self.commit(build)

        self.assertEqual(
            self.held(),
            [
                HeldStep(
                    time=datetime(2026, 3, 1, 15, tzinfo=UTC),
                    reference_time=datetime(2026, 3, 1, 6, tzinfo=UTC),
                    arrays=frozenset({"temperature_2m"}),
                    run_steps=len(LEADS),
                    array_steps={"temperature_2m": frozenset(LEADS)},
                )
            ],
        )

    def test_held_is_asked_of_each_array(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0, :2] = 1.0
            self.forecast(store, "wind_u_10m")[0, 1:3] = 1.0

        self.commit(build)

        self.assertEqual(
            [sorted(step.arrays) for step in self.held(TEMPERATURE, WIND)],
            [["temperature_2m"], ["temperature_2m", "wind_u_10m"], ["wind_u_10m"]],
        )

    def test_the_window_is_on_run_time_and_a_run_inside_keeps_every_step(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[:] = 1.0

        self.commit(build)

        # From the second run on. The first run's later steps are valid inside the window, and still not its.
        self.assertEqual(self.steps(window=(RUNS[1], None)), [(run, step) for run in (1, 2) for step in range(4)])
        # Up to the first run: its last step is valid nine hours past the window's end, and is kept.
        self.assertEqual(self.steps(window=(None, RUNS[0])), [(0, step) for step in range(4)])

    def test_a_run_skipped_is_not_asked_about(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[:] = 1.0

        self.commit(build)

        held = read_held_steps(self.storage, [TEMPERATURE], EVERYTHING, skip_runs=frozenset({RUNS[0], RUNS[2]}))

        self.assertEqual({step.reference_time for step in held}, {RUNS[1]})

    def test_a_run_expects_every_lead_time_when_the_store_says_no_more(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0, 0] = 1.0

        self.commit(build)

        self.assertEqual([step.run_steps for step in self.held()], [len(LEADS)])

    def test_a_run_expects_no_step_past_the_stores_own_expected_forecast_length(self):
        def build(store):
            self.coordinates(store)
            length = zarr.create_array(
                store,
                name="expected_forecast_length",
                shape=(len(RUNS),),
                dtype="i8",
                dimension_names=("init_time",),
                attributes={"units": "seconds"},
            )
            # The first run reaches six hours ahead, the second all the way; the third is not yet said.
            length[:] = [6 * 3600, 9 * 3600, -(2**63)]
            self.forecast(store)[:, 0] = 1.0

        self.commit(build)

        expected = dict(zip(RUNS, [3, 4, 4], strict=True))
        self.assertEqual({step.reference_time: step.run_steps for step in self.held()}, expected)

    def test_a_window_holding_no_run_lists_nothing(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[:] = 1.0

        self.commit(build)

        self.assertEqual(self.held(window=(RUNS[2] + timedelta(days=1), None)), [])

    def test_nothing_is_decoded_and_no_data_is_read(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0] = 1.0

        self.commit(build)

        with mock.patch.object(zarr.Array, "__getitem__", autospec=True, side_effect=zarr.Array.__getitem__) as read:
            self.held()

        # The two coordinates are read; the field never is.
        self.assertEqual(sorted(call.args[0].path for call in read.call_args_list), ["init_time", "lead_time"])

    def test_a_store_written_by_xarray_is_read_as_it_encodes_its_times(self):
        data = np.full(SHAPE, np.nan)
        data[1, :2] = 280.0
        dataset = xarray.Dataset(
            {"temperature_2m": (RUN_STEP_GRID, data)},
            coords={
                "init_time": [np.datetime64(run.replace(tzinfo=None), "ns") for run in RUNS],
                "lead_time": [np.timedelta64(int(step.total_seconds()), "s") for step in LEADS],
                "latitude": np.linspace(10, -10, GRID[0]),
                "longitude": np.linspace(20, 50, GRID[1]),
            },
        )
        session = self.repository.writable_session("main")
        dataset.to_zarr(
            session.store,
            zarr_format=3,
            consolidated=False,
            encoding={"temperature_2m": {"chunks": (1, 1, *GRID)}},
        )
        session.commit("as xarray writes it")

        self.assertEqual(self.steps(), [(1, 0), (1, 1)])


class HeldStepsOfAnAnalysisTests(StoreFixture, SimpleTestCase):
    """A store with no runs: one time axis."""

    DAYS = [datetime(2026, 3, n, tzinfo=UTC) for n in (1, 2, 3, 4)]
    RAIN = StoreField("precipitation", ("time", "y", "x"))

    def build(self, store):
        time = zarr.create_array(
            store,
            name="time",
            shape=(4,),
            dtype="i8",
            dimension_names=("time",),
            attributes={"units": "days since 2026-03-01", "calendar": "proleptic_gregorian"},
        )
        time[:] = [0, 1, 2, 3]
        array = zarr.create_array(
            store,
            name="precipitation",
            shape=(4, *GRID),
            chunks=(1, *GRID),
            dtype="f8",
            dimension_names=("time", "latitude", "longitude"),
        )
        for day in (0, 1, 3):  # the third day is allocated and has not arrived
            array[day] = 1.0

    def test_a_held_time_has_no_run_and_the_window_is_on_valid_time(self):
        self.commit(self.build)

        held = self.held(self.RAIN, window=(self.DAYS[1], None))

        found = [(step.reference_time, step.time) for step in held]
        self.assertEqual(found, [(None, self.DAYS[1]), (None, self.DAYS[3])])


class AWebPlaceIsAskedAboutAtEveryListingTests(StoreFixture, SimpleTestCase):
    """A store whose data is kept at a web address: each listing asks for the
    newest file of each field by a reader that follows no redirect, and stops
    if the host refuses it (#828). Listing itself reads no data."""

    PLACE = "https://wn.dynamical.org/chunks/"

    def file(self, run, step):
        return f"{self.PLACE}run-{run}/step-{step}.bin"

    def setUp(self):
        super().setUp()

        def build(store):
            self.coordinates(store)
            self.forecast(store)
            for run, step in ((0, 0), (0, 1), (2, 0), (2, 1)):
                store.set_virtual_ref(
                    f"temperature_2m/c/{run}/{step}/0/0",
                    self.file(run, step),
                    offset=0,
                    length=256,
                    validate_container=False,
                )
            self.forecast(store, name="wind_u_10m")[1, 0] = 1.0  # its data is the store's own

        self.commit(build)
        self.asked = []

    def listed(self, verdict=None, fields=(TEMPERATURE,)):
        def check(address):
            self.asked.append(address)
            return verdict

        return read_held_steps(self.storage, list(fields), EVERYTHING, web=(self.PLACE,), check_chunk=check)

    def test_the_newest_file_of_each_field_kept_there_is_asked_about(self):
        held = self.listed(fields=(TEMPERATURE, WIND))

        self.assertEqual(self.asked, [self.file(2, 1)], "the wind's data is the store's own: nothing to ask")
        self.assertEqual(len(held), 5)

    def test_a_host_that_refuses_its_file_stops_the_listing_saying_why(self):
        with self.assertRaisesRegex(UpstreamUnreachable, "sends readers on to another address"):
            self.listed("it sends readers on to another address (https://elsewhere.example/)")

    def test_a_refusal_reaches_the_harvest_in_its_own_words(self):
        def refuse(address):
            return "it answered 404"

        with (
            mock.patch.object(icechunk, "http_storage", return_value=self.storage),
            mock.patch.object(upstream, "chunk_refusal", side_effect=lambda url, resolve: refuse(url)),
            self.assertRaisesRegex(UpstreamUnreachable, f"{self.PLACE}.*it answered 404"),
        ):
            list_held_steps(STORE, [TEMPERATURE], EVERYTHING, web_places=(self.PLACE,), resolve=lambda host: [])

    def test_a_web_place_only_older_runs_keep_their_data_in_is_asked_about_too(self):
        # As ECMWF's store moved buckets: the newest run elsewhere, older ones still on the web host.
        bucket = "s3://noaa-gfs-bdp-pds/"

        def move(store):
            for step in (0, 1):
                store.set_virtual_ref(
                    f"temperature_2m/c/2/{step}/0/0",
                    f"{bucket}run-2/{step}",
                    offset=0,
                    length=256,
                    validate_container=False,
                )

        self.commit(move)

        self.listed()

        self.assertEqual(self.asked, [self.file(0, 1)])

    def test_runs_a_harvest_does_not_list_again_are_still_looked_into_for_a_file(self):
        # A harvest that finds nothing new still asks: the tile server reads the settled runs.
        held = read_held_steps(
            self.storage,
            [TEMPERATURE],
            EVERYTHING,
            skip_runs=frozenset(RUNS),
            web=(self.PLACE,),
            check_chunk=lambda address: self.asked.append(address),
        )

        self.assertEqual(held, [])
        self.assertEqual(self.asked, [self.file(2, 1)])

    def test_each_web_place_is_asked_about_once(self):
        other = "https://wn2.dynamical.org/chunks/"

        # A run is kept in one place: the oldest one in the other.
        def split(store):
            for step in (0, 1):
                store.set_virtual_ref(
                    f"temperature_2m/c/0/{step}/0/0",
                    f"{other}run-0/step-{step}.bin",
                    offset=0,
                    length=256,
                    validate_container=False,
                )

        self.commit(split)
        asked = []

        read_held_steps(self.storage, [TEMPERATURE], EVERYTHING, web=(self.PLACE, other), check_chunk=asked.append)

        self.assertEqual(asked, [self.file(2, 1), f"{other}run-0/step-1.bin"])

    def test_a_web_place_no_held_step_reads_from_is_not_asked_about(self):
        unused = "https://unused.example.org/chunks/"

        read_held_steps(
            self.storage, [TEMPERATURE], EVERYTHING, web=(self.PLACE, unused), check_chunk=self.asked.append
        )

        self.assertEqual(self.asked, [self.file(2, 1)])

    def test_a_web_place_is_never_listed_with_no_way_to_judge_its_host(self):
        with self.assertRaises(ValueError):
            list_held_steps(STORE, [TEMPERATURE], EVERYTHING, web_places=(self.PLACE,))

    def test_with_no_web_place_pinned_nothing_is_asked(self):
        read_held_steps(self.storage, [TEMPERATURE], EVERYTHING)

        self.assertEqual(self.asked, [])

    def test_the_real_client_asks_where_its_host_resolves(self):
        client = upstream.StacApiClient("https://stac.upstream.example/catalog.json")
        with mock.patch.object(upstream, "list_held_steps") as listed:
            client.list_held_steps(STORE, [TEMPERATURE], EVERYTHING, web_places=(self.PLACE,))

        self.assertEqual(listed.call_args.kwargs["web_places"], (self.PLACE,))
        self.assertEqual(listed.call_args.kwargs["resolve"], client.resolve_host)


class AStoreThatCannotBeListedTests(StoreFixture, SimpleTestCase):
    def listed(self, *fields):
        """As the real client lists it, with the local repository standing in for the one at its address."""
        with mock.patch.object(icechunk, "http_storage", return_value=self.storage):
            return list_held_steps(STORE, list(fields) or [TEMPERATURE], EVERYTHING)

    def test_a_store_that_opens_is_listed(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0, 0] = 1.0

        self.commit(build)

        self.assertEqual(len(self.listed()), 1)

    def test_a_plain_http_store_is_not_opened_unless_its_feed_allows_plain_http(self):
        self.commit(lambda store: (self.coordinates(store), self.forecast(store).__setitem__((0, 0), 1.0)))
        plain = STORE.replace("https://", "http://")

        with (
            mock.patch.object(icechunk, "http_storage", return_value=self.storage) as opened,
            self.assertLogs(upstream.logger, "WARNING"),
            self.assertRaises(UpstreamUnreachable),
        ):
            list_held_steps(plain, [TEMPERATURE], EVERYTHING)
        opened.assert_not_called()

        with mock.patch.object(icechunk, "http_storage", return_value=self.storage) as opened:
            self.assertEqual(len(list_held_steps(plain, [TEMPERATURE], EVERYTHING, allow_http=True)), 1)
        self.assertEqual(opened.call_args.kwargs["opts"]["allow_http"], "true")

    def test_a_store_that_does_not_open_has_not_answered(self):
        with (
            mock.patch.object(icechunk.Repository, "open", side_effect=RuntimeError("connection reset by peer")),
            self.assertLogs(upstream.logger, "WARNING"),
            self.assertRaisesRegex(UpstreamUnreachable, "could not be opened and listed"),
        ):
            self.listed()

    def test_a_store_that_stops_answering_costs_the_wait_and_no_more(self):
        import threading

        release = threading.Event()
        self.addCleanup(release.set)

        with (
            mock.patch.object(upstream, "read_held_steps", side_effect=lambda *a, **k: release.wait(30)),
            mock.patch.object(icechunk, "http_storage", return_value=self.storage),
            self.assertRaisesRegex(UpstreamUnreachable, "did not answer within 0 seconds"),
        ):
            list_held_steps(STORE, [TEMPERATURE], EVERYTHING, timeout=0.2)

    def test_a_mapped_array_the_store_does_not_have_fails_the_listing_whole(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0] = 1.0

        self.commit(build)

        # A part of the answer would read, to the Mirror, as the rest having been deleted.
        with self.assertLogs(upstream.logger, "WARNING"), self.assertRaises(UpstreamUnreachable):
            self.listed(TEMPERATURE, WIND)

    def test_an_array_not_laid_out_as_its_collection_says_is_said_so(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)

        self.commit(build)

        with self.assertRaisesRegex(UpstreamUnreachable, "not laid out as its collection says"):
            self.listed(StoreField("temperature_2m", ("time", "y", "x")))

    def test_a_store_that_no_longer_keeps_one_map_per_step_is_said_so(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store, chunks=(1, 4, *GRID))[0] = 1.0

        self.commit(build)

        # Asked by chunk, its steps would read as not held, and the Mirror would remove their rows.
        with self.assertRaisesRegex(UpstreamUnreachable, "one map per time step"):
            self.listed()

    def test_a_store_with_no_coordinate_for_its_runs_is_said_so(self):
        self.commit(lambda store: self.forecast(store))

        with self.assertRaisesRegex(UpstreamUnreachable, "init_time"):
            self.listed()


class AnArraysExpectedStepsTests(StoreFixture, SimpleTestCase):
    """Which steps of a run each array is published at (#672, ADR 0051).

    Every array of a run is published at the run's expected lead times — the
    steps themselves, not how many, so a step held past the expected length
    cannot stand in for a missing one. Whether a Variable mapped from it has a
    lead-0 step is the Variable's declaration to say, not the steps the store
    happens to hold.
    """

    def expected(self, *fields):
        """`(run index, array) → lead times it expects`, over every Held step."""
        found = {}
        for step in self.held(*fields):
            for path, leads in step.array_steps.items():
                found[(RUNS.index(step.reference_time), path)] = leads
        return found

    @staticmethod
    def expected_forecast_length(store, hours):
        length = zarr.create_array(
            store,
            name="expected_forecast_length",
            shape=(len(RUNS),),
            dtype="i8",
            dimension_names=("init_time",),
            attributes={"units": "seconds"},
        )
        length[:] = [hours * 3600] * len(RUNS)

    def test_an_array_held_from_a_runs_first_step_expects_every_step(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0] = 1.0

        self.commit(build)

        self.assertEqual(self.expected(), {(0, "temperature_2m"): frozenset(LEADS)})

    def test_an_array_not_held_at_a_runs_first_step_is_still_published_at_every_step(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0] = 1.0
            self.forecast(store, CLOUD.path)[0, 1:] = 1.0

        self.commit(build)

        self.assertEqual(
            self.expected(TEMPERATURE, CLOUD),
            {(0, "temperature_2m"): frozenset(LEADS), (0, CLOUD.path): frozenset(LEADS)},
        )

    def test_a_gap_after_an_arrays_first_step_is_still_expected(self):
        def build(store):
            self.coordinates(store)
            array = self.forecast(store)
            array[0, 0] = 1.0
            array[0, 2] = 1.0

        self.commit(build)

        self.assertEqual(self.expected(), {(0, "temperature_2m"): frozenset(LEADS)})

    def test_the_stores_expected_forecast_length_still_bounds_an_array(self):
        def build(store):
            self.coordinates(store)
            self.expected_forecast_length(store, 6)  # leads 0h, 3h, 6h
            self.forecast(store, CLOUD.path)[0, 1:] = 1.0

        self.commit(build)

        self.assertEqual(self.expected(CLOUD), {(0, CLOUD.path): frozenset(LEADS[:3])})

    def test_a_step_held_past_the_expected_length_is_listed_and_not_expected(self):
        def build(store):
            self.coordinates(store)
            self.expected_forecast_length(store, 6)  # leads 0h, 3h, 6h
            array = self.forecast(store)
            array[0, 0] = 1.0
            array[0, 2] = 1.0
            array[0, 3] = 1.0  # 9h: past the expected length

        self.commit(build)

        self.assertEqual(self.steps(), [(0, 0), (0, 2), (0, 3)])
        self.assertEqual(self.expected(), {(0, "temperature_2m"): frozenset(LEADS[:3])})

    def test_each_run_is_judged_on_its_own(self):
        def build(store):
            self.coordinates(store)
            array = self.forecast(store)
            array[0] = 1.0
            array[1, 2:] = 1.0

        self.commit(build)

        self.assertEqual(
            self.expected(),
            {(0, "temperature_2m"): frozenset(LEADS), (1, "temperature_2m"): frozenset(LEADS)},
        )


class AnArraysDeclaredTimeStatisticTests(StoreFixture, SimpleTestCase):
    """What a store array declares of its time statistic travels with its Held
    steps, to be compared with its Variable's (ADR 0051)."""

    def declared(self, *fields):
        found = {}
        for step in self.held(*fields):
            found.update(step.declared)
        return found

    def test_a_running_total_is_declared_as_one(self):
        def build(store):
            self.coordinates(store)
            array = self.forecast(store, CLOUD.path)
            array.attrs["step_type"] = "avg"
            array[0] = 1.0
            total = self.forecast(store, "precipitation_run_total_surface")
            total.attrs["step_type"] = "accum"
            total[0] = 1.0

        self.commit(build)

        self.assertEqual(
            self.declared(CLOUD, StoreField("precipitation_run_total_surface", FORECAST)),
            {CLOUD.path: Declared("mean", "preceding_interval"), "precipitation_run_total_surface": RUN_TOTAL},
        )

    def test_an_array_that_says_nothing_declares_nothing(self):
        def build(store):
            self.coordinates(store)
            self.forecast(store)[0] = 1.0

        self.commit(build)

        self.assertEqual(self.declared(), {})
