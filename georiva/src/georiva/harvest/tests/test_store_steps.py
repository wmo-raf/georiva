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
EVERYTHING = (None, None)

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
