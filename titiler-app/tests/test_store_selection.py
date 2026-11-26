"""A store array with extra axes is read at one value of each (wmo-raf/georiva#830).

A map-shaped store can hold more than one map per step: one per ensemble member,
per statistic, per pressure level. A **Selection** names one value of each such
axis, and Django publishes it with the store entry beside ``array``. The reader
finds each value among the store's own coordinates, by value, and reads the map
at that index: a value the store does not have is a misdescribed store, never a
blank tile; a member missing at a step is that step not held.

The store here keeps its own chunks, served over local HTTP by the same
stand-in the store tests use.
"""

import json
import os
from datetime import UTC, datetime

import icechunk
import numpy
import pytest
import zarr
from app import store as store_reader
from app import warm

from tests import test_store_tiles
from tests.test_store_tiles import FIELD, LATITUDES, LEADS_HOURS, LONGITUDES, RUNS, point, publish, tile

producer = test_store_tiles.producer
rendering = test_store_tiles.rendering

STATISTICS = ["mean", "p10", "p90"]
MEMBERS = [0, 1, 2]
LEVELS = [850.0, 500.0]
GRID = (5, 8)


def _constant(value: float) -> numpy.ndarray:
    return numpy.full(GRID, value)


def _times(root) -> None:
    init_time = zarr.create_array(root, name="init_time", shape=(2,), dtype="i8", dimension_names=["init_time"])
    init_time[:] = [int(numpy.datetime64(run.rstrip("Z"), "s").astype("int64")) for run in RUNS]
    init_time.attrs.update({"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"})
    lead_time = zarr.create_array(root, name="lead_time", shape=(3,), dtype="i8", dimension_names=["lead_time"])
    lead_time[:] = [hours * 3600 for hours in LEADS_HOURS]
    lead_time.attrs.update({"units": "seconds"})
    zarr.create_array(root, name="latitude", shape=(5,), dtype="f8", dimension_names=["latitude"])[:] = LATITUDES
    zarr.create_array(root, name="longitude", shape=(8,), dtype="f8", dimension_names=["longitude"])[:] = LONGITUDES


@pytest.fixture
def axes_store(tmp_path, producer):
    """A forecast store whose arrays have extra axes, held at the newest run's second step:

    - ``temperature_2m`` by ``statistic`` (strings): ``mean`` holds 10, ``p90`` 90; ``p10`` is not held.
    - ``temperature_ens`` by ``ensemble_member`` (whole numbers) and ``pressure_level`` (hPa): member
      ``m`` at level ``l`` holds ``100 m + l``.
    - ``wind_u_10m`` / ``wind_v_10m`` by ``ensemble_member``: member ``m`` holds u = 3 m, v = 4 m.
    - ``lumped``: ``statistic`` in one chunk, not one map per value.
    - ``plain``: no extra axis, holding 7.
    """
    _handler, base = producer
    folder = os.path.join(tmp_path, "axes", "v0.1.0.icechunk")
    repository = icechunk.Repository.create(icechunk.local_filesystem_storage(folder))
    session = repository.writable_session("main")
    root = session.store
    _times(root)
    statistic = zarr.create_array(root, name="statistic", shape=(3,), dtype=str, dimension_names=["statistic"])
    statistic[:] = numpy.array(STATISTICS)
    zarr.create_array(root, name="ensemble_member", shape=(3,), dtype="i8", dimension_names=["ensemble_member"])[:] = (
        MEMBERS
    )
    zarr.create_array(root, name="pressure_level", shape=(2,), dtype="f8", dimension_names=["pressure_level"])[:] = (
        LEVELS
    )

    def array(name, shape, chunks, dimensions):
        return zarr.create_array(
            root,
            name=name,
            shape=shape,
            chunks=chunks,
            dtype="f8",
            fill_value=numpy.nan,
            dimension_names=dimensions,
        )

    by_statistic = array(
        "temperature_2m",
        (2, 3, 3, 5, 8),
        (1, 1, 1, 5, 8),
        ["init_time", "statistic", "lead_time", "latitude", "longitude"],
    )
    by_statistic[1, 0, 1] = _constant(10.0)
    by_statistic[1, 2, 1] = _constant(90.0)

    by_member_and_level = array(
        "temperature_ens",
        (2, 3, 3, 5, 8, 2),
        (1, 1, 1, 5, 8, 1),
        ["init_time", "lead_time", "ensemble_member", "latitude", "longitude", "pressure_level"],
    )
    for member in MEMBERS:
        for level_index, level in enumerate(LEVELS):
            by_member_and_level[1, 1, member, :, :, level_index] = _constant(100.0 * member + level)

    for name, factor in (("wind_u_10m", 3.0), ("wind_v_10m", 4.0)):
        wind = array(
            name,
            (2, 3, 3, 5, 8),
            (1, 1, 1, 5, 8),
            ["init_time", "lead_time", "ensemble_member", "latitude", "longitude"],
        )
        for member in MEMBERS:
            wind[1, 1, member] = _constant(factor * member)

    lumped = array(
        "lumped", (2, 3, 3, 5, 8), (1, 3, 1, 5, 8), ["init_time", "statistic", "lead_time", "latitude", "longitude"]
    )
    lumped[1, :, 1] = numpy.stack([_constant(1.0)] * 3)

    plain = array("plain", (2, 3, 5, 8), (1, 1, 5, 8), ["init_time", "lead_time", "latitude", "longitude"])
    plain[1, 1] = _constant(7.0)
    session.commit("build")

    class Built:
        href = f"{base}/axes/v0.1.0.icechunk"

    return Built()


def entry(href, array="temperature_2m", selection=None, **extra):
    """What Django publishes for one timestep of a Store asset read at ``selection``."""
    published = {"store": href, "array": array, "chunk_locations": {}, "allow_http": True, **extra}
    if selection is not None:
        published["selection"] = selection
    return json.dumps(published)


def vector_entry(href, selection, transform="vector_magnitude"):
    return json.dumps(
        {
            "store": href,
            "vector": {"u": "wind_u_10m", "v": "wind_v_10m", "transform": transform},
            "selection": selection,
            "chunk_locations": {},
            "allow_http": True,
        }
    )


def value_at(client):
    response = point(client, 0.5, 0.5)
    assert response.status_code == 200, response.text
    return response.json()["values"][0]


class TestAFieldIsReadAtItsSelection:
    def test_a_statistic_is_read_at_the_one_named_not_another(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p90"})})

        assert value_at(client) == 90.0

    def test_the_same_array_at_another_statistic_is_another_field(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p90"})})
        assert value_at(client) == 90.0

        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "mean"})})
        assert value_at(client) == 10.0

    def test_an_ensemble_member_and_a_level_are_read_together(self, client, rendering, axes_store):
        selection = {"ensemble_member": 2, "pressure_level": 500}
        publish(rendering, {FIELD: entry(axes_store.href, "temperature_ens", selection)})

        # 500 matches the level 500.0: a number matches a numeric coordinate by value.
        assert value_at(client) == 700.0

    def test_a_tile_is_drawn_at_the_selection(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "mean"})})

        response = tile(client)

        assert response.status_code == 200, response.text

    def test_a_vector_reads_u_and_v_at_the_same_selection(self, client, rendering, axes_store):
        publish(rendering, {FIELD: vector_entry(axes_store.href, {"ensemble_member": 1})})
        assert value_at(client) == pytest.approx(5.0)

        publish(rendering, {FIELD: vector_entry(axes_store.href, {"ensemble_member": 2})})
        assert value_at(client) == pytest.approx(10.0)

    def test_an_entry_without_a_selection_reads_as_before(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, "plain")})
        assert value_at(client) == 7.0

        publish(rendering, {FIELD: entry(axes_store.href, "plain", selection={})})
        assert value_at(client) == 7.0


class TestASelectionTheStoreCannotAnswerIsMisdescribed:
    def test_a_value_the_store_does_not_have_is_an_error_naming_it(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p99"})})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 502
        detail = response.json()["detail"]
        assert "statistic" in detail and "p99" in detail
        assert "time step" not in detail

    def test_a_value_of_the_wrong_kind_does_not_match(self, client, rendering, axes_store):
        selection = {"ensemble_member": "0", "pressure_level": 850}
        publish(rendering, {FIELD: entry(axes_store.href, "temperature_ens", selection)})

        assert point(client, 0.5, 0.5).status_code == 502

    def test_a_value_not_found_is_an_error_tile_not_a_blank_one(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p99"})})

        response = tile(client)

        assert response.status_code == 502
        assert response.headers["content-type"] == "application/json"

    def test_a_dimension_the_array_does_not_have_is_an_error_naming_it(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, "plain", {"ensemble_member": 0})})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 502
        assert "ensemble_member" in response.json()["detail"]

    def test_an_extra_axis_not_selected_is_still_refused(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, "temperature_ens", {"ensemble_member": 0})})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 502
        assert "pressure_level" in response.json()["detail"]

    def test_an_axis_not_kept_one_map_per_value_is_refused(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, "lumped", {"statistic": "mean"})})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 502
        assert "statistic" in response.json()["detail"]


class TestAMemberMissingAtAStepIsThatStepNotHeld:
    def test_a_selected_value_with_no_map_at_the_step_is_not_held(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p10"})})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 404
        assert "time step" in response.json()["detail"]


class TestTheSelectionIsPartOfTheEntry:
    @pytest.mark.parametrize(
        "selection",
        [
            [["statistic", "mean"]],
            "statistic=mean",
            {"statistic": True},
            {"statistic": None},
            {"statistic": ["mean"]},
            {"statistic": {"value": "mean"}},
            {"": "mean"},
        ],
    )
    def test_a_selection_that_is_not_names_to_one_value_each_is_a_malformed_entry(
        self, client, rendering, axes_store, selection
    ):
        published = json.loads(entry(axes_store.href, selection={}))
        published["selection"] = selection
        publish(rendering, {FIELD: json.dumps(published)})

        assert point(client, 0.5, 0.5).status_code == 502
        with pytest.raises(store_reader.StoreMisdescribed):
            store_reader.published_store(published)

    def test_a_slice_carries_its_selection_sorted_and_names_it(self, axes_store):
        target = store_reader.StoreSlice.published(
            json.loads(entry(axes_store.href, "temperature_ens", {"pressure_level": 850, "ensemble_member": 0})),
            datetime(2026, 3, 23, 6, tzinfo=UTC),
            datetime(2026, 3, 23, tzinfo=UTC),
        )

        assert target.selection == (("ensemble_member", 0), ("pressure_level", 850))
        assert "ensemble_member=0" in str(target) and "pressure_level=850" in str(target)
        assert hash(target)

    def test_two_selections_of_one_array_are_kept_as_two_fields(self, client, rendering, axes_store):
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "p90"})})
        value_at(client)
        publish(rendering, {FIELD: entry(axes_store.href, selection={"statistic": "mean"})})
        value_at(client)

        kept = {target.selection for target in store_reader._fields._kept}
        assert kept == {(("statistic", "p90"),), (("statistic", "mean"),)}

    def test_a_vectors_components_carry_its_selection(self, axes_store):
        target = store_reader.StoreSlice.published(
            json.loads(vector_entry(axes_store.href, {"ensemble_member": 1})),
            datetime(2026, 3, 23, 6, tzinfo=UTC),
            datetime(2026, 3, 23, tzinfo=UTC),
        )

        assert [part.selection for part in target.components()] == [(("ensemble_member", 1),)] * 2

    def test_the_warm_up_opens_an_array_with_extra_axes(self, rendering, axes_store):
        selection = {"ensemble_member": 0, "pressure_level": 850}
        publish(rendering, {FIELD: entry(axes_store.href, "temperature_ens", selection)})

        warm.warm_stores()

        opened = store_reader.opened_store(axes_store.href, (), True)
        assert "temperature_ens" in opened._arrays
        assert {"ensemble_member", "pressure_level"} <= set(opened._coordinates)


class TestAValueIsMatchedByItsKind:
    @pytest.mark.parametrize(
        ("values", "wanted", "index"),
        [
            (numpy.array(["mean", "p90"]), "p90", 1),
            (numpy.array([b"mean", b"p90"]), "p90", 1),
            (numpy.array(["mean", "p90"], dtype=object), "p90", 1),
            (numpy.array(["mean", "p90"], dtype=numpy.dtypes.StringDType()), "p90", 1),
            (numpy.array([0, 1, 2]), 1, 1),
            (numpy.array([0, 1, 2], dtype="u1"), 2, 2),
            (numpy.array([1000.0, 850.0]), 850, 1),
            (numpy.array([1000.0, 850.5]), 850.5, 1),
            (numpy.array([0, 1, 1]), 1, 1),
        ],
    )
    def test_a_value_of_the_coordinates_kind_is_found(self, values, wanted, index):
        assert store_reader._selected_index(values, "axis", wanted) == index

    @pytest.mark.parametrize(
        ("values", "wanted"),
        [
            (numpy.array(["0", "1"]), 0),
            (numpy.array([0, 1]), "0"),
            (numpy.array([850.0]), 851),
            (numpy.array(["mean"]), "Mean"),
            (numpy.array(["2026-03-23"], dtype="datetime64[D]"), "2026-03-23"),
        ],
    )
    def test_a_value_of_another_kind_or_not_there_is_misdescribed(self, values, wanted):
        with pytest.raises(store_reader.StoreMisdescribed, match="axis"):
            store_reader._selected_index(values, "axis", wanted)
