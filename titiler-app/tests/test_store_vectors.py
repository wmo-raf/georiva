"""A vector Store asset is computed from its store's u and v as it is drawn (ADR 0048).

Wind speed and direction are not arrays of an Upstream store. Django publishes a
vector Variable's timestep as the store and the two arrays it is computed from;
this service reads both fields at that step from one open store and draws their
magnitude or direction, like any field.

The store here keeps its own chunks (no references into a producer's bucket),
served over local HTTP by the same stand-in the store tests use.
"""

import json
import os
import time as clock

import icechunk
import numpy
import pytest
import zarr
from app import store as store_reader
from app import warm

from tests import test_store_tiles
from tests.test_store_tiles import FIELD, LATITUDES, LEADS_HOURS, LONGITUDES, RUNS, point, publish, tile

# The store tests' fixtures, shared: the stand-in producer and the rendering config.
producer = test_store_tiles.producer
rendering = test_store_tiles.rendering

U, V = "wind_u_10m", "wind_v_10m"

#: At the newest run's second step: u = 3 and v = 4 everywhere but the first row, where u = −6 and v = 0.
EASTWARD = numpy.full((5, 8), 3.0)
EASTWARD[0] = -6.0
NORTHWARD = numpy.full((5, 8), 4.0)
NORTHWARD[0] = 0.0


@pytest.fixture
def wind_store(tmp_path, producer):
    """A forecast store holding u and v at the newest run's second step; ``.href`` is its address."""
    return _wind_store(tmp_path, producer, {U: EASTWARD, V: NORTHWARD})


def _wind_store(tmp_path, producer, held):
    """A forecast store whose arrays `held` names hold their values at the newest
    run's second step; u and v are both laid out. ``.repository`` writes to it."""
    _handler, base = producer
    folder = os.path.join(tmp_path, "wind", "v0.1.0.icechunk")
    repository = icechunk.Repository.create(icechunk.local_filesystem_storage(folder))
    session = repository.writable_session("main")
    root = session.store
    init_time = zarr.create_array(root, name="init_time", shape=(2,), dtype="i8", dimension_names=["init_time"])
    init_time[:] = [int(numpy.datetime64(run.rstrip("Z"), "s").astype("int64")) for run in RUNS]
    init_time.attrs.update({"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"})
    lead_time = zarr.create_array(root, name="lead_time", shape=(3,), dtype="i8", dimension_names=["lead_time"])
    lead_time[:] = [hours * 3600 for hours in LEADS_HOURS]
    lead_time.attrs.update({"units": "seconds"})
    zarr.create_array(root, name="latitude", shape=(5,), dtype="f8", dimension_names=["latitude"])[:] = LATITUDES
    zarr.create_array(root, name="longitude", shape=(8,), dtype="f8", dimension_names=["longitude"])[:] = LONGITUDES
    for path in (U, V):
        array = zarr.create_array(
            root,
            name=path,
            shape=(2, 3, 5, 8),
            chunks=(1, 1, 5, 8),
            dtype="f8",
            fill_value=numpy.nan,
            dimension_names=["init_time", "lead_time", "latitude", "longitude"],
        )
        if path in held:
            array[1, 1] = held[path]
    session.commit("build")

    class Built:
        href = f"{base}/wind/v0.1.0.icechunk"

    Built.repository = repository
    return Built()


def vector_entry(href, transform="vector_magnitude", **vector):
    return json.dumps(
        {
            "store": href,
            "vector": {"u": U, "v": V, "transform": transform, **vector},
            "chunk_locations": {},
            "allow_http": True,
        }
    )


class TestAVectorIsComputedWhenDrawn:
    def test_a_point_of_speed_is_the_length_of_u_and_v(self, client, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href)})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 200, response.text
        assert response.json()["values"] == [pytest.approx(5.0)]

    def test_a_point_of_direction_is_the_bearing_the_wind_blows_from(self, client, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href, "vector_direction")})

        # u = 3, v = 4: from about 216.87°. In the first row, u = −6, v = 0: from the east, 90°.
        assert point(client, 0.5, 0.5).json()["values"] == [pytest.approx(216.8699, abs=1e-3)]
        assert point(client, 0.5, 89.5).json()["values"] == [pytest.approx(90.0)]

    def test_a_tile_of_speed_is_drawn(self, client, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href)})

        response = tile(client)

        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "image/png"

    def test_both_parts_are_read_from_one_open_store_and_kept_once_computed(
        self, client, rendering, wind_store, monkeypatch
    ):
        reads = []
        field = store_reader.OpenStore.field
        monkeypatch.setattr(
            store_reader.OpenStore, "field", lambda self, path, *at: reads.append(path) or field(self, path, *at)
        )
        publish(rendering, {FIELD: vector_entry(wind_store.href)})

        for _ in range(3):
            assert point(client, 0.5, 0.5).status_code == 200

        assert sorted(reads) == [U, V]
        assert len(store_reader.open_keys()) == 1

    def test_an_entry_naming_no_v_is_refused(self, client, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href, v="")})

        response = point(client, 0.5, 0.5)

        assert response.status_code >= 500
        assert "incomplete" in response.text

    def test_an_entry_naming_a_quantity_this_service_cannot_compute_is_refused(self, client, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href, "vector_curl")})

        assert point(client, 0.5, 0.5).status_code >= 500

    def test_the_warm_up_opens_both_arrays(self, rendering, wind_store):
        publish(rendering, {FIELD: vector_entry(wind_store.href)})

        stores = warm.published_stores()

        assert list(stores.values()) == [{U, V}]


class TestAVectorIsReadAsOne:
    """Both parts of a vector field come from one snapshot of the store, within one timeout."""

    def test_a_reopen_for_one_part_reads_the_other_again_from_the_same_snapshot(
        self, client, rendering, tmp_path, producer
    ):
        # The worker opened the store when it held only u, as 30 east; then u was
        # corrected to 3 and v written as 4, in one commit.
        built = _wind_store(tmp_path, producer, {U: numpy.full((5, 8), 30.0)})
        store_reader.opened_store(built.href, (), True)
        session = built.repository.writable_session("main")
        zarr.open_array(session.store, path=U)[1, 1] = EASTWARD
        zarr.open_array(session.store, path=V)[1, 1] = NORTHWARD
        session.commit("correct u, add v")
        publish(rendering, {FIELD: vector_entry(built.href)})

        response = point(client, 0.5, 0.5)

        assert response.status_code == 200, response.text
        # hypot(3, 4) — never the old u with the new v, hypot(30, 4).
        assert response.json()["values"] == [pytest.approx(5.0)]

    def test_both_parts_are_read_within_one_timeout(self, client, rendering, wind_store, monkeypatch):
        monkeypatch.setattr(store_reader, "STORE_READ_TIMEOUT", 1.0)
        field = store_reader.OpenStore.field

        def slow(self, path, *at):
            clock.sleep(0.7)
            return field(self, path, *at)

        monkeypatch.setattr(store_reader.OpenStore, "field", slow)
        publish(rendering, {FIELD: vector_entry(wind_store.href)})

        response = point(client, 0.5, 0.5)

        # Each part alone answers in time; the two together do not.
        assert response.status_code == 504
        assert "did not answer within" in response.json()["detail"]
