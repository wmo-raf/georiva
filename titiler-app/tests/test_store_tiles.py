"""A Store asset is tiled from the Upstream store itself (ADR 0044 §9, §11; #661).

A store-backed Variable has no file per timestep. Its rows name a whole
Icechunk repository, an array in it, and a run and step; Django publishes those
with the feed's Pinned locations, and this service opens the store and draws
the field.

So these tests build a small repository on local disk, shaped like a virtual
map-shaped forecast store: its arrays' chunks are references to GRIB messages
in a producer's bucket, decoded by the codec the store names (``gribberish``).
The repository is served over local HTTP, and the producer's Google bucket by a
stand-in on the same server: the provider's public endpoint is the one thing a
test points elsewhere. Nothing reaches a network.
"""

import contextlib
import http.server
import json
import os
import threading
import time as clock
import urllib.parse
from datetime import UTC, datetime, timedelta

import icechunk
import numpy
import pytest
import zarr
from app import config, dependencies, warm
from app import store as store_reader
from app.main import app
from fastapi.testclient import TestClient
from gribberish.zarr import GribberishCodec
from rasterio.io import MemoryFile

from tests.conftest import CATALOG, COLLECTION, ORG, VARIABLE, kvp
from tests.grib import message

ROUTE = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}"
HASH = f"georiva:remote-assets:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"
PALETTE = f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"

#: Where the producer keeps its GRIB: a Google bucket, as ECMWF's AIFS store does.
ECMWF = "gs://ecmwf-open-data/"
#: A place the store also declares, and the feed did not pin.
GFS = "s3://noaa-gfs-bdp-pds/"

#: Two runs of three steps, six hours apart, on a 45° grid.
RUNS = ["2026-03-22T12:00:00Z", "2026-03-23T00:00:00Z"]
LEADS_HOURS = [0, 6, 12]
LATITUDES = [90.0, 45.0, 0.0, -45.0, -90.0]
#: As the GRIB is written: 0°E eastward. The codec turns it to −180…180.
GRIB_LONGITUDES = [0.0, 45.0, 90.0, 135.0, 180.0, 225.0, 270.0, 315.0]
LONGITUDES = [-180.0, -135.0, -90.0, -45.0, 0.0, 45.0, 90.0, 135.0]

#: The newest run's second step: held, its data in the pinned Google bucket.
REFTIME = "2026-03-23T00:00:00Z"
TIME = "2026-03-23T06:00:00Z"
FIELD = "20260323T060000/20260323T000000"

#: The rendering config: the newest run's values are 300…349.
TILE_CONFIG = {
    "vmin": 300.0,
    "vmax": 350.0,
    "colormap": {str(i): [i, 0, 255 - i, 255] for i in range(256)},
}


def grib_values(run: int, step: int) -> numpy.ndarray:
    """What each held field holds, row north to south from 0°E: distinct per
    run and step, so a value read back names the field it came from."""
    return numpy.arange(40, dtype="f8").reshape(5, 8) + 250 + 50 * run + 5 * step


class Producer(http.server.BaseHTTPRequestHandler):
    """Serves a folder by range, as an object store does: the repository at
    its path, and a Google bucket at ``/{bucket}/{object}`` — the shape of the
    provider's own endpoint, which is what the stand-in has to be."""

    protocol_version = "HTTP/1.1"
    root = ""
    requests: list = []
    #: Seconds to wait before answering anything, for the test of a store that does not answer.
    stall = 0.0

    def log_message(self, *args):
        pass

    def do_GET(self):
        type(self).requests.append(self.path)
        if type(self).stall:
            clock.sleep(type(self).stall)
        path = os.path.join(self.root, urllib.parse.unquote(self.path.split("?")[0]).lstrip("/"))
        if not os.path.isfile(path):
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with open(path, "rb") as source:
            body = source.read()
        wanted = self.headers.get("Range")
        if wanted:
            first, _, last = wanted[len("bytes=") :].partition("-")
            first, last = int(first), min(int(last) if last else len(body) - 1, len(body) - 1)
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {first}-{last}/{len(body)}")
            body = body[first : last + 1]
        else:
            self.send_response(200)
        # What an object store says of every object, and a reader of one expects.
        self.send_header("Last-Modified", "Mon, 23 Mar 2026 00:00:00 GMT")
        self.send_header("ETag", f'"{os.path.getsize(path)}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def producer(tmp_path, monkeypatch):
    """A local HTTP server for the repository and the producer's bucket, and
    the Google endpoint pointed at it."""
    handler = type("Handler", (Producer,), {"root": str(tmp_path), "requests": [], "stall": 0.0})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    monkeypatch.setitem(store_reader.GCS_OPTIONS, "google_base_url", base)
    monkeypatch.setitem(store_reader.GCS_OPTIONS, "allow_http", "true")
    store_reader.forget_all()
    try:
        yield handler, base
    finally:
        store_reader.forget_all()
        server.shutdown()
        server.server_close()


@pytest.fixture
def upstream_store(tmp_path, producer):
    """The repository, built and served; ``.href`` is its address."""
    handler, base = producer

    class Built:
        folder = os.path.join(tmp_path, "noaa-gfs-forecast-virtual", "v0.1.0.icechunk")
        href = f"{base}/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
        #: Where the producer keeps the newest run's GRIB.
        bucket = os.path.join(tmp_path, "ecmwf-open-data", "20260323", "00z")
        requests = handler.requests

        def stall(self, seconds):
            handler.stall = seconds

    built = Built()
    bucket = built.bucket
    os.makedirs(bucket)

    config = icechunk.RepositoryConfig.default()
    # The repository's own word on where its data is and how to reach it: an
    # internal address that wants credentials, which the reader must not follow.
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            GFS, icechunk.s3_store(endpoint_url="http://minio.internal:9000", region="internal", anonymous=False)
        )
    )
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(ECMWF, icechunk.gcs_store({"google_base_url": "http://gcs.internal:4443"}))
    )
    repository = icechunk.Repository.create(icechunk.local_filesystem_storage(built.folder), config=config)
    repository.save_config()
    session = repository.writable_session("main")
    root = session.store

    epoch_seconds = [int(numpy.datetime64(run.rstrip("Z"), "s").astype("int64")) for run in RUNS]
    init_time = zarr.create_array(root, name="init_time", shape=(2,), dtype="i8", dimension_names=["init_time"])
    init_time[:] = epoch_seconds
    init_time.attrs.update({"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"})
    lead_time = zarr.create_array(root, name="lead_time", shape=(3,), dtype="i8", dimension_names=["lead_time"])
    lead_time[:] = [hours * 3600 for hours in LEADS_HOURS]
    lead_time.attrs.update({"units": "seconds"})
    zarr.create_array(root, name="latitude", shape=(5,), dtype="f8", dimension_names=["latitude"])[:] = LATITUDES
    zarr.create_array(root, name="longitude", shape=(8,), dtype="f8", dimension_names=["longitude"])[:] = LONGITUDES

    zarr.create_array(
        root,
        name="temperature_2m",
        shape=(2, 3, 5, 8),
        chunks=(1, 1, 5, 8),
        dtype="f8",
        serializer=GribberishCodec(var="TMP", adjust_longitude_range=True, north_up=True),
        compressors=None,
        filters=None,
        fill_value=numpy.nan,
        dimension_names=["init_time", "lead_time", "latitude", "longitude"],
    )
    # The newest run: steps 0 and 1 have arrived, in the Google bucket; step 2 has not.
    for step in (0, 1):
        body = message(grib_values(1, step), latitudes=LATITUDES, longitudes=GRIB_LONGITUDES)
        with open(os.path.join(bucket, f"step-{step}.grib2"), "wb") as sink:
            sink.write(body)
        root.set_virtual_ref(
            f"temperature_2m/c/1/{step}/0/0",
            f"{ECMWF}20260323/00z/step-{step}.grib2",
            offset=0,
            length=len(body),
            validate_container=False,
        )
    # The older run's second step names more bytes than its object holds, in the pinned place.
    root.set_virtual_ref(
        "temperature_2m/c/0/1/0/0",
        f"{ECMWF}20260323/00z/step-0.grib2",
        offset=0,
        length=1_000_000,
        validate_container=False,
    )
    # The older run keeps its data in the declared place the feed did not pin.
    root.set_virtual_ref(
        "temperature_2m/c/0/0/0/0", f"{GFS}gfs.20260322/12/step-0.grib2", offset=0, length=256, validate_container=False
    )
    session.commit("build")
    return built


def published(store_href, chunk_locations=None, array="temperature_2m"):
    """What Django publishes for one timestep of a Store asset."""
    return json.dumps(
        {
            "store": store_href,
            "array": array,
            "chunk_locations": {ECMWF: ""} if chunk_locations is None else chunk_locations,
        }
    )


@pytest.fixture
def rendering(fake_redis):
    fake_redis.store[PALETTE] = json.dumps(TILE_CONFIG)
    return fake_redis


def publish(fake_redis, fields):
    fake_redis.hashes[HASH] = {**fields, "published": "1"}


def tile(client, **query):
    return client.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": TIME, "reftime": REFTIME, **query})


def point(client, lon, lat, **query):
    return client.get(f"{ROUTE}/point/{lon},{lat}", params={"time": TIME, "reftime": REFTIME, **query})


def palette_colours(png: bytes) -> set[tuple[int, int, int]]:
    """The colours of a tile's opaque pixels."""
    with MemoryFile(png) as memory, memory.open() as image:
        red, green, blue, alpha = image.read()
    opaque = alpha > 0
    return set(zip(red[opaque].tolist(), green[opaque].tolist(), blue[opaque].tolist(), strict=True))


class TestATileIsDrawnFromTheStore:
    def test_a_tile_is_drawn_from_a_gs_place_that_is_pinned_and_styled_like_any_variable(
        self, client, rendering, upstream_store
    ):
        publish(rendering, {FIELD: published(upstream_store.href)})

        response = tile(client)

        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "image/png"
        colours = palette_colours(response.content)
        assert colours, "the tile is not blank"
        # Every pixel is a colour of the Variable's palette: (i, 0, 255 − i).
        assert all(green == 0 and red + blue == 255 for red, green, blue in colours)
        assert len(colours) > 1, "the field's values spread over the palette"

    def test_the_value_read_is_the_one_the_codec_decodes_at_that_place(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})

        # 0°E, 0°N is the GRIB's third row, first column, rolled by the codec to −180…180.
        response = point(client, 0.5, 0.5)

        assert response.status_code == 200, response.text
        assert response.json()["values"] == [grib_values(1, 1)[2, 0]]

    def test_the_grid_is_placed_by_the_stores_own_latitudes_and_longitudes(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})

        # −180°E, 90°N: the first cell after the roll is the GRIB's 180°E column.
        response = point(client, -179.5, 89.5)

        assert response.json()["values"] == [grib_values(1, 1)[0, 4]]

    def test_the_whole_field_reads_as_one_value_encoded_texture(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})

        response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME, "reftime": REFTIME})

        assert response.status_code == 200, response.text
        assert response.headers["X-Image-Bounds"] == "-202.5,-112.5,157.5,112.5"

    def test_the_store_is_opened_once_and_kept_across_tiles(self, client, rendering, upstream_store, monkeypatch):
        publish(rendering, {FIELD: published(upstream_store.href)})
        opened = []
        real = store_reader.open_repository
        monkeypatch.setattr(store_reader, "open_repository", lambda *args: opened.append(args[1]) or real(*args))

        assert tile(client).status_code == 200
        another = client.get(f"{ROUTE}/tiles/WebMercatorQuad/1/1/0.png", params={"time": TIME, "reftime": REFTIME})
        assert another.status_code == 200
        assert point(client, 10, 10).status_code == 200

        assert len(opened) == 1

    def test_requests_with_other_pins_open_a_store_of_their_own(self, client, rendering, upstream_store, monkeypatch):
        opened = []
        real = store_reader.open_repository
        monkeypatch.setattr(store_reader, "open_repository", lambda *args: opened.append(args[1]) or real(*args))

        publish(rendering, {FIELD: published(upstream_store.href)})
        assert tile(client).status_code == 200
        publish(rendering, {FIELD: published(upstream_store.href, {ECMWF: "", GFS: "us-east-1"})})
        assert tile(client).status_code == 200

        assert len(opened) == 2


class TestWmts:
    def test_get_tile_draws_a_store_backed_layer(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})

        response = client.get(f"/{ORG}/wmts", params=kvp(TIME=TIME, REFTIME=REFTIME))

        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "image/png"
        assert palette_colours(response.content)


class TestTiers:
    def test_a_private_collection_tiles_from_redis_with_no_callback(
        self, client, rendering, django_configs, upstream_store
    ):
        # Django's callback knows nothing of a private collection; Redis is its whole word.
        publish(rendering, {FIELD: published(upstream_store.href)})

        assert tile(client).status_code == 200
        assert django_configs.asked == []

    def test_after_redis_is_flushed_a_public_collection_tiles_via_the_callback(
        self, client, django_configs, upstream_store
    ):
        entry = json.loads(published(upstream_store.href))
        django_configs[(ORG, CATALOG, COLLECTION, VARIABLE, None)] = (200, {**TILE_CONFIG, "remote_store": entry})

        response = tile(client)

        assert response.status_code == 200, response.text
        assert (TIME, REFTIME) in django_configs.asked


class TestOnlyPinnedPlacesAreFollowed:
    def test_a_chunk_in_a_declared_place_that_is_not_pinned_is_an_error_that_names_the_cause(
        self, client, rendering, upstream_store
    ):
        publish(rendering, {"20260322T120000/20260322T120000": published(upstream_store.href)})

        response = tile(client, time="2026-03-22T12:00:00Z", reftime="2026-03-22T12:00:00Z")

        assert response.status_code == 502
        assert response.headers["content-type"] == "application/json"
        detail = response.json()["detail"]
        assert "not allowed to read" in detail
        assert "Re-check the upstream" in detail
        assert response.headers["cache-control"] == "no-store"

    def test_a_broken_object_in_a_pinned_place_is_not_blamed_on_the_pins(self, client, rendering, upstream_store):
        publish(rendering, {"20260322T180000/20260322T120000": published(upstream_store.href)})

        response = tile(client, time="2026-03-22T18:00:00Z", reftime="2026-03-22T12:00:00Z")

        assert response.status_code == 504
        assert "not allowed to read" not in response.json()["detail"]

    def test_the_repositorys_own_containers_are_never_followed(self, client, rendering, upstream_store):
        # Pinned nowhere: the repository's own config names the place, and still nothing is read from it.
        publish(rendering, {FIELD: published(upstream_store.href, {})})

        response = tile(client)

        assert response.status_code == 502
        assert "not allowed to read" in response.json()["detail"]
        assert not any(path.startswith("/ecmwf-open-data/") for path in upstream_store.requests)

    def test_a_pin_of_any_other_kind_fails_closed(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href, {ECMWF: "", "https://data.example/": ""})})

        response = tile(client)

        assert response.status_code == 502
        assert "never read from" in response.json()["detail"]

    def test_the_settings_a_store_is_read_with_come_from_the_pins_for_both_kinds(self, upstream_store, monkeypatch):
        monkeypatch.delitem(store_reader.GCS_OPTIONS, "google_base_url")
        monkeypatch.delitem(store_reader.GCS_OPTIONS, "allow_http")

        opened = store_reader.open_repository(
            icechunk.local_filesystem_storage(upstream_store.folder), ((ECMWF, ""), (GFS, "us-east-1"))
        )
        containers = opened.config.virtual_chunk_containers

        assert containers.keys() == {GFS, ECMWF}
        s3 = str(containers[GFS].store)
        assert 'region: "us-east-1"' in s3
        assert "endpoint_url: None" in s3, "the provider's public endpoint, not the repository's"
        assert "anonymous: True" in s3
        assert "minio.internal" not in s3
        gcs = str(containers[ECMWF].store)
        assert "gcs.internal" not in gcs
        assert "base_url" not in gcs

    def test_with_nothing_pinned_the_repositorys_own_places_are_dropped(self, upstream_store):
        opened = store_reader.open_repository(icechunk.local_filesystem_storage(upstream_store.folder), ())

        assert opened.config.virtual_chunk_containers == {}


class TestFailures:
    def test_a_step_the_store_does_not_hold_is_an_error_not_a_blank(self, client, rendering, upstream_store):
        publish(rendering, {"20260323T120000/20260323T000000": published(upstream_store.href)})

        response = tile(client, time="2026-03-23T12:00:00Z")

        assert response.status_code == 404
        assert response.headers["content-type"] == "application/json"
        assert "time step" in response.json()["detail"]

    def test_a_run_the_store_does_not_have_is_an_error(self, client, rendering, upstream_store):
        publish(rendering, {"20260324T060000/20260324T000000": published(upstream_store.href)})

        response = tile(client, time="2026-03-24T06:00:00Z", reftime="2026-03-24T00:00:00Z")

        assert response.status_code == 404

    def test_an_unreachable_store_is_an_error(self, client, rendering, producer):
        _, base = producer
        publish(rendering, {FIELD: published(f"{base}/no-such-store/v0.icechunk")})

        response = tile(client)

        assert response.status_code == 504
        assert "upstream store" in response.json()["detail"]

    def test_a_store_that_does_not_answer_is_an_error_within_the_timeout(
        self, client, rendering, upstream_store, monkeypatch
    ):
        monkeypatch.setattr(store_reader, "STORE_READ_TIMEOUT", 1.0)
        publish(rendering, {FIELD: published(upstream_store.href)})
        upstream_store.stall(3)

        started = clock.monotonic()
        response = tile(client)

        assert response.status_code == 504
        assert "did not answer within" in response.json()["detail"]
        assert clock.monotonic() - started < 2.5

    def test_a_failed_open_is_not_kept(self, client, rendering, upstream_store, monkeypatch):
        monkeypatch.setattr(store_reader, "STORE_READ_TIMEOUT", 1.0)
        publish(rendering, {FIELD: published(upstream_store.href)})
        upstream_store.stall(3)
        assert tile(client).status_code == 504

        upstream_store.stall(0)
        monkeypatch.setattr(store_reader, "STORE_READ_TIMEOUT", 30.0)

        assert tile(client).status_code == 200

    def test_a_published_entry_that_is_not_djangos_shape_is_refused(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: json.dumps({"store": "file:///etc/", "array": "temperature_2m"})})

        response = tile(client)

        assert response.status_code == 502
        assert response.headers["cache-control"] == "no-store"


class TestCogsAreUnchanged:
    def test_a_remote_cog_beside_a_store_entry_is_still_read_by_its_href(self, client, rendering, seed_cog):
        seed_cog(time=TIME, reftime=REFTIME)
        # A hash with a store at another timestep: this one is local.
        publish(rendering, {"20260323T000000/20260323T000000": published("https://example.org/store.icechunk")})

        assert tile(client).status_code == 200


def repository(built):
    """The built repository, opened as its producer opens it: on local disk."""
    return icechunk.Repository.open(icechunk.local_filesystem_storage(built.folder))


def arrive(built, step: int) -> None:
    """The producer commits the newest run's ``step``: its GRIB in the bucket,
    a reference to it in the repository, on the branch Titiler reads."""
    body = message(grib_values(1, step), latitudes=LATITUDES, longitudes=GRIB_LONGITUDES)
    with open(os.path.join(built.bucket, f"step-{step}.grib2"), "wb") as sink:
        sink.write(body)
    session = repository(built).writable_session("main")
    session.store.set_virtual_ref(
        f"temperature_2m/c/1/{step}/0/0",
        f"{ECMWF}20260323/00z/step-{step}.grib2",
        offset=0,
        length=len(body),
        validate_container=False,
    )
    session.commit(f"step {step}")


def commit_masked_array(built) -> None:
    """A second array, kept natively, whose newest run's second step is held
    and every value of it NaN: a masked field, not a missing one."""
    session = repository(built).writable_session("main")
    array = zarr.create_array(
        session.store,
        name="masked",
        shape=(2, 3, 5, 8),
        chunks=(1, 1, 5, 8),
        dtype="f8",
        fill_value=numpy.nan,
        dimension_names=["init_time", "lead_time", "latitude", "longitude"],
        config={"write_empty_chunks": True},
    )
    array[1, 1] = numpy.full((5, 8), numpy.nan)
    session.commit("masked")


def history(built) -> list[str]:
    """The snapshots on the branch Titiler reads, newest first."""
    return [snapshot.id for snapshot in repository(built).ancestry(branch="main")]


#: The newest run's third step, which arrives while Titiler has the store open.
LATE = "20260323T120000/20260323T000000"
LATE_TIME = "2026-03-23T12:00:00Z"


@pytest.fixture
def opens(monkeypatch):
    """Every store opened, by the pins it was opened with."""
    opened = []
    real = store_reader.open_repository
    monkeypatch.setattr(store_reader, "open_repository", lambda *args: opened.append(args[1]) or real(*args))
    return opened


class TestAMissingStepRefreshesTheStore:
    def test_a_step_that_arrived_after_the_store_was_opened_is_drawn_after_one_reopen(
        self, client, rendering, upstream_store, opens
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200
        arrive(upstream_store, 2)

        response = tile(client, time=LATE_TIME)

        assert response.status_code == 200, response.text
        assert palette_colours(response.content)
        assert point(client, 0.5, 0.5, time=LATE_TIME).json()["values"] == [grib_values(1, 2)[2, 0]]
        assert len(opens) == 2

    def test_the_reopened_store_replaces_the_old_one_for_later_requests(self, client, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200
        arrive(upstream_store, 2)
        assert tile(client, time=LATE_TIME).status_code == 200

        assert tile(client, time=LATE_TIME).status_code == 200
        assert tile(client).status_code == 200
        assert len(opens) == 2

    def test_a_step_still_not_held_after_the_reopen_is_an_error_not_an_image(
        self, client, rendering, upstream_store, opens
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200

        response = tile(client, time=LATE_TIME)

        assert response.status_code == 404
        assert response.headers["content-type"] == "application/json"
        assert "not available upstream" in response.json()["detail"]
        assert len(opens) == 2

    def test_requests_for_held_steps_never_reopen(self, client, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href)})

        for _ in range(3):
            assert tile(client).status_code == 200
        assert point(client, 10, 10).status_code == 200

        assert len(opens) == 1

    def test_a_held_step_that_is_all_nan_is_drawn_as_it_is_with_no_reopen(
        self, client, rendering, upstream_store, opens
    ):
        commit_masked_array(upstream_store)
        publish(rendering, {FIELD: published(upstream_store.href, array="masked")})

        response = tile(client)

        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "image/png"
        assert not palette_colours(response.content), "every pixel is nodata"
        assert len(opens) == 1

    def test_concurrent_misses_cause_one_reopen(self, upstream_store, monkeypatch):
        entry = json.loads(published(upstream_store.href))
        reftime = datetime(2026, 3, 23, tzinfo=UTC)
        held = store_reader.StoreSlice.published(entry, reftime + timedelta(hours=6), reftime)
        late = store_reader.StoreSlice.published(entry, reftime + timedelta(hours=12), reftime)
        store_reader.read_field(held)
        arrive(upstream_store, 2)

        opened = []
        real = store_reader.open_repository

        def slow(*args):
            opened.append(args[1])
            clock.sleep(0.5)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", slow)
        together = threading.Barrier(6)
        results = []

        def request():
            together.wait()
            results.append(store_reader.read_field(late))

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert len(results) == 6
        assert all(found.values[2, 4] == grib_values(1, 2)[2, 0] for found in results)
        assert len(opened) == 1

    def test_concurrent_misses_share_one_failed_reopen(self, upstream_store, monkeypatch):
        entry = json.loads(published(upstream_store.href))
        reftime = datetime(2026, 3, 23, tzinfo=UTC)
        store_reader.read_field(store_reader.StoreSlice.published(entry, reftime + timedelta(hours=6), reftime))
        late = store_reader.StoreSlice.published(entry, reftime + timedelta(hours=12), reftime)

        attempts = []

        def broken(*args):
            attempts.append(args[1])
            clock.sleep(0.5)
            raise OSError("connection reset")

        monkeypatch.setattr(store_reader, "open_repository", broken)
        together = threading.Barrier(6)
        failures = []

        def request():
            together.wait()
            try:
                store_reader.read_field(late)
            except store_reader.StoreError as failed:
                failures.append(failed)

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert len(failures) == 6
        assert all(isinstance(failed, store_reader.StoreUnreachable) for failed in failures)
        assert len(attempts) == 1

    def test_a_failed_reopen_keeps_the_open_store_for_the_steps_it_holds(
        self, client, rendering, upstream_store, monkeypatch
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200
        kept = store_reader.opened_store(upstream_store.href, ((ECMWF, ""),))

        def broken(*args):
            raise OSError("connection reset")

        monkeypatch.setattr(store_reader, "open_repository", broken)

        failed = tile(client, time=LATE_TIME)
        assert failed.status_code == 504
        assert failed.headers["content-type"] == "application/json"

        assert tile(client).status_code == 200
        assert store_reader.opened_store(upstream_store.href, ((ECMWF, ""),)) is kept

    def test_a_missing_step_marks_nothing_and_writes_nothing(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        before = dict(rendering.hashes[HASH])
        snapshots = history(upstream_store)

        assert tile(client, time=LATE_TIME).status_code == 404

        assert rendering.hashes[HASH] == before
        assert history(upstream_store) == snapshots


#: A step of the newest run the store never holds, in this module's repository.
LOST = "20260323T180000/20260323T000000"
LOST_TIME = "2026-03-23T18:00:00Z"


@pytest.fixture
def fake_monotonic(monkeypatch):
    """The reader's ``monotonic()``, set by hand: ``.now`` is the time it reads."""

    class Clock:
        now = 1000.0

    monkeypatch.setattr(store_reader, "monotonic", lambda: Clock.now)
    return Clock


class TestAStepStillMissingIsRemembered:
    """A step a reopen did not find is remembered for ``STORE_MISS_TTL`` seconds,
    so a step the store has lost does not cost a reopen per request (#677)."""

    def test_repeated_requests_for_a_lost_step_reopen_once_per_window(
        self, client, rendering, upstream_store, opens, fake_monotonic
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200

        for _ in range(4):
            response = tile(client, time=LATE_TIME)
            assert response.status_code == 404
            assert "not available upstream" in response.json()["detail"]
            fake_monotonic.now += 10

        assert len(opens) == 2

    def test_once_the_window_has_passed_the_store_is_reopened_again(
        self, client, rendering, upstream_store, opens, fake_monotonic
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client, time=LATE_TIME).status_code == 404
        assert len(opens) == 2

        fake_monotonic.now += store_reader.STORE_MISS_TTL + 1
        assert tile(client, time=LATE_TIME).status_code == 404

        assert len(opens) == 3

    def test_a_remembered_step_that_arrives_is_drawn_after_the_window_and_one_reopen(
        self, client, rendering, upstream_store, opens, fake_monotonic
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client, time=LATE_TIME).status_code == 404
        arrive(upstream_store, 2)
        assert tile(client, time=LATE_TIME).status_code == 404, "remembered: no reopen yet"

        fake_monotonic.now += store_reader.STORE_MISS_TTL + 1
        response = tile(client, time=LATE_TIME)

        assert response.status_code == 200, response.text
        assert len(opens) == 3

    def test_a_remembered_step_another_reopen_picked_up_is_drawn_at_once(
        self, client, rendering, upstream_store, opens, fake_monotonic
    ):
        publish(
            rendering,
            {
                FIELD: published(upstream_store.href),
                LATE: published(upstream_store.href),
                LOST: published(upstream_store.href),
            },
        )
        assert tile(client, time=LATE_TIME).status_code == 404
        arrive(upstream_store, 2)
        # Another step's miss reopens the store, and the newer snapshot holds the remembered one.
        assert tile(client, time=LOST_TIME).status_code == 404

        response = tile(client, time=LATE_TIME)

        assert response.status_code == 200, response.text
        assert len(opens) == 3

    def test_a_miss_found_after_its_store_was_let_go_is_not_remembered(
        self, client, rendering, upstream_store, monkeypatch
    ):
        publish(rendering, {LATE: published(upstream_store.href)})
        key = (upstream_store.href, ((ECMWF, ""),))
        real = store_reader.open_repository
        calls = []

        def let_go_while_reopening(*args):
            calls.append(1)
            if len(calls) == 2:  # the reopen the miss asks for
                store_reader.forget(*key)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", let_go_while_reopening)

        assert tile(client, time=LATE_TIME).status_code == 404
        assert len(calls) == 2
        assert store_reader._opened.misses == {}

    def test_remembering_one_step_does_not_stop_another_from_reopening(
        self, client, rendering, upstream_store, opens, fake_monotonic
    ):
        publish(rendering, {LATE: published(upstream_store.href), LOST: published(upstream_store.href)})
        assert tile(client, time=LATE_TIME).status_code == 404

        assert tile(client, time=LOST_TIME).status_code == 404

        assert len(opens) == 3

    def test_a_failed_reopen_is_not_remembered_as_a_miss(
        self, client, rendering, upstream_store, monkeypatch, fake_monotonic
    ):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200
        attempts = []
        real = store_reader.open_repository

        def broken(*args):
            attempts.append(args[1])
            raise OSError("connection reset")

        monkeypatch.setattr(store_reader, "open_repository", broken)
        assert tile(client, time=LATE_TIME).status_code == 504
        monkeypatch.setattr(store_reader, "open_repository", real)
        arrive(upstream_store, 2)

        assert tile(client, time=LATE_TIME).status_code == 200
        assert len(attempts) == 1

    def test_a_remembered_miss_is_warned_of_once_naming_the_store_array_and_step(
        self, client, rendering, upstream_store, caplog, fake_monotonic
    ):
        publish(rendering, {LATE: published(upstream_store.href)})

        with caplog.at_level("WARNING", logger="app.store"):
            for _ in range(3):
                assert tile(client, time=LATE_TIME).status_code == 404

        warned = [record.getMessage() for record in caplog.records if "not held" in record.getMessage()]
        assert len(warned) == 1
        assert upstream_store.href in warned[0]
        assert "temperature_2m" in warned[0]
        assert "2026-03-23T12:00:00" in warned[0]

    def test_concurrent_misses_on_one_reopen_warn_once(self, upstream_store, monkeypatch, caplog, fake_monotonic):
        entry = json.loads(published(upstream_store.href))
        reftime = datetime(2026, 3, 23, tzinfo=UTC)
        store_reader.read_field(store_reader.StoreSlice.published(entry, reftime + timedelta(hours=6), reftime))
        late = store_reader.StoreSlice.published(entry, reftime + timedelta(hours=12), reftime)
        real = store_reader.open_repository

        def slow(*args):
            clock.sleep(0.5)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", slow)
        together = threading.Barrier(4)

        def request():
            together.wait()
            with contextlib.suppress(store_reader.StoreError):
                store_reader.read_field(late)

        with caplog.at_level("WARNING", logger="app.store"):
            threads = [threading.Thread(target=request) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)

        assert len([record for record in caplog.records if "not held" in record.getMessage()]) == 1

    def test_the_window_is_a_setting(self):
        from app import config

        assert store_reader.STORE_MISS_TTL is config.STORE_MISS_TTL


def grib_reads(built, step: int = 1) -> int:
    """How many times the producer was asked for the newest run's ``step``: one upstream read of that field each."""
    return sum(1 for path in built.requests if f"step-{step}.grib2" in urllib.parse.unquote(path))


def every_tile(client, **query) -> list[int]:
    """Twenty different tiles of the one field: all of zoom 1 and zoom 2."""
    statuses = []
    for zoom in (1, 2):
        for x in range(2**zoom):
            for y in range(2**zoom):
                response = client.get(
                    f"{ROUTE}/tiles/WebMercatorQuad/{zoom}/{x}/{y}.png",
                    params={"time": TIME, "reftime": REFTIME, **query},
                )
                statuses.append(response.status_code)
    return statuses


def slice_at(href: str, hours: int) -> store_reader.StoreSlice:
    """The newest run's field ``hours`` into it, of the store at ``href``, as a request for it names it."""
    reftime = datetime(2026, 3, 23, tzinfo=UTC)
    entry = json.loads(published(href))
    return store_reader.StoreSlice.published(entry, reftime + timedelta(hours=hours), reftime)


def decoded(megabytes: float) -> store_reader.Field:
    """A field of about ``megabytes``, for the cache on its own."""
    values = numpy.zeros((int(megabytes * 2**20 / 8) // 2, 2))
    return store_reader.Field(values=values, west=-180.0, north=90.0, x_resolution=1.0, y_resolution=1.0)


@pytest.fixture
def cache_of(monkeypatch):
    """Give the reader a field cache of so many bytes."""

    def sized(size: int) -> store_reader.FieldCache:
        cache = store_reader.FieldCache(size)
        monkeypatch.setattr(store_reader, "_fields", cache)
        return cache

    return sized


class TestDecodedFieldsAreKept:
    """Every tile of a store's map needs the same whole field, so each worker
    keeps the fields it decoded, bounded, least recently used out first (ADR 0044 §13; #663)."""

    def test_twenty_tiles_of_one_field_read_it_upstream_once(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})

        assert every_tile(client) == [200] * 20

        assert grib_reads(upstream_store) == 1

    def test_concurrent_requests_for_an_uncached_field_wait_on_one_read(self, upstream_store, monkeypatch):
        target = slice_at(upstream_store.href, 6)
        reads = []
        real = store_reader.OpenStore.field

        def slow(self, *args):
            reads.append(args)
            clock.sleep(0.5)
            return real(self, *args)

        monkeypatch.setattr(store_reader.OpenStore, "field", slow)
        together = threading.Barrier(6)
        results = []

        def request():
            together.wait()
            results.append(store_reader.read_field(target))

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert len(results) == 6
        assert all(found.values[2, 4] == grib_values(1, 1)[2, 0] for found in results)
        assert len(reads) == 1

    def test_concurrent_requests_share_one_failed_read_and_the_next_request_reads_again(
        self, upstream_store, monkeypatch
    ):
        target = slice_at(upstream_store.href, 6)
        attempts = []
        real = store_reader.OpenStore.field

        def broken(self, *args):
            attempts.append(args)
            clock.sleep(0.5)
            raise OSError("connection reset")

        monkeypatch.setattr(store_reader.OpenStore, "field", broken)
        together = threading.Barrier(6)
        failures = []

        def request():
            together.wait()
            try:
                store_reader.read_field(target)
            except store_reader.StoreError as failed:
                failures.append(failed)

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert len(failures) == 6
        assert len(attempts) == 1

        monkeypatch.setattr(store_reader.OpenStore, "field", real)
        assert store_reader.read_field(target).values[2, 4] == grib_values(1, 1)[2, 0]

    def test_a_failed_read_is_retried_on_the_next_tile_not_served_from_the_cache(
        self, client, rendering, upstream_store, monkeypatch
    ):
        publish(rendering, {FIELD: published(upstream_store.href)})
        real = store_reader.OpenStore.field

        def broken(self, *args):
            raise OSError("connection reset")

        monkeypatch.setattr(store_reader.OpenStore, "field", broken)
        assert tile(client).status_code == 504

        monkeypatch.setattr(store_reader.OpenStore, "field", real)
        assert tile(client).status_code == 200

    def test_a_step_not_held_is_not_kept(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client, time=LATE_TIME).status_code == 404
        arrive(upstream_store, 2)
        store_reader.forget_all()  # so the miss is not remembered: the field is all that is left to keep it

        assert tile(client, time=LATE_TIME).status_code == 200

    def test_with_the_cache_at_zero_tiles_still_draw_and_each_reads_upstream(
        self, client, rendering, upstream_store, cache_of
    ):
        cache_of(0)
        publish(rendering, {FIELD: published(upstream_store.href)})

        assert tile(client).status_code == 200
        assert tile(client).status_code == 200

        assert grib_reads(upstream_store) == 2

    def test_fields_of_other_steps_are_kept_beside_each_other(self, client, rendering, upstream_store):
        first = "20260323T000000/20260323T000000"
        publish(rendering, {FIELD: published(upstream_store.href), first: published(upstream_store.href)})

        for _ in range(3):
            assert tile(client).status_code == 200
            assert tile(client, time=REFTIME).status_code == 200

        assert grib_reads(upstream_store, 1) == 1
        assert grib_reads(upstream_store, 0) == 1

    def test_requests_with_other_pins_do_not_share_a_field(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})
        assert tile(client).status_code == 200

        publish(rendering, {FIELD: published(upstream_store.href, chunk_locations={})})
        assert tile(client).status_code == 502

    def test_the_cache_never_holds_more_than_its_size_and_drops_the_least_recently_used(self):
        cache = store_reader.FieldCache(3 * 2**20)
        one, two, three = (slice_at(f"https://example.org/{n}", 6) for n in "abc")

        cache.put(one, decoded(1))
        cache.put(two, decoded(1))
        assert cache.get(one) is not None  # one is now the more recently used
        cache.put(three, decoded(1.5))

        assert cache.size <= 3 * 2**20
        assert cache.get(two) is None
        assert cache.get(one) is not None
        assert cache.get(three) is not None

    def test_a_field_larger_than_the_cache_is_not_kept_and_evicts_nothing(self):
        cache = store_reader.FieldCache(2 * 2**20)
        small, large = (slice_at(f"https://example.org/{n}", 6) for n in "ab")
        cache.put(small, decoded(1))

        cache.put(large, decoded(3))

        assert cache.get(large) is None
        assert cache.get(small) is not None
        assert cache.size <= 2 * 2**20

    def test_a_kept_field_cannot_be_changed_by_a_request(self, upstream_store):
        found = store_reader.read_field(slice_at(upstream_store.href, 6))

        with pytest.raises(ValueError):
            found.values[0, 0] = 0

    def test_a_reopen_keeps_the_fields_already_decoded(self, client, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        assert tile(client).status_code == 200
        arrive(upstream_store, 2)
        assert tile(client, time=LATE_TIME).status_code == 200

        assert tile(client).status_code == 200

        assert grib_reads(upstream_store, 1) == 1
        assert len(opens) == 2

    def test_the_size_is_a_setting(self):
        from app import config

        assert store_reader.STORE_FIELD_CACHE_BYTES is config.STORE_FIELD_CACHE_BYTES
        assert store_reader._fields.capacity == config.STORE_FIELD_CACHE_BYTES


# ---------------------------------------------------------------------------
# Each worker opens the published stores at start (#684)
# ---------------------------------------------------------------------------

#: Another Variable of the same collection, read from the same store.
OTHER_HASH = f"georiva:remote-assets:{ORG}:{CATALOG}:{COLLECTION}:wind"
#: A COG timestep of this module's Variable: absent from its hash, so read from our storage.
COG_TIME = "2026-03-23T12:00:00Z"


def wait_for_warm_up() -> None:
    """Wait for a warm-up the app started to finish, so a test's teardown never runs under it."""
    for thread in threading.enumerate():
        if thread.name == warm.THREAD_NAME:
            thread.join(timeout=30)


class TestStoresAreWarmedAtStart:
    def test_after_warming_the_first_tile_opens_no_store(self, client, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href)})
        warm.warm_stores()
        assert len(opens) == 1

        response = tile(client)

        assert response.status_code == 200, response.text
        assert len(opens) == 1

    def test_a_warmed_store_has_its_published_arrays_and_their_coordinates(self, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})
        warm.warm_stores()

        opened = store_reader.opened_store(upstream_store.href, ((ECMWF, ""),))
        assert set(opened._arrays) == {"temperature_2m"}
        assert set(opened._coordinates) >= {"init_time", "lead_time", "latitude", "longitude"}

    def test_a_store_published_under_the_same_pins_is_opened_once(self, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href), LATE: published(upstream_store.href)})
        rendering.hashes[OTHER_HASH] = {FIELD: published(upstream_store.href), "published": "1"}

        warm.warm_stores()

        assert opens == [((ECMWF, ""),)]

    def test_the_same_store_under_other_pins_is_opened_for_each(self, rendering, upstream_store, opens):
        publish(rendering, {FIELD: published(upstream_store.href)})
        rendering.hashes[OTHER_HASH] = {
            FIELD: published(upstream_store.href, {ECMWF: "", GFS: "us-east-1"}),
            "published": "1",
        }

        warm.warm_stores()

        assert sorted(opens) == [((ECMWF, ""),), ((ECMWF, ""), (GFS, "us-east-1"))]

    def test_remote_cogs_and_malformed_entries_are_passed_over(self, client, rendering, upstream_store, opens):
        rendering.hashes[OTHER_HASH] = {
            FIELD: "https://upstream.example/field.tif",
            LATE: json.dumps({"store": "file:///etc/", "array": "temperature_2m"}),
            LOST: "{not json",
            "published": "1",
        }
        publish(rendering, {FIELD: published(upstream_store.href)})

        warm.warm_stores()

        assert opens == [((ECMWF, ""),)]
        assert tile(client).status_code == 200
        assert len(opens) == 1

    def test_a_store_that_will_not_open_does_not_stop_the_next(
        self, client, rendering, upstream_store, producer, opens
    ):
        _, base = producer
        rendering.hashes[OTHER_HASH] = {FIELD: published(f"{base}/no-such-store/v0.icechunk"), "published": "1"}
        publish(rendering, {FIELD: published(upstream_store.href)})

        warm.warm_stores()

        assert tile(client).status_code == 200
        assert len(opens) == 2, "the unreachable store's attempt, and the good one's open; none by the tile"

    def test_an_array_that_will_not_open_does_not_stop_the_others(self, rendering, upstream_store):
        rendering.hashes[OTHER_HASH] = {FIELD: published(upstream_store.href, array="no_such_array"), "published": "1"}
        publish(rendering, {FIELD: published(upstream_store.href)})

        warm.warm_stores()

        assert "temperature_2m" in store_reader.opened_store(upstream_store.href, ((ECMWF, ""),))._arrays

    def test_a_key_that_is_not_a_hash_costs_only_that_key(self, rendering, upstream_store, opens, monkeypatch):
        rendering.hashes[OTHER_HASH] = {}
        publish(rendering, {FIELD: published(upstream_store.href)})
        hgetall = rendering.hgetall

        def wrong_type(key):
            if key == OTHER_HASH:
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            return hgetall(key)

        monkeypatch.setattr(rendering, "hgetall", wrong_type)

        warm.warm_stores()

        assert opens == [((ECMWF, ""),)]

    def test_redis_that_cannot_be_read_ends_the_warm_up_quietly(self, monkeypatch, opens):
        class Down:
            def scan_iter(self, *args, **kwargs):
                raise ConnectionError("Redis is not there")

        monkeypatch.setattr(dependencies, "redis_client", Down())

        warm.warm_stores()

        assert opens == []

    def test_the_app_answers_while_a_store_is_still_opening(
        self, fake_redis, django_configs, rendering, upstream_store, seed_cog, monkeypatch
    ):
        publish(rendering, {FIELD: published(upstream_store.href)})
        seed_cog(time=COG_TIME)
        entered, release = threading.Event(), threading.Event()
        opened = []
        real = store_reader.open_repository

        def slow_open(*args):
            opened.append(args[1])
            entered.set()
            release.wait(timeout=30)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", slow_open)

        try:
            with TestClient(app) as started:
                assert entered.wait(timeout=10), "the warm-up never began opening the store"

                assert started.get("/healthz").status_code == 200
                cog = started.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": COG_TIME})
                assert cog.status_code == 200, cog.text

                drawn = {}
                waiting = threading.Thread(target=lambda: drawn.setdefault("response", tile(started)))
                waiting.start()
                clock.sleep(0.3)
                assert "response" not in drawn, "the store tile waits on the open under way"
                release.set()
                waiting.join(timeout=30)
        finally:
            release.set()
            wait_for_warm_up()

        assert drawn["response"].status_code == 200, drawn["response"].text
        assert len(opened) == 1

    def test_warming_can_be_switched_off(self, django_configs, rendering, upstream_store, opens, monkeypatch):
        monkeypatch.setattr(warm, "STORE_WARM", False)
        publish(rendering, {FIELD: published(upstream_store.href)})

        with TestClient(app) as started:
            assert started.get("/healthz").status_code == 200
        wait_for_warm_up()

        assert opens == []

    def test_the_switch_is_a_setting(self):
        assert warm.STORE_WARM is config.STORE_WARM


# ---------------------------------------------------------------------------
# A running worker follows what Django publishes (#706)
# ---------------------------------------------------------------------------


def eventually(holds, timeout: float = 10.0) -> None:
    """Wait for ``holds()`` to be true, as a background thread makes it so."""
    deadline = clock.monotonic() + timeout
    while not holds():
        assert clock.monotonic() < deadline, "it never happened"
        clock.sleep(0.02)


def announce(fake_redis, about=HASH) -> None:
    """What Django says when the stores it publishes under ``about`` change."""
    assert fake_redis.publish(config.STORES_CHANNEL, about) == 1, "the worker is not listening"


@pytest.fixture
def quick(monkeypatch):
    """A listener that settles and reconnects in moments, not seconds."""
    monkeypatch.setattr(warm, "SETTLE_SECONDS", 0.05)
    monkeypatch.setattr(warm, "RECONNECT_FIRST_SECONDS", 0.05)


@pytest.fixture
def worker(django_configs, rendering, quick):
    """The app running as a worker does: started, listening, and stopped at the end."""
    try:
        with TestClient(app) as started:
            eventually(lambda: rendering.subscribers)
            yield started
    finally:
        wait_for_warm_up()


@pytest.fixture
def readings(monkeypatch):
    """How many times the published stores were read."""
    read = []
    real = warm.published_stores
    monkeypatch.setattr(warm, "published_stores", lambda: read.append(1) or real())
    return read


class TestARunningWorkerFollowsWhatIsPublished:
    def test_a_store_published_while_it_runs_is_opened_before_its_first_tile(
        self, rendering, upstream_store, opens, worker
    ):
        publish(rendering, {FIELD: published(upstream_store.href)})
        announce(rendering)
        eventually(lambda: opens)

        response = tile(worker)

        assert response.status_code == 200, response.text
        assert opens == [((ECMWF, ""),)]

    def test_new_pins_are_opened_and_the_store_under_the_old_ones_dropped(
        self, rendering, upstream_store, opens, worker
    ):
        publish(rendering, {FIELD: published(upstream_store.href)})
        announce(rendering)
        eventually(lambda: opens)
        new_pins = {ECMWF: "", GFS: "us-east-1"}

        publish(rendering, {FIELD: published(upstream_store.href, new_pins)})
        announce(rendering)

        new_key = (upstream_store.href, ((ECMWF, ""), (GFS, "us-east-1")))
        eventually(lambda: store_reader.open_keys() == {new_key})
        assert tile(worker).status_code == 200
        assert len(opens) == 2, "the tile found the store under its new pins open"

    def test_an_adopted_address_is_opened_and_the_old_one_dropped(
        self, rendering, upstream_store, producer, opens, worker
    ):
        _, base = producer
        old = f"{base}/no-longer-here/v0.icechunk"
        publish(rendering, {FIELD: published(old)})
        announce(rendering)
        eventually(lambda: (old, ((ECMWF, ""),)) in warm._named)
        store_reader._opened.stores[(old, ((ECMWF, ""),))] = object()  # as if a tile had opened it

        publish(rendering, {FIELD: published(upstream_store.href)})
        announce(rendering)

        eventually(lambda: store_reader.open_keys() == {(upstream_store.href, ((ECMWF, ""),))})

    def test_a_connection_lost_is_listened_to_again_and_what_it_missed_is_opened(
        self, rendering, upstream_store, opens, worker
    ):
        rendering.drop_subscribers()
        publish(rendering, {FIELD: published(upstream_store.href)})  # announced to nobody

        eventually(lambda: opens)
        eventually(lambda: rendering.subscribers)
        assert tile(worker).status_code == 200
        assert len(opens) == 1

    def test_a_burst_of_announcements_reads_the_published_stores_once(
        self, rendering, upstream_store, readings, worker, monkeypatch
    ):
        eventually(lambda: readings)  # the reading as it began listening
        monkeypatch.setattr(warm, "SETTLE_SECONDS", 0.3)
        publish(rendering, {FIELD: published(upstream_store.href)})

        for _ in range(5):
            announce(rendering)

        eventually(lambda: len(readings) == 2)
        clock.sleep(0.5)
        assert len(readings) == 2

    def test_it_stops_listening_when_the_app_stops(self, django_configs, rendering, quick):
        with TestClient(app):
            eventually(lambda: rendering.subscribers)
        wait_for_warm_up()

        assert not [thread for thread in threading.enumerate() if thread.name == warm.THREAD_NAME]
        assert rendering.subscribers == []


class TestWarmingDropsWhatNoAssetNames:
    def test_a_store_no_hash_names_any_more_is_dropped_with_its_fields(self, client, rendering, upstream_store):
        publish(rendering, {FIELD: published(upstream_store.href)})
        warm.warm_stores()
        assert tile(client).status_code == 200
        assert store_reader._fields.size > 0

        publish(rendering, {})
        warm.warm_stores()

        assert store_reader.open_keys() == set()
        assert store_reader._fields.size == 0

    def test_a_store_a_tile_opened_with_no_hash_to_name_it_is_kept(self, client, rendering, upstream_store):
        # Opened as a tile opens it when Django answers in the hash's place (#706 review).
        key = (upstream_store.href, ((ECMWF, ""),))
        store_reader.opened_store(*key)

        warm.warm_stores()

        assert store_reader.open_keys() == {key}

    def test_an_open_under_way_when_its_store_is_let_go_is_not_kept(self, upstream_store, monkeypatch):
        key = (upstream_store.href, ((ECMWF, ""),))
        real = store_reader.open_repository

        def let_go_while_opening(*args):
            store_reader.forget(*key)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", let_go_while_opening)

        opened = store_reader.opened_store(*key)

        assert opened is not None, "the request that opened it still draws from it"
        assert store_reader.open_keys() == set()

    def test_a_reopen_under_way_when_its_store_is_let_go_is_not_kept(self, upstream_store, monkeypatch):
        key = (upstream_store.href, ((ECMWF, ""),))
        stale = store_reader.opened_store(*key)
        real = store_reader.open_repository

        def let_go_while_reopening(*args):
            store_reader.forget(*key)
            return real(*args)

        monkeypatch.setattr(store_reader, "open_repository", let_go_while_reopening)

        assert store_reader.reopened_store(*key, stale) is not stale
        assert store_reader.open_keys() == set()

    def test_a_field_read_under_way_when_its_store_is_let_go_is_not_kept(self):
        cache = store_reader.FieldCache(3 * 2**20)
        target = slice_at("https://example.org/a", 6)

        def let_go_while_reading(_):
            cache.forget((target.store, target.chunk_locations))
            return decoded(1)

        assert cache.kept_or_read(target, let_go_while_reading) is not None
        assert cache.get(target) is None
        assert cache.size == 0

    def test_a_store_opened_again_after_it_was_let_go_is_kept(self, upstream_store):
        key = (upstream_store.href, ((ECMWF, ""),))
        store_reader.opened_store(*key)
        store_reader.forget(*key)

        store_reader.opened_store(*key)

        assert store_reader.open_keys() == {key}

    def test_a_store_opened_while_the_hashes_are_read_is_kept(self, rendering, upstream_store, monkeypatch):
        key = (upstream_store.href, ((ECMWF, ""),))
        real = warm.published_stores

        def a_tile_opens_it_meanwhile():
            found = real()
            store_reader.opened_store(*key)
            return found

        monkeypatch.setattr(warm, "published_stores", a_tile_opens_it_meanwhile)

        warm.warm_stores()

        assert store_reader.open_keys() == {key}

    def test_hashes_that_cannot_be_read_drop_nothing(self, client, rendering, upstream_store, monkeypatch):
        publish(rendering, {FIELD: published(upstream_store.href)})
        assert tile(client).status_code == 200

        def down(*args, **kwargs):
            raise ConnectionError("Redis is not there")

        monkeypatch.setattr(rendering, "scan_iter", down)
        warm.warm_stores()

        assert store_reader.open_keys() == {(upstream_store.href, ((ECMWF, ""),))}

    def test_the_channel_is_the_one_django_announces_on(self):
        assert config.STORES_CHANNEL == "georiva:stores-published"
