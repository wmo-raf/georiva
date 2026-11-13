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

import http.server
import json
import os
import threading
import time as clock
import urllib.parse

import icechunk
import numpy
import pytest
import zarr
from app import store as store_reader
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
        requests = handler.requests

        def stall(self, seconds):
            handler.stall = seconds

    built = Built()
    bucket = os.path.join(tmp_path, "ecmwf-open-data", "20260323", "00z")
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
