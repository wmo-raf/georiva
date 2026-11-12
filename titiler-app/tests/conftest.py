"""The Titiler app's test harness (#357) — its first.

Tests drive the application at the HTTP boundary through FastAPI's TestClient,
with the three dependency edges faked where they already are:

- **Redis** — ``app.dependencies.redis_client`` swapped for an in-memory map.
- **The Django internal API** — ``app.dependencies._fetch_config_from_django``
  swapped for a lookup into per-test canned responses; the default answer is
  the 404 Django gives for an address it does not know.
- **Storage** — not faked at all, redirected: ``MINIO_HOST`` points at a
  temporary directory before ``app.config`` is imported, so ``build_cog_url``
  yields local file paths and rasterio reads real (tiny) COGs written by the
  ``seed_cog`` fixture. The whole path grammar stays exercised.

Nothing here reaches into call structure — fakes answer at the same seams the
real services answer at, and assertions live on responses.
"""

import http.server
import os
import shutil
import tempfile
import threading
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

# The app reads these at import time, so they are pinned before any test
# module can pull in ``app.main``.
_STORAGE_ROOT = tempfile.mkdtemp(prefix="titiler-test-storage-")
os.environ["MINIO_HOST"] = _STORAGE_ROOT
os.environ["MINIO_BUCKET_NAME"] = "georiva-assets"
os.environ["REDIS_URL"] = "redis://never-connected:6379/0"
os.environ["DJANGO_BASE_URL"] = "http://never-connected:8000"

import numpy
import pytest
from app import dependencies
from app.main import app
from fastapi.testclient import TestClient
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

#: The address every test speaks unless it says otherwise.
ORG, CATALOG, COLLECTION, VARIABLE = "kenya", "forecasts", "gfs", "temperature"

#: A complete, valid KVP GetTile query for that address.
KVP_BASE = {
    "SERVICE": "WMTS",
    "VERSION": "1.0.0",
    "REQUEST": "GetTile",
    "LAYER": f"{CATALOG}:{COLLECTION}:{VARIABLE}",
    "STYLE": "",
    "TILEMATRIXSET": "WebMercatorQuad",
    "TILEMATRIX": "0",
    "TILEROW": "0",
    "TILECOL": "0",
    "FORMAT": "image/png",
    "TIME": "2026-03-23T12:00:00Z",
}


def overriding(base, **overrides):
    """``base`` with parameters replaced, or removed via ``NAME=None``.

    How every suite here varies a complete, valid query one parameter at a
    time: a test says what is different about its request and nothing else, so
    what it is actually exercising is the line you can read.
    """
    params = dict(base)
    for name, value in overrides.items():
        if value is None:
            params.pop(name, None)
        else:
            params[name] = value
    return params


def kvp(**overrides):
    """``KVP_BASE`` with parameters replaced, or removed via ``NAME=None``."""
    return overriding(KVP_BASE, **overrides)


def exception_of(response):
    """The single ``ows:Exception`` element of an ExceptionReport response."""
    assert response.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(response.content)
    assert root.tag == "{http://www.opengis.net/ows/1.1}ExceptionReport"
    return root.find("{http://www.opengis.net/ows/1.1}Exception")


#: The rendering config Django would answer with — vmin/vmax/colormap in the
#: shape ``palette_cache.build_variable_payload`` writes.
TILE_CONFIG = {
    "vmin": 0.0,
    "vmax": 50.0,
    "colormap": {str(i): [i, 0, 255 - i, 255] for i in range(256)},
}

#: The world in WebMercator metres — what a seeded COG covers unless a test
#: hands ``seed_cog`` a smaller box to sit in.
WORLD_EXTENT = 20037508.342789244

#: The seeded grid's side, and the values on it: a 64×64 ramp from 0 to 50,
#: read row-major. Small enough to write per test, and every cell distinct, so
#: a test asserting "the value under this pixel" is asserting the pixel too —
#: a read one cell off answers a different number.
COG_SIZE = 64
COG_VALUES = numpy.linspace(0, 50, COG_SIZE * COG_SIZE, dtype="float32").reshape(
    COG_SIZE,
    COG_SIZE,
)


class FakeRedis:
    """The two methods the app calls on its Redis client.

    ``store`` holds the string keys (rendering config) and ``hashes`` the hash
    keys (where a variable's Remote assets are). A hash that is absent reads as
    every field missing, which is what Redis answers too.
    """

    def __init__(self):
        self.store = {}
        self.hashes = {}

    def get(self, key):
        return self.store.get(key)

    def hmget(self, key, fields):
        return [self.hashes.get(key, {}).get(field) for field in fields]


@pytest.fixture(autouse=True)
def empty_storage():
    """Each test starts with an empty bucket — seeded COGs must not outlive
    the test that seeded them, or an absence assertion can find a leftover."""
    bucket = os.path.join(_STORAGE_ROOT, os.environ["MINIO_BUCKET_NAME"])
    yield
    if os.path.isdir(bucket):
        shutil.rmtree(bucket)


@pytest.fixture
def fake_redis(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(dependencies, "redis_client", fake)
    return fake


@pytest.fixture
def django_configs(monkeypatch):
    """Canned tile-config answers, keyed ``(org, catalog, collection, variable, style)``.

    Unlisted addresses answer ``(404, None)`` — exactly what Django says for an
    unknown variable or style. ``.asked`` records each call's ``time`` and
    ``reftime``, for the tests about which timestep Django was asked for.
    """

    class Responses(dict):
        def __init__(self):
            super().__init__()
            self.asked = []

    responses = Responses()

    def fetch(org, catalog, collection, variable, style=None, time=None, reftime=None):
        responses.asked.append((time, reftime))
        return responses.get((org, catalog, collection, variable, style), (404, None))

    monkeypatch.setattr(dependencies, "_fetch_config_from_django", fetch)
    return responses


@pytest.fixture
def client(fake_redis, django_configs):
    """A TestClient over the real app with both network edges already faked."""
    return TestClient(app)


def cog_bytes(bounds=None, data=None, nodata=None) -> bytes:
    """The seeded grid as a byte string: one definition of what a test COG is.

    ``seed_cog`` puts these bytes on disk at the key the app will derive; the
    storage-recovery suite serves the same bytes over HTTP, because the defect
    it tests lives in GDAL's ``/vsicurl/`` layer and never touches a filesystem
    path. Both must read the same raster or a value assertion means two things.
    """
    if data is None:
        data = COG_VALUES
    height, width = data.shape
    left, bottom, right, top = bounds or (-WORLD_EXTENT, -WORLD_EXTENT, WORLD_EXTENT, WORLD_EXTENT)

    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype="float32",
            crs="EPSG:3857",
            nodata=nodata,
            transform=from_bounds(left, bottom, right, top, width, height),
        ) as dst:
            dst.write(data, 1)
        return memfile.read()


@pytest.fixture
def seed_cog():
    """Write a small COG at the storage path the app will derive for a time.

    Uses the same filename grammar as ``build_cog_url`` on purpose: a test
    that seeds ``time=X`` and requests ``TIME=X`` exercises the real
    time-to-key derivation end to end.

    ``bounds`` narrows the footprint in WebMercator metres, for the tests that
    need somewhere a click can honestly land outside the data; ``data`` and
    ``nodata`` replace the ramp and declare a no-data value, for the tests that
    need a pixel holding nothing.
    """

    def write(
        time="2026-03-23T12:00:00Z",
        reftime=None,
        org=ORG,
        catalog=CATALOG,
        collection=COLLECTION,
        variable=VARIABLE,
        bounds=None,
        data=None,
        nodata=None,
    ):
        time_dt = datetime.fromisoformat(time.replace("Z", "+00:00")).astimezone(UTC)
        date_path = time_dt.strftime("%Y/%m/%d")
        time_str = time_dt.strftime("%H%M%S")
        if reftime:
            ref_dt = datetime.fromisoformat(reftime.replace("Z", "+00:00")).astimezone(UTC)
            filename = f"{variable}_{time_str}__ref{ref_dt.strftime('%Y%m%dT%H%M%S')}.tif"
        else:
            filename = f"{variable}_{time_str}.tif"

        path = os.path.join(
            _STORAGE_ROOT,
            os.environ["MINIO_BUCKET_NAME"],
            org,
            catalog,
            collection,
            variable,
            date_path,
            filename,
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "wb") as sink:
            sink.write(cog_bytes(bounds=bounds, data=data, nodata=nodata))
        return path

    return write


class ObjectStore:
    """An HTTP object store that can be made to fail, and counts what it is asked.

    The counting is the point of several tests below: "GDAL never asked" and
    "GDAL asked again" are the two states this whole change is about, and
    neither is visible in a response body.
    """

    def __init__(self, port):
        self._port = port
        self._objects: dict[str, bytes] = {}
        #: Per path, either a status to keep answering or a queue of statuses
        #: to answer once each.
        self._failures: dict[str, int | list[int]] = {}
        self.requests: list[str] = []

    # -- what a test arranges ------------------------------------------------

    def url_for(self, path: str) -> str:
        return f"http://127.0.0.1:{self._port}/{path}"

    def unique_path(self) -> str:
        """A path no earlier test can have taught GDAL anything about.

        GDAL's caches are per *process*, not per test, so a path reused across
        tests would carry the previous test's verdict into this one.
        """
        return f"georiva-assets/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/2026/03/23/{uuid.uuid4().hex}.tif"

    def write(self, path: str, body: bytes | None = None) -> None:
        """The COG lands."""
        self._objects[path] = body if body is not None else cog_bytes()

    def fail(self, path: str, status: int, times: int | None = None) -> None:
        """Answer ``status`` for the next ``times`` requests, or until recovery."""
        self._failures[path] = [status] * times if times is not None else status

    def recovers(self, path: str) -> None:
        """Whatever was wrong with ``path`` stops being wrong."""
        self._failures.pop(path, None)

    # -- what the store answers ----------------------------------------------

    def answer(self, path: str, byte_range: str | None) -> tuple[int, bytes, str | None]:
        """``(status, body, content_range)`` for one request, recording it."""
        self.requests.append(path)

        pending = self._failures.get(path)
        if isinstance(pending, int):
            return pending, b"", None
        if pending:
            return pending.pop(0), b"", None
        if pending is not None:
            del self._failures[path]

        body = self._objects.get(path)
        if body is None:
            return 404, b"", None
        if not (byte_range or "").startswith("bytes="):
            return 200, body, None

        start, _, end = byte_range[len("bytes=") :].partition("-")
        first = int(start)
        last = min(int(end) if end else len(body) - 1, len(body) - 1)
        return 206, body[first : last + 1], f"bytes {first}-{last}/{len(body)}"


@pytest.fixture
def store():
    """A running object store, torn down with the test that raised it."""
    holder = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        # GDAL's curl speaks 1.1 and keeps connections alive; the stdlib
        # default of 1.0 makes it re-dial for every range read.
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _respond(self, with_body: bool):
            status, body, content_range = holder["store"].answer(
                self.path.lstrip("/"),
                self.headers.get("Range"),
            )
            self.send_response(status)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            if content_range:
                self.send_header("Content-Range", content_range)
            self.end_headers()
            if with_body:
                self.wfile.write(body)

        def do_GET(self):
            self._respond(with_body=True)

        def do_HEAD(self):
            self._respond(with_body=False)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    holder["store"] = ObjectStore(server.server_address[1])
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield holder["store"]
    finally:
        server.shutdown()
        server.server_close()
