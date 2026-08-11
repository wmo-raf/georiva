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
import os
import shutil
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# The app reads these at import time, so they are pinned before any test
# module can pull in ``app.main``.
_STORAGE_ROOT = tempfile.mkdtemp(prefix="titiler-test-storage-")
os.environ["MINIO_HOST"] = _STORAGE_ROOT
os.environ["MINIO_BUCKET_NAME"] = "georiva-assets"
os.environ["REDIS_URL"] = "redis://never-connected:6379/0"
os.environ["DJANGO_BASE_URL"] = "http://never-connected:8000"

import numpy
import pytest
import rasterio
from fastapi.testclient import TestClient
from rasterio.transform import from_bounds

from app import dependencies
from app.main import app

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


class FakeRedis:
    """The one method the app calls on its Redis client."""

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)


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
    unknown variable or style.
    """
    responses = {}

    def fetch(org, catalog, collection, variable, style=None):
        return responses.get((org, catalog, collection, variable, style), (404, None))

    monkeypatch.setattr(dependencies, "_fetch_config_from_django", fetch)
    return responses


@pytest.fixture
def client(fake_redis, django_configs):
    """A TestClient over the real app with both network edges already faked."""
    return TestClient(app)


@pytest.fixture
def seed_cog():
    """Write a small COG at the storage path the app will derive for a time.

    Uses the same filename grammar as ``build_cog_url`` on purpose: a test
    that seeds ``time=X`` and requests ``TIME=X`` exercises the real
    time-to-key derivation end to end.
    """

    def write(time="2026-03-23T12:00:00Z", reftime=None,
              org=ORG, catalog=CATALOG, collection=COLLECTION, variable=VARIABLE):
        time_dt = datetime.fromisoformat(time.replace("Z", "+00:00")).astimezone(timezone.utc)
        date_path = time_dt.strftime("%Y/%m/%d")
        time_str = time_dt.strftime("%H%M%S")
        if reftime:
            ref_dt = datetime.fromisoformat(reftime.replace("Z", "+00:00")).astimezone(timezone.utc)
            filename = f"{variable}_{time_str}__ref{ref_dt.strftime('%Y%m%dT%H%M%S')}.tif"
        else:
            filename = f"{variable}_{time_str}.tif"

        path = os.path.join(
            _STORAGE_ROOT, os.environ["MINIO_BUCKET_NAME"],
            org, catalog, collection, variable, date_path, filename,
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)

        bound = 20037508.342789244  # WebMercator world extent
        data = numpy.linspace(0, 50, 64 * 64, dtype="float32").reshape(64, 64)
        with rasterio.open(
            path, "w", driver="GTiff", width=64, height=64, count=1,
            dtype="float32", crs="EPSG:3857",
            transform=from_bounds(-bound, -bound, bound, bound, 64, 64),
        ) as dst:
            dst.write(data, 1)
        return path

    return write
