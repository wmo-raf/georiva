"""A Remote asset is tiled from upstream, by range (ADR 0043 §3, #638).

A harvested asset's bytes are not in our storage: they are at an upstream URL
that no path grammar leads to. Django publishes that URL in Redis and answers
for it on the tile-config callback; this service reads it and hands it to GDAL.

So these tests serve a COG over real local HTTP, at an address shaped like an
upstream's and nothing like ours, and ask the ordinary routes for it. The
upstream COG and the one in our storage hold different values wherever both
exist, so the value a route answers says which bytes it read.
"""

import json
import uuid

import numpy
import pytest

from tests.conftest import (
    CATALOG,
    COG_SIZE,
    COLLECTION,
    ORG,
    TILE_CONFIG,
    VARIABLE,
    cog_bytes,
    kvp,
)

TIME = "2026-03-23T12:00:00Z"
REFTIME = "2026-03-23T00:00:00Z"
ROUTE = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}"
HASH = f"georiva:remote-assets:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"
PALETTE = f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"

#: What every cell of the upstream COG holds. The seeded local ramp tops out at 50.
UPSTREAM_VALUE = 77.0


@pytest.fixture
def upstream(store):
    """An upstream object host holding one COG, at a path of its own choosing."""

    class Upstream:
        def __init__(self):
            # Unique per test: GDAL's caches are per process, not per test.
            self.path = f"chirps/v2/{uuid.uuid4().hex}/chirps.2026.03.23.tif"
            self.href = store.url_for(self.path)
            store.write(
                self.path,
                cog_bytes(data=numpy.full((COG_SIZE, COG_SIZE), UPSTREAM_VALUE, dtype="float32")),
            )

        def was_read(self) -> bool:
            return self.path in store.requests

    return Upstream()


@pytest.fixture
def rendering(fake_redis):
    """The variable renders: its config is in Redis, as Django warms it."""
    fake_redis.store[PALETTE] = json.dumps(TILE_CONFIG)
    return fake_redis


def publish(fake_redis, fields):
    """What Django publishes for the variable: its Remote timesteps, and the marker."""
    fake_redis.hashes[HASH] = {**fields, "published": "1"}


def point_value(client, **query):
    response = client.get(f"{ROUTE}/point/0,0", params={"time": TIME, **query})
    assert response.status_code == 200, response.text
    return response.json()["values"][0]


class TestARemoteTimestepIsReadFromUpstream:
    def test_a_tile_is_rendered_from_the_upstream_cog(self, client, rendering, upstream):
        publish(rendering, {"20260323T120000": upstream.href})

        response = client.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "image/png"
        assert upstream.was_read()

    def test_the_value_under_a_point_is_the_upstream_s(self, client, rendering, upstream):
        publish(rendering, {"20260323T120000": upstream.href})

        assert point_value(client) == UPSTREAM_VALUE

    def test_a_forecast_timestep_is_found_by_its_time_and_reference_time(self, client, rendering, upstream):
        publish(rendering, {"20260323T120000/20260323T000000": upstream.href})

        assert point_value(client, reftime=REFTIME) == UPSTREAM_VALUE

    def test_the_encoded_texture_is_read_from_upstream(self, client, rendering, upstream):
        publish(rendering, {"20260323T120000": upstream.href})

        response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert upstream.was_read()

    def test_a_wmts_gettile_is_read_from_upstream(self, client, rendering, upstream):
        publish(rendering, {"20260323T120000": upstream.href})

        response = client.get(f"/{ORG}/wmts", params=kvp(TIME=TIME))

        assert response.status_code == 200, response.text
        assert upstream.was_read()

    def test_an_upstream_that_no_longer_has_the_object_is_a_404(self, client, rendering, store):
        publish(rendering, {"20260323T120000": store.url_for(f"gone/{uuid.uuid4().hex}.tif")})

        response = client.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": TIME})

        assert response.status_code == 404
        assert response.headers["cache-control"] == "no-store"


class TestAMixedCollection:
    """Some timesteps local, some Remote: each is read from where it is."""

    def test_a_local_timestep_is_read_from_our_storage(self, client, rendering, upstream, seed_cog, django_configs):
        seed_cog(time=TIME)
        publish(rendering, {"20260324T120000": upstream.href})

        assert point_value(client) != UPSTREAM_VALUE
        assert not upstream.was_read()

    def test_django_is_not_asked_about_a_timestep_redis_has_settled(self, client, rendering, seed_cog, django_configs):
        seed_cog(time=TIME)
        publish(rendering, {})

        point_value(client)

        assert django_configs.asked == []

    def test_the_remote_timestep_beside_it_is_read_from_upstream(self, client, rendering, upstream, seed_cog):
        seed_cog(time=TIME)
        publish(rendering, {"20260324T120000": upstream.href})

        assert point_value(client, time="2026-03-24T12:00:00Z") == UPSTREAM_VALUE


class TestRedisHasNotBeenTold:
    """No published hash — a flushed Redis, or a variable not yet warmed — so
    Django is asked, on the tile-config callback, for this one timestep."""

    ADDRESS = (ORG, CATALOG, COLLECTION, VARIABLE, None)

    def test_django_s_answer_names_the_upstream_and_it_is_read(self, client, django_configs, upstream):
        django_configs[self.ADDRESS] = (200, {**TILE_CONFIG, "remote_href": upstream.href})

        assert point_value(client, reftime=REFTIME) == UPSTREAM_VALUE
        assert (TIME, REFTIME) in django_configs.asked

    def test_an_answer_naming_no_upstream_means_our_storage(self, client, django_configs, upstream, seed_cog):
        seed_cog(time=TIME)
        django_configs[self.ADDRESS] = (200, TILE_CONFIG)

        assert point_value(client) != UPSTREAM_VALUE
        assert (TIME, None) in django_configs.asked

    def test_a_variable_django_does_not_answer_for_falls_back_to_our_storage(self, client, rendering, seed_cog):
        # A private variable: its config is in Redis, the callback answers 404.
        seed_cog(time=TIME)

        assert point_value(client) != UPSTREAM_VALUE


class TestTheAddressIsOnlyEverAUrl:
    """The href is handed to GDAL, which opens local paths as readily as URLs."""

    @pytest.mark.parametrize("href", ["/etc/passwd", "file:///etc/passwd", "/vsimem/x.tif", "s3://bucket/x.tif"])
    def test_an_address_that_is_not_http_is_refused(self, client, rendering, href):
        publish(rendering, {"20260323T120000": href})

        response = client.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": TIME})

        assert response.status_code == 502
