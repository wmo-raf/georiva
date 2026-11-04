"""The two-band vector texture (#593, ADR 0021 amended).

One RGBA PNG for a u/v pair: R = u, G = v, both rescaled over the *same* range
(the union of the two variables' configured ranges) so that the angle decoded
from the pixel is the angle in the source data. Alpha is 0 wherever either
component is missing or the catalog's display crop masks it.
"""

import json
import math

import numpy as np
from rasterio.io import MemoryFile

from tests.conftest import CATALOG, COLLECTION, ORG, TILE_CONFIG, WORLD_EXTENT
from tests.test_display_boundary import WEST_HALF

PATH = f"/{ORG}/{CATALOG}/{COLLECTION}/vector-preview.png"
QUERY = {"u_variable": "u10", "v_variable": "v10", "time": "2026-03-23T12:00:00Z"}

#: Different ranges on purpose: the texture must use the union, -20..10.
U_RANGE, V_RANGE = (-10.0, 10.0), (-20.0, 5.0)
LO, HI = -20.0, 10.0

SIZE = 32


def key(variable):
    return f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{variable}"


def config(vrange, **extra):
    return json.dumps({**TILE_CONFIG, "vmin": vrange[0], "vmax": vrange[1], **extra})


def seed_pair(
    fake_redis, seed_cog, u, v, boundary=None, u_nodata=None, v_nodata=None, v_bounds=None, bounds=None, reftime=None
):
    extra = {"boundary": boundary} if boundary else {}
    fake_redis.store[key("u10")] = config(U_RANGE, **extra)
    fake_redis.store[key("v10")] = config(V_RANGE, **extra)
    seed_cog(variable="u10", data=u.astype("float32"), nodata=u_nodata, bounds=bounds, reftime=reftime)
    seed_cog(variable="v10", data=v.astype("float32"), nodata=v_nodata, bounds=v_bounds or bounds, reftime=reftime)


def decode(png):
    """(u, v, alpha) with u and v unscaled the way a client does."""
    with MemoryFile(png) as memfile, memfile.open() as src:
        assert src.count == 4
        r, g, b, a = (src.read(i) for i in range(1, 5))
    scale = (HI - LO) / 255
    return LO + r * scale, LO + g * scale, b, a


def ramp(lo, hi):
    return np.linspace(lo, hi, SIZE * SIZE, dtype="float32").reshape(SIZE, SIZE)


class TestTheTexture:
    def test_a_pair_decodes_to_the_source_components(self, client, fake_redis, seed_cog):
        u, v = ramp(-10, 10), np.full((SIZE, SIZE), -15.0)
        seed_pair(fake_redis, seed_cog, u, v)

        response = client.get(PATH, params=QUERY)

        assert response.status_code == 200
        du, dv, blue, alpha = decode(response.content)
        assert np.abs(du - u).max() < 0.2
        assert np.abs(dv - v).max() < 0.2
        assert (blue == 0).all()
        assert (alpha == 255).all()

    def test_direction_is_the_source_direction(self, client, fake_redis, seed_cog):
        """Both bands share one range, so atan2 of the decoded pair is atan2 of the data.

        Encoding each over its own range would bend every angle off the axes.
        """
        u, v = np.full((SIZE, SIZE), 6.0), np.full((SIZE, SIZE), 6.0)
        seed_pair(fake_redis, seed_cog, u, v)

        du, dv, _, _ = decode(client.get(PATH, params=QUERY).content)

        angle = math.degrees(math.atan2(dv[5, 5], du[5, 5]))
        assert abs(angle - 45.0) < 1.5

    def test_the_response_declares_the_shared_range(self, client, fake_redis, seed_cog):
        seed_pair(fake_redis, seed_cog, ramp(-10, 10), ramp(-20, 5))

        response = client.get(PATH, params=QUERY)

        assert response.headers["x-image-unscale"] == f"{LO},{HI}"
        assert "immutable" in response.headers["cache-control"]

    def test_it_works_for_a_regional_grid(self, client, fake_redis, seed_cog):
        region = (3_000_000, -500_000, 4_500_000, 600_000)
        u, v = np.full((SIZE, SIZE), 5.0), np.full((SIZE, SIZE), -5.0)
        seed_pair(fake_redis, seed_cog, u, v, bounds=region)

        du, dv, _, alpha = decode(client.get(PATH, params=QUERY).content)

        assert abs(du[3, 3] - 5.0) < 0.2 and abs(dv[3, 3] + 5.0) < 0.2
        assert (alpha == 255).all()


class TestWhereThereIsNothing:
    def test_a_missing_component_blanks_the_pixel(self, client, fake_redis, seed_cog):
        u, v = np.full((SIZE, SIZE), 1.0), np.full((SIZE, SIZE), 1.0)
        v[0, 0] = -9999
        seed_pair(fake_redis, seed_cog, u, v, v_nodata=-9999)

        _, _, _, alpha = decode(client.get(PATH, params=QUERY).content)

        assert alpha[0, 0] == 0
        assert alpha[1, 1] == 255

    def test_the_display_crop_applies(self, client, fake_redis, seed_cog):
        u, v = np.full((SIZE, SIZE), 1.0), np.full((SIZE, SIZE), 1.0)
        seed_pair(fake_redis, seed_cog, u, v, boundary=WEST_HALF)

        _, _, _, alpha = decode(client.get(PATH, params=QUERY).content)

        assert alpha[SIZE // 2, SIZE // 4] == 255
        assert alpha[SIZE // 2, 3 * SIZE // 4] == 0


class TestRefusals:
    def test_grids_that_differ_are_refused(self, client, fake_redis, seed_cog):
        u = np.full((SIZE, SIZE), 1.0)
        v = np.full((SIZE // 2, SIZE // 2), 1.0)
        seed_pair(fake_redis, seed_cog, u, v)

        response = client.get(PATH, params=QUERY)

        assert response.status_code == 422
        assert "grid" in response.json()["detail"].lower()

    def test_bounds_that_differ_are_refused(self, client, fake_redis, seed_cog):
        u = v = np.full((SIZE, SIZE), 1.0)
        seed_pair(fake_redis, seed_cog, u, v, v_bounds=(0, 0, WORLD_EXTENT, WORLD_EXTENT))

        assert client.get(PATH, params=QUERY).status_code == 422

    def test_both_variables_are_required(self, client, fake_redis, seed_cog):
        for name in ("u_variable", "v_variable", "time"):
            params = {k: v for k, v in QUERY.items() if k != name}
            assert client.get(PATH, params=params).status_code == 422

    def test_an_unknown_variable_is_not_found(self, client, fake_redis, seed_cog):
        fake_redis.store[key("u10")] = config(U_RANGE)

        assert client.get(PATH, params=QUERY).status_code == 404

    def test_one_variable_twice_is_refused(self, client, fake_redis, seed_cog):
        seed_pair(fake_redis, seed_cog, ramp(-10, 10), ramp(-20, 5))

        response = client.get(PATH, params={**QUERY, "v_variable": "u10"})

        assert response.status_code == 422

    def test_a_component_without_a_range_is_refused_not_a_crash(self, client, fake_redis, seed_cog):
        seed_pair(fake_redis, seed_cog, ramp(-10, 10), ramp(-20, 5))
        fake_redis.store[key("v10")] = json.dumps({**TILE_CONFIG, "vmin": None, "vmax": None})

        assert client.get(PATH, params=QUERY).status_code == 422

    def test_a_missing_component_cog_is_not_found(self, client, fake_redis, seed_cog):
        seed_pair(fake_redis, seed_cog, ramp(-10, 10), ramp(-20, 5))

        assert client.get(PATH, params={**QUERY, "time": "2026-03-23T18:00:00Z"}).status_code == 404


class TestAForecastRun:
    def test_the_reference_time_selects_the_cogs(self, client, fake_redis, seed_cog):
        u, v = np.full((SIZE, SIZE), 2.0), np.full((SIZE, SIZE), 2.0)
        seed_pair(fake_redis, seed_cog, u, v, reftime="2026-03-23T00:00:00Z")

        with_ref = client.get(PATH, params={**QUERY, "reftime": "2026-03-23T00:00:00Z"})
        without = client.get(PATH, params=QUERY)

        assert with_ref.status_code == 200
        assert without.status_code == 404
