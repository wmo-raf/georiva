"""Values become codes through the Variable's encoding curve (#787, spec #784).

Django hands this service a Variable's ``curve`` beside its range: ``linear``,
``sqrt`` or ``log``. Wherever a value becomes one of the 256 codes, the curve
decides which: the encoded texture holds ``encode(value)``, and a tile or a
WMTS GetTile paints that code's colour from the payload's colour table.

Each test seeds a field holding one value, so every pixel is that value
whatever the resampling, and reads one pixel back. The expected codes are
worked by hand from ``code = int(255 · f(t))``.
"""

import json

import numpy
import pytest
from rasterio.io import MemoryFile

from tests.conftest import CATALOG, COG_SIZE, COLLECTION, ORG, TILE_CONFIG, VARIABLE, kvp

TIME = "2026-03-23T12:00:00Z"
ROUTE = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}"

#: (curve, vmin, vmax, value, code). Every code sits well inside its truncation
#: step, so no rounding can move it.
CASES = [
    # t = 0.09 → 22.95
    ("linear", 0.0, 100.0, 9.0, 22),
    # √0.09 = 0.3 → 76.5
    ("sqrt", 0.0, 100.0, 9.0, 76),
    # log(10/1) / log(100/1) = 0.5 → 127.5
    ("log", 1.0, 100.0, 10.0, 127),
]


def pixels(png):
    with MemoryFile(png) as memfile, memfile.open() as src:
        return src.read()


@pytest.fixture
def layer(fake_redis, seed_cog):
    """A Variable of one value, rendered with ``curve`` over ``vmin``…``vmax``."""

    def make(curve, vmin, vmax, value):
        config = {**TILE_CONFIG, "vmin": vmin, "vmax": vmax, "curve": curve}
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(config)
        seed_cog(data=numpy.full((COG_SIZE, COG_SIZE), value, dtype="float32"))
        return config

    return make


@pytest.mark.parametrize(("curve", "vmin", "vmax", "value", "code"), CASES)
class TestAValueBecomesItsCurvesCode:
    def test_the_encoded_texture_holds_the_code(self, client, layer, curve, vmin, vmax, value, code):
        layer(curve, vmin, vmax, value)

        response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert pixels(response.content)[0, 0, 0] == code

    def test_the_encoded_texture_declares_its_curve(self, client, layer, curve, vmin, vmax, value, code):
        layer(curve, vmin, vmax, value)

        response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME})

        assert response.headers["X-Image-Curve"] == curve
        assert response.headers["X-Image-Unscale"] == f"{vmin},{vmax}"

    def test_a_tile_paints_the_codes_colour(self, client, layer, curve, vmin, vmax, value, code):
        config = layer(curve, vmin, vmax, value)

        response = client.get(f"{ROUTE}/tiles/WebMercatorQuad/0/0/0.png", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert list(pixels(response.content)[:, 128, 128]) == config["colormap"][str(code)]

    def test_a_wmts_get_tile_paints_the_codes_colour(self, client, layer, curve, vmin, vmax, value, code):
        config = layer(curve, vmin, vmax, value)

        response = client.get(f"/{ORG}/wmts", params=kvp())

        assert response.status_code == 200, response.text
        assert list(pixels(response.content)[:, 128, 128]) == config["colormap"][str(code)]


class TestTheCurveShapesTheTexture:
    def test_sqrt_and_log_textures_differ_from_linear(self, client, layer):
        textures = {}
        for curve in ("linear", "sqrt", "log"):
            layer(curve, 1.0, 100.0, 10.0)
            textures[curve] = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME}).content

        assert textures["sqrt"] != textures["linear"]
        assert textures["log"] != textures["linear"]

    def test_a_config_from_before_curves_encodes_linearly(self, client, fake_redis, seed_cog):
        config = {**TILE_CONFIG, "vmin": 0.0, "vmax": 100.0}
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(config)
        seed_cog(data=numpy.full((COG_SIZE, COG_SIZE), 9.0, dtype="float32"))

        response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME})

        assert pixels(response.content)[0, 0, 0] == 22
        assert response.headers["X-Image-Curve"] == "linear"

    def test_values_outside_the_range_take_the_end_codes(self, client, layer):
        for value, code in ((0.5, 0), (500.0, 255)):
            layer("log", 1.0, 100.0, value)

            response = client.get(f"{ROUTE}/encoded-preview.png", params={"time": TIME})

            assert pixels(response.content)[0, 0, 0] == code, value
