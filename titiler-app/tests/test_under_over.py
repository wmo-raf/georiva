"""A transparent under or over leaves values beyond a style's stops undrawn (#788).

Django carries a style's under and over into its colour table as alpha-0
entries, and this service colours a tile code by code from that table. So a
pixel whose value lies below the first stop of a style with a transparent under
is see-through in a tile and in a WMTS GetTile alike: the basemap shows.
"""

import json

import numpy as np
from rasterio.io import MemoryFile

from tests.conftest import CATALOG, COG_SIZE, COLLECTION, ORG, TILE_CONFIG, VARIABLE, kvp

TIME = "2026-03-23T12:00:00Z"
KEY = f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"

#: The north half holds 5, below the style's first stop at 10; the south half 30, inside its stops.
DATA = np.full((COG_SIZE, COG_SIZE), 30.0, dtype="float32")
DATA[: COG_SIZE // 2] = 5.0

#: The table of a style over 0…50 whose stops start at 10 (code 51) with a
#: transparent under: alpha 0 below, red from there on.
UNDER_TRANSPARENT = {
    **TILE_CONFIG,
    "colormap": {str(code): ([0, 0, 0, 0] if code < 51 else [255, 0, 0, 255]) for code in range(256)},
}


def rgba(png: bytes) -> np.ndarray:
    with MemoryFile(png) as memfile, memfile.open() as src:
        return src.read()


def seed(fake_redis, seed_cog):
    fake_redis.store[KEY] = json.dumps(UNDER_TRANSPARENT)
    seed_cog(data=DATA)


def assert_north_undrawn_south_drawn(png: bytes):
    bands = rgba(png)
    assert bands.shape[0] == 4
    alpha = bands[3]
    side = alpha.shape[0]
    # Away from the seam between the halves, which resampling may blend.
    assert (alpha[: side // 2 - 8] == 0).all()
    assert (alpha[side // 2 + 8 :] == 255).all()
    assert (bands[0][side // 2 + 8 :] == 255).all()


class TestTransparentUnder:
    def test_a_pixel_below_the_first_stop_is_transparent_in_a_tile(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        route = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/tiles/WebMercatorQuad/0/0/0.png"
        tile = client.get(route, params={"time": TIME})

        assert tile.status_code == 200
        assert_north_undrawn_south_drawn(tile.content)

    def test_a_pixel_below_the_first_stop_is_transparent_in_a_wmts_gettile(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        tile = client.get(f"/{ORG}/wmts", params=kvp())

        assert tile.status_code == 200
        assert_north_undrawn_south_drawn(tile.content)
