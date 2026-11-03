"""A map cropped to a boundary, and only the map (ADR 0038).

A catalog in ``display`` clip mode stores whole data and sends the tile server an
outline in the tile config. What is pinned here: the tile is transparent outside
it and opaque inside, a click answers what the map shows, a layer without an
outline is untouched, and the outline projected into a tile's CRS lands where
the geometry says rather than where its degrees would.
"""

import json

import numpy as np
from app.boundary import contains, inside_mask
from rasterio.io import MemoryFile

from tests.conftest import CATALOG, COLLECTION, KVP_BASE, ORG, TILE_CONFIG, VARIABLE, overriding

#: Everything west of the prime meridian, as a GeoJSON polygon. The cropped half
#: of a world-sized tile is then the right half, which a test can read off.
WEST_HALF = {
    "type": "Polygon",
    "coordinates": [[[-180, -80], [0, -80], [0, 80], [-180, 80], [-180, -80]]],
}

KEY = f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"
GFI = overriding(KVP_BASE, REQUEST="GetFeatureInfo", INFOFORMAT="application/json", I="64", J="128")


def seed(fake_redis, seed_cog, boundary=None):
    config = dict(TILE_CONFIG)
    if boundary is not None:
        config["boundary"] = boundary
    fake_redis.store[KEY] = json.dumps(config)
    return seed_cog()


def alpha_of(png: bytes) -> np.ndarray:
    """The alpha channel: the last band, whether the PNG is RGBA (a tile) or value+alpha (a texture)."""
    with MemoryFile(png) as memfile, memfile.open() as src:
        return src.read(src.count)


def identify(client, i):
    return client.get(f"/{ORG}/wmts", params=overriding(GFI, I=str(i))).json()["value"]


class TestContains:
    def test_a_point_inside_a_polygon_is_inside(self):
        assert contains(WEST_HALF, -90, 0)

    def test_a_point_outside_is_not(self):
        assert not contains(WEST_HALF, 90, 0)

    def test_a_hole_is_outside(self):
        ring = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
        hole = [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]
        polygon = {"type": "Polygon", "coordinates": [ring, hole]}

        assert contains(polygon, 1, 1)
        assert not contains(polygon, 5, 5)

    def test_any_part_of_a_multipolygon_counts(self):
        multi = {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
            ],
        }

        assert contains(multi, 10.5, 10.5)
        assert not contains(multi, 5, 5)

    def test_something_that_is_not_an_area_contains_nothing(self):
        assert not contains({"type": "Point", "coordinates": [0, 0]}, 0, 0)


class TestInsideMask:
    def test_a_tile_in_the_outlines_own_crs_splits_where_the_outline_does(self):
        # Eight degrees wide, outline covers the western half.
        mask = inside_mask(
            {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 8], [0, 8], [0, 0]]]},
            (0, 0, 8, 8),
            "EPSG:4326",
            (8, 8),
        )

        assert mask[:, :3].all()
        assert not mask[:, 5:].any()

    def test_the_outline_is_projected_into_a_mercator_tile(self):
        mask = inside_mask(WEST_HALF, (-20037508.34, -20037508.34, 20037508.34, 20037508.34), "EPSG:3857", (64, 64))

        # The prime meridian is the middle column of a world tile, in either CRS.
        assert mask[32, :30].all()
        assert not mask[32, 34:].any()


class TestTheMapIsCropped:
    def test_the_tile_is_transparent_outside_and_opaque_inside(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog, boundary=WEST_HALF)

        alpha = alpha_of(client.get(f"/{ORG}/wmts", params=KVP_BASE).content)

        assert alpha[128, 64] == 255
        assert alpha[128, 192] == 0

    def test_a_layer_without_an_outline_is_drawn_whole(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        alpha = alpha_of(client.get(f"/{ORG}/wmts", params=KVP_BASE).content)

        assert alpha[128, 64] == 255
        assert alpha[128, 192] == 255


class TestAClickFollowsTheMap:
    def test_a_click_inside_reads_the_value(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog, boundary=WEST_HALF)

        assert identify(client, 64) is not None

    def test_a_click_outside_says_no_data(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog, boundary=WEST_HALF)

        assert identify(client, 192) is None

    def test_without_an_outline_the_same_click_reads_the_value(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        assert identify(client, 192) is not None


class TestTheEncodedTextureIsCroppedToo:
    """``encoded-preview.png`` reads the COG without going through the tile algorithm.

    It is what WeatherLayers draws and reads hover values from, so a crop that
    stopped at the tiles would leave the whole extent live on every layer built on it.
    """

    PATH = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/encoded-preview.png"
    QUERY = {"time": "2026-03-23T12:00:00Z"}

    def test_the_texture_is_transparent_outside_and_opaque_inside(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog, boundary=WEST_HALF)

        response = client.get(self.PATH, params=self.QUERY)

        assert response.status_code == 200
        alpha = alpha_of(response.content)
        height, width = alpha.shape
        assert alpha[height // 2, width // 4] == 255
        assert alpha[height // 2, 3 * width // 4] == 0

    def test_without_an_outline_the_whole_extent_is_encoded(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        alpha = alpha_of(client.get(self.PATH, params=self.QUERY).content)

        assert (alpha == 255).all()

    def test_the_values_inside_are_the_cogs_own(self, client, fake_redis, seed_cog):
        """Cropping masks pixels; it does not touch the ones it keeps."""
        seed(fake_redis, seed_cog, boundary=WEST_HALF)
        whole = client.get(self.PATH, params=self.QUERY)
        fake_redis.store[KEY] = json.dumps(TILE_CONFIG)
        uncropped = client.get(self.PATH, params=self.QUERY)

        def red(png):
            with MemoryFile(png) as memfile, memfile.open() as src:
                return src.read(1)

        cropped_red, whole_red = red(whole.content), red(uncropped.content)
        height, width = cropped_red.shape
        # Inside the outline both ways: the west half, clear of the poles it stops short of.
        inside = (slice(height // 4, 3 * height // 4), slice(0, width // 2 - 2))
        assert (cropped_red[inside] == whole_red[inside]).all()
