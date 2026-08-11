"""KVP GetFeatureInfo — the identify click, answered as JSON (#363).

Everything here is behavior at the HTTP boundary. Three things are worth
pinning and each has a suite: the number returned is the COG's own physical
value at the pixel that was clicked, the pixel-to-lon/lat conversion is right
at the grid's edges and at every zoom, and a click that finds nothing answers
an empty-but-valid body rather than an error — legacy identify tools read an
error as a broken layer.

The coordinate assertions are made against a second, independent
implementation of the spherical-Mercator formulae written below, not against
the library the shim uses: an assertion that calls the code under test to work
out what to expect only proves it is consistent with itself.
"""
import json
import math

import pytest

from tests.conftest import (
    CATALOG, COG_SIZE, COG_VALUES, COLLECTION, KVP_BASE, ORG, TILE_CONFIG,
    VARIABLE, WORLD_EXTENT, exception_of, overriding,
)

TIME = "2026-03-23T12:00:00Z"
LAYER = f"{CATALOG}:{COLLECTION}:{VARIABLE}"

#: A complete, valid GetFeatureInfo query — the GetTile one with the identify
#: parameters put in place of the ones only a tile needs.
GFI_BASE = overriding(
    KVP_BASE,
    REQUEST="GetFeatureInfo",
    INFOFORMAT="application/json",
    I="128",
    J="128",
)

#: The tile side in pixels, on every matrix of WebMercatorQuad.
TILE_SIZE = 256

#: The sphere the WebMercator formulae below are written on, matching the
#: WebMercatorQuad definition the service serves tiles from.
EARTH_RADIUS = 6378137.0


def gfi(**overrides):
    """``GFI_BASE`` with parameters replaced, or removed via ``NAME=None``."""
    return overriding(GFI_BASE, **overrides)


def lonlat_of(zoom, col, row, i, j):
    """Where pixel ``(i, j)`` of tile ``(col, row, zoom)`` is, from first principles."""
    resolution = 2 * WORLD_EXTENT / (TILE_SIZE * 2 ** zoom)
    x = -WORLD_EXTENT + (col * TILE_SIZE + i + 0.5) * resolution
    y = WORLD_EXTENT - (row * TILE_SIZE + j + 0.5) * resolution
    lon = math.degrees(x / EARTH_RADIUS)
    lat = math.degrees(2 * math.atan(math.exp(y / EARTH_RADIUS)) - math.pi / 2)
    return lon, lat


def address_of(lon, lat, zoom):
    """The tile and pixel a place falls in at ``zoom`` — the inverse of the above."""
    x = math.radians(lon) * EARTH_RADIUS
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * EARTH_RADIUS
    across = TILE_SIZE * 2 ** zoom
    col, i = divmod(int((x + WORLD_EXTENT) / (2 * WORLD_EXTENT) * across), TILE_SIZE)
    row, j = divmod(int((WORLD_EXTENT - y) / (2 * WORLD_EXTENT) * across), TILE_SIZE)
    return col, row, i, j


def seeded_value(zoom, col, row, i, j):
    """The seeded ramp's value at that pixel.

    Both grids are linear in WebMercator metres over the same world extent, so
    which cell of the 64×64 ramp a tile pixel falls in is a matter of counting
    pixels — no projection maths, and so no chance of agreeing with the code
    under test by sharing its mistake.
    """
    across = TILE_SIZE * 2 ** zoom
    cell_col = int((col * TILE_SIZE + i + 0.5) * COG_SIZE / across)
    cell_row = int((row * TILE_SIZE + j + 0.5) * COG_SIZE / across)
    return COG_VALUES[cell_row][cell_col]


def seed_default(fake_redis, seed_cog, **cog_kwargs):
    fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)
    return seed_cog(**cog_kwargs)


def identify(client, **overrides):
    return client.get(f"/{ORG}/wmts", params=gfi(**overrides))


class TestValueRead:
    def test_the_value_is_the_cogs_own_at_the_clicked_pixel(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client)

        assert response.status_code == 200
        assert response.json()["value"] == pytest.approx(
            seeded_value(0, 0, 0, 128, 128), rel=1e-6,
        )

    def test_the_value_is_physical_not_the_number_the_pixel_was_painted_with(
            self, client, fake_redis, seed_cog):
        """The rendering config rescales 0–50 onto 0–255 for a tile; identify
        reads past all of that, or a forecaster would be told the temperature
        is 129 degrees."""
        seed_default(fake_redis, seed_cog)
        raw = seeded_value(0, 0, 0, 128, 128)

        value = identify(client).json()["value"]

        assert value == pytest.approx(raw, rel=1e-6)
        rendered = raw / TILE_CONFIG["vmax"] * 255
        assert value != pytest.approx(rendered, rel=1e-3)

    def test_neighbouring_pixels_read_different_values(self, client, fake_redis, seed_cog):
        """A read one cell off would still look plausible — the ramp is smooth
        — so the pixel actually addressed is pinned by moving across a cell
        boundary and seeing the answer move with it."""
        seed_default(fake_redis, seed_cog)
        step = TILE_SIZE // COG_SIZE

        here = identify(client, I="128").json()["value"]
        next_cell = identify(client, I=str(128 + step)).json()["value"]

        assert here != pytest.approx(next_cell, rel=1e-6)
        assert next_cell == pytest.approx(
            seeded_value(0, 0, 0, 128 + step, 128), rel=1e-6,
        )

    def test_the_answer_echoes_the_request_it_belongs_to(self, client, fake_redis, seed_cog):
        reftime = "2026-03-23T00:00:00Z"
        seed_default(fake_redis, seed_cog, reftime=reftime)

        response = identify(client, REFTIME=reftime)

        body = response.json()
        assert body["layer"] == LAYER
        assert body["time"] == TIME
        assert body["reftime"] == reftime
        lon, lat = lonlat_of(0, 0, 0, 128, 128)
        assert body["longitude"] == pytest.approx(lon)
        assert body["latitude"] == pytest.approx(lat)

    def test_an_observation_click_echoes_a_null_reftime(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        assert identify(client).json()["reftime"] is None

    def test_the_answer_is_json(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client)

        assert response.headers["content-type"].startswith("application/json")

    def test_units_are_carried_only_when_the_tile_config_already_holds_them(
            self, client, fake_redis, seed_cog):
        """No new metadata plumbing (#363): the answer says what the
        per-request rendering payload says, and stays silent otherwise."""
        seed_default(fake_redis, seed_cog)
        assert "units" not in identify(client).json()

        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(
            dict(TILE_CONFIG, units="degC", label="2m Temperature"),
        )

        body = identify(client).json()
        assert body["units"] == "degC"
        assert body["label"] == "2m Temperature"


class TestCoordinateMath:
    @pytest.mark.parametrize(
        "zoom,col,row,i,j",
        [
            (0, 0, 0, 0, 0),        # the north-west corner of the world
            (0, 0, 0, 255, 255),    # and its south-east one
            (0, 0, 0, 128, 128),    # the middle, where the meridians cross
            (1, 1, 0, 0, 0),        # the first pixel east of Greenwich
            (3, 7, 7, 255, 255),    # the last pixel of the deepest corner tile
            (12, 2400, 2100, 17, 200),
        ],
    )
    def test_the_pixel_lands_where_the_mercator_formulae_put_it(
            self, client, fake_redis, seed_cog, zoom, col, row, i, j):
        seed_default(fake_redis, seed_cog)

        body = identify(
            client, TILEMATRIX=str(zoom), TILECOL=str(col), TILEROW=str(row),
            I=str(i), J=str(j),
        ).json()

        lon, lat = lonlat_of(zoom, col, row, i, j)
        assert body["longitude"] == pytest.approx(lon, abs=1e-9)
        assert body["latitude"] == pytest.approx(lat, abs=1e-9)

    def test_the_grid_edges_stay_inside_the_world(self, client, fake_redis, seed_cog):
        """Half a pixel in from either edge, never past it: a click on the
        outermost pixel of level 0 must still name a real place."""
        seed_default(fake_redis, seed_cog)

        west = identify(client, I="0", J="0").json()
        east = identify(client, I="255", J="255").json()

        # Half a level-0 pixel is 1.40625° of longitude, and the grid stops at
        # ±85.0511°, so both centres sit just inside those limits.
        assert -180 < west["longitude"] < -179.29
        assert 84.9 < west["latitude"] < 85.0511
        assert 179.29 < east["longitude"] < 180
        assert -85.0511 < east["latitude"] < -84.9

    @pytest.mark.parametrize("zoom", [0, 3, 6, 12])
    def test_a_place_reads_the_same_value_at_every_zoom(
            self, client, fake_redis, seed_cog, zoom):
        """The same spot, addressed through four different tile matrices —
        the conversion has to agree with itself across the pyramid or an
        identify would answer differently as the user zooms."""
        seed_default(fake_redis, seed_cog)
        # Comfortably inside one cell of the seeded ramp, so sub-pixel
        # differences between zooms cannot tip the read into a neighbour.
        lon, lat = lonlat_of(6, 42, 33, 128, 128)
        col, row, i, j = address_of(lon, lat, zoom)

        body = identify(
            client, TILEMATRIX=str(zoom), TILECOL=str(col), TILEROW=str(row),
            I=str(i), J=str(j),
        ).json()

        assert body["value"] == pytest.approx(seeded_value(6, 42, 33, 128, 128), rel=1e-6)
        # Each matrix can only answer to its own resolution, so the place
        # comes back within a pixel of itself — coarse at level 0, fine at 12.
        pixel = 360 / (TILE_SIZE * 2 ** zoom)
        assert body["longitude"] == pytest.approx(lon, abs=pixel)
        assert body["latitude"] == pytest.approx(lat, abs=pixel)


class TestNothingUnderTheCursor:
    """A well-formed click that finds no value answers 200 and a null.

    An exception here would be read by legacy identify tools as a broken
    layer, and the client did nothing wrong: it asked about a real place on a
    real layer and the honest answer is that there is nothing there.
    """

    def test_a_nodata_pixel_answers_an_empty_but_valid_body(self, client, fake_redis, seed_cog):
        blanked = COG_VALUES.copy()
        blanked[:, :] = -9999.0
        seed_default(fake_redis, seed_cog, data=blanked, nodata=-9999.0)

        response = identify(client)

        assert response.status_code == 200
        body = response.json()
        assert body["value"] is None
        assert body["layer"] == LAYER
        assert body["time"] == TIME

    def test_data_beside_the_nodata_still_reads(self, client, fake_redis, seed_cog):
        """The nodata answer must be the pixel's, not the whole file's."""
        holed = COG_VALUES.copy()
        holed[: COG_SIZE // 2] = -9999.0
        seed_default(fake_redis, seed_cog, data=holed, nodata=-9999.0)

        northern = identify(client, J="64").json()
        southern = identify(client, J="192").json()

        assert northern["value"] is None
        assert southern["value"] == pytest.approx(
            seeded_value(0, 0, 0, 128, 192), rel=1e-6,
        )

    def test_a_click_outside_the_grids_footprint_answers_the_same_way(
            self, client, fake_redis, seed_cog):
        # A COG covering only the eastern tropics: the level-0 centre pixel
        # sits well outside it.
        seed_default(
            fake_redis, seed_cog,
            bounds=(3_000_000.0, -1_000_000.0, 4_000_000.0, 1_000_000.0),
        )

        response = identify(client)

        assert response.status_code == 200
        body = response.json()
        assert body["value"] is None
        assert body["longitude"] == pytest.approx(lonlat_of(0, 0, 0, 128, 128)[0])

    def test_a_click_inside_that_footprint_still_reads_a_value(
            self, client, fake_redis, seed_cog):
        """The empty answer must belong to the click, not to the small file."""
        bounds = (3_000_000.0, -1_000_000.0, 4_000_000.0, 1_000_000.0)
        seed_default(fake_redis, seed_cog, bounds=bounds)
        lon = math.degrees(3_500_000.0 / EARTH_RADIUS)
        col, row, i, j = address_of(lon, 0.0, 6)

        body = identify(
            client, TILEMATRIX="6", TILECOL=str(col), TILEROW=str(row),
            I=str(i), J=str(j),
        ).json()

        assert body["value"] is not None


class TestInfoFormat:
    def test_json_is_served(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        assert identify(client, INFOFORMAT="application/json").status_code == 200

    def test_an_unserved_info_format_is_refused(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, INFOFORMAT="text/html")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "INFOFORMAT"

    def test_an_omitted_info_format_means_the_only_one_there_is(
            self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        assert identify(client, INFOFORMAT=None).status_code == 200


class TestPixelParameters:
    @pytest.mark.parametrize("name", ["I", "J"])
    def test_a_missing_pixel_coordinate_names_itself_in_the_locator(
            self, client, fake_redis, seed_cog, name):
        seed_default(fake_redis, seed_cog)

        response = identify(client, **{name: None})

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "MissingParameterValue"
        assert exc.get("locator") == name

    @pytest.mark.parametrize("name", ["I", "J"])
    def test_a_pixel_outside_the_tile_answers_point_ij_out_of_range(
            self, client, fake_redis, seed_cog, name):
        seed_default(fake_redis, seed_cog)

        response = identify(client, **{name: "256"})

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "PointIJOutOfRange"
        assert exc.get("locator") == name

    def test_a_non_numeric_pixel_coordinate_is_refused(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, I="middle-ish")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "I"


class TestRefusalsSharedWithGetTile:
    """What identify refuses, it refuses in the same words a tile does.

    An address that cannot be rendered cannot be identified either, and a
    client that gets a different story from the two operations has no way to
    tell which one to believe (ADR 0023).
    """

    def test_an_unknown_layer_is_a_404_exception_report(self, client, fake_redis, seed_cog):
        response = identify(client, LAYER="no:such:layer")

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "LAYER"

    def test_an_unknown_style_is_a_hard_404(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, STYLE="no-such-style")

        assert response.status_code == 404
        assert exception_of(response).get("exceptionCode") == "InvalidParameterValue"

    def test_a_time_the_archive_does_not_hold_is_a_404(self, client, fake_redis, seed_cog):
        """Not an empty body: a missing file is a dimension the archive lacks,
        which the client can fix by picking another, unlike an empty sea."""
        seed_default(fake_redis, seed_cog)

        response = identify(client, TIME="2020-01-01T00:00:00Z")

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("locator") == "TIME"

    def test_an_unparseable_time_is_a_400(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, TIME="yesterday")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "TIME"

    def test_an_unserved_tile_matrix_set_is_refused(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, TILEMATRIXSET="EPSG:4326")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "TILEMATRIXSET"

    def test_a_tile_outside_the_matrix_answers_tile_out_of_range(
            self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, TILEMATRIX="1", TILECOL="2")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "TileOutOfRange"
        assert exc.get("locator") == "TILECOL"
