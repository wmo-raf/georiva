"""Maps are drawn in the source unit; numbers leave in the served unit (ADR 0046 §14–§16, #742).

Django hands this service the raw range and raw stops a Variable's tiles are
drawn with, and beside them ``scale``, ``offset`` and the served ``unit``. A
tile or encoded texture of a Variable served in K and of the same Variable
served in °C are therefore the same bytes; a point read and GetFeatureInfo
answer ``value·scale + offset`` and name the unit.
"""

import json

import pytest

from tests.conftest import CATALOG, COG_VALUES, COLLECTION, ORG, TILE_CONFIG, VARIABLE, kvp

TIME = "2026-03-23T12:00:00Z"
#: The seeded ramp, read as Kelvin: 0…50 shifted to 250…300 K.
KELVIN = COG_VALUES + 250.0
RAW_RANGE = {"vmin": 250.0, "vmax": 300.0}

#: The same Variable, served in two units: what ``build_variable_payload`` writes for each.
SERVED_IN = {
    "kelvin": {**TILE_CONFIG, **RAW_RANGE, "scale": 1.0, "offset": 0.0, "unit": "K"},
    "celsius": {**TILE_CONFIG, **RAW_RANGE, "scale": 1.0, "offset": -273.15, "unit": "degC"},
}


@pytest.fixture
def both_units(fake_redis, seed_cog):
    """Two layers over the same Kelvin bytes, one served in K and one in °C."""
    for variable, config in SERVED_IN.items():
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{variable}"] = json.dumps(config)
        seed_cog(variable=variable, data=KELVIN)
    return fake_redis


def route(variable):
    return f"/{ORG}/{CATALOG}/{COLLECTION}/{variable}"


class TestPixelsAreNotConverted:
    def test_a_tile_is_the_same_png_whichever_unit_the_variable_is_served_in(self, client, both_units):
        tiles = [client.get(f"{route(v)}/tiles/WebMercatorQuad/1/1/0.png", params={"time": TIME}) for v in SERVED_IN]

        assert [t.status_code for t in tiles] == [200, 200]
        assert tiles[0].content == tiles[1].content

    def test_an_encoded_texture_is_the_same_png_whichever_unit_the_variable_is_served_in(self, client, both_units):
        textures = [client.get(f"{route(v)}/encoded-preview.png", params={"time": TIME}) for v in SERVED_IN]

        assert [t.status_code for t in textures] == [200, 200]
        assert textures[0].content == textures[1].content

    def test_an_encoded_texture_is_unscaled_into_the_served_unit(self, client, both_units):
        texture = client.get(f"{route('celsius')}/encoded-preview.png", params={"time": TIME})

        low, high = (float(end) for end in texture.headers["X-Image-Unscale"].split(","))
        assert (low, high) == pytest.approx((250.0 - 273.15, 300.0 - 273.15))


class TestNumbersAreServed:
    def test_a_point_read_is_in_the_served_unit_and_names_it(self, client, both_units):
        kelvin = client.get(f"{route('kelvin')}/point/0.1,0.1", params={"time": TIME}).json()
        celsius = client.get(f"{route('celsius')}/point/0.1,0.1", params={"time": TIME}).json()

        assert celsius["values"][0] == pytest.approx(kelvin["values"][0] - 273.15, abs=1e-4)
        assert (kelvin["unit"], celsius["unit"]) == ("K", "degC")

    def test_get_feature_info_is_in_the_served_unit_and_names_it(self, client, both_units):
        answers = {}
        for variable in SERVED_IN:
            response = client.get(
                f"/{ORG}/wmts",
                params=kvp(
                    REQUEST="GetFeatureInfo",
                    LAYER=f"{CATALOG}:{COLLECTION}:{variable}",
                    INFOFORMAT="application/json",
                    I="128",
                    J="128",
                ),
            )
            assert response.status_code == 200, response.text
            answers[variable] = response.json()

        assert answers["celsius"]["value"] == pytest.approx(answers["kelvin"]["value"] - 273.15, abs=1e-4)
        assert (answers["kelvin"]["units"], answers["celsius"]["units"]) == ("K", "degC")

    def test_a_config_from_before_units_were_served_reads_as_it_is(self, client, fake_redis, seed_cog):
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)
        seed_cog(data=KELVIN)

        response = client.get(f"{route(VARIABLE)}/point/0.1,0.1", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert 250.0 <= response.json()["values"][0] <= 300.0


class TestStatisticsAreServed:
    """Statistics are numbers that leave as numbers (ADR 0046 §15): of the
    values, not of the colours they are drawn with, in the served unit."""

    def statistics(self, client, variable):
        response = client.get(f"{route(variable)}/statistics", params={"time": TIME})
        assert response.status_code == 200, response.text
        [band] = response.json().values()
        return band

    def test_statistics_are_of_the_values_in_the_served_unit_and_name_it(self, client, both_units):
        kelvin = self.statistics(client, "kelvin")
        celsius = self.statistics(client, "celsius")

        assert kelvin["min"] == pytest.approx(250.0, abs=0.1)
        assert kelvin["max"] == pytest.approx(300.0, abs=0.1)
        for key in ("min", "max", "mean", "median"):
            assert celsius[key] == pytest.approx(kelvin[key] - 273.15, abs=1e-3), key
        # A spread is scaled, never offset; a sum gains the offset once per value.
        assert celsius["std"] == pytest.approx(kelvin["std"], abs=1e-6)
        assert celsius["sum"] == pytest.approx(kelvin["sum"] - 273.15 * kelvin["count"], rel=1e-6)
        assert celsius["count"] == kelvin["count"]
        assert celsius["histogram"][1][0] == pytest.approx(kelvin["histogram"][1][0] - 273.15, abs=1e-3)
        assert celsius["unit"] == "degC"

    def test_statistics_of_a_feature_are_in_the_served_unit(self, client, both_units):
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0.5, 0], [0.5, 0.5], [0, 0.5], [0, 0]]]},
        }
        answers = {}
        for variable in SERVED_IN:
            response = client.post(f"{route(variable)}/statistics", params={"time": TIME}, json=feature)
            assert response.status_code == 200, response.text
            [answers[variable]] = response.json()["properties"]["statistics"].values()

        assert answers["celsius"]["mean"] == pytest.approx(answers["kelvin"]["mean"] - 273.15, abs=1e-3)
        assert answers["celsius"]["unit"] == "degC"

    def test_info_names_the_served_unit(self, client, both_units):
        response = client.get(f"{route('celsius')}/info", params={"time": TIME})

        assert response.status_code == 200, response.text
        assert response.json()["unit"] == "degC"
