"""KVP GetTile answers through the existing semantic tile machinery (#357).

Everything here is behavior at the HTTP boundary: the KVP spelling of a tile
request must answer exactly what the REST spelling answers — same bytes on
success, same status on refusal — with only the error vocabulary changing.
"""
import json

from tests.conftest import (
    CATALOG, COLLECTION, ORG, TILE_CONFIG, VARIABLE, exception_of, kvp,
)

TIME = "2026-03-23T12:00:00Z"


def seed_default(fake_redis, seed_cog, **cog_kwargs):
    fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)
    seed_cog(**cog_kwargs)


class TestGetTile:
    def test_kvp_gettile_returns_the_same_png_as_the_rest_tile_route(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        rest = client.get(
            f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/tiles/WebMercatorQuad/0/0/0.png",
            params={"time": TIME},
        )
        kvp_resp = client.get(f"/{ORG}/wmts", params=kvp())

        assert rest.status_code == 200
        assert kvp_resp.status_code == 200
        assert kvp_resp.headers["content-type"] == "image/png"
        assert kvp_resp.content == rest.content

    def test_tilematrix_may_be_qualified_with_its_set(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        plain = client.get(f"/{ORG}/wmts", params=kvp())
        qualified = client.get(f"/{ORG}/wmts", params=kvp(TILEMATRIX="WebMercatorQuad:0"))

        assert qualified.status_code == 200
        assert qualified.content == plain.content

    def test_parameter_names_are_case_insensitive(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = client.get(
            f"/{ORG}/wmts",
            params={name.lower(): value for name, value in kvp().items()},
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "image/png"


class TestTimeAndReftime:
    def test_reftime_addresses_the_forecast_run_specific_asset(self, client, fake_redis, seed_cog):
        reftime = "2026-03-23T00:00:00Z"
        seed_default(fake_redis, seed_cog, reftime=reftime)

        with_reftime = client.get(f"/{ORG}/wmts", params=kvp(REFTIME=reftime))
        without = client.get(f"/{ORG}/wmts", params=kvp())

        assert with_reftime.status_code == 200
        # Only the run-specific file was seeded, so the styleless-time spelling
        # honestly finds nothing.
        assert without.status_code == 404

    def test_a_nonexistent_time_reftime_combination_is_a_404_exception_report(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = client.get(f"/{ORG}/wmts", params=kvp(TIME="2020-01-01T00:00:00Z"))

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "TIME"

    def test_an_unparseable_time_is_a_400_exception_report(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = client.get(f"/{ORG}/wmts", params=kvp(TIME="yesterday"))

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "TIME"


class TestStyle:
    def test_empty_and_literal_default_style_resolve_to_the_default_alias(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        empty = client.get(f"/{ORG}/wmts", params=kvp(STYLE=""))
        literal = client.get(f"/{ORG}/wmts", params=kvp(STYLE="default"))
        omitted = client.get(f"/{ORG}/wmts", params=kvp(STYLE=None))

        assert empty.status_code == literal.status_code == omitted.status_code == 200
        assert empty.content == literal.content == omitted.content

    def test_a_named_style_renders_with_that_styles_config(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)
        analyst = dict(TILE_CONFIG, colormap={str(i): [0, i, i, 255] for i in range(256)})
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}:analyst"] = json.dumps(analyst)

        styled = client.get(f"/{ORG}/wmts", params=kvp(STYLE="analyst"))
        default = client.get(f"/{ORG}/wmts", params=kvp())

        assert styled.status_code == 200
        assert styled.content != default.content

    def test_an_unknown_style_is_a_hard_404_never_a_fallback(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = client.get(f"/{ORG}/wmts", params=kvp(STYLE="no-such-style"))

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert "style" in exc.findtext(
            "{http://www.opengis.net/ows/1.1}ExceptionText"
        ).lower()

    def test_an_unknown_layer_is_a_404_exception_report(self, client, fake_redis, seed_cog):
        response = client.get(f"/{ORG}/wmts", params=kvp(LAYER="no:such:layer"))

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "LAYER"

    def test_an_unknown_layer_with_a_named_style_accuses_neither_alone(self, client, fake_redis, seed_cog):
        # The config 404 cannot say whether the layer or the style was the
        # stranger, so the report names both and pins no locator.
        response = client.get(f"/{ORG}/wmts", params=kvp(LAYER="no:such:layer", STYLE="analyst"))

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("locator") is None
        text = exc.findtext("{http://www.opengis.net/ows/1.1}ExceptionText").lower()
        assert "layer" in text and "style" in text
