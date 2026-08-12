"""RESTful GetFeatureInfo — the identify a ResourceURL client can reach (#379).

The same operation the KVP suite covers, reached by the other binding. So this
file deliberately does *not* re-derive the Mercator formulae or re-check the
value under the pixel: those are pinned once in ``test_wmts_kvp_getfeatureinfo``
and the two bindings share :func:`app.wmts._identify`. What is tested here is
everything that is genuinely new — the route's own address, the path segments
it validates itself, the query it reads its dimensions from, and the promise
that its refusals are ExceptionReport XML like its sibling's.

The suite opens by pinning the two bindings against *each other* on the same
click, which is the assertion that makes the omissions above safe: if the REST
route ever stopped going through the shared identify, that test fails before
any of the narrower ones do.
"""

import json

import pytest

from tests.conftest import (
    CATALOG,
    COLLECTION,
    ORG,
    TILE_CONFIG,
    VARIABLE,
    exception_of,
    kvp,
)

TIME = "2026-03-23T12:00:00Z"
LAYER = f"{CATALOG}:{COLLECTION}:{VARIABLE}"

#: The tile the clicks below land in, and the pixel within it.
ZOOM, COL, ROW, I, J = "0", "0", "0", "128", "128"


def rest_url(
    zoom=ZOOM,
    col=COL,
    row=ROW,
    j=J,
    i=I,
    org=ORG,
    catalog=CATALOG,
    collection=COLLECTION,
    variable=VARIABLE,
    tile_matrix_set="WebMercatorQuad",
):
    """The identify address, spelt as the capabilities template writes it.

    Column before row and ``{J}`` before ``{I}``, matching
    ``wmts_rest_featureinfo_template`` in Django's machine-plane module — the
    two spellings are the whole contract between the document and this route,
    and this helper is where a drift in either shows up.
    """
    return f"/{org}/{catalog}/{collection}/{variable}/tiles/{tile_matrix_set}/{zoom}/{col}/{row}/{j}/{i}.json"


def seed_default(fake_redis, seed_cog, style=None, **cog_kwargs):
    key = f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"
    if style:
        key = f"{key}:{style}"
    fake_redis.store[key] = json.dumps(TILE_CONFIG)
    return seed_cog(**cog_kwargs)


def identify(client, params=None, **url_kwargs):
    query = {"time": TIME} if params is None else params
    return client.get(rest_url(**url_kwargs), params=query)


class TestOneIdentifyTwoBindings:
    """The two bindings answer one click identically, or the document lies.

    A capabilities Layer advertises a KVP endpoint and a REST template for the
    same operation on the same layer. A client that picked one binding and got
    a different number, or a different place, than a client that picked the
    other would have no way to tell which of them was looking at the data.
    """

    def test_the_rest_answer_is_the_kvp_answer_for_the_same_pixel(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        from_rest = identify(client).json()
        from_kvp = client.get(
            f"/{ORG}/wmts",
            params=kvp(
                REQUEST="GetFeatureInfo",
                INFOFORMAT="application/json",
                TILEMATRIX=ZOOM,
                TILECOL=COL,
                TILEROW=ROW,
                I=I,
                J=J,
                TIME=TIME,
            ),
        ).json()

        assert from_rest == from_kvp

    def test_the_answer_names_the_layer_the_document_advertises(self, client, fake_redis, seed_cog):
        """The REST caller spelt the address as path segments and never sent a
        LAYER, so the identifier in its receipt is rebuilt rather than echoed —
        and it has to come back as the very string the Layer is listed under,
        or a client cannot match the answer to what it clicked."""
        seed_default(fake_redis, seed_cog)

        assert identify(client).json()["layer"] == LAYER

    def test_the_answer_is_json(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client)

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")

    def test_a_value_comes_back_at_all(self, client, fake_redis, seed_cog):
        """Deliberately shallow — that it is the *right* value is the shared
        identify's business, pinned in the KVP suite."""
        seed_default(fake_redis, seed_cog)

        body = identify(client).json()

        assert body["value"] is not None
        assert body["time"] == TIME
        assert body["reftime"] is None


class TestTheQuery:
    """Dimensions and style arrive the way the tile route beside it reads them."""

    def test_a_forecast_click_names_its_run(self, client, fake_redis, seed_cog):
        reftime = "2026-03-23T00:00:00Z"
        seed_default(fake_redis, seed_cog, reftime=reftime)

        body = identify(client, {"time": TIME, "reftime": reftime}).json()

        assert body["reftime"] == reftime
        assert body["value"] is not None

    def test_time_is_required(self, client, fake_redis, seed_cog):
        """The COG's key is built from the time, so there is nothing to read
        without one — the same refusal the KVP binding gives, and for the same
        reason: this service cannot know which run is latest."""
        seed_default(fake_redis, seed_cog)

        response = identify(client, {})

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "MissingParameterValue"
        assert exc.get("locator") == "TIME"

    def test_a_named_style_is_resolved_like_the_tile_route_resolves_it(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog, style="analyst")

        response = identify(client, {"time": TIME, "style": "analyst"})

        assert response.status_code == 200
        assert response.json()["value"] is not None

    def test_an_unknown_style_is_refused_rather_than_quietly_defaulted(self, client, fake_redis, seed_cog):
        """ADR 0023 on this route too: identifying under a style the layer does
        not have would report a number nobody was shown."""
        seed_default(fake_redis, seed_cog)

        response = identify(client, {"time": TIME, "style": "nonexistent"})

        assert response.status_code == 404
        assert exception_of(response) is not None

    def test_the_default_style_alias_reaches_the_styleless_config(self, client, fake_redis, seed_cog):
        """``default`` is what the document advertises for a variable with no
        named styles, and a client will send it back."""
        seed_default(fake_redis, seed_cog)

        assert identify(client, {"time": TIME, "style": "default"}).status_code == 200


class TestRefusalsAreExceptionReports:
    """Every refusal on this route is OWS XML, never framework JSON.

    The route exists only to serve a ``ResourceURL``, so whoever called it is a
    WMTS client — including for the malformed path segments FastAPI would
    otherwise answer with its own 422, which is why the segments arrive as
    strings and are validated here rather than declared as ints.
    """

    @pytest.mark.parametrize(
        "field,value,locator",
        [
            ("zoom", "notanumber", "TILEMATRIX"),
            ("col", "notanumber", "TILECOL"),
            ("row", "notanumber", "TILEROW"),
            ("i", "notanumber", "I"),
            ("j", "notanumber", "J"),
        ],
    )
    def test_a_malformed_segment_is_an_ows_exception_not_a_422(
        self, client, fake_redis, seed_cog, field, value, locator
    ):
        seed_default(fake_redis, seed_cog)

        response = identify(client, **{field: value})

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == locator

    def test_a_tile_outside_its_matrix_is_refused(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, col="1")

        assert response.status_code == 400
        assert exception_of(response).get("exceptionCode") == "TileOutOfRange"

    def test_a_pixel_outside_the_tile_is_refused(self, client, fake_redis, seed_cog):
        """WMTS 1.0's own code for it (Table 30): a pixel off the tile names no
        place on the map, unlike a pixel on it that finds no data."""
        seed_default(fake_redis, seed_cog)

        response = identify(client, i="256")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "PointIJOutOfRange"
        assert exc.get("locator") == "I"

    def test_another_tile_matrix_set_is_refused(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, tile_matrix_set="EPSG:4326")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "TILEMATRIXSET"

    def test_an_unknown_layer_is_reported_as_a_wmts_exception(self, client, seed_cog):
        """No tile config anywhere for this address — the same 404 the tile
        route gives, in the vocabulary this binding's clients parse."""
        response = identify(client, variable="not-a-variable")

        assert response.status_code == 404
        assert exception_of(response) is not None

    def test_a_time_the_archive_does_not_hold_is_reported_the_same_way(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = identify(client, {"time": "2011-01-01T00:00:00Z"})

        assert response.status_code == 404
        exc = exception_of(response)
        assert exc.get("locator") == "TIME"

    def test_a_segment_that_could_reshape_the_path_is_reported_absent(self, client):
        """The four segments leave this route spliced into two more addresses,
        so a segment that is not slug-shaped is refused at the door — as
        absent, because which institutions exist is not the caller's business."""
        response = identify(client, org="not a slug")

        assert response.status_code == 404
        assert exception_of(response) is not None


class TestItShadowsNothing:
    def test_the_tile_route_beside_it_still_serves_a_tile(self, client, fake_redis, seed_cog):
        """Two segments deeper than the deepest route the tile factory
        registers, so mounting it on the same prefix may not capture tiles."""
        seed_default(fake_redis, seed_cog)

        response = client.get(
            f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/tiles/WebMercatorQuad/0/0/0.png",
            params={"time": TIME},
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "image/png"

    def test_the_kvp_endpoint_still_answers_its_own_grammar(self, client, fake_redis, seed_cog):
        seed_default(fake_redis, seed_cog)

        response = client.get(f"/{ORG}/wmts", params=kvp())

        assert response.status_code == 200
        assert response.headers["content-type"] == "image/png"
