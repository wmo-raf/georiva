"""KVP parameter validation answers OGC ExceptionReport XML (#357).

Every refusal on the KVP endpoint must be parseable by a legacy OGC client:
an OWS 1.1 ExceptionReport with an exceptionCode the spec names and a locator
pointing at the offending parameter — never framework-default JSON.
"""
import xml.etree.ElementTree as ET

import pytest

from tests.conftest import ORG, exception_of, kvp


def get(client, **overrides):
    return client.get(f"/{ORG}/wmts", params=kvp(**overrides))


class TestMissingParameters:
    @pytest.mark.parametrize(
        "name", ["REQUEST", "LAYER", "TILEMATRIXSET", "TILEMATRIX", "TILEROW", "TILECOL", "TIME"],
    )
    def test_a_missing_required_parameter_names_itself_in_the_locator(self, client, name):
        response = get(client, **{name: None})

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "MissingParameterValue"
        assert exc.get("locator") == name


class TestInvalidParameters:
    def test_a_malformed_layer_identifier_is_refused(self, client):
        response = get(client, LAYER="only-two:parts")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "LAYER"

    @pytest.mark.parametrize("layer", ["a/b:c:d", "a:c:d?x=1", "a b:c:d", "a:c:"])
    def test_a_layer_part_that_could_reshape_the_path_is_refused(self, client, layer):
        response = get(client, LAYER=layer)

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "LAYER"

    def test_an_unserved_tile_matrix_set_is_refused(self, client):
        response = get(client, TILEMATRIXSET="EPSG:4326")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "TILEMATRIXSET"

    def test_an_unserved_format_is_refused(self, client):
        response = get(client, FORMAT="image/jpeg")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "FORMAT"

    def test_a_wrong_service_is_refused(self, client):
        response = get(client, SERVICE="WMS")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "SERVICE"

    def test_a_wrong_version_is_refused(self, client):
        response = get(client, VERSION="2.0.0")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "VERSION"

    def test_a_non_numeric_tile_coordinate_is_refused(self, client):
        response = get(client, TILEROW="up-a-bit")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "TILEROW"

    def test_a_tile_matrix_beyond_the_grid_is_refused(self, client):
        response = get(client, TILEMATRIX="25")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "TILEMATRIX"

    def test_a_tile_outside_the_matrix_answers_tile_out_of_range(self, client):
        response = get(client, TILEMATRIX="1", TILECOL="2")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "TileOutOfRange"
        assert exc.get("locator") == "TILECOL"

    def test_a_repeated_parameter_is_refused_whatever_its_spelling(self, client):
        response = client.get(
            f"/{ORG}/wmts",
            params=[*kvp().items(), ("layer", "an:other:layer")],
        )

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "LAYER"


class TestOperations:
    def test_a_later_slice_operation_answers_operation_not_supported(self, client):
        response = get(client, REQUEST="GetFeatureInfo")

        assert response.status_code == 501
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "OperationNotSupported"
        assert exc.get("locator") == "REQUEST"

    def test_an_unknown_operation_is_refused(self, client):
        response = get(client, REQUEST="GetLunch")

        assert response.status_code == 400
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "InvalidParameterValue"
        assert exc.get("locator") == "REQUEST"

    def test_the_exception_report_is_well_formed_xml_with_the_ows_namespace(self, client):
        response = get(client, REQUEST=None)

        root = ET.fromstring(response.content)
        assert root.tag == "{http://www.opengis.net/ows/1.1}ExceptionReport"
        assert root.get("version") == "1.1.0"
        text = root.find(
            "{http://www.opengis.net/ows/1.1}Exception/{http://www.opengis.net/ows/1.1}ExceptionText"
        )
        assert text is not None and text.text
