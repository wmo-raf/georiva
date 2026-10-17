"""KVP GetCapabilities is Django's document, proxied through (#362).

The single paste-able URL completed: a KVP-only client asks this endpoint for
capabilities and gets the very bytes Django rendered, so discovery and tiles
live at one address. Everything asserted here is behavior at the two HTTP
boundaries this slice spans — what the client receives, and what Titiler asks
Django for — because a proxy that fetched the wrong document, or fetched the
right one without the caller's credential, would look identical from the
inside.
"""

import httpx
import pytest
from app import dependencies
from app.config import DJANGO_BASE_URL

from tests.conftest import ORG, exception_of, overriding

#: What Django answers with. Opaque on purpose: Titiler interprets none of it,
#: so the test asserts the bytes survive rather than that they parse.
DOCUMENT = b'<?xml version="1.0" encoding="UTF-8"?><Capabilities version="1.0.0"/>'

#: The minimal query a legacy client sends for discovery — no TILEMATRIXSET, no
#: FORMAT, none of the tile parameters. Requiring any of them here would refuse
#: every honest capabilities request.
CAPABILITIES_KVP = {"SERVICE": "WMTS", "REQUEST": "GetCapabilities"}


def capabilities_kvp(**overrides):
    """``CAPABILITIES_KVP`` with parameters replaced, or removed via ``NAME=None``."""
    return overriding(CAPABILITIES_KVP, **overrides)


class FakeDjango:
    """Django's REST capabilities endpoint, answering at the HTTP edge.

    Faked one layer lower than the tile-config edge in ``conftest`` — a
    transport rather than a function — because the address Titiler dials and
    what it carries there *are* this slice: a fake taking the org and the key
    as arguments would agree with the proxy about the grammar under test
    instead of witnessing it.
    """

    def __init__(self):
        self.requests = []
        self.answers(
            httpx.Response(
                200,
                content=DOCUMENT,
                headers={"content-type": "application/xml"},
            )
        )

    def answers(self, answer):
        """Answer every fetch with ``answer`` — a Response, or a callable."""
        self._answer = answer if callable(answer) else (lambda request: answer)

    def handle(self, request):
        self.requests.append(request)
        return self._answer(request)

    @property
    def fetch(self):
        """The one fetch made — and the assertion that there was exactly one."""
        assert len(self.requests) == 1
        return self.requests[0]


@pytest.fixture
def django(monkeypatch):
    fake = FakeDjango()
    monkeypatch.setattr(
        dependencies,
        "django_client",
        lambda: httpx.AsyncClient(
            base_url=DJANGO_BASE_URL,
            transport=httpx.MockTransport(fake.handle),
        ),
    )
    return fake


def get(client, **overrides):
    return client.get(f"/{ORG}/wmts", params=capabilities_kvp(**overrides))


class TestProxyThrough:
    def test_the_document_django_rendered_is_what_the_client_receives(self, client, django):
        response = get(client)

        assert response.status_code == 200
        assert response.content == DOCUMENT
        assert response.headers["content-type"].startswith("application/xml")

    def test_the_document_is_fetched_for_the_organisation_in_the_path(self, client, django):
        get(client)

        assert django.fetch.url.path == f"/api/wmts/{ORG}/WMTSCapabilities.xml"

    def test_a_different_organisation_fetches_a_different_document(self, client, django):
        client.get("/somaliland/wmts", params=capabilities_kvp())

        assert django.fetch.url.path == "/api/wmts/somaliland/WMTSCapabilities.xml"

    def test_the_dialled_host_travels_with_the_fetch(self, client, django):
        # Django resolves the organisation from the Host and makes every URL in
        # the document absolute against it. Dropping the header would hand the
        # client a document addressed to whichever site Django defaults to.
        get(client)

        assert django.fetch.headers["host"] == "testserver"

    def test_the_request_is_lowercase_spellable_like_every_other(self, client, django):
        response = client.get(f"/{ORG}/wmts", params={"service": "wmts", "request": "getcapabilities"})

        assert response.status_code == 200
        assert response.content == DOCUMENT

    def test_no_tile_parameters_are_required_for_discovery(self, client, django):
        # The whole point of GetCapabilities is that the client does not yet
        # know a layer, a matrix or a time to name.
        response = get(client)

        assert response.status_code == 200

    def test_a_wrong_service_is_still_refused(self, client, django):
        response = get(client, SERVICE="WMS")

        assert response.status_code == 400
        assert exception_of(response).get("locator") == "SERVICE"
        assert django.requests == []


class TestCredentialForwarding:
    def test_the_callers_api_key_is_forwarded(self, client, django):
        get(client, api_key="grv_secret")

        assert django.fetch.url.params["api_key"] == "grv_secret"

    def test_a_document_asked_for_without_a_key_is_fetched_without_one(self, client, django):
        get(client)

        assert "api_key" not in django.fetch.url.params

    def test_a_bearer_credential_is_forwarded_too(self, client, django):
        client.get(
            f"/{ORG}/wmts",
            params=capabilities_kvp(),
            headers={"Authorization": "Bearer grv_secret"},
        )

        assert django.fetch.headers["authorization"] == "Bearer grv_secret"

    def test_djangos_cache_directive_for_a_keyed_document_survives(self, client, django):
        # A keyed document lists private layers and carries the caller's own
        # key in its URLs; Django marks it private, and dropping that here
        # would leave it cacheable by everything between the two services.
        django.answers(
            httpx.Response(
                200,
                content=DOCUMENT,
                headers={"content-type": "application/xml", "cache-control": "private"},
            )
        )

        response = get(client, api_key="grv_secret")

        assert response.headers["cache-control"] == "private"

    def test_titiler_adds_no_cache_directive_of_its_own(self, client, django):
        response = get(client)

        assert "cache-control" not in response.headers


class TestFailurePaths:
    @pytest.mark.parametrize(
        "failure",
        [
            httpx.ConnectError("connection refused"),
            httpx.ReadTimeout("timed out"),
        ],
    )
    def test_an_unreachable_django_answers_an_exception_report(self, client, django, failure):
        def raise_it(request):
            raise failure

        django.answers(raise_it)

        response = get(client)

        assert response.status_code == 503
        exc = exception_of(response)
        assert exc.get("exceptionCode") == "NoApplicableCode"

    def test_an_unknown_organisation_answers_a_404_exception_report(self, client, django):
        django.answers(httpx.Response(404))

        response = get(client)

        assert response.status_code == 404
        assert exception_of(response).get("exceptionCode") == "InvalidParameterValue"

    def test_a_refused_credential_answers_a_401_exception_report(self, client, django):
        django.answers(httpx.Response(401))

        response = get(client, api_key="grv_expired")

        assert response.status_code == 401
        exc = exception_of(response)
        assert exc.get("locator") == "API_KEY"

    def test_a_broken_django_answers_a_502_exception_report(self, client, django):
        django.answers(httpx.Response(500))

        response = get(client)

        assert response.status_code == 502
        assert exception_of(response).get("exceptionCode") == "NoApplicableCode"

    def test_a_malformed_organisation_segment_is_never_spliced_into_the_fetch(self, client, django):
        response = client.get("/not.a.slug/wmts", params=capabilities_kvp())

        assert response.status_code == 404
        assert exception_of(response).get("exceptionCode") == "InvalidParameterValue"
        assert django.requests == []
