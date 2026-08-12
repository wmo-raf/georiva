"""The refusals the KVP endpoint answers *before* this service is reached (#372).

Story 18 of #354 promises a legacy client an OGC ExceptionReport for every
error on the WMTS endpoint, and the shim in ``app.wmts`` keeps that promise for
every refusal it issues. Two refusals never reach it: nginx runs the
``auth_request`` gate first, so an expired ``api_key`` is answered as a bare 401
and an unknown organisation — or one disagreeing with the dialled host, or a
collection this caller may not see — as a 404, both in HTML. They are exactly
the two a real user hits, on the one endpoint whose reason for existing is
clients that can parse nothing else.

The fix is in ``deploy/nginx/nginx.conf`` because that is who answers, so this
suite runs *that file* under the same nginx the instance ships, with the two
upstreams it gates replaced by stubs:

- **Django** stands in as the gate alone, reading the ``api_key`` the real
  gateway forwards into the subrequest and answering 401, 403 or 204 — the
  three statuses ``TileAuthView`` may return, and nothing else about it.
- **Titiler** stands in as a service that answers a report of its own on the
  KVP path, so the arm under test can be shown *not* to overwrite one.

Nothing here re-implements the config. The routing regex, the status mapping
and the report bodies are all read out of the shipped file by running it, which
is the only way to test an nginx arm honestly: the mistakes this class of change
makes — a status that does not survive ``error_page``, a body served as
``text/plain``, a regex that also catches the tile routes — are all invisible to
a config parser and all fatal to the client.

What the reports *say* is asserted against the shim rather than against literals
written out here again: both are rendered through ``exception_report``, and the
401 is rendered from the very exception ``_translate_capabilities_error`` raises
for one, so neither side can be reworded alone. The 404 keeps a text of its own
and the constants below say why. The gateway and the shim are separate services
and no import binds them; this suite is the binding.

Skipped, not failed, where Docker is unavailable.
"""
import os
import re
import socket
import subprocess
import time
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path

import httpx
import pytest

from app.wmts import (
    API_KEY_PARAM,
    OWS_NS,
    WMTSException,
    _translate_capabilities_error,
    exception_report,
)
from tests.conftest import ORG, exception_of

REPO_ROOT = Path(__file__).resolve().parents[2]
NGINX_CONF = REPO_ROOT / "deploy/nginx/nginx.conf"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

#: The gateway's own upstream names, as ``deploy/nginx/nginx.conf`` spells
#: them. The stubs answer to these as network aliases, so the config under test
#: needs no rewriting to find them.
DJANGO_HOST = "georiva"
TITILER_HOST = "georiva-titiler-app"

#: What the stub gate makes of a key, standing in for the three answers
#: ``TileAuthView`` gives: a key Django refused, a caller it will not serve, and
#: a caller it will. Distinct values per case, because the gateway caches its
#: verdicts for 60s keyed on the credential — two cases sharing a key would
#: share an answer.
EXPIRED_KEY = "expired-key"
DENIED_KEY = "denied-key"
GOOD_KEY = "good-key"

#: The body the Titiler stub answers a KVP request with: a report of the
#: shim's own, recognisable and structurally unlike the gateway's.
SHIM_REPORT = f'<ExceptionReport xmlns="{OWS_NS}" version="1.1.0">shim</ExceptionReport>'

DJANGO_STUB = f"""
worker_processes 1;
events {{ worker_connections 64; }}
http {{
  access_log off;
  server {{
    listen 8000;
    location = /internal/tile-auth/ {{
      if ($arg_{API_KEY_PARAM} = "{EXPIRED_KEY}") {{
        add_header WWW-Authenticate 'Api-Key realm="georiva"' always;
        return 401;
      }}
      if ($arg_{API_KEY_PARAM} = "{GOOD_KEY}") {{ return 204; }}
      return 403;
    }}
  }}
}}
"""

TITILER_STUB = f"""
worker_processes 1;
events {{ worker_connections 64; }}
http {{
  access_log off;
  server {{
    listen 8000;
    location ~ /wmts$ {{
      default_type application/xml;
      return 404 '{SHIM_REPORT}';
    }}
    location / {{
      default_type image/png;
      return 200 "TILE";
    }}
  }}
}}
"""


def _run(*argv):
    return subprocess.run(argv, capture_output=True, text=True, timeout=120)


def _free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _proxy_image():
    """The nginx the instance actually ships, read from ``docker-compose.yml``.

    Pinned there and not here: a gateway tested on a different nginx than it
    runs on would prove nothing about the arm, whose whole behaviour is that
    server's ``error_page`` and ``return`` semantics.
    """
    match = re.search(
        r"georiva-web-proxy:\s*\n\s*image:\s*(\S+)", COMPOSE_FILE.read_text(),
    )
    assert match, "could not find the web proxy's image in docker-compose.yml"
    return match.group(1)


@pytest.fixture(scope="module")
def gateway(tmp_path_factory):
    """The shipped nginx.conf, running, with its two gated upstreams stubbed."""
    if _run("docker", "info").returncode != 0:
        pytest.skip("Docker unavailable — the gateway arm can only be run, not parsed")

    image = _proxy_image()
    if _run("docker", "image", "inspect", image).returncode != 0:
        pull = _run("docker", "pull", image)
        if pull.returncode != 0:
            pytest.skip(f"could not obtain {image}: {pull.stderr.strip()}")

    root = tmp_path_factory.mktemp("wmts-gateway")
    (root / "django.conf").write_text(DJANGO_STUB)
    (root / "titiler.conf").write_text(TITILER_STUB)

    tag = f"georiva-wmts-gate-{os.getpid()}"
    network, port = f"{tag}-net", _free_port()
    containers = [f"{tag}-django", f"{tag}-titiler", f"{tag}-proxy"]

    def mount(path, target="/etc/nginx/nginx.conf"):
        return ["-v", f"{path}:{target}:ro"]

    try:
        _run("docker", "network", "create", network)
        for name, conf, alias in (
            (containers[0], root / "django.conf", DJANGO_HOST),
            (containers[1], root / "titiler.conf", TITILER_HOST),
        ):
            started = _run(
                "docker", "run", "--rm", "-d", "--name", name,
                "--network", network, "--network-alias", alias,
                *mount(conf), image,
            )
            assert started.returncode == 0, started.stderr
        started = _run(
            "docker", "run", "--rm", "-d", "--name", containers[2],
            "--network", network, "-p", f"127.0.0.1:{port}:80",
            *mount(NGINX_CONF), image,
        )
        assert started.returncode == 0, started.stderr

        base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(f"{base_url}/titiler/{ORG}/wmts", timeout=2)
                break
            except httpx.HTTPError:
                if time.monotonic() > deadline:
                    logs = _run("docker", "logs", containers[2])
                    raise AssertionError(f"gateway never came up: {logs.stderr}")
                time.sleep(0.2)
        yield base_url
    finally:
        _run("docker", "rm", "-f", *containers)
        _run("docker", "network", "rm", network)


#: The credential the gateway refuses, taken from the shim rather than written
#: out again: this is the very exception ``_translate_capabilities_error``
#: raises for a 401, so the gateway is held to the shim's wording *and* its
#: locator, and neither side can be reworded alone.
REFUSED = _translate_capabilities_error(401)

#: The denial cannot come from the shim the same way, and the difference is the
#: point. Every 404 branch over there names what was wrong — an unknown layer,
#: an unserved organisation — because it knows. The gate's one 404 covers an
#: unknown organisation, a host disagreeing with the path segment, and a
#: collection this caller may not see, and must not say which (ADR 0014), so
#: its text is broader than any single branch there. Rendered through the shim's
#: own renderer all the same, which is what keeps the report's *shape* shared.
DENIED = WMTSException(
    404, "InvalidParameterValue", None, "No WMTS service or layer at this address",
)


#: The three things a client reads out of an ``ows:Exception``. Named because
#: the tests compare whole reports and one of them reads a single field.
Vocabulary = namedtuple("Vocabulary", "code locator text")


def _vocabulary(element):
    return Vocabulary(
        element.get("exceptionCode"),
        element.get("locator"),
        element.findtext(f"{{{OWS_NS}}}ExceptionText"),
    )


def _as_the_shim_would(exc):
    rendered = ET.fromstring(exception_report(exc).body)
    return _vocabulary(rendered.find(f"{{{OWS_NS}}}Exception"))


def get(gateway, path, api_key=DENIED_KEY, **params):
    return httpx.get(
        f"{gateway}{path}", params={API_KEY_PARAM: api_key, **params}, timeout=10,
    )


def kvp(gateway, api_key=DENIED_KEY, request="GetTile", **params):
    """A KVP request on the org-scoped endpoint, as it looks behind the proxy."""
    return get(
        gateway, f"/titiler/{ORG}/wmts", api_key=api_key,
        SERVICE="WMTS", VERSION="1.0.0", REQUEST=request,
        LAYER="forecasts:gfs:temperature", **params,
    )


def tile(gateway, api_key=DENIED_KEY):
    """A REST tile on the same organisation — the endpoint's neighbour."""
    return get(
        gateway,
        f"/titiler/{ORG}/forecasts/gfs/temperature/tiles/WebMercatorQuad/6/38/32.png",
        api_key=api_key,
    )


class TestTheGateDeniesTheRequest:
    """An unknown organisation, a host disagreeing with the path segment, or a
    collection this caller may not see — one answer for all three (ADR 0014)."""

    def test_a_denial_answers_an_exception_report(self, gateway):
        response = kvp(gateway)

        assert response.status_code == 404
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(DENIED)

    def test_the_report_does_not_say_which_denial_it_was(self, gateway):
        """The status a private tile gets must not distinguish "forbidden" from
        "absent", and neither may the text that now accompanies it."""
        text = _vocabulary(exception_of(kvp(gateway))).text.lower()

        assert "private" not in text
        assert "permission" not in text and "forbidden" not in text


class TestTheGateRefusesTheCredential:
    def test_a_broken_key_answers_an_exception_report(self, gateway):
        response = kvp(gateway, api_key=EXPIRED_KEY)

        assert response.status_code == 401
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(REFUSED)

    def test_the_401_survives_as_a_401(self, gateway):
        """ADR 0014's distinction: a denial is 404 and a broken key is 401, which
        is what tells a QGIS user their key expired rather than leaving them
        debugging a dataset that appears to have vanished."""
        assert kvp(gateway, api_key=EXPIRED_KEY).status_code == 401
        assert kvp(gateway, api_key=DENIED_KEY).status_code == 404

    def test_the_challenge_header_still_reaches_the_client(self, gateway):
        """``auth_request`` forwards Django's ``WWW-Authenticate`` on a 401, and
        rendering a body for it must not cost the client the header."""
        response = kvp(gateway, api_key=EXPIRED_KEY)

        assert "www-authenticate" in response.headers


class TestItIsPerEndpointAndNotPerOperation:
    @pytest.mark.parametrize("request_", ["GetTile", "GetCapabilities", "GetFeatureInfo"])
    def test_every_operation_is_denied_the_same_way(self, gateway, request_):
        response = kvp(gateway, request=request_)

        assert response.status_code == 404
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(DENIED)

    @pytest.mark.parametrize("request_", ["GetTile", "GetCapabilities", "GetFeatureInfo"])
    def test_every_operation_refuses_a_broken_key_the_same_way(self, gateway, request_):
        response = kvp(gateway, api_key=EXPIRED_KEY, request=request_)

        assert response.status_code == 401
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(REFUSED)

    def test_an_operationless_request_is_still_the_endpoint(self, gateway):
        """The arm is chosen by the path, so a client that has not said what it
        wants yet is owed the same parseable answer."""
        response = get(gateway, f"/titiler/{ORG}/wmts")

        assert response.status_code == 404
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(DENIED)

    def test_a_trailing_slash_is_still_the_endpoint(self, gateway):
        """``scope_of`` drops empty path segments, so Django scopes, judges and
        denies ``…/wmts/`` as the endpoint. The gateway has to agree, or a
        client that appends a slash is refused correctly and told so in a
        language it cannot read."""
        response = get(gateway, f"/titiler/{ORG}/wmts/", REQUEST="GetTile")

        assert response.status_code == 404
        assert _vocabulary(exception_of(response)) == _as_the_shim_would(DENIED)


class TestNothingElseChanged:
    def test_the_tile_routes_keep_nginx_own_error_page(self, gateway):
        """Their clients are browsers and scripting clients, which are not
        helped by XML and were never promised it."""
        response = tile(gateway)

        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/html")

    def test_a_tile_route_401_is_untouched_too(self, gateway):
        response = tile(gateway, api_key=EXPIRED_KEY)

        assert response.status_code == 401
        assert response.headers["content-type"].startswith("text/html")

    @pytest.mark.parametrize("path", ["/titiler/{org}/wmtsfoo", "/titiler/{org}/wmts/tiles"])
    def test_an_address_that_merely_looks_like_the_endpoint_is_not_it(self, gateway, path):
        response = get(gateway, path.format(org=ORG))

        assert response.headers["content-type"].startswith("text/html")

    def test_the_shims_own_report_is_not_overwritten(self, gateway):
        """The arm answers the *gate's* refusals. Titiler's own 404 says which
        layer, style or TIME was the stranger, and must reach the client
        intact — ``proxy_intercept_errors`` stays off."""
        response = kvp(gateway, api_key=GOOD_KEY)

        assert response.status_code == 404
        assert "shim" in response.text

    def test_an_allowed_request_still_reaches_the_tile_server(self, gateway):
        response = tile(gateway, api_key=GOOD_KEY)

        assert response.status_code == 200
        assert response.text == "TILE"
