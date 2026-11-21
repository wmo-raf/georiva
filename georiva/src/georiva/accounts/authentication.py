"""Order matters in ``DEFAULT_AUTHENTICATION_CLASSES``: this runs before session
auth, so a request that presents a key is judged on the key even if the browser
happens to be signed in as somebody else — a script's credential is the explicit
one, and silently preferring a stale cookie would be surprising in exactly the
wrong direction.
"""

from rest_framework import exceptions
from rest_framework.authentication import BaseAuthentication, get_authorization_header

from .models import KEY_PREFIX, ApiKey

QUERY_PARAM = "api_key"

#: The ``Authorization`` scheme. ``Bearer`` rather than a bespoke word so the
#: header is the one every HTTP client already knows how to send.
SCHEME = "Bearer"


def presented_secret(request):
    """The API key this request presents, or ``None``.

    A ``Bearer`` token that is not ours is not ours: it is left for another
    authenticator rather than rejected, so adding a second scheme later does not
    mean unpicking this one.
    """
    header = get_authorization_header(request).split()
    if len(header) == 2 and header[0].lower() == SCHEME.lower().encode():
        candidate = header[1].decode("latin-1", errors="replace")
        if candidate.startswith(KEY_PREFIX):
            return candidate
        return None
    return request.query_params.get(QUERY_PARAM) or None


def query_presented_secret(request):
    """The judged secret, when it travelled as ``?api_key=`` — else ``None``.

    For surfaces that write the caller's credential back into URLs (the keyed
    WMTS capabilities document, #360). Only the query transport qualifies: a
    caller who can send an ``Authorization`` header can send it on the next
    request too, and a credential should not move to the weaker transport
    uninvited. And only the secret :func:`presented_secret` actually judged —
    with a Bearer header presented the header wins, so a query string nobody
    validated must never be advertised. Lives here so the header-over-query
    precedence has one author.
    """
    secret = request.query_params.get(QUERY_PARAM) or None
    if secret is not None and presented_secret(request) == secret:
        return secret
    return None


class ApiKeyAuthentication(BaseAuthentication):
    """Authenticate ``Authorization: Bearer grv_…`` or ``?api_key=grv_…``."""

    def authenticate(self, request):
        secret = presented_secret(request)
        if secret is None:
            # No key offered — not this authenticator's request. Session auth
            # gets its turn, and an anonymous caller still reads public data.
            return None

        key = ApiKey.objects.resolve(secret)
        if key is None:
            raise exceptions.AuthenticationFailed("Invalid or expired API key.")

        return key.user, key

    def authenticate_header(self, request):
        """Makes DRF answer 401 rather than 403 when this authenticator fails."""
        return SCHEME
