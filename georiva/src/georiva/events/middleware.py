"""Set the event context for authenticated admin and upload requests (ADR 0030).

A chain that a person starts begins with that person: Actor ``user:<id>`` and the
organisation the request dials. Anything the request enqueues carries those
values in its task headers, so a manual upload or an operator clicking "re-run"
is attributed without any view having to say so.

An inbound ``X-Correlation-ID`` header is honoured on these routes only, so an
integrator's id and GeoRiva's chain can line up. Public read APIs and the tile
servers are untouched: an anonymous caller must not be able to name the
Correlation of anything.

No third-party correlation package: it is one small class.
"""

import uuid

from .context import new_context, reset_context, set_context

CORRELATION_HEADER = "X-Correlation-ID"

#: Authenticated routes that start work. The upload views live under the admin.
CONTEXT_PATH_PREFIXES = ("/admin/",)


def inbound_correlation_id(request):
    raw = request.headers.get(CORRELATION_HEADER, "").strip()
    try:
        return uuid.UUID(raw) if raw else None
    except ValueError:
        return None


class EventContextMiddleware:
    """Must run after ``AuthenticationMiddleware`` and ``OrganisationMiddleware``."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = getattr(request, "user", None)
        if not request.path.startswith(CONTEXT_PATH_PREFIXES) or user is None or not user.is_authenticated:
            return self.get_response(request)

        organisation = getattr(request, "active_org", None)
        context = new_context(
            actor=f"user:{user.pk}",
            organisation_id=organisation.pk if organisation else None,
            correlation_id=inbound_correlation_id(request),
        )
        token = set_context(context)
        try:
            return self.get_response(request)
        finally:
            reset_context(token)
