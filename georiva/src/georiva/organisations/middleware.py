"""Host → Site → Organisation resolution, failing closed.

Tenancy on this instance lives in the hostname: there are no org path segments
anywhere. This middleware is the single place that turns a request's Host into
``request.active_org``, and it is deliberately stricter than Wagtail's own
``Site.find_for_request`` — that helper falls back to the default Site when no
hostname matches, which here would silently serve the central org's data to a
request for an unknown host. Unknown hostname is a 404, always.

Membership is re-read from the database on every request. A session that
outlives its membership row loses access on its next request, not at logout.
"""
import logging

from django.conf import settings
from django.http import Http404
from wagtail.models import Site

from .models import Organisation, OrganisationMembership

logger = logging.getLogger(__name__)


def exempt_path_prefixes():
    """URL prefixes served without an organisation.

    Kept to the paths no tenant owns: the container healthcheck, and the static
    and media files the dev server hands out. Everything else must resolve to an
    organisation or 404.
    """
    prefixes = ["/health/"]
    for url in (getattr(settings, "STATIC_URL", None), getattr(settings, "MEDIA_URL", None)):
        if url and url.startswith("/"):
            prefixes.append(url)
    return tuple(prefixes)


def resolve_organisation_for_host(hostname, port=None):
    """The Organisation serving ``hostname``, or ``None``.

    The hostname must match a Site — never Wagtail's default-site fallback. The
    port only breaks ties between Sites sharing a hostname: a request's port is
    routinely not the Site's (the dev server answers on 8000, the proxy on 443,
    while Sites are provisioned on 80), so a hostname match with a different
    port is still a match.
    """
    sites = Site.objects.filter(hostname=hostname.lower())
    site = None
    if port is not None:
        site = sites.filter(port=port).first()
    if site is None:
        site = sites.first()
    if site is None:
        return None
    return Organisation.objects.filter(site=site).select_related("site").first()


class OrganisationMiddleware:
    """Attaches ``request.active_org`` and ``request.active_org_role``.

    Must run after ``AuthenticationMiddleware``: the role depends on
    ``request.user``.
    """

    def __init__(self, get_response):
        self.get_response = get_response
        self.exempt_prefixes = exempt_path_prefixes()

    def __call__(self, request):
        if request.path.startswith(self.exempt_prefixes):
            # Infrastructure endpoints (container healthcheck, static and media
            # files in dev) belong to no organisation and must answer on the
            # internal hostname the platform dials them on.
            request.active_org = None
            request.active_org_role = None
            return self.get_response(request)

        hostname = request.get_host().split(":")[0]
        try:
            port = int(request.get_port())
        except (TypeError, ValueError):
            port = None

        organisation = resolve_organisation_for_host(hostname, port)
        if organisation is None:
            logger.warning("Rejected request for unknown organisation host: %s", hostname)
            raise Http404("No organisation is served at this address.")

        request.active_org = organisation
        # Keep Wagtail's page-serving view on the same Site we resolved, rather
        # than letting its default-site fallback pick a different one.
        request._wagtail_site = organisation.site
        request.active_org_role = self._resolve_role(request, organisation)

        return self.get_response(request)

    @staticmethod
    def _resolve_role(request, organisation):
        """The requesting user's live role in this organisation, or ``None``.

        Superusers are the instance admin: they enter any host as an org admin
        without holding a membership row.
        """
        user = getattr(request, "user", None)
        if user is None or not user.is_authenticated or not user.is_active:
            return None
        if user.is_superuser:
            return OrganisationMembership.Role.ADMIN
        membership = organisation.membership_for(user)
        return membership.role if membership else None
