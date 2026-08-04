"""Host → Site → Organisation resolution, failing closed.

Tenancy on this instance lives in the hostname. This middleware is the single
place that turns a request's Host into ``request.active_org``, and it is
deliberately stricter than Wagtail's own ``Site.find_for_request`` — that helper
falls back to the default Site when no hostname matches, which here would
silently serve the central org's data to a request for an unknown host. Unknown
hostname is a 404.

Two paths are allowed to survive that 404, both on the machine plane: the
tile-config callback Titiler dials on an internal container name that is
genuinely nobody's, and the tile gateway nginx subrequests before proxying a
tile. Neither is exempt from tenancy — the first scopes on its own first path
segment, the second denies outright when no organisation answered. See
:meth:`OrganisationMiddleware._host_optional` for how narrowly that is drawn,
and ADR 0013 for why the machine plane needs it at all.

Membership is re-read from the database on every request. A session that
outlives its membership row loses access on its next request, not at logout.
"""
import logging

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.http import Http404
from django.urls import reverse
from wagtail.models import Site

from .access import resolve_org_role
from .models import Organisation

logger = logging.getLogger(__name__)


#: The tile-config endpoint Titiler calls back on a palette-cache miss. Titiler
#: dials it server-to-server on an internal container name, which is nobody's
#: hostname, so a 404 for an unknown host would break every call — see
#: :meth:`OrganisationMiddleware._host_optional`, which is where that is
#: allowed for and how narrowly.
TILE_CONFIG_PREFIX = "/api/tile-config/"

#: The gateway nginx subrequests before proxying a tile (#274). It is listed
#: here for a different reason than the one above: the Host it carries is the
#: browser's own and normally resolves fine. But ``auth_request`` understands
#: only 2xx, 401 and 403 — a 404 from this middleware would make nginx fail the
#: tile with a 500 instead of denying it — so an unknown host has to reach
#: ``TileAuthView``, which denies it in a language nginx speaks.
TILE_AUTH_PREFIX = "/internal/tile-auth/"


def admin_auth_paths():
    """The admin's own sign-in and sign-out URLs.

    Reversed rather than spelled out so the pair cannot drift from
    ``wagtail.admin.urls``, and resolved on call rather than at import because
    URL loading is not finished when this module is imported.
    """
    return (reverse("wagtailadmin_login"), reverse("wagtailadmin_logout"))


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
            if self._host_optional(request):
                request.active_org = None
                request.active_org_role = None
                return self.get_response(request)
            logger.warning("Rejected request for unknown organisation host: %s", hostname)
            raise Http404("No organisation is served at this address.")

        request.active_org = organisation
        # Keep Wagtail's page-serving view on the same Site we resolved, rather
        # than letting its default-site fallback pick a different one.
        request._wagtail_site = organisation.site
        request.active_org_role = self._resolve_role(request, organisation)

        self._guard_admin(request)

        return self.get_response(request)

    @staticmethod
    def _host_optional(request):
        """Whether this path may be served when no organisation answers the host.

        Two paths may, both on the machine plane and each for its own reason.

        The tile-config callback, because Titiler dials it on an internal
        container name that is nobody's hostname. It is *not* exempt from
        tenancy — it carries the organisation in its own first path segment and
        ``core.tile_config_view`` filters on it.

        The tile gateway, because it must answer a status ``auth_request``
        understands even when the host is unknown, and a 404 raised here is not
        one. It is not exempt from tenancy either: ``core.tile_auth_view`` denies
        every request that reaches it without an organisation, so surviving this
        404 buys such a request a 403 and nothing else.

        The narrowness is the point. Making a path unconditionally exempt would
        have been simpler and wrong: ``/api/`` is publicly proxied, so an
        anonymous caller on any tenant's host could then have read any other
        organisation's rendering config, and learned from a 200 which catalogs
        that organisation runs. The relaxation applies only where the stated
        reason actually holds — no organisation answers this host — and a
        request arriving on a host that *does* resolve is treated as the
        ordinary tenant request it is, path segment and all.
        """
        return request.path.startswith((TILE_CONFIG_PREFIX, TILE_AUTH_PREFIX))

    @staticmethod
    def _guard_admin(request):
        """Keep signed-in non-members out of an organisation's admin entirely.

        The admin is the one plane where every URL is tenant work, so membership
        is checked once here rather than view by view — row scoping still
        applies underneath, but a stranger never gets as far as an empty listing.
        Anonymous requests fall through: the login page lives under the same
        prefix, and a login is how somebody stops being anonymous.

        The two auth URLs themselves fall through for signed-in users too, and
        for the mirror of that reason. Sessions span every organisation's host —
        ``SESSION_COOKIE_DOMAIN`` is the shared base domain — so a member of one
        organisation typing another's hostname arrives here signed in and is
        refused. Without this exemption the sign-out button on that refusal is
        itself under ``/admin/`` and is refused identically, and the only way out
        of the dead end is clearing a cookie by hand. The relaxation is safe
        because it is drawn at exactly two views that read no organisation-scoped
        row: one ends a session, the other starts one, and whatever the new
        session may see is decided by this same check on its next request.

        The check re-reads the role the middleware just resolved from the
        database, so a revoked membership locks its former holder out on their
        very next request rather than at logout.
        """
        if not request.path.startswith(settings.GEORIVA_ADMIN_PATH_PREFIX):
            return
        if request.path in admin_auth_paths():
            return
        user = getattr(request, "user", None)
        if user is None or not user.is_authenticated:
            return
        if request.active_org_role is None:
            logger.warning(
                "Rejected %s from %s's admin: not a member",
                user, request.active_org.slug,
            )
            raise PermissionDenied("You are not a member of this organisation.")

    @staticmethod
    def _resolve_role(request, organisation):
        """The requesting user's live role in this organisation, or ``None``.

        The rule itself lives in ``access.resolve_org_role``, because the
        serving planes have to ask the same question of a user this middleware
        never saw — an API-key request is still anonymous here (#273).
        """
        return resolve_org_role(organisation, getattr(request, "user", None))
