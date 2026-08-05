"""The gate nginx asks before it proxies a tile.

Titiler and Martin are the two services on this instance that answer without
resolving a tenant (ADR 0013), and until now that meant they answered everyone.
Django hid a ``private`` collection on all four of its own planes (ADR 0014) and
served its tiles to anybody who could spell the URL — a side door wide enough
that ADR 0014 had to tell operators not to treat the tier as a security boundary
for rasters at all.

This closes it, and the shape is chosen so that neither tile server learns
anything. Nginx issues an ``auth_request`` subrequest here before proxying,
carrying the original request line in ``X-Original-URI`` and the caller's own
credentials — session cookie, ``Authorization`` header, ``?api_key=`` — because
a subrequest inherits them. Django answers 204 or it does not, nginx proxies or
it does not, and Titiler and Martin still hold no tenancy logic. That is the
property ADR 0013 set out to protect and this had to preserve: a gate copied
into a second and third service is a gate that drifts.

Three rules decide the answer, and each is the same one the Django planes use:

*The address is read by the module that writes it.*
``machine_plane.scope_of`` is the inverse of the builders beside it, so the
collection this authorises is the collection the tile server will read. A URI it
does not recognise scopes to nothing and is denied — every Titiler route and
Martin's one published source are recognised, so what is left addresses no data.

*The host still names the organisation.* Unlike the tile-config callback, this
subrequest carries the browser's real Host, so there is a Host to check the path
segment against and it is checked: an org segment that disagrees with the host
is denied. The machine plane's path segment is the only copy of the tenant where
there is no Host (ADR 0013) — here there is one, so it wins.

*Public is answered without asking who is calling.* One indexed query over
``Collection.objects.public()`` settles the overwhelming majority of tiles and
never touches a membership row. Only when that misses does the private tier get
asked, through ``visible_to`` — the same seam STAC, EDR and the portal read, so
the tile plane cannot drift from them on what a caller may see. ``internal`` is
in neither queryset, so it is denied without a rule of its own.

Denials are 403, which nginx turns into the 404 the other planes already give:
a private tile a caller may not see does not exist, and a status that
distinguished "forbidden" from "absent" would answer precisely the question the
tier exists to keep quiet. A *broken* key still surfaces as 401 — nginx passes
that through untouched — because that answer is the same whether the collection
is there or not, and it is what tells a QGIS user their key expired rather than
leaving them debugging a dataset that appears to have vanished (ADR 0014).
"""
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.machine_plane import scope_of
from georiva.core.models import Collection

#: The header nginx forwards the original request line in. The subrequest's own
#: URI is this endpoint's, so the address being judged has to travel separately.
ORIGINAL_URI_HEADER = "HTTP_X_ORIGINAL_URI"


class TileAuthView(APIView):
    """Answer 204 if this request may be proxied to a tile server, else 403.

    Authentication is the project default — API key first, then session — so a
    tile URL carrying ``?api_key=`` and a browser holding a cookie arrive at the
    same identity, and ``organisations.access`` decides everything after that.
    """

    permission_classes = []

    def get(self, request):
        scope = scope_of(request.META.get(ORIGINAL_URI_HEADER, ""))
        active_org = getattr(request, "active_org", None)
        if scope is None or active_org is None or active_org.slug != scope.org:
            # No recognised address, no organisation on this host, or a path
            # segment naming a different one than the host does. Nothing here is
            # a question about the caller, so nothing here is worth a distinct
            # answer.
            return self.deny()

        collections = Collection.objects.filter(
            catalog__organisation=active_org,
            catalog__slug=scope.catalog,
            slug=scope.collection,
        )
        if collections.public().exists():
            # The fast path, and the reason it is first: a public tile is
            # settled by one indexed query and never reads a membership row.
            return self.allow()
        if collections.visible_to(request).exists():
            return self.allow()
        return self.deny()

    @staticmethod
    def allow():
        return Response(status=status.HTTP_204_NO_CONTENT)

    @staticmethod
    def deny():
        """403, which nginx rewrites to the 404 every other plane answers.

        Left as 403 rather than answered as 404 here because ``auth_request``
        only understands 2xx, 401 and 403; any other status makes nginx fail the
        outer request with a 500. The 404 the caller actually sees is one
        ``error_page`` line away, and is in ``deploy/nginx/nginx.conf`` beside
        the gate it applies to.
        """
        return Response(status=status.HTTP_403_FORBIDDEN)
