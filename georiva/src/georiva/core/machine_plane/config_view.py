"""Internal tile-config endpoint consumed by Titiler on Redis cache miss.

GET /api/tile-config/{org_slug}/{catalog_slug}/{collection_slug}/{variable_slug}/

The one lookup on the instance that can take its organisation from the path.
Titiler calls it server-to-server over an internal container name that belongs
to no organisation, so on that call there is no Host to resolve one from; the
org travels in the path instead, exactly as it does in the Titiler route that
provoked the call, and Titiler simply forwards the segment it was given.
``OrganisationMiddleware`` allows for that by letting this one prefix through
when no organisation answers the host, rather than 404ing it.

Two rules follow, and the endpoint is only safe with both:

*It scopes itself.* Catalog slugs are unique only within an organisation, so the
lookup is by the ``(org, catalog, collection, variable)`` quad and nothing
looser.

*A resolved Host still wins.* ``/api/`` is publicly proxied, so this URL is
reachable by anyone — and if the path segment were trusted there too, a caller
on one tenant's host could read another's rendering config and learn from the
200 which catalogs it runs. So when the request *does* belong to an
organisation, the segment must name that same one.

Both of which are also why this endpoint serves ``public`` collections and
nothing else. Every other Django plane widened to the ``private`` tier in #273
by asking who is calling; this one cannot, because on the call it exists for
there is nobody to ask — Titiler forwards no credential and holds no session.
Answering for a private collection would therefore mean answering for anyone,
so it answers for no one, and the palette cache Django warms directly — for
every active variable, whatever its tier — is how a private variable renders.

The nginx gateway (#274, ADR 0015) does not change that, though it is easy to
assume it does. It authorises the *browser's* request before Titiler ever sees
it; this is Titiler's own onward call, made from a container name, carrying
nothing of the caller. Reading the gateway's earlier decision here would mean
inventing something for Titiler to prove it with. What the gateway does change
is when this matters: a private variable's tiles now reach Titiler, so a palette
cache miss on one is a tile that does not render rather than one that was never
asked for.

``?style=<slug>`` selects one of the variable's named styles; omission means
the default (ADR 0023). An unknown slug is a 404 and never a fallback —
silently serving the wrong style would be worse than failing — and 404 rather
than anything more talkative for the same reason the org mismatch above is:
which styles a variable carries is nobody's business but a caller who can
already see it.

Returns the Redis palette cache's payload structure plus a ``styles`` index —
discovery rides this endpoint rather than a new one (ADR 0023), so a client
holding a tile-config response can build a style picker from it. The index
lists every style whichever one was resolved; the Redis values themselves stay
index-free, being Titiler's rendering config and nothing more:
  With a style: {"vmin", "vmax", "scale_type", "colormap": {0-255 entries},
                 "styles": [{"slug", "title", "is_default"}, ...]}
  Without one:  {"vmin", "vmax", "scale_type", "styles": []}
"""

from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status

from georiva.core.models import Collection, Variable
from georiva.core.machine_plane.palette_cache import build_variable_payload


class TileConfigView(APIView):
    """Return rendering config for a variable (internal use by Titiler)."""

    permission_classes = []
    authentication_classes = []

    def get(self, request, org_slug, catalog_slug, collection_slug, variable_slug):
        active_org = getattr(request, "active_org", None)
        if active_org is not None and active_org.slug != org_slug:
            # A host that resolved is the authority; the path may only agree
            # with it. Reported as absent, not forbidden — which catalogs
            # another institution runs is not this caller's business.
            return Response(status=status.HTTP_404_NOT_FOUND)

        try:
            variable = (
                Variable.objects.select_related("collection__catalog__organisation")
                .prefetch_related("styles")
                .get(
                    collection__catalog__organisation__slug=org_slug,
                    collection__catalog__slug=catalog_slug,
                    collection__slug=collection_slug,
                    slug=variable_slug,
                    is_active=True,
                    collection__visibility=Collection.Visibility.PUBLIC,
                )
            )
        except Variable.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        style = None
        style_slug = request.query_params.get("style") or None
        if style_slug is not None:
            # Scanned in Python, not `.filter()`: the styles are prefetched
            # above, and cardinality is styles-per-variable (ADR 0023).
            style = next(
                (s for s in variable.styles.all() if s.slug == style_slug),
                None,
            )
            if style is None:
                return Response(status=status.HTTP_404_NOT_FOUND)

        payload = build_variable_payload(variable, style)
        payload["styles"] = [
            {"slug": s.slug, "title": s.name, "is_default": s.is_default} for s in variable.styles.all()
        ]
        return Response(payload)
