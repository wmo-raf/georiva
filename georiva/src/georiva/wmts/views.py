"""GET /api/wmts/{org_slug}/WMTSCapabilities.xml — the REST capabilities document.

The metadata-plane spelling of WMTS discovery (#354): what API tooling reads
directly, and what the KVP shim in ``titiler-app`` fetches and hands on unread
when answering ``REQUEST=GetCapabilities`` (#362), so one pasted URL discovers
layers and then serves their tiles. The org appears twice — in the dialled
host and in the path — and the host is the authority: the path may only agree
with it, exactly as on the tile-config callback. A mismatch is reported as
absent, not forbidden, because which catalogs another institution runs is not
this caller's business.

A DRF view rather than a plain one so a presented API key becomes
``request.user`` through the project's one identity path (#360): the same
``ApiKeyAuthentication``-then-session order every serving plane uses, so a
member's key widens ``visible_to`` here exactly as it does the tile gate, and
a broken key answers the same 401 (ADR 0014).
"""

from django.http import Http404, HttpResponse
from django.utils.cache import patch_cache_control
from rest_framework.negotiation import BaseContentNegotiation
from rest_framework.views import APIView

from georiva.organisations.access import require_active_org

from .cache import capabilities_document, is_personal


class OneRepresentation(BaseContentNegotiation):
    """Serve the document to whatever ``Accept`` a legacy client sends.

    The capabilities body has exactly one representation, built below and
    returned as a plain ``HttpResponse`` — DRF's negotiation never renders it.
    Left to the default, a client asking for ``application/xml`` (which some
    OGC clients do) would be refused with a 406 for not matching the JSON
    renderers, on a document that was never JSON.
    """

    def select_renderer(self, request, renderers, format_suffix):
        return renderers[0], renderers[0].media_type


class WMTSCapabilitiesView(APIView):
    permission_classes = []
    content_negotiation_class = OneRepresentation

    def get(self, request, org_slug):
        organisation = require_active_org(request)
        if organisation.slug != org_slug:
            raise Http404
        response = HttpResponse(
            capabilities_document(request, organisation),
            content_type="application/xml",
        )
        if is_personal(request):
            # A credentialed document may list private layers and may carry the
            # caller's own key in its URLs; whatever the transport, it is
            # personal and must never enter a shared cache (#360). The same
            # predicate keeps it out of the shared Redis entry (#361), so the
            # header and the cache cannot come to disagree.
            patch_cache_control(response, private=True)
        return response
