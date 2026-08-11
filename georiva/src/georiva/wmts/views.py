"""GET /api/wmts/{org_slug}/WMTSCapabilities.xml — the REST capabilities document.

The metadata-plane spelling of WMTS discovery (#354): what API tooling reads
directly, and what Titiler will fetch when proxying a KVP GetCapabilities in a
later slice. The org appears twice — in the dialled host and in the path — and
the host is the authority: the path may only agree with it, exactly as on the
tile-config callback. A mismatch is reported as absent, not forbidden, because
which catalogs another institution runs is not this caller's business.
"""
from django.http import Http404, HttpResponse
from django.views import View

from georiva.organisations.access import require_active_org

from .capabilities import build_capabilities


class WMTSCapabilitiesView(View):

    def get(self, request, org_slug):
        organisation = require_active_org(request)
        if organisation.slug != org_slug:
            raise Http404
        return HttpResponse(
            build_capabilities(request, organisation),
            content_type="application/xml",
        )
