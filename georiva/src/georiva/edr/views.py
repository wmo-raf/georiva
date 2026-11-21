"""
GeoRiva EDR API Views

Implements OGC API - Environmental Data Retrieval 1.1 (19-086r6)
Currently: Collection metadata endpoints only.

Endpoint map:
    GET /api/edr/                              → Landing page
    GET /api/edr/conformance/                  → Conformance classes
    GET /api/edr/collections/                  → All EDR collections (summary)
    GET /api/edr/collections/{collection_id}/  → EDR collection detail (full)

Collection ID = `{catalog}.{collection}` (ADR 0045, #687). A collection slug is
unique only within its catalog; the pair is unique within an organisation. A
bare slug still answers while it names one collection the caller may see — a
redirect to the qualified id, sub-path and query kept — and is a 404 listing the
qualified ids when it names more than one.

Each host is one organisation's whole EDR service, resolved from the hostname
exactly as STAC's is (#271): the organisation never appears in an id, and
`?catalog=` names a catalog inside *this* organisation or matches nothing.

Cross-API relationships:
    EDR collection detail  → links to STAC collection via canonical rel
    STAC collection        → can link to EDR collection detail
"""

from django.db.models import Exists, OuterRef
from django.http import Http404, HttpResponseRedirect
from django.urls import reverse
from django.utils.encoding import iri_to_uri
from rest_framework.exceptions import NotFound
from rest_framework.parsers import JSONParser
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.models import Collection, Item
from georiva.core.utils import get_base_stac_api_url, get_full_url_by_request
from georiva.organisations.access import (
    get_org_object_or_404,
    require_active_org,
    scoped_queryset,
)

from .ids import edr_collection_href, edr_collection_id, split_edr_collection_id
from .renderers import EDRJSONRenderer
from .serializers import (
    EDRCollectionSerializer,
    EDRCollectionSummarySerializer,
)


def _org_collections(request: Request, queryset=None):
    """The collections this organisation will serve to this caller."""
    queryset = Collection.objects.all() if queryset is None else queryset
    return scoped_queryset(request, queryset.visible_to(request))


def resolve_edr_collection(request, collection_id: str, queryset=None):
    """The collection ``collection_id`` names on this host, or the redirect to its qualified id.

    A qualified id (``{catalog}.{collection}``) is the collection or a 404. A
    bare slug is the legacy form: when it names exactly one collection this
    caller may see, the answer is a redirect to the same path under the
    qualified id — whatever follows the id, a sub-route or a query string, is
    kept. When it names several, a 404 lists their ids, so the caller can
    choose. Collections the caller may not see neither make a slug ambiguous
    nor appear in that list.

    ``queryset`` narrows or prefetches; organisation and visibility are applied
    here whatever it is.
    """
    collections = _org_collections(request, queryset)
    qualified = split_edr_collection_id(collection_id)
    if qualified is not None:
        catalog_slug, collection_slug = qualified
        return get_org_object_or_404(request, collections, catalog__slug=catalog_slug, slug=collection_slug)

    matches = list(collections.filter(slug=collection_id).select_related("catalog").order_by("catalog__slug"))
    if not matches:
        raise Http404(f"No collection {collection_id!r}.")
    if len(matches) == 1:
        # Whatever follows the id — a sub-route, a query string — is kept.
        bare_prefix = reverse("edr:collection-detail", args=[collection_id])
        rest = request.path.removeprefix(bare_prefix) if request.path.startswith(bare_prefix) else ""
        target = reverse("edr:collection-detail", args=[edr_collection_id(matches[0])]) + rest
        query = request.META.get("QUERY_STRING", "")
        return HttpResponseRedirect(f"{target}?{iri_to_uri(query)}" if query else target)

    base_url = get_full_url_by_request(request, "/api/edr/")
    raise NotFound(
        {
            "detail": (
                f"{collection_id!r} names {len(matches)} collections; an EDR collection id is "
                "'{catalog}.{collection}'. Use one of these."
            ),
            "collections": [
                {"id": edr_collection_id(match), "title": match.name, "href": edr_collection_href(base_url, match)}
                for match in matches
            ],
        }
    )


class EDRAPIView(APIView):
    renderer_classes = [EDRJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


class EDRLandingPageView(EDRAPIView):
    def get(self, request: Request) -> Response:
        base_url = get_full_url_by_request(request, "/api/edr/")
        organisation = require_active_org(request)

        return Response(
            {
                "title": f"{organisation.name} EDR API",
                "description": (
                    organisation.description
                    or (
                        "OGC API - Environmental Data Retrieval service for "
                        f"{organisation.name}'s Earth observation and meteorological data."
                    )
                ),
                "links": [
                    {
                        "rel": "self",
                        "href": base_url,
                        "type": "application/json",
                        "title": "This document",
                    },
                    {
                        "rel": "conformance",
                        "href": f"{base_url}conformance/",
                        "type": "application/json",
                        "title": "Conformance classes",
                    },
                    {
                        "rel": "data",
                        "href": f"{base_url}collections/",
                        "type": "application/json",
                        "title": "Access the data",
                    },
                    {
                        "rel": "related",
                        "href": get_base_stac_api_url(request),
                        "type": "application/json",
                        "title": f"{organisation.name} STAC API",
                    },
                ],
            }
        )


class EDRConformanceView(EDRAPIView):
    def get(self, request: Request) -> Response:
        return Response(
            {
                "conformsTo": [
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core",
                    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/oas30",
                    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/html",
                    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/geojson",
                ]
            }
        )


class EDRCollectionListView(EDRAPIView):
    """
    List all EDR Collections.

    GET /api/edr/collections/

    Annotates each Collection with has_reference_time (single subquery)
    so EDRCollectionSummarySerializer can read it without extra DB hits.

    Query Parameters:
        catalog (str): Filter by catalog slug. e.g. ?catalog=chirps
    """

    def get(self, request: Request) -> Response:
        base_url = get_full_url_by_request(request, "/api/edr/")

        queryset = (
            _org_collections(request)
            .select_related(
                "catalog",
            )
            .prefetch_related(
                "variables",
            )
            .annotate(
                has_reference_time=Exists(
                    Item.objects.filter(
                        collection=OuterRef("pk"),
                        reference_time__isnull=False,
                    )
                )
            )
            .order_by("catalog__name", "sort_order", "name")
        )

        catalog_slug = request.query_params.get("catalog")
        if catalog_slug:
            queryset = queryset.filter(catalog__slug=catalog_slug)

        collections_data = EDRCollectionSummarySerializer(
            queryset,
            many=True,
            context={"request": request},
        ).data

        return Response(
            {
                "links": [
                    {
                        "rel": "self",
                        "href": f"{base_url}collections/",
                        "type": "application/json",
                        "title": "EDR Collections",
                    },
                ],
                "collections": collections_data,
            }
        )


class EDRCollectionDetailView(EDRAPIView):
    """
    EDR Collection Detail — the GetCapabilities equivalent.

    GET /api/edr/collections/{collection_id}/

    Returns the full EDR collection metadata for a single GeoRiva Collection:

        - extent.spatial.bbox        → map bounds
        - extent.temporal.values     → explicit timestep list for time slider
        - extent.temporal.interval   → coarse time range
        - parameter_names            → variables with labels, units, palette
        - parameter_names[slug]['x-georiva']
                                     → WeatherLayers palette, value range,
                                        scale type, rendering hints
        - data_queries               → advertised query endpoints
        - links[rel=canonical]       → cross-link to STAC collection
    """

    def get(self, request: Request, collection_id: str) -> Response:
        collection = resolve_edr_collection(
            request,
            collection_id,
            Collection.objects.select_related("catalog").prefetch_related(
                "variables",
                "variables__styles__ramp",
            ),
        )
        if isinstance(collection, HttpResponseRedirect):
            return collection

        data = EDRCollectionSerializer(
            collection,
            context={"request": request},
        ).data

        return Response(data)
