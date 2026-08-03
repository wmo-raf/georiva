"""
GeoRiva EDR API Views

Implements OGC API - Environmental Data Retrieval 1.1 (19-086r6)
Currently: Collection metadata endpoints only.

Endpoint map:
    GET /api/edr/                              → Landing page
    GET /api/edr/conformance/                  → Conformance classes
    GET /api/edr/collections/                  → All EDR collections (summary)
    GET /api/edr/collections/{collection_slug}/→ EDR collection detail (full)

Collection ID = Collection.slug

Each host is one organisation's whole EDR service, resolved from the hostname
exactly as STAC's is (#271): the collection ids stay bare, and `?catalog=` names
a catalog inside *this* organisation or matches nothing.

Cross-API relationships:
    EDR collection detail  → links to STAC collection via canonical rel
    STAC collection        → can link to EDR collection detail

Future additions:
    GET /api/edr/collections/{slug}/position/
    GET /api/edr/collections/{slug}/area/
    GET /api/edr/collections/{slug}/locations/
    GET /api/edr/collections/{slug}/instances/
"""

from django.db.models import Exists, OuterRef
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
from .renderers import EDRJSONRenderer
from .serializers import (
    EDRCollectionSerializer,
    EDRCollectionSummarySerializer,
)


# =============================================================================
# Helpers
# =============================================================================

def _org_collections(request: Request):
    """The collections this organisation will serve to this caller."""
    return scoped_queryset(request, Collection.objects.visible_to(request))


# =============================================================================
# Base view
# =============================================================================

class EDRAPIView(APIView):
    """Base view for all EDR endpoints."""
    renderer_classes = [EDRJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


# =============================================================================
# Landing Page
# =============================================================================

class EDRLandingPageView(EDRAPIView):
    """
    EDR API Landing Page.

    GET /api/edr/

    Returns service metadata and links to conformance,
    collections, and API definition.
    """
    
    def get(self, request: Request) -> Response:
        base_url = get_full_url_by_request(request, '/api/edr/')
        organisation = require_active_org(request)

        return Response({
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
        })


# =============================================================================
# Conformance
# =============================================================================

class EDRConformanceView(EDRAPIView):
    """
    EDR Conformance Declaration.

    GET /api/edr/conformance/

    Declares which OGC conformance classes this implementation satisfies.
    Currently: core + collections metadata only.
    """
    
    def get(self, request: Request) -> Response:
        return Response({
            "conformsTo": [
                # OGC API Common
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                # OGC API EDR core
                "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core",
                # Encoding
                "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/oas30",
                "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/html",
                "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/geojson",
                # Future
                # "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/covjson",
            ]
        })


# =============================================================================
# Collection List
# =============================================================================

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
        base_url = get_full_url_by_request(request, '/api/edr/')
        
        queryset = _org_collections(request).select_related(
            'catalog',
        ).prefetch_related(
            'variables',
        ).annotate(
            # Single EXISTS subquery per collection — no per-row extra queries
            has_reference_time=Exists(
                Item.objects.filter(
                    collection=OuterRef('pk'),
                    reference_time__isnull=False,
                )
            )
        ).order_by('catalog__name', 'sort_order', 'name')
        
        catalog_slug = request.query_params.get('catalog')
        if catalog_slug:
            queryset = queryset.filter(catalog__slug=catalog_slug)
        
        collections_data = EDRCollectionSummarySerializer(
            queryset,
            many=True,
            context={'request': request},
        ).data
        
        return Response({
            "links": [
                {
                    "rel": "self",
                    "href": f"{base_url}collections/",
                    "type": "application/json",
                    "title": "EDR Collections",
                },
            ],
            "collections": collections_data,
        })


# =============================================================================
# Collection Detail
# =============================================================================

class EDRCollectionDetailView(EDRAPIView):
    """
    EDR Collection Detail — the GetCapabilities equivalent.

    GET /api/edr/collections/{collection_slug}/

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
    
    def get(self, request: Request, collection_slug: str) -> Response:
        collection = get_org_object_or_404(
            request,
            _org_collections(request).select_related(
                'catalog',
            ).prefetch_related(
                'variables',
                'variables__palette',
                'variables__palette__stops',
            ),
            slug=collection_slug,
        )

        data = EDRCollectionSerializer(
            collection,
            context={'request': request},
        ).data
        
        return Response(data)
