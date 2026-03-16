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

Cross-API relationships:
    EDR collection detail  → links to STAC collection via canonical rel
    STAC collection        → can link to EDR collection detail

Future additions:
    GET /api/edr/collections/{slug}/position/
    GET /api/edr/collections/{slug}/area/
    GET /api/edr/collections/{slug}/locations/
    GET /api/edr/collections/{slug}/instances/
"""

from django.shortcuts import get_object_or_404
from rest_framework.parsers import JSONParser
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.models import Collection
from .renderers import EDRJSONRenderer
from .serializers import (
    EDRCollectionSerializer,
    EDRCollectionSummarySerializer,
)


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
        base_url = request.build_absolute_uri('/api/edr/')
        
        return Response({
            "title": "GeoRiva EDR API",
            "description": (
                "OGC API - Environmental Data Retrieval service for "
                "Earth observation and meteorological data across African NMHSs."
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
                    "href": request.build_absolute_uri('/api/stac/'),
                    "type": "application/json",
                    "title": "GeoRiva STAC API",
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

    Returns a summary of every active Collection across all Catalogs.
    One GeoRiva Collection = one EDR Collection.

    Query Parameters:
        catalog (str): Filter by catalog slug. e.g. ?catalog=chirps
                       Returns only collections belonging to that catalog.

    Response uses the lightweight summary serializer — temporal.values
    and palette details are omitted for performance. Clients fetch
    the collection detail endpoint for the full metadata.
    """
    
    def get(self, request: Request) -> Response:
        base_url = request.build_absolute_uri('/api/edr/')
        
        queryset = Collection.objects.filter(
            is_active=True,
            catalog__is_active=True,
        ).select_related(
            'catalog',
        ).prefetch_related(
            'variables',
        ).order_by('catalog__name', 'sort_order', 'name')
        
        # Optional catalog filter
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
        collection = get_object_or_404(
            Collection.objects.select_related(
                'catalog',
            ).prefetch_related(
                'variables',
                'variables__palette',
                'variables__palette__stops',
            ),
            slug=collection_slug,
            is_active=True,
            catalog__is_active=True,
        )
        
        data = EDRCollectionSerializer(
            collection,
            context={'request': request},
        ).data
        
        return Response(data)
