"""
GeoRiva STAC API Views

Implements STAC API v1.0.0 endpoints:
- GET /stac/ - Landing page (catalog)
- GET /stac/conformance - Conformance classes
- GET /stac/collections - List collections
- GET /stac/collections/{collectionId} - Get collection
- GET /stac/collections/{collectionId}/items - List items
- GET /stac/collections/{collectionId}/items/{itemId} - Get item
- GET/POST /stac/search - Search items
"""

from datetime import datetime
from typing import Optional

from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.models import Catalog, Collection, Item
from .renderers import STACJSONRenderer, GeoJSONRenderer
from .serializers import (
    STACCatalogSerializer,
    STACCollectionSerializer,
    STACItemCollectionSerializer,
    STACItemSerializer,
)


# Base classes with renderer configuration
class STACAPIView(APIView):
    """Base view for STAC catalog/collection endpoints."""
    renderer_classes = [STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


class STACGeoAPIView(APIView):
    """Base view for STAC item endpoints (GeoJSON)."""
    renderer_classes = [GeoJSONRenderer, STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


# =============================================================================
# Landing Page
# =============================================================================

class STACLandingPageView(STACAPIView):
    """
    STAC API Landing Page (Root Catalog)
    
    GET /stac/
    """
    
    def get(self, request: Request) -> Response:
        catalogs = Catalog.objects.filter(is_active=True).prefetch_related('collections')
        
        data = STACCatalogSerializer(
            {'catalogs': catalogs},
            context={'request': request}
        ).data
        
        return Response(data)


# =============================================================================
# Conformance
# =============================================================================

class STACConformanceView(STACAPIView):
    """
    STAC API Conformance Declaration
    
    GET /stac/conformance
    """
    
    def get(self, request: Request) -> Response:
        return Response({
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/ogcapi-features",
                "https://api.stacspec.org/v1.0.0/item-search",
                "https://api.stacspec.org/v1.0.0/item-search#filter",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            ]
        })


# =============================================================================
# Collections
# =============================================================================

class STACCollectionsView(STACAPIView):
    """
    List all STAC Collections
    
    GET /stac/collections
    """
    
    def get(self, request: Request) -> Response:
        collections = Collection.objects.filter(
            is_active=True,
            catalog__is_active=True
        ).select_related('catalog').prefetch_related('variables')
        
        serialized = [
            STACCollectionSerializer(c, context={'request': request}).data
            for c in collections
        ]
        
        return Response({
            "collections": serialized,
            "links": [
                {
                    "rel": "self",
                    "href": request.build_absolute_uri(),
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": request.build_absolute_uri('/stac/'),
                    "type": "application/json",
                },
            ]
        })


class STACCollectionDetailView(STACAPIView):
    """
    Get a specific STAC Collection
    
    GET /stac/collections/{catalog_slug}/{collection_slug}
    """
    
    def get(self, request: Request, catalog_slug: str, collection_slug: str) -> Response:
        collection = get_object_or_404(
            Collection.objects.select_related('catalog').prefetch_related('variables'),
            catalog__slug=catalog_slug,
            slug=collection_slug,
            is_active=True,
        )
        
        data = STACCollectionSerializer(collection, context={'request': request}).data
        return Response(data)


# =============================================================================
# Items
# =============================================================================

class STACItemsView(STACGeoAPIView):
    """
    List Items in a Collection (OGC Features)
    
    GET /stac/collections/{catalog_slug}/{collection_slug}/items
    
    Query Parameters:
        - limit: Max items to return (default 100, max 1000)
        - datetime: Temporal filter (single datetime or range)
        - bbox: Bounding box filter [west,south,east,north]
        - token: Pagination token (datetime of last item)
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            collection_slug: str
    ) -> Response:
        collection = get_object_or_404(
            Collection,
            catalog__slug=catalog_slug,
            slug=collection_slug,
            is_active=True,
        )
        
        # Parse query parameters
        limit = min(int(request.query_params.get('limit', 100)), 1000)
        datetime_param = request.query_params.get('datetime')
        bbox_param = request.query_params.get('bbox')
        token = request.query_params.get('token')
        
        # Build query
        queryset = Item.objects.filter(collection=collection)
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Apply datetime filter
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        # Apply bbox filter
        if bbox_param:
            queryset = self._apply_bbox_filter(queryset, bbox_param)
        
        # Apply pagination token
        if token:
            try:
                token_dt = datetime.fromisoformat(token.replace('Z', '+00:00'))
                queryset = queryset.filter(time__lt=token_dt)
            except ValueError:
                pass
        
        # Order and limit
        queryset = queryset.order_by('-time')
        total_count = queryset.count()
        items = list(queryset[:limit + 1])
        
        # Check for next page
        has_next = len(items) > limit
        if has_next:
            items = items[:limit]
            next_token = items[-1].time.isoformat()
        else:
            next_token = None
        
        # Serialize
        data = STACItemCollectionSerializer(
            {
                'items': items,
                'total_count': total_count,
                'limit': limit,
                'next_token': next_token,
            },
            context={'request': request}
        ).data
        
        return Response(data)
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        """Apply datetime filter per OGC API."""
        if '/' in datetime_param:
            # Range: start/end
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] != '..' else None
            end = parts[1] if len(parts) > 1 and parts[1] != '..' else None
            
            if start:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                queryset = queryset.filter(time__gte=start_dt)
            if end:
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                queryset = queryset.filter(time__lte=end_dt)
        else:
            # Single datetime - match exactly or within a day
            dt = datetime.fromisoformat(datetime_param.replace('Z', '+00:00'))
            queryset = queryset.filter(time=dt)
        
        return queryset
    
    def _apply_bbox_filter(self, queryset, bbox_param: str):
        """Apply bounding box filter."""
        try:
            bbox = [float(x) for x in bbox_param.split(',')]
            if len(bbox) == 4:
                west, south, east, north = bbox
                # Filter items whose bounds intersect with bbox
                # This is a simplified check - for proper spatial queries use PostGIS
                queryset = queryset.filter(
                    bounds__0__lte=east,  # item.west <= query.east
                    bounds__2__gte=west,  # item.east >= query.west
                    bounds__1__lte=north,  # item.south <= query.north
                    bounds__3__gte=south,  # item.north >= query.south
                )
        except (ValueError, TypeError):
            pass
        return queryset


class STACItemDetailView(STACGeoAPIView):
    """
    Get a specific STAC Item
    
    GET /stac/collections/{catalog_slug}/{collection_slug}/items/{item_id}
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            collection_slug: str,
            item_id: str
    ) -> Response:
        collection = get_object_or_404(
            Collection,
            catalog__slug=catalog_slug,
            slug=collection_slug,
        )
        
        # Parse item_id to extract datetime
        # Format: {collection_slug}_{datetime} or {collection_slug}_{ref_datetime}_{valid_datetime}
        item = self._find_item(collection, item_id)
        
        if not item:
            return Response(
                {"code": "NotFound", "description": f"Item {item_id} not found"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        data = STACItemSerializer(item, context={'request': request}).data
        return Response(data)
    
    def _find_item(self, collection, item_id: str) -> Optional[Item]:
        """Parse item_id and find the corresponding Item."""
        # Try to parse the item_id
        parts = item_id.split('_')
        
        # Remove collection slug prefix if present
        if parts[0] == collection.slug:
            parts = parts[1:]
        
        try:
            if len(parts) == 1:
                # Single datetime: valid_time only
                valid_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                return Item.objects.filter(
                    collection=collection,
                    time=valid_time,
                    reference_time__isnull=True
                ).prefetch_related('assets', 'assets__variable').first()
            
            elif len(parts) == 2:
                # Two datetimes: reference_time + valid_time
                ref_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                valid_time = datetime.strptime(parts[1], '%Y%m%dT%H%M%SZ')
                return Item.objects.filter(
                    collection=collection,
                    time=valid_time,
                    reference_time=ref_time
                ).prefetch_related('assets', 'assets__variable').first()
        except ValueError:
            pass
        
        return None


# =============================================================================
# Search
# =============================================================================

class STACSearchView(STACGeoAPIView):
    """
    STAC Item Search
    
    GET/POST /stac/search
    
    Supports cross-collection search with filters:
        - collections: List of collection IDs
        - ids: List of item IDs
        - bbox: Bounding box [west, south, east, north]
        - datetime: Temporal filter
        - limit: Max results (default 100)
        - intersects: GeoJSON geometry
        - filter: CQL2 filter expression (future)
    """
    
    def get(self, request: Request) -> Response:
        params = {
            'collections': request.query_params.getlist('collections'),
            'ids': request.query_params.getlist('ids'),
            'bbox': request.query_params.get('bbox'),
            'datetime': request.query_params.get('datetime'),
            'limit': request.query_params.get('limit', 100),
            'token': request.query_params.get('token'),
        }
        return self._search(request, params)
    
    def post(self, request: Request) -> Response:
        params = request.data
        return self._search(request, params)
    
    def _search(self, request: Request, params: dict) -> Response:
        queryset = Item.objects.filter(
            collection__is_active=True,
            collection__catalog__is_active=True
        )
        queryset = queryset.select_related('collection', 'collection__catalog')
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Filter by collections
        collections = params.get('collections', [])
        if collections:
            q_filter = Q()
            for coll_id in collections:
                if '/' in coll_id:
                    catalog_slug, coll_slug = coll_id.split('/', 1)
                    q_filter |= Q(
                        collection__catalog__slug=catalog_slug,
                        collection__slug=coll_slug
                    )
                else:
                    q_filter |= Q(collection__slug=coll_id)
            queryset = queryset.filter(q_filter)
        
        # Filter by datetime
        datetime_param = params.get('datetime')
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        # Filter by bbox
        bbox = params.get('bbox')
        if bbox:
            if isinstance(bbox, str):
                bbox = [float(x) for x in bbox.split(',')]
            if len(bbox) == 4:
                west, south, east, north = bbox
                queryset = queryset.filter(
                    bounds__0__lte=east,
                    bounds__2__gte=west,
                    bounds__1__lte=north,
                    bounds__3__gte=south,
                )
        
        # Filter by intersects (GeoJSON geometry)
        intersects = params.get('intersects')
        if intersects:
            # For proper spatial queries, use PostGIS
            # This is a simplified bbox-based check
            if intersects.get('type') == 'Polygon':
                coords = intersects['coordinates'][0]
                bbox = [
                    min(c[0] for c in coords),
                    min(c[1] for c in coords),
                    max(c[0] for c in coords),
                    max(c[1] for c in coords),
                ]
                queryset = queryset.filter(
                    bounds__0__lte=bbox[2],
                    bounds__2__gte=bbox[0],
                    bounds__1__lte=bbox[3],
                    bounds__3__gte=bbox[1],
                )
        
        # Pagination
        limit = min(int(params.get('limit', 100)), 1000)
        token = params.get('token')
        if token:
            try:
                token_dt = datetime.fromisoformat(token.replace('Z', '+00:00'))
                queryset = queryset.filter(time__lt=token_dt)
            except ValueError:
                pass
        
        # Order and execute
        queryset = queryset.order_by('-time')
        total_count = queryset.count()
        items = list(queryset[:limit + 1])
        
        has_next = len(items) > limit
        if has_next:
            items = items[:limit]
            next_token = items[-1].time.isoformat()
        else:
            next_token = None
        
        # Serialize
        data = STACItemCollectionSerializer(
            {
                'items': items,
                'total_count': total_count,
                'limit': limit,
                'next_token': next_token,
            },
            context={'request': request}
        ).data
        
        return Response(data)
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        """Apply datetime filter."""
        if '/' in datetime_param:
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] != '..' else None
            end = parts[1] if len(parts) > 1 and parts[1] != '..' else None
            
            if start:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                queryset = queryset.filter(time__gte=start_dt)
            if end:
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                queryset = queryset.filter(time__lte=end_dt)
        else:
            dt = datetime.fromisoformat(datetime_param.replace('Z', '+00:00'))
            queryset = queryset.filter(time=dt)
        
        return queryset


# =============================================================================
# Queryables (for filter extension)
# =============================================================================

class STACQueryablesView(STACAPIView):
    """
    STAC Queryables (for CQL2 filter extension)
    
    GET /stac/queryables
    GET /stac/collections/{collectionId}/queryables
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str = None,
            collection_slug: str = None
    ) -> Response:
        queryables = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": request.build_absolute_uri(),
            "type": "object",
            "title": "GeoRiva Queryables",
            "properties": {
                "datetime": {
                    "title": "Datetime",
                    "type": "string",
                    "format": "date-time",
                },
                "bbox": {
                    "title": "Bounding Box",
                    "type": "array",
                    "items": {"type": "number"},
                    "minItems": 4,
                    "maxItems": 4,
                },
                "collection": {
                    "title": "Collection",
                    "type": "string",
                },
            },
            "additionalProperties": True,
        }
        
        # Add collection-specific queryables
        if collection_slug:
            collection = get_object_or_404(
                Collection,
                catalog__slug=catalog_slug,
                slug=collection_slug,
            )
            
            # Add variable names as queryables
            for var in collection.variables.all():
                queryables["properties"][f"variable:{var.slug}"] = {
                    "title": var.name,
                    "type": "boolean",
                    "description": f"Has {var.name} data",
                }
        
        return Response(queryables)
