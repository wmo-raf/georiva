"""
GeoRiva STAC API Views

Hierarchical structure:
- GET /stac/                                              → Root Catalog
- GET /stac/collections/                                  → List Catalogs (as Collections)
- GET /stac/collections/{catalog}/                        → Catalog detail (as Collection)
- GET /stac/collections/{catalog}/{variable}              → Variable as Collection
- GET /stac/collections/{catalog}/{variable}/items        → List Items (filtered to variable)
- GET /stac/collections/{catalog}/{variable}/items/{id}   → Item detail (filtered to variable)
- GET/POST /stac/search                                   → Cross-collection search

STAC Collection = (Catalog, Variable)
STAC Collection ID = variable.slug
URL pattern: /collections/{catalog.slug}/{variable.slug}

Implements STAC API v1.0.0
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

from georiva.core.models import Catalog, Collection, Item, Variable
from .renderers import STACJSONRenderer, GeoJSONRenderer
from .serializers import (
    STACRootCatalogSerializer,
    STACCatalogAsCollectionSerializer,
    STACCatalogListSerializer,
    STACVariableCollectionSerializer,
    STACVariableCollectionListSerializer,
    STACItemCollectionSerializer,
    STACItemSerializer,
)


# =============================================================================
# Helpers
# =============================================================================

def _resolve_variable(catalog_slug: str, variable_slug: str) -> Variable:
    """
    Resolve a Variable from a catalog slug and variable slug.

    Looks up the variable across all active collections in the catalog.
    Variable slugs must be unique within a catalog.
    """
    return get_object_or_404(
        Variable.objects.select_related(
            'collection', 'collection__catalog'
        ).filter(
            collection__catalog__slug=catalog_slug,
            collection__catalog__is_active=True,
            collection__is_active=True,
            is_active=True,
        ),
        slug=variable_slug,
    )


# =============================================================================
# Base Views
# =============================================================================

class STACAPIView(APIView):
    """Base view for STAC catalog/collection endpoints."""
    renderer_classes = [STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


class STACGeoAPIView(APIView):
    """Base view for STAC item endpoints (GeoJSON)."""
    renderer_classes = [GeoJSONRenderer, STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


# =============================================================================
# Root & Conformance
# =============================================================================

class STACLandingPageView(STACAPIView):
    """
    STAC API Landing Page (Root Catalog)

    GET /stac/
    """
    
    def get(self, request: Request) -> Response:
        catalogs = Catalog.objects.filter(is_active=True).prefetch_related(
            'collections'
        )
        
        data = STACRootCatalogSerializer(
            {
                'id': 'georiva',
                'title': 'GeoRiva STAC API',
                'description': 'Geospatial data catalog for Earth observation and meteorological data',
                'catalogs': catalogs,
            },
            context={'request': request}
        ).data
        
        return Response(data)


class STACConformanceView(STACAPIView):
    """
    STAC API Conformance Declaration

    GET /stac/conformance/
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
# Catalog Views
# =============================================================================

class STACCatalogListView(STACAPIView):
    """
    List all Catalogs as top-level STAC Collections.

    GET /stac/collections/
    """
    
    def get(self, request: Request) -> Response:
        catalogs = Catalog.objects.filter(is_active=True).prefetch_related(
            'collections',
            'collections__variables',
        )
        
        data = STACCatalogListSerializer(
            {'catalogs': catalogs},
            context={'request': request}
        ).data
        
        return Response(data)


class STACCatalogDetailView(STACAPIView):
    """
    Get a single Catalog as a STAC Collection.

    GET /stac/collections/{catalog_slug}

    Child links point to per-variable collections.
    """
    
    def get(self, request: Request, catalog_slug: str) -> Response:
        catalog = get_object_or_404(
            Catalog.objects.prefetch_related(
                'collections',
                'collections__variables',
            ),
            slug=catalog_slug,
            is_active=True,
        )
        
        data = STACCatalogAsCollectionSerializer(
            catalog,
            context={'request': request}
        ).data
        
        return Response(data)


# =============================================================================
# Variable Collection Views
# =============================================================================

class STACCollectionListView(STACAPIView):
    """
    List variable collections within a Catalog.

    GET /stac/collections/{catalog_slug}/collections/

    Returns one STAC Collection per active variable.
    """
    
    def get(self, request: Request, catalog_slug: str) -> Response:
        catalog = get_object_or_404(
            Catalog,
            slug=catalog_slug,
            is_active=True,
        )
        
        variables = Variable.objects.filter(
            collection__catalog=catalog,
            collection__is_active=True,
            is_active=True,
        ).select_related(
            'collection', 'collection__catalog'
        ).order_by('collection__sort_order', 'sort_order')
        
        data = STACVariableCollectionListSerializer(
            {
                'catalog': catalog,
                'variables': variables,
            },
            context={'request': request}
        ).data
        
        return Response(data)


class STACCollectionDetailView(STACAPIView):
    """
    Get a variable as a STAC Collection.

    GET /stac/collections/{catalog_slug}/{variable_slug}
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            variable_slug: str,
    ) -> Response:
        variable = _resolve_variable(catalog_slug, variable_slug)
        
        data = STACVariableCollectionSerializer(
            variable,
            context={'request': request}
        ).data
        
        return Response(data)


# =============================================================================
# Item Views
# =============================================================================

class STACItemsView(STACGeoAPIView):
    """
    List Items in a variable collection.

    GET /stac/collections/{catalog_slug}/{variable_slug}/items

    Items are from the variable's parent Collection, with assets
    filtered to only this variable.

    Query Parameters:
        - limit: Max items to return (default 100, max 1000)
        - datetime: Temporal filter (single datetime or range)
        - bbox: Bounding box filter [west,south,east,north]
        - token: Pagination token (datetime of last item)
    """
    
    DEFAULT_LIMIT = 100
    MAX_LIMIT = 1000
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            variable_slug: str,
    ) -> Response:
        variable = _resolve_variable(catalog_slug, variable_slug)
        collection = variable.collection
        
        # Parse query parameters
        limit = self._parse_limit(request)
        datetime_param = request.query_params.get('datetime')
        bbox_param = request.query_params.get('bbox')
        token = request.query_params.get('token')
        
        # Build query — items that have assets for this variable
        queryset = Item.objects.filter(
            collection=collection,
            assets__variable=variable,
        ).distinct()
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Apply filters
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        if bbox_param:
            queryset = self._apply_bbox_filter(queryset, bbox_param)
        
        if token:
            queryset = self._apply_pagination_token(queryset, token)
        
        # Order and execute
        queryset = queryset.order_by('-time')
        total_count = queryset.count()
        items = list(queryset[:limit + 1])
        
        # Determine pagination
        has_next = len(items) > limit
        if has_next:
            items = items[:limit]
            next_token = items[-1].time.isoformat()
        else:
            next_token = None
        
        # Serialize — pass variable in context for asset filtering
        data = STACItemCollectionSerializer(
            {
                'items': items,
                'collection': collection,
                'total_count': total_count,
                'limit': limit,
                'next_token': next_token,
            },
            context={
                'request': request,
                'variable': variable,
            }
        ).data
        
        return Response(data)
    
    def _parse_limit(self, request: Request) -> int:
        try:
            limit = int(
                request.query_params.get('limit', self.DEFAULT_LIMIT)
            )
            return min(max(1, limit), self.MAX_LIMIT)
        except (ValueError, TypeError):
            return self.DEFAULT_LIMIT
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        if '/' in datetime_param:
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] not in ('..', '') else None
            end = (
                parts[1] if len(parts) > 1 and parts[1] not in ('..', '')
                else None
            )
            
            if start:
                start_dt = self._parse_datetime(start)
                if start_dt:
                    queryset = queryset.filter(time__gte=start_dt)
            if end:
                end_dt = self._parse_datetime(end)
                if end_dt:
                    queryset = queryset.filter(time__lte=end_dt)
        else:
            dt = self._parse_datetime(datetime_param)
            if dt:
                queryset = queryset.filter(time=dt)
        
        return queryset
    
    def _apply_bbox_filter(self, queryset, bbox_param: str):
        try:
            bbox = [float(x.strip()) for x in bbox_param.split(',')]
            if len(bbox) == 4:
                west, south, east, north = bbox
                queryset = queryset.filter(
                    bounds__0__lte=east,
                    bounds__2__gte=west,
                    bounds__1__lte=north,
                    bounds__3__gte=south,
                )
        except (ValueError, TypeError):
            pass
        return queryset
    
    def _apply_pagination_token(self, queryset, token: str):
        try:
            token_dt = self._parse_datetime(token)
            if token_dt:
                queryset = queryset.filter(time__lt=token_dt)
        except ValueError:
            pass
        return queryset
    
    def _parse_datetime(self, dt_string: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


class STACItemDetailView(STACGeoAPIView):
    """
    Get a specific STAC Item.

    GET /stac/collections/{catalog_slug}/{variable_slug}/items/{item_id}

    Assets are filtered to the specified variable only.

    Item ID format:
        - Non-forecast: {YYYYMMDDTHHMMSSz}
        - Forecast: {ref_time}_{valid_time}
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            variable_slug: str,
            item_id: str,
    ) -> Response:
        variable = _resolve_variable(catalog_slug, variable_slug)
        collection = variable.collection
        
        item = self._find_item(collection, item_id)
        
        if not item:
            return Response(
                {
                    "code": "NotFound",
                    "description": (
                        f"Item '{item_id}' not found in "
                        f"collection '{variable_slug}'"
                    ),
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        
        data = STACItemSerializer(
            item,
            context={'request': request, 'variable': variable},
        ).data
        
        return Response(data)
    
    def _find_item(
            self, collection: Collection, item_id: str
    ) -> Optional[Item]:
        parts = item_id.split('_')
        
        try:
            if len(parts) == 1:
                valid_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                return Item.objects.filter(
                    collection=collection,
                    time=valid_time,
                    reference_time__isnull=True,
                ).prefetch_related(
                    'assets', 'assets__variable'
                ).first()
            
            elif len(parts) == 2:
                ref_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                valid_time = datetime.strptime(parts[1], '%Y%m%dT%H%M%SZ')
                return Item.objects.filter(
                    collection=collection,
                    time=valid_time,
                    reference_time=ref_time,
                ).prefetch_related(
                    'assets', 'assets__variable'
                ).first()
        
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

    Cross-collection search with filters:
        - collections: List of collection IDs ({catalog}/{variable} or {variable})
        - ids: List of item IDs
        - bbox: Bounding box [west, south, east, north]
        - datetime: Temporal filter
        - limit: Max results (default 100)
        - intersects: GeoJSON geometry
    """
    
    DEFAULT_LIMIT = 100
    MAX_LIMIT = 1000
    
    def get(self, request: Request) -> Response:
        params = {
            'collections': request.query_params.getlist('collections'),
            'ids': request.query_params.getlist('ids'),
            'bbox': request.query_params.get('bbox'),
            'datetime': request.query_params.get('datetime'),
            'limit': request.query_params.get('limit', self.DEFAULT_LIMIT),
            'token': request.query_params.get('token'),
        }
        return self._search(request, params)
    
    def post(self, request: Request) -> Response:
        return self._search(request, request.data)
    
    def _search(self, request: Request, params: dict) -> Response:
        queryset = Item.objects.filter(
            collection__is_active=True,
            collection__catalog__is_active=True,
        )
        queryset = queryset.select_related(
            'collection', 'collection__catalog'
        )
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Resolve variable context from collections param
        variable = None
        collections_param = params.get('collections', [])
        queryset, variable = self._apply_collections_filter(
            queryset, collections_param
        )
        
        # Filter by item IDs
        queryset = self._apply_ids_filter(queryset, params.get('ids', []))
        
        # Filter by datetime
        datetime_param = params.get('datetime')
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        # Filter by bbox
        queryset = self._apply_bbox_filter(queryset, params.get('bbox'))
        
        # Filter by intersects geometry
        queryset = self._apply_intersects_filter(
            queryset, params.get('intersects')
        )
        
        # Pagination
        limit = self._parse_limit(params.get('limit', self.DEFAULT_LIMIT))
        token = params.get('token')
        if token:
            queryset = self._apply_pagination_token(queryset, token)
        
        # Order and execute
        queryset = queryset.order_by('-time')
        total_count = queryset.count()
        items = list(queryset[:limit + 1])
        
        # Determine pagination
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
            context={
                'request': request,
                'variable': variable,
            },
        ).data
        
        return Response(data)
    
    def _apply_collections_filter(self, queryset, collections: list):
        """
        Filter by collection IDs.

        Collection IDs are in the format {catalog}/{variable}.
        Resolves to the parent GeoRiva Collection and filters
        assets to the specified variable.

        Returns (queryset, variable) — variable is set when filtering
        by a single collection (for asset filtering in serializer).
        """
        if not collections:
            return queryset, None
        
        q_filter = Q()
        resolved_variable = None
        
        for coll_id in collections:
            if '/' in coll_id:
                catalog_slug, variable_slug = coll_id.split('/', 1)
                try:
                    var = Variable.objects.select_related(
                        'collection', 'collection__catalog'
                    ).get(
                        collection__catalog__slug=catalog_slug,
                        slug=variable_slug,
                        collection__is_active=True,
                        is_active=True,
                    )
                    q_filter |= Q(collection=var.collection)
                    # Track variable for single-collection queries
                    if len(collections) == 1:
                        resolved_variable = var
                except Variable.DoesNotExist:
                    continue
            else:
                # Just variable slug — match across catalogs
                variables = Variable.objects.filter(
                    slug=coll_id,
                    collection__is_active=True,
                    is_active=True,
                ).select_related('collection')
                for var in variables:
                    q_filter |= Q(collection=var.collection)
                if len(collections) == 1 and variables.count() == 1:
                    resolved_variable = variables.first()
        
        if q_filter:
            queryset = queryset.filter(q_filter)
        
        return queryset, resolved_variable
    
    def _apply_ids_filter(self, queryset, ids: list):
        if not ids:
            return queryset
        
        time_filters = []
        for item_id in ids:
            parts = item_id.split('_')
            try:
                if len(parts) == 1:
                    valid_time = datetime.strptime(
                        parts[0], '%Y%m%dT%H%M%SZ'
                    )
                    time_filters.append(
                        Q(time=valid_time, reference_time__isnull=True)
                    )
                elif len(parts) >= 2:
                    ref_time = datetime.strptime(
                        parts[-2], '%Y%m%dT%H%M%SZ'
                    )
                    valid_time = datetime.strptime(
                        parts[-1], '%Y%m%dT%H%M%SZ'
                    )
                    time_filters.append(
                        Q(time=valid_time, reference_time=ref_time)
                    )
            except ValueError:
                continue
        
        if time_filters:
            combined = time_filters[0]
            for f in time_filters[1:]:
                combined |= f
            queryset = queryset.filter(combined)
        
        return queryset
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        if '/' in datetime_param:
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] not in ('..', '') else None
            end = (
                parts[1] if len(parts) > 1 and parts[1] not in ('..', '')
                else None
            )
            
            if start:
                start_dt = self._parse_datetime(start)
                if start_dt:
                    queryset = queryset.filter(time__gte=start_dt)
            if end:
                end_dt = self._parse_datetime(end)
                if end_dt:
                    queryset = queryset.filter(time__lte=end_dt)
        else:
            dt = self._parse_datetime(datetime_param)
            if dt:
                queryset = queryset.filter(time=dt)
        
        return queryset
    
    def _apply_bbox_filter(self, queryset, bbox):
        if not bbox:
            return queryset
        
        try:
            if isinstance(bbox, str):
                bbox = [float(x.strip()) for x in bbox.split(',')]
            
            if len(bbox) == 4:
                west, south, east, north = bbox
                queryset = queryset.filter(
                    bounds__0__lte=east,
                    bounds__2__gte=west,
                    bounds__1__lte=north,
                    bounds__3__gte=south,
                )
        except (ValueError, TypeError):
            pass
        
        return queryset
    
    def _apply_intersects_filter(self, queryset, intersects: dict):
        if not intersects:
            return queryset
        
        geom_type = intersects.get('type')
        
        if geom_type == 'Polygon':
            coords = intersects.get('coordinates', [[]])[0]
            if coords:
                bbox = [
                    min(c[0] for c in coords),
                    min(c[1] for c in coords),
                    max(c[0] for c in coords),
                    max(c[1] for c in coords),
                ]
                return self._apply_bbox_filter(queryset, bbox)
        
        elif geom_type == 'Point':
            coords = intersects.get('coordinates', [])
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                queryset = queryset.filter(
                    bounds__0__lte=lon,
                    bounds__2__gte=lon,
                    bounds__1__lte=lat,
                    bounds__3__gte=lat,
                )
        
        return queryset
    
    def _apply_pagination_token(self, queryset, token: str):
        try:
            token_dt = self._parse_datetime(token)
            if token_dt:
                queryset = queryset.filter(time__lt=token_dt)
        except ValueError:
            pass
        return queryset
    
    def _parse_limit(self, limit) -> int:
        try:
            return min(max(1, int(limit)), self.MAX_LIMIT)
        except (ValueError, TypeError):
            return self.DEFAULT_LIMIT
    
    def _parse_datetime(self, dt_string: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


# =============================================================================
# Queryables
# =============================================================================

class STACQueryablesView(STACAPIView):
    """
    STAC Queryables (for CQL2 filter extension)

    GET /stac/queryables
    GET /stac/collections/{catalog_slug}/queryables
    GET /stac/collections/{catalog_slug}/{variable_slug}/queryables
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str = None,
            variable_slug: str = None,
    ) -> Response:
        base_url = request.build_absolute_uri()
        
        queryables = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": base_url,
            "type": "object",
            "title": "GeoRiva Queryables",
            "properties": {
                "id": {
                    "title": "Item ID",
                    "type": "string",
                },
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
                    "title": "Collection ID",
                    "type": "string",
                },
            },
            "additionalProperties": True,
        }
        
        # Catalog-level: list available variable collections
        if catalog_slug and not variable_slug:
            catalog = get_object_or_404(
                Catalog, slug=catalog_slug, is_active=True
            )
            variables = Variable.objects.filter(
                collection__catalog=catalog,
                collection__is_active=True,
                is_active=True,
            )
            queryables["properties"]["collection"]["enum"] = [
                v.slug for v in variables
            ]
        
        # Variable-level: add forecast queryables if applicable
        if catalog_slug and variable_slug:
            variable = _resolve_variable(catalog_slug, variable_slug)
            collection = variable.collection
            
            # Variable info
            queryables["properties"]["georiva:variable"] = {
                "title": variable.name,
                "type": "string",
                "const": variable.slug,
            }
            if variable.units:
                queryables["properties"]["georiva:units"] = {
                    "title": "Units",
                    "type": "string",
                    "const": variable.units,
                }
            
            # Forecast queryables
            has_forecasts = collection.items.filter(
                reference_time__isnull=False
            ).exists()
            
            if has_forecasts:
                queryables["properties"]["forecast:reference_datetime"] = {
                    "title": "Forecast Reference Time",
                    "type": "string",
                    "format": "date-time",
                }
                queryables["properties"]["forecast:horizon"] = {
                    "title": "Forecast Horizon",
                    "type": "string",
                    "description": "ISO 8601 duration (e.g., PT6H)",
                }
        
        return Response(queryables)
