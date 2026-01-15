"""
GeoRiva STAC API Views

Hierarchical structure:
- GET /stac/                                           → Root Catalog
- GET /stac/collections/                               → List Catalogs (as Collections)
- GET /stac/collections/{catalog}/                     → Catalog detail (as Collection)
- GET /stac/collections/{catalog}/{collection}         → Collection detail
- GET /stac/collections/{catalog}/{collection}/items   → List Items
- GET /stac/collections/{catalog}/{collection}/items/{id} → Item detail
- GET/POST /stac/search                                → Cross-collection search

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

from georiva.core.models import Catalog, Collection, Item
from .renderers import STACJSONRenderer, GeoJSONRenderer
from .serializers import (
    STACRootCatalogSerializer,
    STACCatalogAsCollectionSerializer,
    STACCatalogListSerializer,
    STACCollectionSerializer,
    STACCollectionListSerializer,
    STACItemCollectionSerializer,
    STACItemSerializer,
)


class STACAPIView(APIView):
    """Base view for STAC catalog/collection endpoints."""
    renderer_classes = [STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


class STACGeoAPIView(APIView):
    """Base view for STAC item endpoints (GeoJSON)."""
    renderer_classes = [GeoJSONRenderer, STACJSONRenderer, JSONRenderer]
    parser_classes = [JSONParser]


class STACLandingPageView(STACAPIView):
    """
    STAC API Landing Page (Root Catalog)
    
    GET /stac/
    
    Returns the root catalog with links to all top-level collections (Catalogs).
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


class STACCatalogListView(STACAPIView):
    """
    List all Catalogs as top-level STAC Collections
    
    GET /stac/collections/
    
    Returns Catalogs serialized as Collections, each containing child collections.
    """
    
    def get(self, request: Request) -> Response:
        catalogs = Catalog.objects.filter(is_active=True).prefetch_related(
            'collections',
            'collections__variables'
        )
        
        data = STACCatalogListSerializer(
            {'catalogs': catalogs},
            context={'request': request}
        ).data
        
        return Response(data)


class STACCatalogDetailView(STACAPIView):
    """
    Get a single Catalog as a STAC Collection
    
    GET /stac/collections/{catalog_slug}
    
    Returns the Catalog with links to its child Collections.
    """
    
    def get(self, request: Request, catalog_slug: str) -> Response:
        catalog = get_object_or_404(
            Catalog.objects.prefetch_related(
                'collections',
                'collections__variables'
            ),
            slug=catalog_slug,
            is_active=True,
        )
        
        data = STACCatalogAsCollectionSerializer(
            catalog,
            context={'request': request}
        ).data
        
        return Response(data)


class STACCollectionListView(STACAPIView):
    """
    List Collections within a Catalog
    
    GET /stac/collections/{catalog_slug}/collections/
    
    Optional - provides explicit list endpoint for collections within a catalog.
    The catalog detail view already includes child links.
    """
    
    def get(self, request: Request, catalog_slug: str) -> Response:
        catalog = get_object_or_404(
            Catalog,
            slug=catalog_slug,
            is_active=True,
        )
        
        collections = Collection.objects.filter(
            catalog=catalog,
            is_active=True
        ).prefetch_related('variables')
        
        data = STACCollectionListSerializer(
            {
                'catalog': catalog,
                'collections': collections,
            },
            context={'request': request}
        ).data
        
        return Response(data)


class STACCollectionDetailView(STACAPIView):
    """
    Get a specific STAC Collection
    
    GET /stac/collections/{catalog_slug}/{collection_slug}
    
    Returns the Collection with links to its Items.
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            collection_slug: str
    ) -> Response:
        collection = get_object_or_404(
            Collection.objects.select_related('catalog').prefetch_related('variables'),
            catalog__slug=catalog_slug,
            slug=collection_slug,
            is_active=True,
        )
        
        data = STACCollectionSerializer(
            collection,
            context={'request': request}
        ).data
        
        return Response(data)


class STACItemsView(STACGeoAPIView):
    """
    List Items in a Collection (OGC Features)
    
    GET /stac/collections/{catalog_slug}/{collection_slug}/items
    
    Query Parameters:
        - limit: Max items to return (default 100, max 1000)
        - datetime: Temporal filter (single datetime or range)
        - bbox: Bounding box filter [west,south,east,north]
        - token: Pagination token (datetime of last item)
        - variable: Filter by variable slug
    """
    
    DEFAULT_LIMIT = 100
    MAX_LIMIT = 1000
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            collection_slug: str
    ) -> Response:
        collection = get_object_or_404(
            Collection.objects.select_related('catalog'),
            catalog__slug=catalog_slug,
            slug=collection_slug,
            is_active=True,
        )
        
        # Parse query parameters
        limit = self._parse_limit(request)
        datetime_param = request.query_params.get('datetime')
        bbox_param = request.query_params.get('bbox')
        token = request.query_params.get('token')
        variable = request.query_params.get('variable')
        
        # Build query
        queryset = Item.objects.filter(collection=collection)
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Apply filters
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        if bbox_param:
            queryset = self._apply_bbox_filter(queryset, bbox_param)
        
        if variable:
            queryset = queryset.filter(assets__variable__slug=variable).distinct()
        
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
                'collection': collection,
                'total_count': total_count,
                'limit': limit,
                'next_token': next_token,
            },
            context={'request': request}
        ).data
        
        return Response(data)
    
    def _parse_limit(self, request: Request) -> int:
        try:
            limit = int(request.query_params.get('limit', self.DEFAULT_LIMIT))
            return min(max(1, limit), self.MAX_LIMIT)
        except (ValueError, TypeError):
            return self.DEFAULT_LIMIT
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        """Apply datetime filter per OGC API."""
        if '/' in datetime_param:
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] not in ('..', '') else None
            end = parts[1] if len(parts) > 1 and parts[1] not in ('..', '') else None
            
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
        """Apply bounding box filter."""
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
        """Apply cursor-based pagination."""
        try:
            token_dt = self._parse_datetime(token)
            if token_dt:
                queryset = queryset.filter(time__lt=token_dt)
        except ValueError:
            pass
        return queryset
    
    def _parse_datetime(self, dt_string: str) -> Optional[datetime]:
        """Parse ISO datetime string."""
        try:
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


class STACItemDetailView(STACGeoAPIView):
    """
    Get a specific STAC Item
    
    GET /stac/collections/{catalog_slug}/{collection_slug}/items/{item_id}
    
    Item ID format:
        - Non-forecast: {YYYYMMDDTHHMMSSz}
        - Forecast: {ref_time}_{valid_time}
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str,
            collection_slug: str,
            item_id: str
    ) -> Response:
        collection = get_object_or_404(
            Collection.objects.select_related('catalog'),
            catalog__slug=catalog_slug,
            slug=collection_slug,
        )
        
        item = self._find_item(collection, item_id)
        
        if not item:
            return Response(
                {
                    "code": "NotFound",
                    "description": f"Item '{item_id}' not found in collection '{collection_slug}'"
                },
                status=status.HTTP_404_NOT_FOUND
            )
        
        data = STACItemSerializer(item, context={'request': request}).data
        return Response(data)
    
    def _find_item(self, collection: Collection, item_id: str) -> Optional[Item]:
        """Parse item_id and find the corresponding Item."""
        parts = item_id.split('_')
        
        try:
            if len(parts) == 1:
                # Single datetime: valid_time only (non-forecast)
                valid_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                return Item.objects.filter(
                    collection=collection,
                    time=valid_time,
                    reference_time__isnull=True
                ).prefetch_related('assets', 'assets__variable').first()
            
            elif len(parts) == 2:
                # Two datetimes: reference_time + valid_time (forecast)
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


class STACSearchView(STACGeoAPIView):
    """
    STAC Item Search
    
    GET/POST /stac/search
    
    Cross-collection search with filters:
        - collections: List of collection IDs (catalog/collection or just collection)
        - ids: List of item IDs
        - bbox: Bounding box [west, south, east, north]
        - datetime: Temporal filter
        - limit: Max results (default 100)
        - intersects: GeoJSON geometry
        - filter: CQL2 filter expression (future)
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
            collection__catalog__is_active=True
        )
        queryset = queryset.select_related('collection', 'collection__catalog')
        queryset = queryset.prefetch_related('assets', 'assets__variable')
        
        # Filter by collections
        queryset = self._apply_collections_filter(queryset, params.get('collections', []))
        
        # Filter by item IDs
        queryset = self._apply_ids_filter(queryset, params.get('ids', []))
        
        # Filter by datetime
        datetime_param = params.get('datetime')
        if datetime_param:
            queryset = self._apply_datetime_filter(queryset, datetime_param)
        
        # Filter by bbox
        queryset = self._apply_bbox_filter(queryset, params.get('bbox'))
        
        # Filter by intersects geometry
        queryset = self._apply_intersects_filter(queryset, params.get('intersects'))
        
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
            context={'request': request}
        ).data
        
        return Response(data)
    
    def _apply_collections_filter(self, queryset, collections: list):
        """Filter by collection IDs."""
        if not collections:
            return queryset
        
        q_filter = Q()
        for coll_id in collections:
            if '/' in coll_id:
                # Full path: catalog/collection
                catalog_slug, coll_slug = coll_id.split('/', 1)
                q_filter |= Q(
                    collection__catalog__slug=catalog_slug,
                    collection__slug=coll_slug
                )
            else:
                # Just collection slug (matches across catalogs)
                q_filter |= Q(collection__slug=coll_id)
        
        return queryset.filter(q_filter)
    
    def _apply_ids_filter(self, queryset, ids: list):
        """Filter by item IDs."""
        if not ids:
            return queryset
        
        # Item IDs are time-based, so we need to parse them
        # This is a simplified implementation - could be optimized
        time_filters = []
        for item_id in ids:
            parts = item_id.split('_')
            try:
                if len(parts) == 1:
                    valid_time = datetime.strptime(parts[0], '%Y%m%dT%H%M%SZ')
                    time_filters.append(Q(time=valid_time, reference_time__isnull=True))
                elif len(parts) >= 2:
                    ref_time = datetime.strptime(parts[-2], '%Y%m%dT%H%M%SZ')
                    valid_time = datetime.strptime(parts[-1], '%Y%m%dT%H%M%SZ')
                    time_filters.append(Q(time=valid_time, reference_time=ref_time))
            except ValueError:
                continue
        
        if time_filters:
            combined = time_filters[0]
            for f in time_filters[1:]:
                combined |= f
            queryset = queryset.filter(combined)
        
        return queryset
    
    def _apply_datetime_filter(self, queryset, datetime_param: str):
        """Apply datetime filter."""
        if '/' in datetime_param:
            parts = datetime_param.split('/')
            start = parts[0] if parts[0] not in ('..', '') else None
            end = parts[1] if len(parts) > 1 and parts[1] not in ('..', '') else None
            
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
        """Apply bounding box filter."""
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
        """Apply GeoJSON geometry intersection filter."""
        if not intersects:
            return queryset
        
        # Simplified: extract bbox from geometry
        # For proper spatial queries, use PostGIS ST_Intersects
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
        """Apply cursor-based pagination."""
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


class STACQueryablesView(STACAPIView):
    """
    STAC Queryables (for CQL2 filter extension)
    
    GET /stac/queryables
    GET /stac/collections/{catalog_slug}/queryables
    GET /stac/collections/{catalog_slug}/{collection_slug}/queryables
    """
    
    def get(
            self,
            request: Request,
            catalog_slug: str = None,
            collection_slug: str = None
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
        
        # Add catalog-level queryables
        if catalog_slug and not collection_slug:
            catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
            
            # Add collection choices within this catalog
            collections = catalog.collections.filter(is_active=True)
            queryables["properties"]["collection"]["enum"] = [
                c.slug for c in collections
            ]
        
        # Add collection-specific queryables
        if catalog_slug and collection_slug:
            collection = get_object_or_404(
                Collection,
                catalog__slug=catalog_slug,
                slug=collection_slug,
            )
            
            # Add variables as queryables
            for var in collection.variables.filter(is_active=True):
                queryables["properties"][f"georiva:variable:{var.slug}"] = {
                    "title": var.name,
                    "type": "boolean",
                    "description": f"Item has {var.name} asset",
                }
            
            # Add forecast-specific queryables if applicable
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
