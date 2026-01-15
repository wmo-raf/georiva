"""
GeoRiva STAC API Serializers

Hierarchical structure:
- Root Catalog
  └── Catalogs (as Collections)
      └── Collections
          └── Items
              └── Assets

Implements STAC Spec v1.0.0
"""

from typing import Optional

from rest_framework import serializers


class STACLinkSerializer(serializers.Serializer):
    """STAC Link object."""
    rel = serializers.CharField()
    href = serializers.CharField()
    type = serializers.CharField(required=False)
    title = serializers.CharField(required=False)
    method = serializers.CharField(required=False)


class STACProviderSerializer(serializers.Serializer):
    """STAC Provider object."""
    name = serializers.CharField()
    url = serializers.URLField(required=False, allow_null=True)
    roles = serializers.ListField(child=serializers.CharField(), required=False)


class STACAssetSerializer(serializers.Serializer):
    """Serializes GeoRiva Asset to STAC Asset format."""
    
    href = serializers.CharField()
    type = serializers.CharField(source='media_type')
    title = serializers.CharField(source='name')
    roles = serializers.ListField(child=serializers.CharField())
    
    def to_representation(self, instance):
        data = super().to_representation(instance)
        request = self.context.get('request')
        
        # Build absolute href
        if request and not data['href'].startswith('http'):
            data['href'] = request.build_absolute_uri(instance.url)
        else:
            data['href'] = instance.url
        
        # Raster extension for data assets
        if instance.is_data:
            raster_bands = [{
                'nodata': instance.nodata,
                'unit': instance.units,
            }]
            if instance.stats_min is not None:
                raster_bands[0]['statistics'] = {
                    'minimum': instance.stats_min,
                    'maximum': instance.stats_max,
                    'mean': instance.stats_mean,
                    'stddev': instance.stats_std,
                }
            data['raster:bands'] = raster_bands
        
        # File extension
        if instance.file_size:
            data['file:size'] = instance.file_size
        if instance.checksum:
            data['file:checksum'] = instance.checksum
        
        return {k: v for k, v in data.items() if v is not None}


class STACItemSerializer(serializers.Serializer):
    """Serializes GeoRiva Item to STAC Item format."""
    
    type = serializers.SerializerMethodField()
    stac_version = serializers.SerializerMethodField()
    stac_extensions = serializers.SerializerMethodField()
    id = serializers.SerializerMethodField()
    geometry = serializers.SerializerMethodField()
    bbox = serializers.SerializerMethodField()
    properties = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    assets = serializers.SerializerMethodField()
    collection = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_type(self, obj):
        return "Feature"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_stac_extensions(self, obj):
        extensions = [
            "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json",
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        ]
        if obj.is_forecast:
            extensions.append(
                "https://stac-extensions.github.io/forecast/v0.1.0/schema.json"
            )
        return extensions
    
    def get_id(self, obj):
        time_str = obj.time.strftime('%Y%m%dT%H%M%SZ')
        if obj.reference_time:
            ref_str = obj.reference_time.strftime('%Y%m%dT%H%M%SZ')
            return f"{ref_str}_{time_str}"
        return time_str
    
    def get_geometry(self, obj):
        if obj.geometry:
            return obj.geometry
        if obj.bounds:
            west, south, east, north = obj.bounds
            return {
                "type": "Polygon",
                "coordinates": [[
                    [west, south],
                    [east, south],
                    [east, north],
                    [west, north],
                    [west, south],
                ]]
            }
        return None
    
    def get_bbox(self, obj):
        return obj.bounds
    
    def get_properties(self, obj):
        props = {
            "datetime": obj.time.isoformat() if obj.time else None,
            "created": obj.created.isoformat() if obj.created else None,
            "updated": obj.modified.isoformat() if obj.modified else None,
        }
        
        # Forecast extension
        if obj.is_forecast:
            props["forecast:reference_datetime"] = obj.reference_time.isoformat()
            if obj.horizon_hours is not None:
                props["forecast:horizon"] = f"PT{int(obj.horizon_hours)}H"
        
        # Projection extension
        if obj.width and obj.height:
            props["proj:shape"] = [obj.height, obj.width]
        if obj.crs:
            props["proj:epsg"] = self._parse_epsg(obj.crs)
        if obj.resolution_x and obj.resolution_y:
            props["proj:transform"] = self._build_transform(obj)
        
        # Merge custom properties
        if obj.properties:
            props.update(obj.properties)
        
        return {k: v for k, v in props.items() if v is not None}
    
    def _parse_epsg(self, crs: str) -> Optional[int]:
        if crs and crs.upper().startswith('EPSG:'):
            try:
                return int(crs.split(':')[1])
            except (ValueError, IndexError):
                pass
        return None
    
    def _build_transform(self, obj) -> Optional[list]:
        """Build affine transform [a, b, c, d, e, f]."""
        if obj.bounds and obj.resolution_x:
            west, south, east, north = obj.bounds
            return [obj.resolution_x, 0, west, 0, -abs(obj.resolution_y), north]
        return None
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        catalog_slug = obj.collection.catalog.slug
        collection_slug = obj.collection.slug
        item_id = self.get_id(obj)
        
        collection_url = f"{base_url}collections/{catalog_slug}/{collection_slug}"
        item_url = f"{collection_url}/items/{item_id}"
        
        return [
            {"rel": "self", "href": item_url, "type": "application/geo+json"},
            {"rel": "parent", "href": collection_url, "type": "application/json"},
            {"rel": "collection", "href": collection_url, "type": "application/json"},
            {"rel": "root", "href": base_url, "type": "application/json"},
        ]
    
    def get_assets(self, obj):
        assets = {}
        for asset in obj.assets.all():
            key = f"{asset.variable.slug}_{asset.format}" if asset.format else asset.variable.slug
            assets[key] = STACAssetSerializer(asset, context=self.context).data
        return assets
    
    def get_collection(self, obj):
        return f"{obj.collection.catalog.slug}/{obj.collection.slug}"


class STACCollectionSerializer(serializers.Serializer):
    """
    Serializes GeoRiva Collection to STAC Collection format.
    Parent is the Catalog.
    """
    
    type = serializers.SerializerMethodField()
    stac_version = serializers.SerializerMethodField()
    stac_extensions = serializers.SerializerMethodField()
    id = serializers.SerializerMethodField()
    title = serializers.CharField(source='name')
    description = serializers.CharField()
    license = serializers.SerializerMethodField()
    extent = serializers.SerializerMethodField()
    summaries = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    providers = serializers.SerializerMethodField()
    keywords = serializers.SerializerMethodField()
    
    # Item assets declaration
    item_assets = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_type(self, obj):
        return "Collection"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_stac_extensions(self, obj):
        return [
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
        ]
    
    def get_id(self, obj):
        # ID is just the collection slug (catalog is the parent context)
        return obj.slug
    
    def get_license(self, obj):
        return obj.catalog.license or "proprietary"
    
    def get_extent(self, obj):
        spatial_bbox = obj.bounds or [-180, -90, 180, 90]
        
        temporal_interval = [None, None]
        if obj.time_start:
            temporal_interval[0] = obj.time_start.isoformat()
        if obj.time_end:
            temporal_interval[1] = obj.time_end.isoformat()
        
        return {
            "spatial": {"bbox": [spatial_bbox]},
            "temporal": {"interval": [temporal_interval]},
        }
    
    def get_summaries(self, obj):
        summaries = {}
        
        # Variables summary
        variables = list(obj.variables.filter(is_active=True))
        if variables:
            summaries["georiva:variables"] = [v.slug for v in variables]
        
        if obj.time_resolution:
            summaries["georiva:time_resolution"] = obj.time_resolution
        
        if obj.crs:
            summaries["proj:epsg"] = [self._parse_epsg(obj.crs)]
        
        return summaries
    
    def _parse_epsg(self, crs: str) -> Optional[int]:
        if crs and crs.upper().startswith('EPSG:'):
            try:
                return int(crs.split(':')[1])
            except (ValueError, IndexError):
                pass
        return None
    
    def get_item_assets(self, obj):
        """Declare expected assets based on Variables."""
        item_assets = {}
        for variable in obj.variables.filter(is_active=True):
            item_assets[variable.slug] = {
                "title": variable.name,
                "description": variable.description or f"{variable.name} data",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
            }
            if variable.units:
                item_assets[variable.slug]["unit"] = variable.units
        return item_assets
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        catalog_slug = obj.catalog.slug
        
        catalog_url = f"{base_url}collections/{catalog_slug}"
        collection_url = f"{catalog_url}/{obj.slug}"
        
        links = [
            {"rel": "self", "href": collection_url, "type": "application/json"},
            {"rel": "parent", "href": catalog_url, "type": "application/json", "title": obj.catalog.name},
            {"rel": "root", "href": base_url, "type": "application/json"},
            {"rel": "items", "href": f"{collection_url}/items", "type": "application/geo+json"},
        ]
        
        # License link
        if obj.catalog.provider_url:
            links.append({
                "rel": "license",
                "href": obj.catalog.provider_url,
                "title": "Data Provider",
            })
        
        return links
    
    def get_providers(self, obj):
        if obj.catalog.provider:
            return [{
                "name": obj.catalog.provider,
                "url": obj.catalog.provider_url or None,
                "roles": ["producer"],
            }]
        return []
    
    def get_keywords(self, obj):
        keywords = [obj.catalog.slug, obj.slug]
        for variable in obj.variables.filter(is_active=True)[:5]:
            keywords.append(variable.slug)
        return keywords


class STACCatalogAsCollectionSerializer(serializers.Serializer):
    """
    Serializes GeoRiva Catalog as a STAC Collection.
    
    This allows Catalogs to appear as top-level collections
    that contain child collections.
    """
    
    type = serializers.SerializerMethodField()
    stac_version = serializers.SerializerMethodField()
    stac_extensions = serializers.SerializerMethodField()
    id = serializers.SlugField(source='slug')
    title = serializers.CharField(source='name')
    description = serializers.CharField()
    license = serializers.CharField(default='proprietary')
    extent = serializers.SerializerMethodField()
    summaries = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    providers = serializers.SerializerMethodField()
    keywords = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_type(self, obj):
        return "Collection"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_stac_extensions(self, obj):
        return [
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
        ]
    
    def get_extent(self, obj):
        """Aggregate extent from all child collections."""
        collections = obj.collections.filter(is_active=True)
        
        # Aggregate spatial bounds
        all_bounds = [c.bounds for c in collections if c.bounds]
        if all_bounds:
            spatial_bbox = [
                min(b[0] for b in all_bounds),  # west
                min(b[1] for b in all_bounds),  # south
                max(b[2] for b in all_bounds),  # east
                max(b[3] for b in all_bounds),  # north
            ]
        else:
            spatial_bbox = [-180, -90, 180, 90]
        
        # Aggregate temporal extent
        time_starts = [c.time_start for c in collections if c.time_start]
        time_ends = [c.time_end for c in collections if c.time_end]
        
        temporal_interval = [
            min(time_starts).isoformat() if time_starts else None,
            max(time_ends).isoformat() if time_ends else None,
        ]
        
        return {
            "spatial": {"bbox": [spatial_bbox]},
            "temporal": {"interval": [temporal_interval]},
        }
    
    def get_summaries(self, obj):
        collections = obj.collections.filter(is_active=True)
        
        summaries = {
            "georiva:collection_count": collections.count(),
            "georiva:file_format": obj.file_format,
        }
        
        # Aggregate variables across collections
        all_variables = set()
        for collection in collections:
            for var in collection.variables.filter(is_active=True):
                all_variables.add(var.slug)
        
        if all_variables:
            summaries["georiva:variables"] = sorted(all_variables)
        
        return summaries
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        catalog_url = f"{base_url}collections/{obj.slug}"
        
        links = [
            {"rel": "self", "href": catalog_url, "type": "application/json"},
            {"rel": "parent", "href": f"{base_url}collections/", "type": "application/json"},
            {"rel": "root", "href": base_url, "type": "application/json"},
        ]
        
        # Child links to collections
        for collection in obj.collections.filter(is_active=True):
            links.append({
                "rel": "child",
                "href": f"{catalog_url}/{collection.slug}",
                "type": "application/json",
                "title": collection.name,
            })
        
        # Provider/license link
        if obj.provider_url:
            links.append({
                "rel": "license",
                "href": obj.provider_url,
                "title": obj.provider or "Data Provider",
            })
        
        return links
    
    def get_providers(self, obj):
        if obj.provider:
            return [{
                "name": obj.provider,
                "url": obj.provider_url or None,
                "roles": ["producer", "licensor"],
            }]
        return []
    
    def get_keywords(self, obj):
        keywords = [obj.slug]
        if obj.file_format:
            keywords.append(obj.file_format)
        if obj.provider:
            keywords.append(obj.provider.lower().replace(' ', '-'))
        return keywords


class STACRootCatalogSerializer(serializers.Serializer):
    """
    Root STAC Catalog - landing page.
    Links to Catalogs as top-level collections.
    """
    
    type = serializers.SerializerMethodField()
    stac_version = serializers.SerializerMethodField()
    id = serializers.SerializerMethodField()
    title = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
    conformsTo = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_type(self, obj):
        return "Catalog"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_id(self, obj):
        return obj.get('id', 'georiva')
    
    def get_title(self, obj):
        return obj.get('title', 'GeoRiva STAC API')
    
    def get_description(self, obj):
        return obj.get(
            'description',
            'Geospatial data catalog for Earth observation and meteorological data'
        )
    
    def get_conformsTo(self, obj):
        return [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/collections",
            "https://api.stacspec.org/v1.0.0/ogcapi-features",
            "https://api.stacspec.org/v1.0.0/item-search",
            "https://api.stacspec.org/v1.0.0/item-search#filter",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
        ]
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        
        links = [
            {"rel": "self", "href": base_url, "type": "application/json", "title": "This catalog"},
            {"rel": "root", "href": base_url, "type": "application/json", "title": "Root catalog"},
            {"rel": "conformance", "href": f"{base_url}conformance/", "type": "application/json"},
            {"rel": "data", "href": f"{base_url}collections/", "type": "application/json", "title": "Collections"},
            {"rel": "search", "href": f"{base_url}search/", "type": "application/geo+json", "method": "GET"},
            {"rel": "search", "href": f"{base_url}search/", "type": "application/geo+json", "method": "POST"},
            {
                "rel": "service-desc",
                "href": f"{base_url}openapi/",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI definition",
            },
        ]
        
        # Child links to each Catalog (as top-level collections)
        for catalog in obj.get('catalogs', []):
            if catalog.is_active:
                links.append({
                    "rel": "child",
                    "href": f"{base_url}collections/{catalog.slug}",
                    "type": "application/json",
                    "title": catalog.name,
                })
        
        return links


class STACCatalogListSerializer(serializers.Serializer):
    """
    List of Catalogs (as Collections) - response for /collections/
    """
    collections = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_collections(self, obj):
        catalogs = obj.get('catalogs', [])
        return [
            STACCatalogAsCollectionSerializer(c, context=self.context).data
            for c in catalogs if c.is_active
        ]
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        return [
            {"rel": "self", "href": f"{base_url}collections/", "type": "application/json"},
            {"rel": "root", "href": base_url, "type": "application/json"},
        ]


class STACCollectionListSerializer(serializers.Serializer):
    """
    List of Collections within a Catalog - response for /collections/{catalog}/collections/
    """
    collections = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    
    def _get_base_url(self):
        request = self.context.get('request')
        return request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
    
    def get_collections(self, obj):
        collections = obj.get('collections', [])
        return [
            STACCollectionSerializer(c, context=self.context).data
            for c in collections if c.is_active
        ]
    
    def get_links(self, obj):
        base_url = self._get_base_url()
        catalog = obj.get('catalog')
        catalog_url = f"{base_url}collections/{catalog.slug}" if catalog else base_url
        
        return [
            {"rel": "self", "href": f"{catalog_url}/", "type": "application/json"},
            {"rel": "parent", "href": catalog_url, "type": "application/json"},
            {"rel": "root", "href": base_url, "type": "application/json"},
        ]


class STACItemCollectionSerializer(serializers.Serializer):
    """
    Serializes a list of Items to STAC ItemCollection (FeatureCollection).
    """
    type = serializers.SerializerMethodField()
    features = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    context = serializers.SerializerMethodField()
    numberMatched = serializers.IntegerField(source='total_count', required=False)
    numberReturned = serializers.SerializerMethodField()
    
    def get_type(self, obj):
        return "FeatureCollection"
    
    def get_features(self, obj):
        items = obj.get('items', [])
        return [STACItemSerializer(item, context=self.context).data for item in items]
    
    def get_links(self, obj):
        request = self.context.get('request')
        links = []
        
        if request:
            current_url = request.build_absolute_uri()
            links.append({"rel": "self", "href": current_url, "type": "application/geo+json"})
            
            # Collection link
            collection = obj.get('collection')
            if collection:
                base_url = request.build_absolute_uri('/api/stac/')
                collection_url = f"{base_url}collections/{collection.catalog.slug}/{collection.slug}"
                links.append({"rel": "collection", "href": collection_url, "type": "application/json"})
            
            # Pagination
            if obj.get('next_token'):
                next_url = self._build_pagination_url(current_url, obj['next_token'])
                links.append({"rel": "next", "href": next_url, "type": "application/geo+json"})
            
            if obj.get('prev_token'):
                prev_url = self._build_pagination_url(current_url, obj['prev_token'])
                links.append({"rel": "prev", "href": prev_url, "type": "application/geo+json"})
        
        return links
    
    def _build_pagination_url(self, base_url: str, token: str) -> str:
        if '?' in base_url:
            return f"{base_url}&token={token}"
        return f"{base_url}?token={token}"
    
    def get_context(self, obj):
        return {
            "returned": len(obj.get('items', [])),
            "matched": obj.get('total_count'),
            "limit": obj.get('limit', 100),
        }
    
    def get_numberReturned(self, obj):
        return len(obj.get('items', []))
