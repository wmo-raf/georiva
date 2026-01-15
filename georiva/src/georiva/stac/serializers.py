"""
GeoRiva STAC API Serializers

Converts GeoRiva models to STAC-compliant JSON format.
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


class STACProviderSerializer(serializers.Serializer):
    """STAC Provider object."""
    name = serializers.CharField()
    url = serializers.URLField(required=False)
    roles = serializers.ListField(child=serializers.CharField(), required=False)


class STACAssetSerializer(serializers.Serializer):
    """
    Serializes GeoRiva Asset to STAC Asset format.
    """
    href = serializers.CharField()
    type = serializers.CharField(source='media_type')
    title = serializers.CharField(source='name')
    roles = serializers.ListField(child=serializers.CharField())
    
    # Additional fields
    file_size = serializers.IntegerField(required=False)
    
    def to_representation(self, instance):
        data = super().to_representation(instance)
        
        # Build href URL
        request = self.context.get('request')
        if request and not data['href'].startswith('http'):
            data['href'] = request.build_absolute_uri(instance.url)
        else:
            data['href'] = instance.url
        
        # Add raster extension fields
        if instance.is_data:
            raster_bands = [{
                'nodata': instance.nodata,
                'unit': instance.units,
                'scale': instance.variable.value_max,
                'offset': instance.variable.value_min,
            }]
            if instance.stats_min is not None:
                raster_bands[0]['statistics'] = {
                    'minimum': instance.stats_min,
                    'maximum': instance.stats_max,
                    'mean': instance.stats_mean,
                    'stddev': instance.stats_std,
                }
            data['raster:bands'] = raster_bands
        
        # Add file extension
        if instance.file_size:
            data['file:size'] = instance.file_size
        if instance.checksum:
            data['file:checksum'] = instance.checksum
        
        # Remove None values
        return {k: v for k, v in data.items() if v is not None}


class STACItemSerializer(serializers.Serializer):
    """
    Serializes GeoRiva Item to STAC Item format.
    """
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
    
    def get_type(self, obj):
        return "Feature"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_stac_extensions(self, obj):
        extensions = [
            "https://stac-extensions.github.io/timestamps/v1.1.0/schema.json",
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        ]
        if obj.is_forecast:
            extensions.append(
                "https://stac-extensions.github.io/forecast/v0.1.0/schema.json"
            )
        return extensions
    
    def get_id(self, obj):
        # Create unique ID from collection and time
        time_str = obj.time.strftime('%Y%m%dT%H%M%SZ')
        if obj.reference_time:
            ref_str = obj.reference_time.strftime('%Y%m%dT%H%M%SZ')
            return f"{obj.collection.slug}_{ref_str}_{time_str}"
        return f"{obj.collection.slug}_{time_str}"
    
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
        
        # Add forecast extension properties
        if obj.is_forecast:
            props["forecast:reference_datetime"] = obj.reference_time.isoformat()
            if obj.horizon_hours is not None:
                props["forecast:horizon"] = f"PT{int(obj.horizon_hours)}H"
        
        # Add raster dimensions
        if obj.width and obj.height:
            props["proj:shape"] = [obj.height, obj.width]
        if obj.crs:
            props["proj:epsg"] = self._parse_epsg(obj.crs)
        
        # Merge custom properties
        if obj.properties:
            props.update(obj.properties)
        
        return {k: v for k, v in props.items() if v is not None}
    
    def _parse_epsg(self, crs: str) -> Optional[int]:
        """Extract EPSG code from CRS string."""
        if crs and crs.upper().startswith('EPSG:'):
            try:
                return int(crs.split(':')[1])
            except (ValueError, IndexError):
                pass
        return None
    
    def get_links(self, obj):
        request = self.context.get('request')
        base_url = request.build_absolute_uri('/api/') if request else ''
        
        collection_url = f"{base_url}stac/collections/{obj.collection.catalog.slug}/{obj.collection.slug}"
        item_url = f"{collection_url}/items/{self.get_id(obj)}"
        
        return [
            {"rel": "self", "href": item_url, "type": "application/geo+json"},
            {"rel": "parent", "href": collection_url, "type": "application/json"},
            {"rel": "collection", "href": collection_url, "type": "application/json"},
            {"rel": "root", "href": f"{base_url}stac/", "type": "application/json"},
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
    
    def get_type(self, obj):
        return "Collection"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_stac_extensions(self, obj):
        return [
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
        ]
    
    def get_id(self, obj):
        return f"{obj.catalog.slug}/{obj.slug}"
    
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
        """Build summaries from variables."""
        summaries = {}
        
        variables = list(obj.variables.filter(is_active=True))
        if variables:
            summaries["georiva:variables"] = [
                {
                    "name": v.slug,
                    "description": v.name,
                    "unit": v.units,
                }
                for v in variables
            ]
        
        if obj.time_resolution:
            summaries["georiva:time_resolution"] = obj.time_resolution
        
        return summaries
    
    def get_links(self, obj):
        request = self.context.get('request')
        base_url = request.build_absolute_uri('/api/') if request else ''
        collection_id = self.get_id(obj)
        
        links = [
            {
                "rel": "self",
                "href": f"{base_url}stac/collections/{collection_id}",
                "type": "application/json",
            },
            {
                "rel": "parent",
                "href": f"{base_url}stac/",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": f"{base_url}stac/",
                "type": "application/json",
            },
            {
                "rel": "items",
                "href": f"{base_url}stac/collections/{collection_id}/items",
                "type": "application/geo+json",
            },
        ]
        
        # Add license link if URL available
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
                "roles": ["producer", "host"],
            }]
        return []
    
    def get_keywords(self, obj):
        keywords = [obj.catalog.slug, obj.slug]
        if obj.catalog.file_format:
            keywords.append(obj.catalog.file_format)
        return keywords


class STACCatalogSerializer(serializers.Serializer):
    """
    Serializes GeoRiva root catalog (landing page) to STAC Catalog format.
    """
    type = serializers.SerializerMethodField()
    stac_version = serializers.SerializerMethodField()
    id = serializers.SerializerMethodField()
    title = serializers.SerializerMethodField()
    description = serializers.SerializerMethodField()
    conformsTo = serializers.SerializerMethodField()
    links = serializers.SerializerMethodField()
    
    def get_type(self, obj):
        return "Catalog"
    
    def get_stac_version(self, obj):
        return "1.0.0"
    
    def get_id(self, obj):
        return "georiva"
    
    def get_title(self, obj):
        return "GeoRiva STAC API"
    
    def get_description(self, obj):
        return "Geospatial data catalog for Earth observation and meteorological data"
    
    def get_conformsTo(self, obj):
        """STAC API conformance classes."""
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
        request = self.context.get('request')
        base_url = request.build_absolute_uri('/api/stac/') if request else '/api/stac/'
        
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
        
        # Add child links for each catalog
        for catalog in obj.get('catalogs', []):
            for collection in catalog.collections.filter(is_active=True):
                links.append({
                    "rel": "child",
                    "href": f"{base_url}collections/{catalog.slug}/{collection.slug}",
                    "type": "application/json",
                    "title": collection.name,
                })
        
        return links


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
            
            # Pagination links
            if obj.get('next_token'):
                links.append({
                    "rel": "next",
                    "href": f"{current_url}&token={obj['next_token']}",
                    "type": "application/geo+json",
                })
        
        return links
    
    def get_context(self, obj):
        return {
            "returned": len(obj.get('items', [])),
            "matched": obj.get('total_count'),
            "limit": obj.get('limit', 100),
        }
    
    def get_numberReturned(self, obj):
        return len(obj.get('items', []))
