"""
GeoRiva EDR API Serializers

EDR hierarchy:
- Landing Page
  └── Collections  (one per GeoRiva Collection)
      └── parameter_names  (one entry per active Variable)
          └── x-georiva    (palette, value range, rendering hints)

Implements OGC API - Environmental Data Retrieval 1.1 (19-086r6)
Phase 1: Collection metadata only (landing page, conformance, collection list, collection detail)
"""

from rest_framework import serializers

from georiva.core.models import Collection, Item


# =============================================================================
# Base URL mixin
# =============================================================================

class EDRBaseURLMixin:
    """Provide EDR base URL from request context."""
    
    def _get_base_url(self) -> str:
        request = self.context.get('request')
        if request:
            return request.build_absolute_uri('/api/edr/')
        return '/api/edr/'
    
    def _get_stac_base_url(self) -> str:
        request = self.context.get('request')
        if request:
            return request.build_absolute_uri('/api/stac/')
        return '/api/stac/'


# =============================================================================
# Parameter serializer — one Variable → one parameter_names entry
# =============================================================================

class EDRParameterSerializer(serializers.Serializer, EDRBaseURLMixin):
    """
    Serializes a GeoRiva Variable as an EDR parameter_names entry.

    Spec shape:
    {
      "type": "Parameter",
      "label": "...",
      "unit": { "symbol": "..." },
      "observedProperty": { "id": "...", "label": "..." },
      "x-georiva": { ... }
    }
    """
    
    def to_representation(self, variable):
        data = {
            "type": "Parameter",
            "label": variable.name,
            "observedProperty": {
                "id": variable.slug,
                "label": variable.name,
            },
        }
        
        # Unit — only include if present
        if variable.units:
            data["unit"] = {"symbol": variable.units}
        
        # Description
        if variable.description:
            data["description"] = variable.description
        
        # x-georiva — GeoRiva-specific rendering hints
        # This is the style information the EDR spec doesn't cover natively
        x_georiva = {
            "value_min": variable.value_min,
            "value_max": variable.value_max,
            "scale_type": variable.scale_type,
            "transform_type": variable.transform_type,
        }
        
        # Palette — convert ColorPalette to WeatherLayers format
        if variable.palette:
            try:
                palette = variable.palette.as_weatherlayers_palette()
                palette_min, palette_max = variable.palette.min_max_from_stops()
                x_georiva["palette"] = palette
                x_georiva["palette_min"] = palette_min
                x_georiva["palette_max"] = palette_max
                x_georiva["palette_name"] = variable.palette.name
                x_georiva["palette_type"] = variable.palette.palette_type
            except Exception:
                pass
        else:
            # No palette — expose value range for grayscale fallback
            x_georiva["palette_min"] = variable.value_min
            x_georiva["palette_max"] = variable.value_max
        
        data["x-georiva"] = x_georiva
        
        return data


# =============================================================================
# Collection detail serializer
# =============================================================================

class EDRCollectionSerializer(serializers.Serializer, EDRBaseURLMixin):
    """
    Serializes a GeoRiva Collection as an EDR Collection.

    One GeoRiva Collection → one EDR Collection.
    All active Variables → parameter_names entries.
    Item.time values → temporal.values
    """
    
    def to_representation(self, collection: Collection):
        base_url = self._get_base_url()
        stac_base_url = self._get_stac_base_url()
        collection_url = f"{base_url}collections/{collection.slug}/"
        
        data = {
            "id": collection.slug,
            "title": collection.name,
            "description": collection.description or f"{collection.name} from {collection.catalog.name}",
            "extent": self._build_extent(collection),
            "parameter_names": self._build_parameter_names(collection),
            "data_queries": self._build_data_queries(collection_url),
            "providers": self._build_providers(collection),
            "links": self._build_links(collection, collection_url, stac_base_url),
            "x-georiva": self._build_georiva_metadata(collection),
        }
        
        return data
    
    # ── Extent ────────────────────────────────────────────────────────────
    
    def _build_extent(self, collection: Collection) -> dict:
        """
        Build the EDR extent object.

        spatial.bbox  — from collection.bounds
        temporal.interval — from collection.time_start / time_end
        temporal.values   — queried from Item.time (explicit timestep list)
        """
        # Spatial
        bbox = collection.bounds or [-180, -90, 180, 90]
        spatial = {
            "bbox": [bbox],
            "crs": collection.crs or "EPSG:4326",
        }
        
        # Temporal interval (coarse range from collection metadata)
        interval_start = collection.time_start.isoformat() if collection.time_start else None
        interval_end = collection.time_end.isoformat() if collection.time_end else None
        
        # Temporal values — explicit list of available timesteps
        temporal_values = self._get_temporal_values(collection)
        
        temporal = {
            "interval": [[interval_start, interval_end]],
            "values": temporal_values,
            "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        }
        
        return {
            "spatial": spatial,
            "temporal": temporal,
        }
    
    def _get_temporal_values(self, collection: Collection) -> list:
        """
        Query all distinct valid times for this collection.

        For non-forecast collections: all item times, oldest → newest.
        For forecast collections: times from the latest reference_time run only
        """
        if collection.is_forecast:
            # Latest forecast run only
            latest_ref = (
                Item.objects
                .filter(collection=collection, reference_time__isnull=False)
                .order_by('-reference_time')
                .values_list('reference_time', flat=True)
                .first()
            )
            if latest_ref:
                qs = Item.objects.filter(
                    collection=collection,
                    reference_time=latest_ref,
                )
            else:
                qs = Item.objects.none()
        else:
            qs = Item.objects.filter(
                collection=collection,
                reference_time__isnull=True,
            )
        
        return [
            t.isoformat()
            for t in qs.values_list('time', flat=True).order_by('time').distinct()
        ]
    
    # ── Parameter names ───────────────────────────────────────────────────
    
    def _build_parameter_names(self, collection: Collection) -> dict:
        """
        Build parameter_names dict — one entry per active Variable.

        Keys are variable slugs. Values are serialized parameter objects
        including x-georiva style hints.
        """
        parameter_names = {}
        variables = collection.variables.filter(is_active=True).order_by('sort_order')
        
        for variable in variables:
            parameter_names[variable.slug] = EDRParameterSerializer(
                variable,
                context=self.context,
            ).data
        
        return parameter_names
    
    # ── Data queries ──────────────────────────────────────────────────────
    
    def _build_data_queries(self, collection_url: str) -> dict:
        """
        Advertise available query types.
        Phase 1: position only (documented but not yet implemented).
        """
        return {
            "position": {
                "link": {
                    "href": f"{collection_url}position/",
                    "rel": "data",
                    "type": "application/prs.coverage+json",
                    "title": "Position query — retrieve data at one or more points",
                }
            },
            # "area": {
            #     "link": {
            #         "href": f"{collection_url}area/",
            #         "rel": "data",
            #         "type": "application/prs.coverage+json",
            #         "title": "Area query — retrieve data within a polygon",
            #     }
            # },
            # "locations": {
            #     "link": {
            #         "href": f"{collection_url}locations/",
            #         "rel": "data",
            #         "type": "application/json",
            #         "title": "Locations — retrieve data for named locations",
            #     }
            # },
        }
    
    # ── Providers ─────────────────────────────────────────────────────────
    
    def _build_providers(self, collection: Collection) -> list:
        catalog = collection.catalog
        providers = []
        if catalog.provider:
            provider = {
                "name": catalog.provider,
                "roles": ["producer"],
            }
            if catalog.provider_url:
                provider["url"] = catalog.provider_url
            providers.append(provider)
        return providers
    
    # ── Links ─────────────────────────────────────────────────────────────
    
    def _build_links(
            self,
            collection: Collection,
            collection_url: str,
            stac_base_url: str,
    ) -> list:
        catalog = collection.catalog
        links = [
            {
                "rel": "self",
                "href": collection_url,
                "type": "application/json",
                "title": collection.name,
            },
            {
                "rel": "root",
                "href": self._get_base_url(),
                "type": "application/json",
                "title": "GeoRiva EDR API",
            },
            {
                "rel": "collection",
                "href": f"{self._get_base_url()}collections/",
                "type": "application/json",
                "title": "All EDR collections",
            },
            # Cross-link to STAC
            {
                "rel": "canonical",
                "href": f"{stac_base_url}collections/{catalog.slug}/{collection.slug}/",
                "type": "application/json",
                "title": "STAC Collection",
            },
        ]
        
        # License
        if catalog.license and catalog.provider_url:
            links.append({
                "rel": "license",
                "href": catalog.provider_url,
                "title": catalog.license,
            })
        
        return links
    
    # ── GeoRiva metadata ──────────────────────────────────────────────────
    
    def _build_georiva_metadata(self, collection: Collection) -> dict:
        """
        GeoRiva-specific metadata that doesn't fit the EDR spec.
        Prefixed x-georiva to mark as an extension.
        """
        catalog = collection.catalog
        meta = {
            "catalog_slug": catalog.slug,
            "catalog_name": catalog.name,
            "collection_slug": collection.slug,
            "time_resolution": collection.time_resolution or None,
            "item_count": collection.item_count,
            "is_forecast": collection.is_forecast,
            "crs": collection.crs,
        }
        
        if collection.is_forecast:
            meta["forecast_horizon_hours"] = collection.forecast_horizon_hours
            meta["retain_past_forecasts"] = collection.retain_past_forecasts
            meta["retain_latest_run_only"] = collection.retain_latest_run_only
        
        if catalog.license:
            meta["license"] = catalog.license
        
        return {k: v for k, v in meta.items() if v is not None}


# =============================================================================
# Collection list serializer — summary shape for /collections/
# =============================================================================

class EDRCollectionSummarySerializer(serializers.Serializer, EDRBaseURLMixin):
    """
    Lightweight summary of a Collection for the list endpoint.

    Omits temporal.values and parameter palette details to keep
    the list response fast — clients fetch the full detail when needed.
    """
    
    def to_representation(self, collection: Collection):
        base_url = self._get_base_url()
        collection_url = f"{base_url}collections/{collection.slug}/"
        
        bbox = collection.bounds or [-180, -90, 180, 90]
        interval_start = collection.time_start.isoformat() if collection.time_start else None
        interval_end = collection.time_end.isoformat() if collection.time_end else None
        
        # Parameter names — slugs and labels only, no palette
        parameter_names = {}
        for variable in collection.variables.filter(is_active=True).order_by('sort_order'):
            parameter_names[variable.slug] = {
                "type": "Parameter",
                "label": variable.name,
                "observedProperty": {"id": variable.slug, "label": variable.name},
                **({"unit": {"symbol": variable.units}} if variable.units else {}),
            }
        
        return {
            "id": collection.slug,
            "title": collection.name,
            "description": collection.description or "",
            "extent": {
                "spatial": {"bbox": [bbox]},
                "temporal": {
                    "interval": [[interval_start, interval_end]],
                    "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
                },
            },
            "parameter_names": parameter_names,
            "links": [
                {
                    "rel": "self",
                    "href": collection_url,
                    "type": "application/json",
                    "title": collection.name,
                }
            ],
            "x-georiva": {
                "catalog_slug": collection.catalog.slug,
                "catalog_name": collection.catalog.name,
                "time_resolution": collection.time_resolution or None,
                "is_forecast": collection.is_forecast,
                "item_count": collection.item_count,
            },
        }
