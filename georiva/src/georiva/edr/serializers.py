"""
GeoRiva EDR API Serializers

EDR hierarchy:
- Landing Page
  └── Collections  (one per GeoRiva Collection)
      └── parameter_names  (one entry per active Variable)
          └── x-georiva    (palette, value range, rendering hints)

Implements OGC API - Environmental Data Retrieval 1.1 (19-086r6)
Phase 1: Collection metadata only (landing page, conformance, collection list, collection detail)

Temporal values strategy:
  - Collections without reference_time → flat ISO string list (EDR-compliant)
  - Collections with reference_time    → flat ISO list in extent.temporal.values
                                          + structured runs in x-georiva.runs
                                          (keeps spec compliance while enabling
                                           frontend URL construction)
"""

from itertools import groupby

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
        
        if variable.unit:
            data["unit"] = {"symbol": variable.unit.symbol}
        
        if variable.description:
            data["description"] = variable.description
        
        x_georiva = {
            "value_min": variable.value_min,
            "value_max": variable.value_max,
            "scale_type": variable.scale_type,
            "transform_type": variable.transform_type,
        }
        
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
            x_georiva["palette"] = variable.weather_layers_palette
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

    Temporal handling:
      - No reference_time (CHIRPS): flat values list, no runs in x-georiva
      - Has reference_time (ECMWF, ERA5): flat values list (EDR-compliant) +
        x-georiva.runs (structured, for frontend URL construction)
    """
    
    def to_representation(self, collection: Collection):
        base_url = self._get_base_url()
        stac_base_url = self._get_stac_base_url()
        collection_url = f"{base_url}collections/{collection.slug}/"
        
        # Use annotated value if the view provided it (avoids extra query),
        # otherwise fall back to a direct DB check.
        has_reference_time = getattr(
            collection,
            'has_reference_time',
            self._check_has_reference_time(collection),
        )
        
        data = {
            "id": collection.slug,
            "title": collection.name,
            "description": collection.description or f"{collection.name} from {collection.catalog.name}",
            "extent": self._build_extent(collection, has_reference_time),
            "parameter_names": self._build_parameter_names(collection),
            "data_queries": self._build_data_queries(collection_url),
            "providers": self._build_providers(collection),
            "links": self._build_links(collection, collection_url, stac_base_url),
            "x-georiva": self._build_georiva_metadata(collection, has_reference_time),
        }
        
        return data
    
    # ── Reference time detection ───────────────────────────────────────────
    
    def _check_has_reference_time(self, collection: Collection) -> bool:
        """
        Fallback DB check — used only when the view hasn't annotated the queryset.

        Covers is_forecast=True but also reanalysis/hindcast datasets
        where reference_time is set but is_forecast may be False.
        """
        return Item.objects.filter(
            collection=collection,
            reference_time__isnull=False,
        ).exists()
    
    # ── Extent ────────────────────────────────────────────────────────────
    
    def _build_extent(self, collection: Collection, has_reference_time: bool) -> dict:
        bbox = collection.spatial_extent or [-180, -90, 180, 90]
        spatial = {
            "bbox": [bbox],
            "crs": collection.crs or "EPSG:4326",
        }
        
        interval_start = collection.time_start.isoformat() if collection.time_start else None
        interval_end = collection.time_end.isoformat() if collection.time_end else None
        
        temporal = {
            "interval": [[interval_start, interval_end]],
            "values": self._get_flat_temporal_values(collection, has_reference_time),
            "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        }
        
        return {"spatial": spatial, "temporal": temporal}
    
    def _get_flat_temporal_values(
            self, collection: Collection, has_reference_time: bool
    ) -> list[str]:
        """
        Return a flat list of ISO 8601 valid_time strings.

        No reference_time → all item times oldest → newest.
        Has reference_time → valid_times from the latest run only
                             (keeps the list bounded and current).
        """
        if has_reference_time:
            latest_ref = (
                Item.objects
                .filter(collection=collection, reference_time__isnull=False)
                .order_by('-reference_time')
                .values_list('reference_time', flat=True)
                .first()
            )
            if not latest_ref:
                return []
            qs = Item.objects.filter(
                collection=collection,
                reference_time=latest_ref,
            ).order_by('time')
        else:
            qs = Item.objects.filter(
                collection=collection,
                reference_time__isnull=True,
            ).order_by('time')
        
        return [
            t.isoformat()
            for t in qs.values_list('time', flat=True).distinct()
        ]
    
    def _get_runs(self, collection: Collection) -> list[dict]:
        """
        Build the structured runs list for x-georiva.runs.

        Single query — groups in Python via itertools.groupby.
        Runs ordered newest → oldest; valid_times oldest → newest within each run.

        Shape:
        [
          {
            "reference_time": "2026-03-22T00:00:00+00:00",
            "valid_times": ["2026-03-22T06:00:00+00:00", ...]
          },
          ...
        ]
        """
        rows = list(
            Item.objects
            .filter(collection=collection, reference_time__isnull=False)
            .order_by('-reference_time', 'time')
            .values_list('reference_time', 'time')
        )
        
        runs = []
        for ref_time, group in groupby(rows, key=lambda r: r[0]):
            runs.append({
                "reference_time": ref_time.isoformat(),
                "valid_times": [t.isoformat() for _, t in group],
            })
        
        return runs
    
    # ── Parameter names ───────────────────────────────────────────────────
    def _build_parameter_names(self, collection: Collection) -> dict:
        parameter_names = {}
        variables = (
            collection.variables
            .filter(is_active=True)
            .select_related('unit', 'palette')
            .order_by('sort_order')
        )
        
        for variable in variables:
            parameter_names[variable.slug] = EDRParameterSerializer(
                variable,
                context=self.context,
            ).data
        
        return parameter_names
    
    # ── Data queries ──────────────────────────────────────────────────────
    
    def _build_data_queries(self, collection_url: str) -> dict:
        return {
            "position": {
                "link": {
                    "href": f"{collection_url}position/",
                    "rel": "data",
                    "type": "application/prs.coverage+json",
                    "title": "Position query — retrieve data at one or more points",
                }
            },
        }
    
    # ── Providers ─────────────────────────────────────────────────────────
    
    def _build_providers(self, collection: Collection) -> list:
        catalog = collection.catalog
        providers = []
        if catalog.provider:
            provider = {"name": catalog.provider, "roles": ["producer"]}
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
            {
                "rel": "canonical",
                "href": f"{stac_base_url}collections/{catalog.slug}/{collection.slug}/",
                "type": "application/json",
                "title": "STAC Collection",
            },
        ]
        
        if catalog.license and catalog.provider_url:
            links.append({
                "rel": "license",
                "href": catalog.provider_url,
                "title": catalog.license,
            })
        
        return links
    
    # ── GeoRiva metadata ──────────────────────────────────────────────────
    
    def _build_georiva_metadata(
            self, collection: Collection, has_reference_time: bool
    ) -> dict:
        """
        GeoRiva-specific metadata that doesn't fit the EDR spec.

        For collections with reference_time, adds:
          - has_reference_time: true
          - runs: [{reference_time, valid_times[]}]
            used by the frontend to construct asset URLs without a DB lookup.

        Asset URL pattern (frontend buildAssetUrl):
          No reference_time:
            {catalog}/{collection}/{variable}/{vt_Y}/{vt_m}/{vt_d}/{variable}_{vt_HHMMSS}.png
          Has reference_time:
            {catalog}/{collection}/{variable}/{vt_Y}/{vt_m}/{vt_d}/{variable}_{vt_HHMMSS}__ref{ref_YYYYMMDDTHHmmss}.png
        """
        catalog = collection.catalog
        meta = {
            "catalog_slug": catalog.slug,
            "catalog_name": catalog.name,
            "collection_slug": collection.slug,
            "time_resolution": collection.time_resolution or None,
            "item_count": collection.item_count,
            "is_forecast": collection.is_forecast,
            "has_reference_time": has_reference_time,
            "crs": collection.crs,
        }
        
        if collection.is_forecast:
            meta["forecast_horizon_hours"] = collection.forecast_horizon_hours
            meta["retain_past_forecasts"] = collection.retain_past_forecasts
            meta["retain_latest_run_only"] = collection.retain_latest_run_only
        
        if catalog.license:
            meta["license"] = catalog.license
        
        if has_reference_time:
            meta["runs"] = self._get_runs(collection)
        
        return {k: v for k, v in meta.items() if v is not None}


# =============================================================================
# Collection list serializer — summary shape for /collections/
# =============================================================================

class EDRCollectionSummarySerializer(serializers.Serializer, EDRBaseURLMixin):
    """
    Lightweight summary of a Collection for the list endpoint.

    Omits temporal.values, runs, and parameter palette details to keep
    the list response fast — clients fetch the full detail when needed.

    Expects the queryset to be annotated with has_reference_time by the view
    (see EDRCollectionListView). Falls back to is_forecast if not annotated.
    """
    
    def to_representation(self, collection: Collection):
        base_url = self._get_base_url()
        collection_url = f"{base_url}collections/{collection.slug}/"
        
        bbox = collection.spatial_extent or [-180, -90, 180, 90]
        interval_start = collection.time_start.isoformat() if collection.time_start else None
        interval_end = collection.time_end.isoformat() if collection.time_end else None
        
        parameter_names = {}
        
        variables = (
            collection.variables
            .filter(is_active=True)
            .select_related('unit', 'palette')
            .order_by('sort_order')
        )
        for variable in variables:
            parameter_names[variable.slug] = {
                "type": "Parameter",
                "label": variable.name,
                "observedProperty": {"id": variable.slug, "label": variable.name},
                **({"unit": {"symbol": variable.unit.symbol}} if variable.unit else {}),
            }
        
        # Use annotated value from view — avoids one EXISTS query per collection.
        # Falls back to is_forecast as a safe approximation if not annotated.
        has_reference_time = getattr(collection, 'has_reference_time', collection.is_forecast)
        
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
                "collection_slug": collection.slug,
                "time_resolution": collection.time_resolution or None,
                "is_forecast": collection.is_forecast,
                "has_reference_time": has_reference_time,
                "item_count": collection.item_count,
            },
        }
