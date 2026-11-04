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

Both lists are bounded on purpose. There is one Item row per
``(reference_time, time)``, so an unbounded answer grows as runs × lead times —
the flat list is the latest run only, and ``x-georiva.runs`` is the newest
``runs_limit`` runs (issue #600). A client that wants the runs beyond that
window follows the ``related`` link to ``/api/runs/{catalog}/{collection}/``,
which knows every run and will hand over one run's valid times on request.
"""

from itertools import groupby

from django.urls import reverse
from rest_framework import serializers

from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Collection, Item
from georiva.core.utils import get_base_stac_api_url, get_full_url_by_request

# How many recent runs x-georiva.runs carries, and the most a caller may ask
# for with ?runs_limit=. The ceiling matches the runs route's own MAX_LIMIT.
DEFAULT_RUNS_LIMIT = 5
MAX_RUNS_LIMIT = 200

# =============================================================================
# Base URL mixin
# =============================================================================


class EDRBaseURLMixin:
    """Provide EDR base URL from request context."""

    def _get_base_url(self) -> str:
        request = self.context.get("request")
        if request:
            return get_full_url_by_request(request, "/api/edr/")
        return "/api/edr/"

    def _get_stac_base_url(self) -> str:
        request = self.context.get("request")
        return get_base_stac_api_url(request)


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

        contract = render_contract(variable)
        # The texture's encoding range; palette_min/palette_max below are the
        # stops' span, for the legend only.
        x_georiva["image_unscale"] = contract.image_unscale

        style = variable.default_style
        if style:
            try:
                x_georiva["palette"] = contract.palette
                x_georiva["palette_min"], x_georiva["palette_max"] = contract.legend_range
                x_georiva["palette_name"] = style.name
                if style.ramp:
                    # This key speaks the legacy palette vocabulary — clients
                    # predate the ramp catalog, whose "qualitative" they knew
                    # as "categorical" — so serving output stays unchanged
                    # across the palette→style migration.
                    x_georiva["palette_type"] = {
                        "qualitative": "categorical",
                    }.get(style.ramp.ramp_type, style.ramp.ramp_type)
            except Exception:
                pass
        else:
            x_georiva["palette"] = contract.palette
            x_georiva["palette_min"], x_georiva["palette_max"] = contract.legend_range

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
            "has_reference_time",
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
            "links": self._build_links(collection, collection_url, stac_base_url, has_reference_time),
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
        return collection.has_runs()

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

    def _get_flat_temporal_values(self, collection: Collection, has_reference_time: bool) -> list[str]:
        """
        Return a flat list of ISO 8601 valid_time strings.

        No reference_time → all item times oldest → newest.
        Has reference_time → valid_times from the latest run only
                             (keeps the list bounded and current).
        """
        if has_reference_time:
            latest_ref = (
                Item.objects.filter(collection=collection, reference_time__isnull=False)
                .order_by("-reference_time")
                .values_list("reference_time", flat=True)
                .first()
            )
            if not latest_ref:
                return []
            qs = Item.objects.filter(
                collection=collection,
                reference_time=latest_ref,
            ).order_by("time")
        else:
            qs = Item.objects.filter(
                collection=collection,
                reference_time__isnull=True,
            ).order_by("time")

        return [t.isoformat() for t in qs.values_list("time", flat=True).distinct()]

    def _runs_limit(self) -> int:
        """How many recent runs this request gets, clamped to a sane window.

        A client says ``?runs_limit=N``; nonsense falls back to the default and
        anything above ``MAX_RUNS_LIMIT`` is capped, because the point of the
        bound is that the server decides how big this document may get.
        """
        request = self.context.get("request")
        raw = request.query_params.get("runs_limit") if request else None
        if raw is None:
            return DEFAULT_RUNS_LIMIT
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_RUNS_LIMIT
        return max(1, min(value, MAX_RUNS_LIMIT))

    def _get_runs(self, collection: Collection, limit: int) -> list[dict]:
        """
        Build the structured runs list for x-georiva.runs — newest ``limit`` runs.

        Two bounded queries: the newest ``limit`` reference times, then the item
        times belonging to them. Groups in Python via itertools.groupby.
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
        reference_times = list(
            Item.objects.filter(collection=collection, reference_time__isnull=False)
            .order_by("-reference_time")
            .values_list("reference_time", flat=True)
            .distinct()[:limit]
        )
        if not reference_times:
            return []

        rows = list(
            Item.objects.filter(collection=collection, reference_time__in=reference_times)
            .order_by("-reference_time", "time")
            .values_list("reference_time", "time")
        )

        runs = []
        for ref_time, group in groupby(rows, key=lambda r: r[0]):
            runs.append(
                {
                    "reference_time": ref_time.isoformat(),
                    "valid_times": [t.isoformat() for _, t in group],
                }
            )

        return runs

    # ── Parameter names ───────────────────────────────────────────────────
    def _build_parameter_names(self, collection: Collection) -> dict:
        parameter_names = {}
        variables = (
            collection.variables.filter(is_active=True)
            .select_related("unit")
            .prefetch_related("styles__ramp")
            .order_by("sort_order")
        )

        for variable in variables:
            parameter_names[variable.slug] = EDRParameterSerializer(
                variable,
                context=self.context,
            ).data

        return parameter_names

    # ── Data queries ──────────────────────────────────────────────────────

    def _build_data_queries(self, collection_url: str) -> dict:
        # Metadata plane only: no data-query route exists (see edr/urls.py), so
        # advertising one would send an EDR client to a 404.
        return {}

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
        has_reference_time: bool = False,
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

        if has_reference_time:
            # x-georiva.runs is a window; this route knows every run, which are
            # complete, and one run's valid times (#600).
            links.append(
                {
                    "rel": "related",
                    "href": get_full_url_by_request(
                        self.context.get("request"),
                        reverse("run-list", args=[catalog.slug, collection.slug]),
                    ),
                    "type": "application/json",
                    "title": f"{collection.name} model runs",
                }
            )

        if catalog.license and catalog.provider_url:
            links.append(
                {
                    "rel": "license",
                    "href": catalog.provider_url,
                    "title": catalog.license,
                }
            )

        return links

    # ── GeoRiva metadata ──────────────────────────────────────────────────

    def _build_georiva_metadata(self, collection: Collection, has_reference_time: bool) -> dict:
        """
        GeoRiva-specific metadata that doesn't fit the EDR spec.

        For collections with reference_time, adds:
          - has_reference_time: true
          - runs: [{reference_time, valid_times[]}] — the newest runs only,
            used by the frontend to pick times without a DB lookup
          - runs_limit: how many runs that window was allowed to hold, so a
            client can tell a short list from a truncated one and knows what
            ``?runs_limit=`` it may raise.
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
            limit = self._runs_limit()
            meta["runs"] = self._get_runs(collection, limit)
            meta["runs_limit"] = limit

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
            collection.variables.filter(is_active=True)
            .select_related("unit")
            .prefetch_related("styles__ramp")
            .order_by("sort_order")
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
        has_reference_time = getattr(collection, "has_reference_time", collection.is_forecast)

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
