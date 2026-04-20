"""

Point and area timeseries API views.

Point  — GET  /api/analysis/timeseries/point/
Area   — POST /api/analysis/timeseries/area/

Both endpoints resolve the variable from the natural key
``catalog_slug/collection_slug/variable_slug`` and delegate
extraction to TimeseriesService.
"""

from __future__ import annotations

import logging

from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import (
    AreaRequestSerializer,
    AreaResponseSerializer,
    PointRequestSerializer,
    PointResponseSerializer,
)
from .service import ManifestNotReady, TimeseriesService

logger = logging.getLogger(__name__)


def _series_to_data(series) -> list[dict]:
    """Convert a pandas Series to the [{time, value}] response format."""
    return [
        {"time": ts.isoformat() + "Z", "value": None if v != v else float(v)}
        for ts, v in series.items()
    ]


class PointTimeseriesView(APIView):
    """
    Extract a point time series from a virtual Zarr manifest.

    GET /api/analysis/timeseries/point/
        ?variable=chirps/chirps-monthly/precipitation
        &lat=-1.286389
        &lon=36.817223
        &time_start=2020-01-01        (optional)
        &time_end=2024-12-31          (optional)

    Returns the full time series for the nearest grid cell to (lat, lon).
    """
    
    def get(self, request: Request) -> Response:
        serializer = PointRequestSerializer(data=request.query_params)
        if not serializer.is_valid():
            return Response(
                serializer.errors,
                status=status.HTTP_400_BAD_REQUEST,
            )
        
        data = serializer.validated_data
        variable = data["variable"]
        lat = data["lat"]
        lon = data["lon"]
        time_start = data.get("time_start")
        time_end = data.get("time_end")
        
        try:
            service = TimeseriesService(internal=True)
            series = service.point(
                variable=variable,
                lat=lat,
                lon=lon,
                time_start=time_start,
                time_end=time_end,
            )
        except ManifestNotReady as exc:
            return Response(
                {"detail": str(exc)},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except Exception as exc:
            logger.exception(
                "Point timeseries failed for %s @ (%.4f, %.4f)",
                variable.slug, lat, lon,
            )
            return Response(
                {"detail": "Timeseries extraction failed.", "error": str(exc)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        payload = {
            "variable": variable.slug,
            "units": variable.unit.symbol if variable.unit else "",
            "lat": lat,
            "lon": lon,
            "time_start": series.index.min().isoformat() + "Z" if len(series) else None,
            "time_end": series.index.max().isoformat() + "Z" if len(series) else None,
            "count": len(series),
            "data": _series_to_data(series),
        }
        
        return Response(
            PointResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )


class AreaTimeseriesView(APIView):
    """
    Zonal statistics over an arbitrary GeoJSON polygon.

    POST /api/analysis/timeseries/area/
    {
        "variable":    "chirps/chirps-monthly/precipitation",
        "geometry":    { "type": "Polygon", "coordinates": [...] },
        "aggregation": "mean",
        "time_start":  "2020-01-01",
        "time_end":    "2024-12-31"
    }

    Runs synchronously.  For large areas or long time ranges, wrap this
    in a Celery task and return a task_id instead (see tasks.py).
    """
    
    def post(self, request: Request) -> Response:
        serializer = AreaRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                serializer.errors,
                status=status.HTTP_400_BAD_REQUEST,
            )
        
        data = serializer.validated_data
        variable = data["variable"]
        geometry = data["geometry"]
        aggregation = data["aggregation"]
        time_start = data.get("time_start")
        time_end = data.get("time_end")
        
        try:
            service = TimeseriesService(internal=True)
            series = service.area(
                variable=variable,
                geometry=geometry,
                aggregation=aggregation,
                time_start=time_start,
                time_end=time_end,
            )
        except ManifestNotReady as exc:
            return Response(
                {"detail": str(exc)},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except ValueError as exc:
            return Response(
                {"detail": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as exc:
            logger.exception(
                "Area timeseries failed for %s", variable.slug
            )
            return Response(
                {"detail": "Timeseries extraction failed.", "error": str(exc)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        payload = {
            "variable": variable.slug,
            "units": variable.unit.symbol if variable.unit else "",
            "aggregation": aggregation,
            "time_start": series.index.min().isoformat() + "Z" if len(series) else None,
            "time_end": series.index.max().isoformat() + "Z" if len(series) else None,
            "count": len(series),
            "data": _series_to_data(series),
        }
        
        return Response(
            AreaResponseSerializer(payload).data,
            status=status.HTTP_200_OK,
        )
