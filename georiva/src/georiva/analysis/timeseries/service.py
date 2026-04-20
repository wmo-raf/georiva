"""

TimeseriesService — extracts time series from virtual Zarr manifests.

Point queries are synchronous and return immediately.
Area queries (arbitrary GeoJSON polygons) are designed for async use via
Celery but can also be called synchronously for small polygons.

Both methods share the same open → subset → load pipeline.  The manifest
is opened fresh per call — there is no process-level cache here.  If you
need caching, wrap the caller (e.g. Django cache on the view layer).
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class ManifestNotReady(Exception):
    """Raised when the VirtualZarrManifest for a variable is not READY."""


class TimeseriesService:
    """
    Extracts time series data from virtual Zarr manifests.

    Parameters
    ----------
    internal : bool
        True  → fetch chunks via the internal MinIO endpoint (default,
                 used from Celery workers and Django views inside the
                 container).
        False → fetch chunks via the public endpoint (external callers).
    """
    
    def __init__(self, *, internal: bool = True) -> None:
        self._internal = internal
    
    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    
    def point(
            self,
            variable: "Variable",
            lat: float,
            lon: float,
            time_start: datetime | None = None,
            time_end: datetime | None = None,
    ) -> pd.Series:
        """
        Extract a point time series for a single lat/lon coordinate.

        Uses xarray nearest-neighbour selection — the closest grid cell to
        (lat, lon) is returned.  No interpolation is performed.

        Parameters
        ----------
        variable : Variable
            The Variable model instance to query.
        lat, lon : float
            WGS84 coordinates of the point.
        time_start, time_end : datetime, optional
            Inclusive time range filter.  If omitted, the full archive is
            returned.

        Returns
        -------
        pd.Series
            Index: UTC datetime.  Values: float (variable's output units).
            Name: variable.slug.

        Raises
        ------
        ManifestNotReady
            If the variable has no READY manifest.
        """
        ds = self._open_dataset(variable)
        
        da = ds[variable.slug]
        da = self._filter_time(da, time_start, time_end)
        
        logger.debug(
            "Point query: %s @ (%.4f, %.4f)", variable.slug, lat, lon
        )
        
        result = (
            da
            .sel(lat=lat, lon=lon, method="nearest")
            .load()
        )
        
        return self._to_series(result, variable.slug)
    
    def area(
            self,
            variable: "Variable",
            geometry: dict,
            aggregation: str = "mean",
            time_start: datetime | None = None,
            time_end: datetime | None = None,
    ) -> pd.Series:
        """
        Zonal statistics over an arbitrary GeoJSON polygon.

        Strategy:
          1. Bbox subset  — reduces data fetched from MinIO to tiles that
                            intersect the polygon bounding box.
          2. Polygon mask — sets pixels outside the geometry to NaN using
                            regionmask.
          3. Aggregation  — mean/sum/min/max/std over the masked spatial dims.

        Parameters
        ----------
        variable : Variable
            The Variable model instance to query.
        geometry : dict
            GeoJSON geometry (Polygon or MultiPolygon).
        aggregation : str
            One of ``mean``, ``sum``, ``min``, ``max``, ``std``.
        time_start, time_end : datetime, optional
            Inclusive time range filter.

        Returns
        -------
        pd.Series
            Index: UTC datetime.  Values: float (aggregated, variable units).
            Name: ``{variable.slug}_{aggregation}``.

        Raises
        ------
        ManifestNotReady
            If the variable has no READY manifest.
        ValueError
            If ``aggregation`` is not one of the supported values.
        """
        supported = {"mean", "sum", "min", "max", "std"}
        if aggregation not in supported:
            raise ValueError(
                f"Unsupported aggregation {aggregation!r}. "
                f"Choose from: {sorted(supported)}"
            )
        
        ds = self._open_dataset(variable)
        
        da = ds[variable.slug]
        da = self._filter_time(da, time_start, time_end)
        
        # --- Bbox subset ----------------------------------------------------
        # Reduce the spatial extent to the polygon's bounding box before
        # loading any data.  Only the intersecting COG tiles are fetched.
        da = self._bbox_subset(da, geometry)
        
        # --- Load into memory ------------------------------------------------
        # Load after bbox subset so we only pull the tiles we need.
        da = da.load()
        
        # --- Polygon mask ----------------------------------------------------
        da = self._apply_polygon_mask(da, geometry)
        
        # --- Spatial aggregation ---------------------------------------------
        agg_fn = {
            "mean": da.mean,
            "sum": da.sum,
            "min": da.min,
            "max": da.max,
            "std": da.std,
        }[aggregation]
        
        result = agg_fn(dim=["lat", "lon"], skipna=True)
        
        series_name = f"{variable.slug}_{aggregation}"
        return self._to_series(result, series_name)
    
    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------
    
    def _open_dataset(self, variable: "Variable"):
        """
        Open the virtual Zarr manifest for a variable as a lazy xarray Dataset.

        Raises ManifestNotReady if the manifest does not exist or is not READY.
        Downloads the manifest JSON from MinIO to a temp file, then opens it
        via the kerchunk engine.
        """
        from georiva.virtual_zarr.models import VirtualZarrManifest
        
        try:
            manifest = VirtualZarrManifest.objects.get(variable=variable)
        except VirtualZarrManifest.DoesNotExist:
            raise ManifestNotReady(
                f"No virtual Zarr manifest found for {variable}. "
                "Run: manage.py build_virtual_zarr "
                f"--collection {variable.collection.catalog.slug}"
                f"/{variable.collection.slug} "
                f"--variable {variable.slug}"
            )
        
        return manifest.open_dataset(internal=self._internal, chunks={})
    
    def _filter_time(self, da, time_start, time_end):
        """Apply an optional time range slice to a DataArray."""
        if time_start is None and time_end is None:
            return da
        
        # Normalise to tz-naive UTC strings for xarray slice
        def _fmt(dt: datetime) -> str:
            if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
                import pytz
                dt = dt.astimezone(pytz.utc).replace(tzinfo=None)
            return str(dt)
        
        start = _fmt(time_start) if time_start else None
        end = _fmt(time_end) if time_end else None
        
        return da.sel(time=slice(start, end))
    
    def _bbox_subset(self, da, geometry: dict):
        """
        Subset a DataArray to the bounding box of a GeoJSON geometry.

        Uses shapely to compute the bbox — no rasterio or GDAL required.
        The subset is inclusive so border pixels are included.
        """
        from shapely.geometry import shape
        
        geom = shape(geometry)
        minx, miny, maxx, maxy = geom.bounds
        
        return da.sel(
            lat=slice(maxy, miny),  # lat is descending (north → south)
            lon=slice(minx, maxx),
        )
    
    def _apply_polygon_mask(self, da, geometry: dict):
        """
        Mask pixels outside the GeoJSON geometry to NaN using regionmask.

        regionmask.Regions([geom]).mask() returns a DataArray of region
        indices (0 inside, NaN outside).  We keep pixels where mask == 0.
        """
        try:
            import regionmask
            from shapely.geometry import shape
            
            geom = shape(geometry)
            regions = regionmask.Regions([geom])
            mask = regions.mask(da)
            return da.where(mask == 0)
        
        except ImportError:
            # regionmask not installed — fall back to bbox only with a warning.
            logger.warning(
                "regionmask not installed — polygon masking skipped. "
                "Install regionmask for precise polygon masking: "
                "pip install regionmask"
            )
            return da
    
    @staticmethod
    def _to_series(result, name: str) -> pd.Series:
        """
        Convert a 1-D time DataArray to a pandas Series.

        Drops NaN values (masked nodata pixels outside the boundary) and
        ensures a clean UTC DatetimeIndex with no timezone info attached
        (consistent with how TimescaleDB returns timestamps).
        """
        series = result.to_series().rename(name)
        
        # Drop NaN — nodata values from masked pixels or missing timesteps
        series = series.dropna()
        
        # Ensure the index is tz-naive UTC
        if hasattr(series.index, "tz") and series.index.tz is not None:
            series.index = series.index.tz_convert("UTC").tz_localize(None)
        
        return series
