"""

TimeseriesService — extracts time series from virtual Zarr manifests, and
from Remote assets directly.

A Variable's virtual store covers its local assets only. A **Remote asset**
(ADR 0042) is read from upstream by range, one asset per timestep, and the two
are joined into one series — so a Mixed collection answers over its whole
record, and a wholly remote one needs no store at all (ADR 0043).

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
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from georiva.core.models import Variable

logger = logging.getLogger(__name__)


#: Upstream reads in flight at once for one request. Each is a few small range
#: requests; the wait is the upstream's latency, not our CPU.
REMOTE_READ_WORKERS = 8


class ManifestNotReady(Exception):
    """Raised when the VirtualZarrManifest for a variable is not READY."""


class RemoteRangeTooLarge(Exception):
    """Raised when a request spans more Remote assets than one request may read.

    Every Remote timestep is its own upstream read, made while the caller
    waits. The bound is ``GEORIVA_TIMESERIES_MAX_REMOTE_ASSETS``.
    """


class NotOfferedOverStore(Exception):
    """Raised when a Variable's data is Store assets: a time series is not read
    from an Upstream store (ADR 0044, out of scope). Its message is for the caller."""


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
        variable: Variable,
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
        from georiva.geoprocessing.sampling import point_value

        logger.debug("Point query: %s @ (%.4f, %.4f)", variable.slug, lat, lon)

        def from_store():
            da = self._open_dataset(variable)[variable.slug]
            da = self._filter_time(da, time_start, time_end)
            return da.sel(lat=lat, lon=lon, method="nearest").load()

        return self._series(
            variable,
            time_start,
            time_end,
            name=variable.slug,
            from_store=from_store,
            from_raster=lambda src: point_value(src, lon, lat),
        )

    def area(
        self,
        variable: Variable,
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
            raise ValueError(f"Unsupported aggregation {aggregation!r}. Choose from: {sorted(supported)}")

        from georiva.geoprocessing.sampling import area_value

        def from_store():
            da = self._open_dataset(variable)[variable.slug]
            da = self._filter_time(da, time_start, time_end)

            # Bbox subset, then load: only the COG tiles intersecting the
            # polygon's bounding box are fetched.
            da = self._bbox_subset(da, geometry).load()
            da = self._apply_polygon_mask(da, geometry)

            agg_fn = {
                "mean": da.mean,
                "sum": da.sum,
                "min": da.min,
                "max": da.max,
                "std": da.std,
            }[aggregation]
            return agg_fn(dim=["lat", "lon"], skipna=True)

        return self._series(
            variable,
            time_start,
            time_end,
            name=f"{variable.slug}_{aggregation}",
            from_store=from_store,
            from_raster=lambda src: area_value(src, geometry, aggregation),
        )

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _series(self, variable, time_start, time_end, *, name, from_store, from_raster) -> pd.Series:
        """One series over the range: the virtual store's timesteps and the
        Remote assets', joined in time order.

        ``from_store`` answers a 1-D time DataArray from the Variable's virtual
        store; ``from_raster`` answers one value from one open raster. The store
        is opened whenever there is something local in the range to read from
        it — and always for a Variable with no Remote asset at all, which keeps a
        missing manifest the error it has always been. A Variable that does have
        Remote assets and nothing of either kind in the range answers empty.
        """
        from georiva.core.models import Asset
        from georiva.core.storage.asset_location import remote_q, store_q

        # Asked first: a Variable of Store assets has no virtual store either,
        # and would otherwise answer as a manifest that is not ready.
        if (
            not self._assets(variable, None, None).exists()
            and Asset.objects.filter(store_q(), variable=variable).exists()
        ):
            raise NotOfferedOverStore(
                "Time series are not available for this collection yet: its data is read from an upstream "
                "data store, which serves maps only."
            )

        remote = self._remote_assets(variable, time_start, time_end)
        has_remote = bool(remote) or self._assets(variable, None, None).filter(remote_q()).exists()
        parts = []
        if not has_remote or self._local_assets(variable, time_start, time_end).exists():
            parts.append(self._to_series(from_store(), name))
        if remote:
            parts.append(self._remote_series(remote, from_raster, name))
        if not parts:
            return pd.Series([], index=pd.DatetimeIndex([]), name=name, dtype="float64")
        if len(parts) == 1:
            return parts[0]
        return pd.concat(parts).sort_index().rename(name)

    @staticmethod
    def _assets(variable, time_start, time_end):
        from georiva.core.models import Asset

        assets = Asset.objects.filter(variable=variable, format=Asset.Format.COG)
        if time_start is not None:
            assets = assets.filter(item__time__gte=time_start)
        if time_end is not None:
            assets = assets.filter(item__time__lte=time_end)
        return assets

    def _local_assets(self, variable, time_start, time_end):
        from georiva.core.storage.asset_location import local_q

        return self._assets(variable, time_start, time_end).filter(local_q())

    def _remote_assets(self, variable, time_start, time_end) -> list:
        """The Remote assets in the range, as ``(time, location)`` in time order.

        Raises RemoteRangeTooLarge rather than start more upstream reads than
        one request may make.
        """
        from django.conf import settings

        from georiva.core.storage.asset_location import AssetLocation, remote_q

        limit = settings.GEORIVA_TIMESERIES_MAX_REMOTE_ASSETS
        rows = list(
            self._assets(variable, time_start, time_end)
            .filter(remote_q())
            .order_by("item__time", "item__reference_time")
            .values_list("item__time", "href")[: limit + 1]
        )
        if len(rows) > limit:
            raise RemoteRangeTooLarge(
                f"This time range spans more than {limit} remote timesteps, each read from upstream "
                "while you wait. Narrow the time range with time_start and time_end."
            )
        # One value per valid time: of several runs' assets at one time, the newest run's.
        return list({time: (time, AssetLocation(href)) for time, href in rows}.values())

    @staticmethod
    def _remote_series(remote: list, from_raster, name: str) -> pd.Series:
        """Read each Remote asset by range and gather the values into a series.

        A read that fails raises, and fails the request: nothing is marked,
        retried or repaired here (ADR 0043 §5).
        """
        from concurrent.futures import ThreadPoolExecutor

        def read(location):
            with location.open_raster() as src:
                return from_raster(src)

        with ThreadPoolExecutor(max_workers=min(REMOTE_READ_WORKERS, len(remote))) as pool:
            values = list(pool.map(read, [location for _, location in remote]))

        index = pd.DatetimeIndex([pd.Timestamp(time).tz_convert("UTC").tz_localize(None) for time, _ in remote])
        return pd.Series(values, index=index, name=name, dtype="float64").dropna()

    def _open_dataset(self, variable: Variable):
        """
        Open the variable's virtual Zarr (Icechunk) repo as a lazy xarray Dataset.

        Raises ManifestNotReady if the manifest does not exist or is not READY.
        Opens a read-only session at the tip of the repo's main branch.
        """
        from georiva.virtual_zarr.models import VirtualZarrManifest

        try:
            manifest = VirtualZarrManifest.objects.get(variable=variable)
        except VirtualZarrManifest.DoesNotExist:
            raise ManifestNotReady(
                f"No virtual Zarr manifest found for {variable}. "
                "Run: manage.py build_virtual_zarr "
                f"--collection {variable.collection.catalog.storage_prefix}"
                f"/{variable.collection.slug} "
                f"--variable {variable.slug}"
            ) from None

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
