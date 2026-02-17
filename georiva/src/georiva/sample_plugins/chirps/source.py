from __future__ import annotations

import gzip
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional, Tuple

import rasterio
import requests

from georiva.sources.fetch import FileRequest, HTTPFetchStrategy
from georiva.sources.source import BaseDataSource, DataSourceType

CHIRPS_NODATA = -9999.0


# -----------------------------------------------------------------------------
# Helpers for advancing from latest stored date (avoid refetching same period)
# -----------------------------------------------------------------------------

def _as_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _next_month_start(dt: datetime) -> datetime:
    dt = _as_utc(dt)
    y, m = dt.year, dt.month
    if m == 12:
        return datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    return datetime(y, m + 1, 1, tzinfo=timezone.utc)


def _next_pentad_start(dt: datetime) -> datetime:
    """
    Pentads start on day 1, 6, 11, 16, 21, 26.
    If latest is within a pentad, jump to the NEXT pentad start.
    """
    dt = _as_utc(dt)
    starts = [1, 6, 11, 16, 21, 26]
    current_start = max(d for d in starts if d <= dt.day)
    if current_start == 26:
        # next pentad begins next month, day 1
        return _next_month_start(dt)
    return datetime(dt.year, dt.month, current_start + 5, tzinfo=timezone.utc)


# -----------------------------------------------------------------------------
# CHIRPS spec
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ChirpsPeriodSpec:
    slug: str  # "monthly" | "pentadal"
    label: str
    file_template: str  # "/africa_monthly/tifs/chirps-v2.0.{YYYY}.{MM}.tif.gz", etc.


class CHIRPSDataSource(BaseDataSource):
    """
    CHIRPS v2.0 rainfall GeoTIFF (gz) source.

    Responsibilities:
      - Generate FileRequests (URLs + timestamps + metadata).
      - (Optional) post_process_fetched_file(): gunzip .tif.gz -> .tif.

    Notes:
      - CHIRPS is DERIVED, not a forecast, so reference_time is None.
      - Filenames include a timestamp that GeoTIFFFormatPlugin can parse.
    """
    
    type = "chirps"
    label = "CHIRPS v2.0"
    
    BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0"
    
    PERIODS: dict[str, ChirpsPeriodSpec] = {
        "monthly": ChirpsPeriodSpec(
            slug="monthly",
            label="Monthly",
            file_template="/africa_monthly/tifs/chirps-v2.0.{YYYY}.{MM}.tif.gz",
        ),
        "pentadal": ChirpsPeriodSpec(
            slug="pentadal",
            label="Pentadal",
            file_template="/africa_pentad/tifs/chirps-v2.0.{YYYY}.{MM}.{P}.tif.gz",
        ),
    }
    
    # For “latest available” probing, how far back to try before giving up
    LATEST_LOOKBACK_DAYS = 120
    
    def __init__(self, config: dict, fetch_strategy=HTTPFetchStrategy):
        super().__init__(config, fetch_strategy)
        
        # Which period to request
        self.enabled_period: str = config.get("period")
        
        # Variables (CHIRPS GeoTIFF is precip totals)
        self.requested_variables = config.get("variables", ["precip"])
        
        self.default_start_date = config.get("default_start_date")
        
        self.head_timeout = int(config.get("head_timeout", 20))
        self._http = requests.Session()
    
    @property
    def name(self) -> str:
        return "CHIRPS v2.0"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.DERIVED
    
    def get_available_variables(self) -> list[dict]:
        return [
            {"slug": "precip", "name": "Precipitation", "units": "mm", "periods": ["monthly", "pentadal"]},
        ]
    
    # -------------------------------------------------------------------------
    # Time window behavior: avoid refetching same period when using db latest
    # -------------------------------------------------------------------------
    
    def advance_start_from_latest(self, latest: datetime, *, collection=None) -> datetime:
        """
        Called by BaseDataSource.get_time_window() (if you adopted that pattern).
        Uses collection.time_resolution to move to the NEXT period.
        """
        latest = _as_utc(latest)
        
        res = getattr(collection, "time_resolution", None)
        if res == "monthly":
            return _next_month_start(latest)
        if res == "pentadal":
            return _next_pentad_start(latest)
        return latest
    
    # -------------------------------------------------------------------------
    # Filename timestamp helper (matches GeoTIFFFormatPlugin patterns)
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _ts_for_filename(dt: datetime) -> str:
        # Your GeoTIFFFormatPlugin parses "YYYY-MM-DDTHH:MM:SS" (no trailing Z)
        dt = _as_utc(dt)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    # -------------------------------------------------------------------------
    # URL helpers
    # -------------------------------------------------------------------------
    
    def _url_exists(self, url: str) -> bool:
        try:
            r = self._http.head(url, allow_redirects=True, timeout=self.head_timeout)
            return r.status_code == 200
        except requests.RequestException:
            return False
    
    def _monthly_url(self, dt: datetime) -> str:
        spec = self.PERIODS["monthly"]
        mm = f"{dt.month:02d}"
        path = spec.file_template.replace("{YYYY}", f"{dt.year}").replace("{MM}", mm)
        return f"{self.BASE_URL}{path}"
    
    def _pentad_num(self, dt: datetime) -> int:
        # Pentad numbering: 1..6 within a month based on day-of-month
        # (1:1-5, 2:6-10, 3:11-15, 4:16-20, 5:21-25, 6:26-end)
        return min(6, ((dt.day - 1) // 5) + 1)
    
    def _pentadal_url(self, dt: datetime) -> str:
        spec = self.PERIODS["pentadal"]
        mm = f"{dt.month:02d}"
        p = self._pentad_num(dt)
        path = (
            spec.file_template
            .replace("{YYYY}", f"{dt.year}")
            .replace("{MM}", mm)
            .replace("{P}", f"{p}")
        )
        return f"{self.BASE_URL}{path}"
    
    def _month_start(self, dt: datetime) -> datetime:
        dt = _as_utc(dt)
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # -------------------------------------------------------------------------
    # Iterators over periods
    # -------------------------------------------------------------------------
    
    def _iter_months(self, start: datetime, end: datetime) -> Iterator[datetime]:
        cur = self._month_start(start)
        end_m = self._month_start(end)
        while cur <= end_m:
            yield cur
            y, m = cur.year, cur.month
            cur = cur.replace(year=y + 1, month=1) if m == 12 else cur.replace(month=m + 1)
    
    def _iter_pentads(self, start: datetime, end: datetime) -> Iterator[datetime]:
        # Step day-by-day but only yield when pentad boundary changes
        start = _as_utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
        end = _as_utc(end).replace(hour=0, minute=0, second=0, microsecond=0)
        
        cur = start
        last_key = None
        while cur <= end:
            key = (cur.year, cur.month, self._pentad_num(cur))
            if key != last_key:
                pentad_start_day = (key[2] - 1) * 5 + 1
                yield cur.replace(day=pentad_start_day)
                last_key = key
            cur += timedelta(days=1)
    
    # -------------------------------------------------------------------------
    # Latest available (remote probing)
    # -------------------------------------------------------------------------
    
    def get_latest_available(self) -> Optional[datetime]:
        """
        Try to discover latest CHIRPS file by probing backwards.
        Prefer pentadal if enabled, fall back to monthly.
        """
        now = datetime.now(timezone.utc)
        
        probes: list[tuple[str, callable]] = []
        if self.enabled_period == "pentadal":
            probes.append(("pentadal", self._pentadal_url))
        if self.enabled_period == "monthly":
            probes.append(("monthly", self._monthly_url))
        
        for period, url_fn in probes:
            for i in range(self.LATEST_LOOKBACK_DAYS):
                dt = now - timedelta(days=i)
                url = url_fn(dt)
                if self._url_exists(url):
                    if period == "monthly":
                        return self._month_start(dt)
                    return _as_utc(dt).replace(hour=0, minute=0, second=0, microsecond=0)
        
        return None
    
    def get_default_start_date(self, *, collection=None) -> datetime:
        default_start_date = self.default_start_date
        
        if default_start_date:
            return datetime.combine(default_start_date, datetime.min.time())
        
        # 2 months before today
        now = datetime.now(timezone.utc)
        default_start_date = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=60)
        
        return default_start_date
    
    # -------------------------------------------------------------------------
    # Request generation (core)
    # -------------------------------------------------------------------------
    
    def generate_requests(
            self,
            start_time: datetime,
            end_time: datetime,
            variables: Optional[list[str]] = None,
            **kwargs,
    ) -> Iterator[FileRequest]:
        variables = variables or self.requested_variables
        
        start_time = _as_utc(start_time)
        end_time = _as_utc(end_time)
        
        if self.enabled_period == "monthly":
            for dt in self._iter_months(start_time, end_time):
                url = self._monthly_url(dt)
                ts = self._ts_for_filename(dt)
                ident = f"chirps-monthly-{dt.year}{dt.month:02d}"
                
                # Timestamp embedded for GeoTIFFFormatPlugin.get_timestamps()
                filename = f"chirps_monthly_precip_{ts}.tif.gz"
                
                yield FileRequest(
                    identifier=ident,
                    filename=filename,
                    valid_time=dt,  # period start
                    reference_time=None,  # CHIRPS is not a forecast
                    params={
                        "url": url,
                        "period": "monthly",
                        "source": "CHIRPS-2.0",
                        "year": dt.year,
                        "month": dt.month,
                        "variables": variables,
                    },
                    expected_format="tif.gz",
                    variables=variables,
                )
        
        if self.enabled_period == "pentadal":
            for dt in self._iter_pentads(start_time, end_time):
                p = self._pentad_num(dt)
                url = self._pentadal_url(dt)
                ts = self._ts_for_filename(dt)
                ident = f"chirps-pentadal-{dt.year}{dt.month:02d}p{p}"
                
                filename = f"chirps_pentadal_precip_{ts}.tif.gz"
                
                yield FileRequest(
                    identifier=ident,
                    filename=filename,
                    valid_time=dt,  # pentad start
                    reference_time=None,  # CHIRPS is not a forecast
                    params={
                        "url": url,
                        "period": "pentadal",
                        "source": "CHIRPS-2.0",
                        "year": dt.year,
                        "month": dt.month,
                        "pentad": p,
                        "variables": variables,
                    },
                    expected_format="tif.gz",
                    variables=variables,
                )
    
    # -------------------------------------------------------------------------
    # Post-fetch hook: gunzip *.tif.gz -> *.tif so GeoTIFFFormatPlugin can read it
    # -------------------------------------------------------------------------
    
    def post_process_fetched_file(self, request, local_path: Path) -> Tuple[Path, Optional[str]]:
        if request.expected_format != "tif.gz":
            return local_path, None
        
        out_path = local_path.with_suffix("")
        
        # Decompress
        with gzip.open(local_path, "rb") as f_in:
            with open(out_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Validate output file
        if not out_path.exists() or out_path.stat().st_size == 0:
            raise RuntimeError(f"Decompressed file is empty: {out_path}")
        
        # Optional but VERY useful: validate with rasterio
        try:
            with rasterio.open(out_path, "r+") as src:
                _ = src.count  # basic validation
                if src.nodata != CHIRPS_NODATA:
                    src.nodata = CHIRPS_NODATA
        
        except Exception as e:
            raise RuntimeError(f"Decompressed file is not a valid GeoTIFF: {e}")
        new_filename = request.filename[:-3]
        return out_path, new_filename
