"""
ECMWF AIFS Data Source (Open Data / HTTPS)

- Downloads from: https://data.ecmwf.int/forecasts
- Forecast-native: generate requests by (run_time, forecast_hour)
- Exposes ONLY the latest *published* run (fallback: today -> yesterday)
- One GRIB2 per step (file contains many variables); variables are metadata for downstream extraction
"""

from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import requests

from georiva.sources.fetch import FileRequest, HTTPFetchStrategy
from georiva.sources.source import BaseDataSource, DataSourceType


class ECMWFAIFSDataSource(BaseDataSource):
    """
    Data source for ECMWF AIFS (Artificial Intelligence Forecasting System), Open Data HTTPS.

    Portal layout:
    https://data.ecmwf.int/forecasts/{YYYYMMDD}/{HH}z/aifs-single/0p25/oper/
        {YYYYMMDDHHMMSS}-{step}h-oper-fc.grib2
        {YYYYMMDDHHMMSS}-{step}h-oper-fc.index
    """
    
    type = "ecmwf-aifs"
    label = "ECMWF AIFS"
    
    # AIFS configuration (AIFS-single cycles)
    CYCLES = [0, 6, 12, 18]
    MAX_FORECAST_HOUR = 360  # 15 days
    FORECAST_STEP = 6  # typical cadence for AIFS output
    
    # (Optional) informational only; selection uses URL existence checks
    AVAILABILITY_DELAY = 7
    
    SURFACE_VARIABLES = {
        "2t": {"name": "2m Temperature", "units": "K", "grib_param": 167},
        "10u": {"name": "10m U Wind", "units": "m/s", "grib_param": 165},
        "10v": {"name": "10m V Wind", "units": "m/s", "grib_param": 166},
        "msl": {"name": "Mean Sea Level Pressure", "units": "Pa", "grib_param": 151},
        "tp": {"name": "Total Precipitation", "units": "m", "grib_param": 228},
        "sp": {"name": "Surface Pressure", "units": "Pa", "grib_param": 134},
    }
    
    PRESSURE_VARIABLES = {
        "t": {"name": "Temperature", "units": "K", "grib_param": 130},
        "u": {"name": "U Wind", "units": "m/s", "grib_param": 131},
        "v": {"name": "V Wind", "units": "m/s", "grib_param": 132},
        "z": {"name": "Geopotential", "units": "m²/s²", "grib_param": 129},
        "q": {"name": "Specific Humidity", "units": "kg/kg", "grib_param": 133},
    }
    
    PRESSURE_LEVELS = [1000, 925, 850, 700, 500, 300, 250, 200, 50]
    
    def __init__(self, config: dict, fetch_strategy=HTTPFetchStrategy):
        """
        Config options:
            variables: list of variable slugs (metadata; extraction is downstream)
            pressure_levels: list of pressure levels (metadata)
            forecast_hours: list of hours, or 'all' (default 0..360 step 6)
            allow_yesterday_fallback: bool (default True) -> if no run found today, try yesterday
            head_timeout: int seconds (default 20)
        """
        super().__init__(config, fetch_strategy)
        
        self.requested_variables = config.get("variables", list(self.SURFACE_VARIABLES.keys()))
        self.pressure_levels = config.get("pressure_levels", self.PRESSURE_LEVELS)
        
        fh_config = config.get("forecast_hours", [0, 6, 12, 18])
        self.forecast_hours = list(fh_config)
        
        rt_config = config.get("run_hours", [0, 12])
        self.run_hours = list(rt_config)
        
        self.allow_yesterday_fallback = bool(config.get("allow_yesterday_fallback", True))
        self.head_timeout = int(config.get("head_timeout", 20))
        
        # Reuse HTTP session for efficiency
        self._http = requests.Session()
    
    @property
    def name(self) -> str:
        return "ECMWF AIFS"
    
    @property
    def source_type(self) -> DataSourceType:
        return DataSourceType.FORECAST
    
    def get_available_variables(self) -> list[dict]:
        variables = []
        
        for slug, info in self.SURFACE_VARIABLES.items():
            variables.append(
                {"slug": slug, "name": info["name"], "units": info["units"], "level_type": "surface"}
            )
        
        for slug, info in self.PRESSURE_VARIABLES.items():
            for level in self.PRESSURE_LEVELS:
                variables.append(
                    {
                        "slug": f"{slug}_{level}",
                        "name": f"{info['name']} at {level} hPa",
                        "units": info["units"],
                        "level_type": "pressure",
                        "level": level,
                    }
                )
        
        return variables
    
    # -------------------------------------------------------------------------
    # Latest-run selection (today -> yesterday fallback)
    # -------------------------------------------------------------------------
    
    def _run_stamp(self, run_time: datetime) -> str:
        # e.g. 20260128060000
        return run_time.strftime("%Y%m%d%H%M%S")
    
    def _step0_url(self, run_time: datetime) -> str:
        base_url = "https://data.ecmwf.int/forecasts"
        date_folder = run_time.strftime("%Y%m%d")
        cycle_folder = f"{run_time.hour:02d}z"
        run_stamp = self._run_stamp(run_time)
        return (
            f"{base_url}/{date_folder}/{cycle_folder}"
            f"/aifs-single/0p25/oper/{run_stamp}-0h-oper-fc.grib2"
        )
    
    def _url_exists(self, url: str) -> bool:
        try:
            r = self._http.head(url, allow_redirects=True, timeout=self.head_timeout)
            return r.status_code == 200
        except requests.RequestException:
            return False
    
    def _candidate_run_times_for_date(self, d) -> list[datetime]:
        # Latest-first for "latest available" behavior
        return [
            datetime(d.year, d.month, d.day, hh, 0, 0, tzinfo=timezone.utc)
            for hh in sorted(self.run_hours, reverse=True)
        ]
    
    def get_latest_available_run(self) -> Optional[datetime]:
        """
        Returns the latest *published* run time:
        - checks today's cycles (18->12->06->00)
        - if none found and allow_yesterday_fallback=True, checks yesterday similarly
        """
        today = datetime.now(timezone.utc).date()
        
        # 1) Today
        for rt in self._candidate_run_times_for_date(today):
            if self._url_exists(self._step0_url(rt)):
                return rt
        
        # 2) Yesterday (fallback)
        if self.allow_yesterday_fallback:
            yesterday = today - timedelta(days=1)
            for rt in self._candidate_run_times_for_date(yesterday):
                if self._url_exists(self._step0_url(rt)):
                    return rt
        
        return None
    
    # -------------------------------------------------------------------------
    # Forecast-native request generation
    # -------------------------------------------------------------------------
    
    def generate_requests(
            self,
            *_,
            variables: Optional[list[str]] = None,
            **kwargs
    ) -> Iterator[FileRequest]:
        """
        Generates requests for ONLY the latest available run (today, else yesterday if enabled).
        """
        variables = variables or self.requested_variables
        
        run_time = self.get_latest_available_run()
        if not run_time:
            return  # nothing published yet (or network issue)
        
        for forecast_hour in self.forecast_hours:
            # keep it sane
            if forecast_hour < 0 or forecast_hour > self.MAX_FORECAST_HOUR:
                continue
            if forecast_hour % self.FORECAST_STEP != 0:
                continue
            
            valid_time = run_time + timedelta(hours=forecast_hour)
            yield from self._generate_open_data_requests(run_time, forecast_hour, valid_time, variables)
    
    # -------------------------------------------------------------------------
    # Open Data requests (one GRIB2 per step)
    # -------------------------------------------------------------------------
    
    def _generate_open_data_requests(
            self,
            run_time: datetime,
            forecast_hour: int,
            valid_time: datetime,
            variables: list[str],
    ) -> Iterator[FileRequest]:
        base_url = "https://data.ecmwf.int/forecasts"
        
        date_folder = run_time.strftime("%Y%m%d")  # e.g. 20260128
        cycle_folder = f"{run_time.hour:02d}z"  # e.g. 06z
        run_stamp = self._run_stamp(run_time)  # e.g. 20260128060000
        step_str = f"{forecast_hour}h"  # e.g. 12h, 102h (NOT zero-padded)
        
        url = (
            f"{base_url}/{date_folder}/{cycle_folder}"
            f"/aifs-single/0p25/oper/{run_stamp}-{step_str}-oper-fc.grib2"
        )
        
        filename = f"aifs_{run_stamp}_{forecast_hour}h_oper_fc.grib2"
        
        yield FileRequest(
            identifier=f"aifs-open-{run_stamp}-{forecast_hour}h",
            filename=filename,
            valid_time=valid_time,
            reference_time=run_time,
            params={
                "url": url,
                "source": "open_data",
                "model": "aifs-single",
                "grid": "0p25",
                "run_stamp": run_stamp,
                "step_hours": forecast_hour,
                "requested_variables": variables,  # metadata only
            },
            expected_format="grib",
            variables=variables,  # metadata only
        )
