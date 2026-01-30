import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Iterator, Optional, Protocol, runtime_checkable

from georiva.sources.fetch.base import FileRequest, BaseFetchStrategy


class DataSourceType(str, Enum):
    """Categories of data sources."""
    FORECAST = 'forecast'  # NWP models (GFS, ECMWF, etc.)
    REANALYSIS = 'reanalysis'  # ERA5, MERRA-2
    SATELLITE = 'satellite'  # MSG, GOES, Sentinel
    OBSERVATION = 'observation'  # Station data, radar
    DERIVED = 'derived'  # CHIRPS, SPI, etc.


@runtime_checkable
class DataSource(Protocol):
    """
    Protocol for data sources.
    
    A DataSource knows:
    - What data is available (or should be available)
    - How to construct requests for that data
    - What variables/collections it provides
    
    It does NOT know how to actually fetch the data - that's FetchStrategy's job.
    """
    
    @property
    def name(self) -> str:
        """Human-readable name."""
        ...
    
    @property
    def source_type(self) -> DataSourceType:
        """Type of data source."""
        ...
    
    def get_available_variables(self) -> list[dict]:
        """
        Return list of variables this source provides.
        
        Each dict should have at least:
        - slug: variable identifier
        - name: human-readable name
        - units: measurement units
        """
        ...
    
    def generate_requests(
            self,
            start_time: datetime,
            end_time: datetime,
            variables: Optional[list[str]] = None,
            **kwargs
    ) -> Iterator[FileRequest]:
        """
        Generate file requests for a time range.
        
        This is the core method - it figures out what files we need
        based on what time range we want to cover.
        """
        ...
    
    def get_latest_available(self) -> Optional[datetime]:
        """
        Get the timestamp of the latest available data.
        
        For forecasts, this is typically the latest model run time.
        For observations, it's the latest observation time.
        """
        ...


class BaseDataSource(ABC):
    """
    Abstract base class for data sources.
    
    Provides common functionality for discovering and requesting data.
    """
    
    type: str = ""  # 'ecmwf-aifs', 'gfs', 'chirps'
    label: str = ""  # 'ECMWF AIFS', 'NOAA GFS', 'CHIRPS'
    
    def __init__(self, config: dict, fetch_strategy: BaseFetchStrategy = None):
        if not self.type:
            raise ValueError(f"{self.__class__.__name__} must define 'type'")
        
        if not self.label:
            raise ValueError(f"{self.__class__.__name__} must define 'label'")
        
        self.fetch_strategy = fetch_strategy
        if not self.fetch_strategy:
            raise ValueError(f"{self.__class__.__name__} must define 'fetch_strategy'")
        
        self.config = config
        self.logger = logging.getLogger(f"georiva.datasource.{self.type}")
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def source_type(self) -> DataSourceType:
        pass
    
    @abstractmethod
    def get_available_variables(self) -> list[dict]:
        pass
    
    @abstractmethod
    def generate_requests(
            self,
            start_time: datetime,
            end_time: datetime,
            variables: Optional[list[str]] = None,
            **kwargs
    ) -> Iterator[FileRequest]:
        pass
    
    def get_latest_available(self) -> Optional[datetime]:
        """Default implementation - subclasses should override for accuracy."""
        return None
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def _round_to_cycle(self, dt: datetime, cycle_hours: list[int]) -> datetime:
        """
        Round datetime to nearest forecast cycle.
        
        Args:
            dt: Input datetime
            cycle_hours: Valid cycle hours (e.g., [0, 6, 12, 18])
        """
        cycle_hours = sorted(cycle_hours)
        
        for cycle in reversed(cycle_hours):
            if dt.hour >= cycle:
                return dt.replace(hour=cycle, minute=0, second=0, microsecond=0)
        
        # Previous day's last cycle
        prev_day = dt - timedelta(days=1)
        return prev_day.replace(
            hour=cycle_hours[-1], minute=0, second=0, microsecond=0
        )
    
    def _generate_forecast_hours(
            self,
            max_hour: int,
            step: int = 1,
            start_hour: int = 0
    ) -> list[int]:
        """Generate list of forecast hours."""
        return list(range(start_hour, max_hour + 1, step))
