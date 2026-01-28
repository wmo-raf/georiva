import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


@dataclass
class FileRequest:
    """
    A request for a specific file/resource.
    
    Unlike RemoteFile (which represents a discovered file), FileRequest
    represents what we WANT to fetch - it may or may not exist yet.
    """
    # Identity
    identifier: str  # Unique ID for this request
    filename: str  # Desired filename
    
    # Temporal context
    valid_time: Optional[datetime] = None  # What time the data represents
    reference_time: Optional[datetime] = None  # Forecast run time (if applicable)
    
    # Source-specific parameters (e.g., CDS request params, URL template vars)
    params: dict = field(default_factory=dict)
    
    # Metadata
    expected_size: Optional[int] = None
    expected_format: Optional[str] = None  # 'grib', 'netcdf', etc.
    variables: list[str] = field(default_factory=list)  # Variables in this file
    
    @property
    def is_forecast(self) -> bool:
        return self.reference_time is not None
    
    @property
    def forecast_hour(self) -> Optional[int]:
        if self.reference_time and self.valid_time:
            delta = self.valid_time - self.reference_time
            return int(delta.total_seconds() / 3600)
        return None


class FetchMode(str, Enum):
    """How the fetch strategy operates."""
    SYNC = 'sync'  # Direct download (HTTP, FTP, S3)
    ASYNC = 'async'  # Queue-based (CDS API, MARS)
    STREAM = 'stream'  # Real-time streaming (MQTT, WebSocket)


@dataclass
class FetchResult:
    """Result of fetching a single file."""
    request: FileRequest
    local_path: Optional[Path] = None
    success: bool = False
    error: Optional[str] = None
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    
    # For async/queued fetches
    job_id: Optional[str] = None
    status: str = 'pending'  # pending, queued, downloading, complete, failed
    
    @property
    def failed(self) -> bool:
        return not self.success


class BaseFetchStrategy(ABC):
    """
    Abstract base class for fetch strategies.
    
    A FetchStrategy knows HOW to retrieve data,
    but NOT what data to retrieve.
    """
    
    type: str = ""  # Unique identifier: 'http', 'ftp', 's3'
    label: str = ""  # Human-readable: 'HTTP/HTTPS', 'FTP/SFTP'
    
    def __init__(self, config: dict = None):
        if not self.type:
            raise ValueError(f"{self.__class__.__name__} must define 'type'")
        
        if not self.label:
            raise ValueError(f"{self.__class__.__name__} must define 'label'")
        
        self.config = config or {}
        self.logger = logging.getLogger(f"georiva.fetch.{self.type}")
    
    @property
    @abstractmethod
    def mode(self) -> FetchMode:
        """Return the fetch mode."""
        pass
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection."""
        pass
    
    @abstractmethod
    def fetch(self, request: FileRequest, local_path: Path) -> FetchResult:
        """Fetch a single file."""
        pass
    
    def check_status(self, job_id: str) -> FetchResult:
        """Check status of async fetch (for ASYNC mode)."""
        raise NotImplementedError("This strategy doesn't support async operations")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
