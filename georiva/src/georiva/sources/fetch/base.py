import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


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
    valid_time: datetime | None = None  # What time the data represents
    reference_time: datetime | None = None  # Forecast run time (if applicable)

    # Source-specific parameters (e.g., CDS request params, URL template vars)
    params: dict = field(default_factory=dict)

    # Metadata
    expected_size: int | None = None
    expected_format: str | None = None  # 'grib', 'netcdf', etc.
    variables: list[str] = field(default_factory=list)  # Variables in this file

    def to_dict(self) -> dict:
        """JSON-safe representation, persisted on FetchedFile so a single
        file can be re-fetched later (PRD #217). Datetimes become ISO strings."""
        from dataclasses import asdict

        payload = asdict(self)
        for key in ("valid_time", "reference_time"):
            if payload[key] is not None:
                payload[key] = payload[key].isoformat()
        return payload

    @classmethod
    def from_dict(cls, payload: dict) -> "FileRequest":
        """Rebuild a request from to_dict() output."""
        data = dict(payload)
        for key in ("valid_time", "reference_time"):
            if data.get(key) is not None:
                data[key] = datetime.fromisoformat(data[key])
        return cls(**data)

    @property
    def is_forecast(self) -> bool:
        return self.reference_time is not None

    @property
    def forecast_hour(self) -> int | None:
        if self.reference_time and self.valid_time:
            delta = self.valid_time - self.reference_time
            return int(delta.total_seconds() / 3600)
        return None


class FetchMode(str, Enum):
    """How the fetch strategy operates."""

    SYNC = "sync"  # Direct download (HTTP, FTP, S3)
    ASYNC = "async"  # Queue-based (CDS API, MARS)
    STREAM = "stream"  # Real-time streaming (MQTT, WebSocket)


class FailureScope(str, Enum):
    """What a failed fetch means for the rest of its run (#473, #477).

    FILE          definitive, and about this file only (a 404, a bad download).
    TRANSIENT     ambiguous: a timeout, a dropped transfer, a 5xx that outlasted
                  the HTTP retries. The default for a failure nobody classified.
    SOURCE        the source as a whole cannot serve us right now: it does not
                  resolve, refuses us, tells us to back off, or our side cannot
                  accept the file.
    NOT_ATTEMPTED on FetchedFile rows only: the run stopped before reaching it.
                  A strategy never returns it.
    """

    FILE = "file"
    TRANSIENT = "transient"
    SOURCE = "source"
    NOT_ATTEMPTED = "not_attempted"

    @property
    def label(self) -> str:
        return self.value.replace("_", " ").capitalize()


@dataclass
class FetchResult:
    """Result of fetching a single file."""

    request: FileRequest
    local_path: Path | None = None
    success: bool = False
    error: str | None = None
    bytes_transferred: int = 0
    duration_seconds: float = 0.0

    # For async/queued fetches
    job_id: str | None = None
    status: str = "pending"  # pending, queued, downloading, complete, failed

    # Why a failed fetch failed, in terms of the run (#477); None on success, and
    # on a failure a strategy did not classify (read as TRANSIENT).
    failure_scope: FailureScope | None = None
    # Seconds the source asked us to wait (`Retry-After`), when it said.
    retry_after: float | None = None

    @property
    def failed(self) -> bool:
        return not self.success

    def fail(self, error: str, scope: FailureScope) -> "FetchResult":
        """Record this fetch as failed, and why."""
        self.success = False
        self.error = error
        self.status = "failed"
        self.failure_scope = scope
        return self


# A single file may take at most this long, wall-clock (#479). The read timeout
# only bounds silence on the socket, so a source that trickles never trips it.
DEFAULT_MAX_FILE_SECONDS = 900


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
        self.max_file_seconds = self.config.get("max_file_seconds", DEFAULT_MAX_FILE_SECONDS)
        # Monotonic seconds; a seam so tests can drive the deadline without waiting.
        self.clock = time.monotonic

    def file_timeout_error(self) -> str:
        return f"download exceeded its time limit ({self.max_file_seconds:g}s)"

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
