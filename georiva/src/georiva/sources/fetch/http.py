"""
GeoRiva HTTP Fetch Strategy

For direct HTTP/HTTPS downloads from:
- ECMWF Open Data
- NOAA NOMADS
- Any direct URL source
"""

import time
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import NewConnectionError
from urllib3.util.retry import Retry

from .base import BaseFetchStrategy, FailureScope, FetchMode, FetchResult, FileRequest


class HTTPFetchStrategy(BaseFetchStrategy):
    """
    Fetch strategy for direct HTTP/HTTPS downloads.

    Features:
    - Automatic retries with exponential backoff
    - Streaming downloads for large files
    - Progress logging
    - Connection pooling
    - Configurable timeouts
    """

    type = "http"
    label = "HTTP/HTTPS"

    def __init__(self, config: dict = None):
        """
        Initialize HTTP fetch strategy.

        Config options:
            timeout: Request timeout in seconds (default: 120)
            connect_timeout: Connection timeout (default: 30)
            max_retries: Number of retries (default: 3)
            backoff_factor: Retry backoff multiplier (default: 1.0)
            chunk_size: Download chunk size in bytes (default: 8192)
            verify_ssl: Verify SSL certificates (default: True)
            headers: Additional HTTP headers (default: {})
            user_agent: Custom User-Agent (default: GeoRiva/1.0)
            max_file_seconds: Wall-clock limit for one file (default: 900)
        """
        super().__init__(config or {})

        self.timeout = self.config.get("timeout", 120)
        self.connect_timeout = self.config.get("connect_timeout", 30)
        self.max_retries = self.config.get("max_retries", 3)
        self.backoff_factor = self.config.get("backoff_factor", 1.0)
        self.chunk_size = self.config.get("chunk_size", 8192)
        self.verify_ssl = self.config.get("verify_ssl", True)
        self.custom_headers = self.config.get("headers", {})
        self.user_agent = self.config.get("user_agent", "GeoRiva/1.0")

        self._session: requests.Session | None = None

    @property
    def mode(self) -> FetchMode:
        return FetchMode.SYNC

    def connect(self) -> None:
        """Create HTTP session with retry configuration."""
        self._session = requests.Session()

        # Configure retries
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET"],
            # Hand back the last response once retries run out, rather than a
            # RetryError: its status and Retry-After classify the failure.
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )

        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        # Set headers
        self._session.headers.update(
            {
                "User-Agent": self.user_agent,
                **self.custom_headers,
            }
        )

        # SSL verification
        self._session.verify = self.verify_ssl

        self.logger.debug("HTTP session initialized")

    def disconnect(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
            self.logger.debug("HTTP session closed")

    def fetch(self, request: FileRequest, local_path: Path) -> FetchResult:
        """
        Download file via HTTP.

        Expects request.params['url'] to contain the download URL. A failed
        result carries its failure scope (#477).
        """
        result = FetchResult(request=request, local_path=local_path)

        url = request.params.get("url")
        if not url:
            return result.fail("No URL in request params", FailureScope.FILE)

        start_time = time.time()
        deadline = self.clock() + self.max_file_seconds

        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Make request with streaming
            response = self._session.get(
                url,
                stream=True,
                timeout=(self.connect_timeout, self.timeout),
            )

            # Handle HTTP errors
            if response.status_code >= 400:
                scope, retry_after = _classify_status(response)
                result.retry_after = retry_after
                return result.fail(f"HTTP {response.status_code} ({response.reason}): {url}", scope)

            # Get expected size
            content_length = response.headers.get("content-length")
            expected_size = int(content_length) if content_length else None

            # Download with progress
            bytes_downloaded = 0
            last_log_time = start_time

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if self.clock() > deadline:
                        response.close()
                        local_path.unlink(missing_ok=True)
                        return result.fail(self.file_timeout_error(), FailureScope.TRANSIENT)
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                        # Log progress every 10 seconds for large files
                        now = time.time()
                        if expected_size and expected_size > 10_000_000 and (now - last_log_time) > 10:
                            pct = (bytes_downloaded / expected_size) * 100
                            self.logger.debug(
                                f"  Download progress: {pct:.1f}% ({bytes_downloaded / 1024 / 1024:.1f} MB)"
                            )
                            last_log_time = now

            # Verify size
            if expected_size and bytes_downloaded != expected_size:
                # Clean up partial file
                if local_path.exists():
                    local_path.unlink()
                return result.fail(
                    f"Size mismatch: expected {expected_size}, got {bytes_downloaded}", FailureScope.FILE
                )

            # Success
            result.success = True
            result.status = "complete"
            result.bytes_transferred = bytes_downloaded
            result.duration_seconds = time.time() - start_time

            speed_mbps = (bytes_downloaded / 1024 / 1024) / max(result.duration_seconds, 0.1)
            self.logger.debug(
                f"Downloaded {bytes_downloaded / 1024 / 1024:.1f} MB "
                f"in {result.duration_seconds:.1f}s ({speed_mbps:.1f} MB/s)"
            )

        except requests.exceptions.Timeout as e:
            # Read or connect timeout: the source may be slow, or briefly gone.
            result.fail(f"Request timeout: {e}", FailureScope.TRANSIENT)
            self.logger.error(f"Timeout downloading {url}: {e}")

        except requests.exceptions.SSLError as e:
            result.fail(f"SSL error: {e}", FailureScope.SOURCE)
            self.logger.error(f"SSL error for {url}: {e}")

        except requests.exceptions.ConnectionError as e:
            # Never connected (no such host, refused) is the source; a connection
            # that dropped once open is ambiguous.
            scope = FailureScope.SOURCE if _never_connected(e) else FailureScope.TRANSIENT
            result.fail(f"Connection error: {e}", scope)
            self.logger.error(f"Connection error for {url}: {e}")

        except requests.exceptions.ChunkedEncodingError as e:
            result.fail(f"Transfer interrupted: {e}", FailureScope.TRANSIENT)
            self.logger.error(f"Transfer interrupted for {url}: {e}")

        except requests.exceptions.RetryError as e:
            result.fail(str(e), FailureScope.TRANSIENT)
            self.logger.error(f"Retries exhausted for {url}: {e}")

        except requests.exceptions.RequestException as e:
            # A malformed URL and the like: about this request, not the source.
            result.fail(str(e), FailureScope.FILE)
            self.logger.error(f"Request failed for {url}: {e}")

        except OSError as e:
            # Our side cannot accept the file: every file after it would fail too.
            result.fail(f"IO error writing file: {e}", FailureScope.SOURCE)
            self.logger.error(f"IO error: {e}")

        return result

    def head(self, url: str, timeout: int = 20) -> dict:
        """
        Make a HEAD request to check URL existence/metadata.

        Returns dict with 'exists', 'size', 'last_modified'.
        """
        try:
            response = self._session.head(
                url,
                allow_redirects=True,
                timeout=timeout,
            )

            return {
                "exists": response.status_code == 200,
                "status_code": response.status_code,
                "size": int(response.headers.get("content-length", 0)) or None,
                "last_modified": response.headers.get("last-modified"),
                "content_type": response.headers.get("content-type"),
            }

        except requests.RequestException as e:
            return {
                "exists": False,
                "error": str(e),
            }


def _classify_status(response) -> tuple[FailureScope, float | None]:
    """The failure scope of an HTTP error response, and its Retry-After if any.

    401/403 and 429 are the source refusing us; so is a 503 that says when to
    come back. Any other 5xx has already outlasted the session's retries, and is
    ambiguous, as is a 408 (the server timed out waiting for us). Any other 4xx is
    about this request.
    """
    status = response.status_code
    retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
    if status in (401, 403, 429) or (status == 503 and retry_after is not None):
        return FailureScope.SOURCE, retry_after
    if status >= 500 or status == 408:
        return FailureScope.TRANSIENT, retry_after
    return FailureScope.FILE, retry_after


def _retry_after_seconds(value: str | None) -> float | None:
    """`Retry-After` as seconds from now: it is either seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


def _never_connected(exc: BaseException) -> bool:
    """Whether a requests ConnectionError means no connection was ever made:
    the host does not resolve, or it refused us. urllib3 reports both as a
    NewConnectionError, wrapped in a MaxRetryError's `reason`."""
    seen = set()
    stack = [exc]
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, NewConnectionError):
            return True
        stack.extend(arg for arg in getattr(current, "args", ()) if isinstance(arg, BaseException))
        stack.extend([getattr(current, "reason", None), current.__cause__, current.__context__])
    return False


class AuthenticatedHTTPFetchStrategy(HTTPFetchStrategy):
    """
    HTTP fetch strategy with authentication support.

    Supports:
    - Basic auth
    - Bearer token
    - API key in header
    - Custom auth header
    """

    def __init__(self, config: dict = None):
        """
        Additional config options:
            auth_type: 'basic', 'bearer', 'api_key', 'custom' (default: None)
            auth_username: Username for basic auth
            auth_password: Password for basic auth
            auth_token: Bearer token or API key
            auth_header_name: Header name for API key (default: 'Authorization')
            auth_header_value: Full header value for custom auth
        """
        super().__init__(config)

        self.auth_type = self.config.get("auth_type")
        self.auth_username = self.config.get("auth_username")
        self.auth_password = self.config.get("auth_password")
        self.auth_token = self.config.get("auth_token")
        self.auth_header_name = self.config.get("auth_header_name", "Authorization")
        self.auth_header_value = self.config.get("auth_header_value")

    def connect(self) -> None:
        """Create authenticated HTTP session."""
        super().connect()

        if self.auth_type == "basic":
            self._session.auth = (self.auth_username, self.auth_password)

        elif self.auth_type == "bearer":
            self._session.headers["Authorization"] = f"Bearer {self.auth_token}"

        elif self.auth_type == "api_key":
            self._session.headers[self.auth_header_name] = self.auth_token

        elif self.auth_type == "custom":
            self._session.headers[self.auth_header_name] = self.auth_header_value

        self.logger.debug(f"Authentication configured: {self.auth_type or 'none'}")
