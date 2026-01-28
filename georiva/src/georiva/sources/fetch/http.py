"""
GeoRiva HTTP Fetch Strategy

For direct HTTP/HTTPS downloads from:
- ECMWF Open Data
- NOAA NOMADS
- Any direct URL source
"""

import time
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseFetchStrategy, FetchMode, FetchResult, FileRequest


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
        """
        super().__init__(config or {})
        
        self.timeout = self.config.get('timeout', 120)
        self.connect_timeout = self.config.get('connect_timeout', 30)
        self.max_retries = self.config.get('max_retries', 3)
        self.backoff_factor = self.config.get('backoff_factor', 1.0)
        self.chunk_size = self.config.get('chunk_size', 8192)
        self.verify_ssl = self.config.get('verify_ssl', True)
        self.custom_headers = self.config.get('headers', {})
        self.user_agent = self.config.get('user_agent', 'GeoRiva/1.0')
        
        self._session: Optional[requests.Session] = None
    
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
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )
        
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
        # Set headers
        self._session.headers.update({
            'User-Agent': self.user_agent,
            **self.custom_headers,
        })
        
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
        
        Expects request.params['url'] to contain the download URL.
        """
        result = FetchResult(request=request, local_path=local_path)
        
        url = request.params.get('url')
        if not url:
            result.success = False
            result.error = "No URL in request params"
            result.status = 'failed'
            return result
        
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        start_time = time.time()
        
        try:
            # Make request with streaming
            response = self._session.get(
                url,
                stream=True,
                timeout=(self.connect_timeout, self.timeout),
            )
            
            # Handle HTTP errors
            if response.status_code == 404:
                result.success = False
                result.error = f"File not found (404): {url}"
                result.status = 'not_found'
                return result
            
            if response.status_code == 403:
                result.success = False
                result.error = f"Access forbidden (403): {url}"
                result.status = 'failed'
                return result
            
            response.raise_for_status()
            
            # Get expected size
            content_length = response.headers.get('content-length')
            expected_size = int(content_length) if content_length else None
            
            # Download with progress
            bytes_downloaded = 0
            last_log_time = start_time
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        
                        # Log progress every 10 seconds for large files
                        now = time.time()
                        if expected_size and expected_size > 10_000_000 and (now - last_log_time) > 10:
                            pct = (bytes_downloaded / expected_size) * 100
                            self.logger.debug(
                                f"  Download progress: {pct:.1f}% "
                                f"({bytes_downloaded / 1024 / 1024:.1f} MB)"
                            )
                            last_log_time = now
            
            # Verify size
            if expected_size and bytes_downloaded != expected_size:
                result.success = False
                result.error = f"Size mismatch: expected {expected_size}, got {bytes_downloaded}"
                result.status = 'failed'
                # Clean up partial file
                if local_path.exists():
                    local_path.unlink()
                return result
            
            # Success
            result.success = True
            result.status = 'complete'
            result.bytes_transferred = bytes_downloaded
            result.duration_seconds = time.time() - start_time
            
            speed_mbps = (bytes_downloaded / 1024 / 1024) / max(result.duration_seconds, 0.1)
            self.logger.debug(
                f"Downloaded {bytes_downloaded / 1024 / 1024:.1f} MB "
                f"in {result.duration_seconds:.1f}s ({speed_mbps:.1f} MB/s)"
            )
        
        except requests.exceptions.Timeout as e:
            result.success = False
            result.error = f"Request timeout: {e}"
            result.status = 'failed'
            self.logger.error(f"Timeout downloading {url}: {e}")
        
        except requests.exceptions.ConnectionError as e:
            result.success = False
            result.error = f"Connection error: {e}"
            result.status = 'failed'
            self.logger.error(f"Connection error for {url}: {e}")
        
        except requests.exceptions.RequestException as e:
            result.success = False
            result.error = str(e)
            result.status = 'failed'
            self.logger.error(f"Request failed for {url}: {e}")
        
        except IOError as e:
            result.success = False
            result.error = f"IO error writing file: {e}"
            result.status = 'failed'
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
                'exists': response.status_code == 200,
                'status_code': response.status_code,
                'size': int(response.headers.get('content-length', 0)) or None,
                'last_modified': response.headers.get('last-modified'),
                'content_type': response.headers.get('content-type'),
            }
        
        except requests.RequestException as e:
            return {
                'exists': False,
                'error': str(e),
            }


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
        
        self.auth_type = self.config.get('auth_type')
        self.auth_username = self.config.get('auth_username')
        self.auth_password = self.config.get('auth_password')
        self.auth_token = self.config.get('auth_token')
        self.auth_header_name = self.config.get('auth_header_name', 'Authorization')
        self.auth_header_value = self.config.get('auth_header_value')
    
    def connect(self) -> None:
        """Create authenticated HTTP session."""
        super().connect()
        
        if self.auth_type == 'basic':
            self._session.auth = (self.auth_username, self.auth_password)
        
        elif self.auth_type == 'bearer':
            self._session.headers['Authorization'] = f'Bearer {self.auth_token}'
        
        elif self.auth_type == 'api_key':
            self._session.headers[self.auth_header_name] = self.auth_token
        
        elif self.auth_type == 'custom':
            self._session.headers[self.auth_header_name] = self.auth_header_value
        
        self.logger.debug(f"Authentication configured: {self.auth_type or 'none'}")
