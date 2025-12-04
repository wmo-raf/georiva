"""
GeoRiva HTTP Loader

Loader implementation for HTTP/HTTPS data sources.
Supports various authentication methods and URL patterns.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.auth import HTTPBasicAuth

from .base import BaseLoader, FetchResult, RemoteFile


class HTTPLoader(BaseLoader):
    """
    Loader for HTTP/HTTPS data sources.
    
    Supports:
    - Direct URL downloads
    - URL patterns with date placeholders
    - Basic auth, Bearer token, API key authentication
    - Custom headers and query parameters
    - HTML directory listing parsing
    - Retry with backoff
    """
    
    def __init__(self, config, collection):
        super().__init__(config, collection)
        self._session: Optional[requests.Session] = None
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self) -> None:
        """Create an HTTP session with configured auth."""
        self._session = requests.Session()
        
        # Set up authentication
        auth_type = self.config.auth_type
        
        if auth_type == 'basic':
            self._session.auth = HTTPBasicAuth(
                self.config.auth_username,
                self.config.auth_password,
            )
        
        elif auth_type == 'bearer':
            self._session.headers['Authorization'] = f'Bearer {self.config.auth_token}'
        
        elif auth_type == 'api_key':
            # API key in header
            header_name = self.config.auth_header_name or 'X-API-Key'
            self._session.headers[header_name] = self.config.auth_token
        
        elif auth_type == 'custom_header':
            header_name = self.config.auth_header_name or 'Authorization'
            self._session.headers[header_name] = self.config.auth_token
        
        # Add custom headers
        if self.config.custom_headers:
            self._session.headers.update(self.config.custom_headers)
        
        # SSL verification
        self._session.verify = self.config.verify_ssl
        
        # Test connection
        try:
            response = self._session.head(
                self.config.base_url,
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            self.logger.info(f"HTTP connection verified: {self.config.base_url}")
        except requests.RequestException as e:
            self.logger.warning(f"HEAD request failed (may still work): {e}")
    
    def disconnect(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
    
    # =========================================================================
    # File Listing
    # =========================================================================
    
    def list_files(self) -> Iterator[RemoteFile]:
        """
        List available files.
        
        Strategy depends on configuration:
        1. If url_pattern has date placeholders -> generate URLs for date range
        2. Otherwise -> parse HTML directory listing from base_url
        """
        if self.config.url_pattern and self._has_date_placeholders(self.config.url_pattern):
            yield from self._list_from_pattern()
        else:
            yield from self._list_from_directory()
    
    def _has_date_placeholders(self, pattern: str) -> bool:
        """Check if pattern contains date placeholders."""
        placeholders = ['{year}', '{month}', '{day}', '{hour}', '{doy}', '{date}']
        return any(p in pattern for p in placeholders)
    
    def _list_from_pattern(self) -> Iterator[RemoteFile]:
        """Generate file URLs from date pattern."""
        pattern = self.config.url_pattern
        
        # Determine date range
        # Default: last 7 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)
        
        # Could be configured or derived from collection
        if hasattr(self.config, 'lookback_days'):
            start_date = end_date - timedelta(days=self.config.lookback_days)
        
        current = start_date
        while current <= end_date:
            # Build URL with placeholders
            url = self._build_url_from_pattern(pattern, current)
            
            # Check if file exists
            remote_file = self._probe_url(url, current)
            if remote_file:
                yield remote_file
            
            # Increment based on resolution
            if '{hour}' in pattern:
                current += timedelta(hours=1)
            else:
                current += timedelta(days=1)
    
    def _build_url_from_pattern(self, pattern: str, dt: datetime) -> str:
        """Build URL from pattern with date substitution."""
        url = pattern.format(
            base_url=self.config.base_url.rstrip('/'),
            year=dt.year,
            month=f'{dt.month:02d}',
            day=f'{dt.day:02d}',
            hour=f'{dt.hour:02d}',
            doy=f'{dt.timetuple().tm_yday:03d}',  # Day of year
            date=dt.strftime('%Y%m%d'),
            datetime=dt.strftime('%Y%m%d%H%M'),
        )
        
        # Handle relative URLs
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.config.base_url, url)
        
        return url
    
    def _probe_url(self, url: str, dt: datetime) -> Optional[RemoteFile]:
        """Check if URL exists and return RemoteFile if so."""
        try:
            response = self._session.head(
                url,
                timeout=self.config.timeout,
                allow_redirects=True,
            )
            
            if response.status_code == 200:
                # Extract filename from URL
                parsed = urlparse(url)
                filename = Path(parsed.path).name
                
                # Try to get size from headers
                size = None
                if 'content-length' in response.headers:
                    size = int(response.headers['content-length'])
                
                # Try to get modified time
                modified = None
                if 'last-modified' in response.headers:
                    try:
                        from email.utils import parsedate_to_datetime
                        modified = parsedate_to_datetime(response.headers['last-modified'])
                    except Exception:
                        pass
                
                return RemoteFile(
                    path=url,
                    filename=filename,
                    size=size,
                    modified=modified or dt,
                    metadata={'url': url},
                )
            
            elif response.status_code == 404:
                self.logger.debug(f"File not found: {url}")
            else:
                self.logger.warning(f"Unexpected status {response.status_code} for {url}")
        
        except requests.RequestException as e:
            self.logger.debug(f"Failed to probe {url}: {e}")
        
        return None
    
    def _list_from_directory(self) -> Iterator[RemoteFile]:
        """Parse HTML directory listing."""
        try:
            response = self._session.get(
                self.config.base_url,
                params=self.config.query_params or {},
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            
            # Parse HTML for links
            yield from self._parse_directory_html(response.text, self.config.base_url)
        
        except requests.RequestException as e:
            self.logger.error(f"Failed to list directory: {e}")
    
    def _parse_directory_html(self, html: str, base_url: str) -> Iterator[RemoteFile]:
        """
        Parse HTML directory listing for file links.
        
        Handles common formats:
        - Apache/nginx autoindex
        - Python SimpleHTTPServer
        - THREDDS catalogs (basic)
        """
        from html.parser import HTMLParser
        
        class LinkParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links = []
            
            def handle_starttag(self, tag, attrs):
                if tag == 'a':
                    href = dict(attrs).get('href')
                    if href:
                        self.links.append(href)
        
        parser = LinkParser()
        parser.feed(html)
        
        # Filter and yield files
        for href in parser.links:
            # Skip parent directory and anchors
            if href in ('..', '../', '.', './') or href.startswith('#'):
                continue
            
            # Skip directories (end with /)
            if href.endswith('/'):
                continue
            
            # Build full URL
            full_url = urljoin(base_url, href)
            filename = Path(urlparse(full_url).path).name
            
            # Apply filename pattern filter
            if not self._matches_pattern(filename):
                continue
            
            # Check file extensions (common data formats)
            ext = Path(filename).suffix.lower()
            if ext not in self._get_allowed_extensions():
                continue
            
            yield RemoteFile(
                path=full_url,
                filename=filename,
                metadata={'url': full_url},
            )
    
    def _matches_pattern(self, filename: str) -> bool:
        """Check if filename matches URL pattern."""
        pattern = self.config.url_pattern
        if not pattern:
            return True
        
        # If pattern is a full URL template, extract filename pattern
        if '{' in pattern:
            # Pattern is a URL template, not a filename pattern
            return True
        
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)
    
    def _get_allowed_extensions(self) -> set:
        """Get allowed file extensions for this collection."""
        # Could be derived from collection's file_format
        return {
            '.nc', '.nc4', '.grib', '.grib2', '.grb', '.grb2',
            '.tif', '.tiff', '.geotiff',
            '.hdf', '.h5', '.hdf5',
            '.zip', '.gz', '.bz2',
        }
    
    # =========================================================================
    # File Fetching
    # =========================================================================
    
    def fetch_file(self, remote_file: RemoteFile, local_path: Path) -> FetchResult:
        """Download a file via HTTP."""
        result = FetchResult(remote_file=remote_file, local_path=local_path)
        
        url = remote_file.metadata.get('url', remote_file.path)
        
        for attempt in range(self.config.max_retries):
            try:
                # Ensure parent directory exists
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Stream download
                response = self._session.get(
                    url,
                    params=self.config.query_params or {},
                    timeout=self.config.timeout,
                    stream=True,
                )
                response.raise_for_status()
                
                # Write to file in chunks
                bytes_written = 0
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)
                
                result.success = True
                result.bytes_transferred = bytes_written
                return result
            
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                result.error = str(e)
                
                if attempt < self.config.max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        result.success = False
        return result


class THREDDSLoader(HTTPLoader):
    """
    Specialized loader for THREDDS Data Server catalogs.
    
    Parses THREDDS XML catalogs to discover available datasets.
    """
    
    def _list_from_directory(self) -> Iterator[RemoteFile]:
        """Parse THREDDS catalog XML."""
        catalog_url = self.config.base_url
        
        # Ensure we're requesting the catalog
        if not catalog_url.endswith('.xml'):
            catalog_url = urljoin(catalog_url, 'catalog.xml')
        
        try:
            response = self._session.get(catalog_url, timeout=self.config.timeout)
            response.raise_for_status()
            
            yield from self._parse_thredds_catalog(response.text, catalog_url)
        
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch THREDDS catalog: {e}")
    
    def _parse_thredds_catalog(self, xml_content: str, catalog_url: str) -> Iterator[RemoteFile]:
        """Parse THREDDS catalog XML for datasets."""
        import xml.etree.ElementTree as ET
        
        # THREDDS namespace
        ns = {'thredds': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
        
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            self.logger.error(f"Failed to parse THREDDS catalog: {e}")
            return
        
        # Find the base service URL for HTTP access
        http_service = None
        for service in root.findall('.//thredds:service', ns):
            service_type = service.get('serviceType', '')
            if service_type.lower() in ('httpserver', 'http'):
                http_service = service.get('base', '')
                break
        
        if not http_service:
            # Fall back to fileServer
            for service in root.findall('.//thredds:service', ns):
                if service.get('serviceType', '').lower() == 'fileserver':
                    http_service = service.get('base', '/thredds/fileServer/')
                    break
        
        # Find all datasets
        for dataset in root.findall('.//thredds:dataset', ns):
            url_path = dataset.get('urlPath')
            if not url_path:
                continue
            
            name = dataset.get('name', Path(url_path).name)
            
            # Build download URL
            base = urlparse(catalog_url)
            download_url = f"{base.scheme}://{base.netloc}{http_service}{url_path}"
            
            # Get size if available
            size = None
            size_elem = dataset.find('thredds:dataSize', ns)
            if size_elem is not None and size_elem.text:
                try:
                    units = size_elem.get('units', 'bytes')
                    size_val = float(size_elem.text)
                    if units.lower() == 'kbytes':
                        size = int(size_val * 1024)
                    elif units.lower() == 'mbytes':
                        size = int(size_val * 1024 * 1024)
                    else:
                        size = int(size_val)
                except ValueError:
                    pass
            
            # Get modification date if available
            modified = None
            date_elem = dataset.find('thredds:date', ns)
            if date_elem is not None and date_elem.text:
                try:
                    modified = datetime.fromisoformat(date_elem.text.replace('Z', '+00:00'))
                except ValueError:
                    pass
            
            yield RemoteFile(
                path=download_url,
                filename=name,
                size=size,
                modified=modified,
                metadata={'url': download_url, 'urlPath': url_path},
            )
