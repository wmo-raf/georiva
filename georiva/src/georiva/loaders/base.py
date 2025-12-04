"""
GeoRiva Base Loader

Abstract base class for all data loaders. Defines the interface
and common utilities for fetching files from external sources.
"""

import hashlib
import logging
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

from django.utils import timezone


@dataclass
class RemoteFile:
    """Represents a file discovered on a remote source."""
    path: str  # Full remote path
    filename: str  # Just the filename
    size: Optional[int] = None  # Size in bytes (if available)
    modified: Optional[datetime] = None  # Last modified time
    checksum: Optional[str] = None  # MD5/SHA hash if available
    metadata: dict = field(default_factory=dict)
    
    @property
    def extension(self) -> str:
        return Path(self.filename).suffix.lower()


@dataclass
class FetchResult:
    """Result of fetching a single file."""
    remote_file: RemoteFile
    local_path: Optional[Path] = None  # Where file was downloaded
    success: bool = False
    error: Optional[str] = None
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    
    @property
    def failed(self) -> bool:
        return not self.success


@dataclass
class LoaderRunResult:
    """Result of a complete loader run."""
    started_at: datetime = field(default_factory=timezone.now)
    finished_at: Optional[datetime] = None
    files_found: int = 0
    files_fetched: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    bytes_transferred: int = 0
    errors: list = field(default_factory=list)
    fetch_results: list = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return self.files_failed == 0 and self.files_fetched > 0
    
    @property
    def partial_success(self) -> bool:
        return self.files_fetched > 0 and self.files_failed > 0
    
    @property
    def status(self) -> str:
        if self.files_fetched == 0 and self.files_failed == 0:
            return 'empty'  # Nothing to fetch
        elif self.success:
            return 'success'
        elif self.partial_success:
            return 'partial'
        else:
            return 'failed'
    
    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0
    
    def finish(self):
        self.finished_at = timezone.now()


class BaseLoader(ABC):
    """
    Abstract base class for all GeoRiva loaders.
    
    A loader is responsible for:
    1. Connecting to a remote data source
    2. Discovering available files
    3. Filtering files based on patterns and dates
    4. Downloading files to local/S3 storage
    5. Triggering ingestion pipeline
    
    Subclasses implement source-specific connection and transfer logic.
    """
    
    def __init__(self, config, collection):
        """
        Initialize loader with its configuration.
        
        Args:
            config: LoaderConfig model instance (FTPLoaderConfig, etc.)
        """
        self.config = config
        self.collection = collection
        self.logger = logging.getLogger(f"georiva.loaders.{self.__class__.__name__}")
        self._temp_dir = None
    
    # =========================================================================
    # Abstract Methods - Must be implemented by subclasses
    # =========================================================================
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the remote source.
        Raises exception on failure.
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection."""
        pass
    
    @abstractmethod
    def list_files(self) -> Iterator[RemoteFile]:
        """
        List available files on the remote source.
        
        Should apply configured path and pattern filters.
        Yields RemoteFile objects for each discovered file.
        """
        pass
    
    @abstractmethod
    def fetch_file(self, remote_file: RemoteFile, local_path: Path) -> FetchResult:
        """
        Download a single file from the remote source.
        
        Args:
            remote_file: The file to download
            local_path: Where to save locally
            
        Returns:
            FetchResult with success/failure info
        """
        pass
    
    # =========================================================================
    # Optional Override Methods
    # =========================================================================
    
    def should_fetch(self, remote_file: RemoteFile) -> bool:
        """
        Determine if a file should be fetched.
        
        Default implementation checks if file already exists in the system.
        Override for custom skip logic.
        """
        # Check if we already have this file
        from georiva.core.storage import storage_manager
        
        incoming_path = self._get_incoming_path(remote_file)
        if storage_manager.exists(incoming_path):
            self.logger.debug(f"Skipping {remote_file.filename} - already exists")
            return False
        
        # Check by checksum if available
        if remote_file.checksum:
            # Could check against processed files metadata
            pass
        
        return True
    
    def validate_file(self, local_path: Path, remote_file: RemoteFile) -> bool:
        """
        Validate a downloaded file.
        
        Default checks file size matches if known.
        Override for format-specific validation.
        """
        if not local_path.exists():
            return False
        
        local_size = local_path.stat().st_size
        
        # Size check
        if remote_file.size is not None and local_size != remote_file.size:
            self.logger.warning(
                f"Size mismatch for {remote_file.filename}: "
                f"expected {remote_file.size}, got {local_size}"
            )
            return False
        
        # Checksum check
        if remote_file.checksum:
            local_checksum = self._compute_checksum(local_path)
            if local_checksum != remote_file.checksum:
                self.logger.warning(f"Checksum mismatch for {remote_file.filename}")
                return False
        
        return True
    
    # =========================================================================
    # Main Run Method
    # =========================================================================
    
    def run(
            self,
            dry_run: bool = False,
            max_files: Optional[int] = None,
            since: Optional[datetime] = None,
    ) -> LoaderRunResult:
        """
        Execute a complete loader run.
        
        Args:
            dry_run: If True, discover files but don't download
            max_files: Maximum number of files to fetch (for testing)
            since: Only fetch files modified after this time
            
        Returns:
            LoaderRunResult with statistics and results
        """
        result = LoaderRunResult()
        
        try:
            self.logger.info(f"Starting loader run for {self.collection}")
            
            # Connect
            self.connect()
            
            # Discover files
            files_to_fetch = []
            for remote_file in self.list_files():
                result.files_found += 1
                
                # Date filter
                if since and remote_file.modified and remote_file.modified < since:
                    result.files_skipped += 1
                    continue
                
                # Check if should fetch
                if not self.should_fetch(remote_file):
                    result.files_skipped += 1
                    continue
                
                files_to_fetch.append(remote_file)
                
                # Limit check
                if max_files and len(files_to_fetch) >= max_files:
                    break
            
            self.logger.info(
                f"Found {result.files_found} files, "
                f"{len(files_to_fetch)} to fetch, "
                f"{result.files_skipped} skipped"
            )
            
            if dry_run:
                result.finish()
                return result
            
            # Fetch files
            for remote_file in files_to_fetch:
                fetch_result = self._fetch_and_store(remote_file)
                result.fetch_results.append(fetch_result)
                
                if fetch_result.success:
                    result.files_fetched += 1
                    result.bytes_transferred += fetch_result.bytes_transferred
                else:
                    result.files_failed += 1
                    result.errors.append(fetch_result.error)
        
        except Exception as e:
            self.logger.exception(f"Loader run failed: {e}")
            result.errors.append(str(e))
        
        finally:
            try:
                self.disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting: {e}")
            
            self._cleanup_temp()
            result.finish()
            
            # Record run in config
            self.config.record_run(
                status=result.status,
                message='; '.join(result.errors[:3]) if result.errors else '',
                files_fetched=result.files_fetched,
            )
        
        return result
    
    # =========================================================================
    # Internal Methods
    # =========================================================================
    
    def _fetch_and_store(self, remote_file: RemoteFile) -> FetchResult:
        """Fetch a file and store it in the incoming directory."""
        import time
        
        start_time = time.time()
        
        # Create temp file for download
        temp_path = self._get_temp_path(remote_file.filename)
        
        try:
            # Download
            fetch_result = self.fetch_file(remote_file, temp_path)
            
            if not fetch_result.success:
                return fetch_result
            
            # Validate
            if not self.validate_file(temp_path, remote_file):
                fetch_result.success = False
                fetch_result.error = "Validation failed"
                return fetch_result
            
            # Move to incoming storage
            incoming_path = self._get_incoming_path(remote_file)
            self._store_file(temp_path, incoming_path)
            
            print(incoming_path, "INCOMING PATH")
            
            # Trigger ingestion (async)
            self._trigger_ingestion(incoming_path)
            
            fetch_result.success = True
            fetch_result.bytes_transferred = temp_path.stat().st_size
            fetch_result.duration_seconds = time.time() - start_time
            
            self.logger.info(f"Fetched {remote_file.filename} -> {incoming_path}")
        
        except Exception as e:
            self.logger.exception(f"Failed to fetch {remote_file.filename}")
            fetch_result = FetchResult(
                remote_file=remote_file,
                success=False,
                error=str(e),
            )
        
        finally:
            # Clean up temp file
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
        
        return fetch_result
    
    def _get_incoming_path(self, remote_file: RemoteFile) -> str:
        """Get the storage path for an incoming file."""
        return f"incoming/{self.collection.slug}/{remote_file.filename}"
    
    def _get_temp_path(self, filename: str) -> Path:
        """Get a temporary file path for download."""
        if self._temp_dir is None:
            self._temp_dir = tempfile.mkdtemp(prefix="georiva_loader_")
        return Path(self._temp_dir) / filename
    
    def _cleanup_temp(self):
        """Clean up temporary directory."""
        import shutil
        if self._temp_dir and Path(self._temp_dir).exists():
            try:
                shutil.rmtree(self._temp_dir)
            except Exception as e:
                self.logger.warning(f"Failed to clean temp dir: {e}")
            self._temp_dir = None
    
    def _store_file(self, local_path: Path, storage_path: str):
        """Store a file in GeoRiva storage."""
        from georiva.core.storage import storage_manager
        
        with open(local_path, 'rb') as f:
            storage_manager.save(storage_path, f)
    
    def _trigger_ingestion(self, file_path: str):
        """Trigger async ingestion of a file."""
        from georiva.core.tasks import process_incoming_file_task
        
        process_incoming_file_task.delay(file_path, self.collection.id)
    
    def _compute_checksum(self, file_path: Path, algorithm: str = 'md5') -> str:
        """Compute file checksum."""
        hash_func = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    
    # =========================================================================
    # Context Manager Support
    # =========================================================================
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        self._cleanup_temp()
        return False
