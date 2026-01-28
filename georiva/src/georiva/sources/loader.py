"""
GeoRiva Loader

Orchestrates data loading by combining DataSource and FetchStrategy.
"""

import logging
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from django.utils import timezone


@dataclass
class LoaderRunResult:
    """Result of a complete loader run."""
    started_at: datetime = field(default_factory=timezone.now)
    finished_at: Optional[datetime] = None
    
    # Counts
    files_requested: int = 0
    files_fetched: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    files_queued: int = 0  # For async sources (CDS API)
    bytes_transferred: int = 0
    
    # Details
    errors: list[str] = field(default_factory=list)
    fetch_results: list = field(default_factory=list)
    
    # Context
    run_time: Optional[datetime] = None  # For forecasts: which model run
    
    @property
    def success(self) -> bool:
        return self.files_failed == 0 and self.files_fetched > 0
    
    @property
    def partial_success(self) -> bool:
        return self.files_fetched > 0 and self.files_failed > 0
    
    @property
    def status(self) -> str:
        if self.files_queued > 0:
            return 'queued'
        if self.files_fetched == 0 and self.files_failed == 0:
            return 'empty'
        elif self.success:
            return 'success'
        elif self.partial_success:
            return 'partial'
        return 'failed'
    
    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0
    
    def finish(self):
        self.finished_at = timezone.now()
    
    def add_error(self, error: str):
        self.errors.append(error)
        # Keep only last 50 errors
        if len(self.errors) > 50:
            self.errors = self.errors[-50:]
    
    def summary(self) -> str:
        return (
            f"LoaderRun[{self.status}]: "
            f"{self.files_fetched} fetched, {self.files_skipped} skipped, "
            f"{self.files_failed} failed, {self.bytes_transferred / 1024 / 1024:.1f} MB "
            f"in {self.duration_seconds:.1f}s"
        )


class Loader:
    """
    Orchestrates data loading by combining DataSource and FetchStrategy.
    
    Usage:
        loader = Loader(
            data_source=ECMWFAIFSDataSource(config),
            fetch_strategy=HTTPFetchStrategy(http_config),
            collection=my_collection,
        )
        
        result = loader.run()
    """
    
    def __init__(
            self,
            data_source,  # DataSource protocol
            fetch_strategy,  # FetchStrategy protocol
            collection,  # GeoRiva Collection model
            *,
            storage_backend=None,  # Optional custom storage
            on_file_fetched: Optional[Callable] = None,  # Callback after each file
    ):
        self.data_source = data_source
        self.fetch_strategy = fetch_strategy
        self.collection = collection
        self.storage_backend = storage_backend
        self.on_file_fetched = on_file_fetched
        
        self.logger = logging.getLogger(
            f"georiva.loader.{data_source.name.replace(' ', '_').lower()}"
        )
        self._temp_dir: Optional[str] = None
    
    # =========================================================================
    # Main Run Method
    # =========================================================================
    
    def run(
            self,
            *,
            variables: Optional[list[str]] = None,
            dry_run: bool = False,
            max_files: Optional[int] = None,
            skip_existing: bool = True,
            trigger_ingestion: bool = True,
    ) -> LoaderRunResult:
        """
        Execute a loader run.
        
        Args:
            variables: Specific variables to fetch (default: all configured)
            dry_run: If True, generate requests but don't fetch
            max_files: Maximum files to fetch (useful for testing)
            skip_existing: Skip files already in storage (default: True)
            trigger_ingestion: Trigger async ingestion after fetch (default: True)
            
        Returns:
            LoaderRunResult with statistics and details
        """
        result = LoaderRunResult()
        
        try:
            self.logger.info(f"Starting loader run for {self.collection}")
            
            # Connect fetch strategy
            self.fetch_strategy.connect()
            self.logger.debug("Fetch strategy connected")
            
            # Generate requests from data source
            requests = list(self.data_source.generate_requests(variables=variables))
            result.files_requested = len(requests)
            
            if not requests:
                self.logger.warning("No file requests generated")
                result.finish()
                return result
            
            # Extract run time from first request (for forecasts)
            if requests[0].reference_time:
                result.run_time = requests[0].reference_time
                self.logger.info(
                    f"Processing forecast run: {result.run_time.isoformat()}"
                )
            
            self.logger.info(f"Generated {len(requests)} file requests")
            
            if dry_run:
                self.logger.info("Dry run - skipping fetch")
                for req in requests:
                    self.logger.debug(f"  Would fetch: {req.filename}")
                result.finish()
                return result
            
            # Filter already-fetched files
            requests_to_fetch = []
            for request in requests:
                if skip_existing and self._already_exists(request):
                    result.files_skipped += 1
                    self.logger.debug(f"Skipping (exists): {request.filename}")
                    continue
                
                requests_to_fetch.append(request)
                
                if max_files and len(requests_to_fetch) >= max_files:
                    self.logger.info(f"Reached max_files limit ({max_files})")
                    break
            
            self.logger.info(
                f"{len(requests_to_fetch)} to fetch, {result.files_skipped} skipped"
            )
            
            # Fetch files
            for i, request in enumerate(requests_to_fetch, 1):
                self.logger.info(
                    f"[{i}/{len(requests_to_fetch)}] Fetching {request.filename}"
                )
                
                fetch_result = self._fetch_and_store(request, trigger_ingestion)
                result.fetch_results.append(fetch_result)
                
                if fetch_result.success:
                    result.files_fetched += 1
                    result.bytes_transferred += fetch_result.bytes_transferred
                    
                    # Callback
                    if self.on_file_fetched:
                        try:
                            self.on_file_fetched(request, fetch_result)
                        except Exception as e:
                            self.logger.warning(f"on_file_fetched callback error: {e}")
                
                elif fetch_result.status == 'queued':
                    result.files_queued += 1
                else:
                    result.files_failed += 1
                    if fetch_result.error:
                        result.add_error(f"{request.filename}: {fetch_result.error}")
        
        except Exception as e:
            self.logger.exception(f"Loader run failed: {e}")
            result.add_error(str(e))
        
        finally:
            # Cleanup
            try:
                self.fetch_strategy.disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting: {e}")
            
            self._cleanup_temp()
            result.finish()
            
            self.logger.info(result.summary())
        
        return result
    
    # =========================================================================
    # File Operations
    # =========================================================================
    
    def _already_exists(self, request) -> bool:
        """Check if file already exists in storage."""
        storage_path = self._get_storage_path(request)
        return self._storage_exists(storage_path)
    
    def _fetch_and_store(self, request, trigger_ingestion: bool):
        """Fetch a file and store it."""
        from .base import FetchResult
        
        start_time = time.time()
        temp_path = self._get_temp_path(request.filename)
        
        try:
            # Fetch to temp location
            fetch_result = self.fetch_strategy.fetch(request, temp_path)
            
            # Handle async/queued results
            if fetch_result.status == 'queued':
                self.logger.info(f"Request queued: {request.filename}")
                return fetch_result
            
            if not fetch_result.success:
                return fetch_result
            
            # Validate downloaded file
            if not self._validate_file(temp_path, request):
                fetch_result.success = False
                fetch_result.error = "File validation failed"
                return fetch_result
            
            # Store in permanent location
            storage_path = self._get_storage_path(request)
            self._store_file(temp_path, storage_path)
            
            self.logger.debug(f"Stored: {storage_path}")
            
            # Trigger ingestion pipeline
            if trigger_ingestion:
                self._trigger_ingestion(storage_path, request)
            
            fetch_result.duration_seconds = time.time() - start_time
        
        except Exception as e:
            self.logger.exception(f"Failed to fetch {request.filename}")
            fetch_result = FetchResult(
                request=request,
                success=False,
                error=str(e),
                status='failed',
            )
        
        finally:
            # Clean up temp file
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
        
        return fetch_result
    
    def _validate_file(self, local_path: Path, request) -> bool:
        """Validate downloaded file."""
        if not local_path.exists():
            self.logger.error(f"File does not exist: {local_path}")
            return False
        
        size = local_path.stat().st_size
        
        # Check minimum size (GRIB files should be at least a few KB)
        if size < 1000:
            self.logger.error(f"File too small ({size} bytes): {local_path}")
            return False
        
        # Check expected size if provided
        if request.expected_size and abs(size - request.expected_size) > 100:
            self.logger.warning(
                f"Size mismatch: expected {request.expected_size}, got {size}"
            )
            # Don't fail on size mismatch, just warn
        
        # Format-specific validation
        if request.expected_format == 'grib':
            return self._validate_grib(local_path)
        
        return True
    
    def _validate_grib(self, path: Path) -> bool:
        """Basic GRIB file validation."""
        try:
            with open(path, 'rb') as f:
                magic = f.read(4)
                # GRIB files start with 'GRIB'
                if magic != b'GRIB':
                    self.logger.error(f"Invalid GRIB magic bytes: {magic}")
                    return False
            return True
        except Exception as e:
            self.logger.error(f"GRIB validation error: {e}")
            return False
    
    # =========================================================================
    # Storage Operations
    # =========================================================================
    
    def _get_storage_path(self, request) -> str:
        """Get the storage path for a file."""
        # Organize by collection and reference time
        if request.reference_time:
            # Forecast: organize by run date
            date_part = request.reference_time.strftime('%Y/%m/%d')
            return f"incoming/{self.collection.slug}/{date_part}/{request.filename}"
        else:
            # Observation/analysis: organize by valid time
            date_part = request.valid_time.strftime('%Y/%m/%d')
            return f"incoming/{self.collection.slug}/{date_part}/{request.filename}"
    
    def _storage_exists(self, path: str) -> bool:
        """Check if path exists in storage."""
        if self.storage_backend:
            return self.storage_backend.exists(path)
        
        # Default: use Django storage
        try:
            from django.core.files.storage import default_storage
            return default_storage.exists(path)
        except Exception:
            # Fallback to filesystem
            return Path(path).exists()
    
    def _store_file(self, local_path: Path, storage_path: str):
        """Store file in permanent storage."""
        if self.storage_backend:
            with open(local_path, 'rb') as f:
                self.storage_backend.save(storage_path, f)
            return
        
        # Default: use Django storage
        try:
            from django.core.files.storage import default_storage
            with open(local_path, 'rb') as f:
                default_storage.save(storage_path, f)
        except Exception:
            # Fallback: copy to filesystem
            dest = Path(storage_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
    
    # =========================================================================
    # Ingestion Trigger
    # =========================================================================
    
    def _trigger_ingestion(self, file_path: str, request):
        """Trigger async ingestion of the downloaded file."""
        try:
            from georiva.core.tasks import process_incoming_file
            
            # Build metadata from request
            metadata = {
                'valid_time': request.valid_time.isoformat() if request.valid_time else None,
                'reference_time': request.reference_time.isoformat() if request.reference_time else None,
                'variables': request.variables,
                'source': request.params.get('source'),
                'model': request.params.get('model'),
                'forecast_hour': request.params.get('step_hours'),
            }
            
            # Trigger async task
            process_incoming_file.delay(
                file_path=file_path,
                collection_id=self.collection.id,
                metadata=metadata,
            )
            
            self.logger.debug(f"Ingestion triggered for {file_path}")
        
        except ImportError:
            self.logger.warning("Celery tasks not available, skipping async ingestion")
        except Exception as e:
            self.logger.error(f"Failed to trigger ingestion: {e}")
    
    # =========================================================================
    # Temp Directory Management
    # =========================================================================
    
    def _get_temp_path(self, filename: str) -> Path:
        """Get a temporary file path."""
        if self._temp_dir is None:
            self._temp_dir = tempfile.mkdtemp(prefix="georiva_loader_")
        return Path(self._temp_dir) / filename
    
    def _cleanup_temp(self):
        """Clean up temporary directory."""
        if self._temp_dir and Path(self._temp_dir).exists():
            try:
                shutil.rmtree(self._temp_dir)
            except Exception as e:
                self.logger.warning(f"Failed to clean temp dir: {e}")
            self._temp_dir = None
    
    # =========================================================================
    # Context Manager Support
    # =========================================================================
    
    def __enter__(self):
        self.fetch_strategy.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fetch_strategy.disconnect()
        self._cleanup_temp()
        return False
