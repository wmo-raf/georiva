"""
GeoRiva Loader

Orchestrates data loading by using DataSource.
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

from georiva.core.storage import storage
from georiva.sources.fetch.base import FetchResult

from django.conf import settings


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
    stored_paths: list[str] = field(default_factory=list)  # storage paths of successfully fetched files
    
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
            f"LoaderRunResult[{self.status}]: "
            f"{self.files_fetched} fetched, {self.files_skipped} skipped, "
            f"{self.files_failed} failed, {self.bytes_transferred / 1024 / 1024:.1f} MB "
            f"in {self.duration_seconds:.1f}s"
        )
    
    def to_dict(self) -> dict:
        return {
            'status': self.status,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'files_requested': self.files_requested,
            'files_fetched': self.files_fetched,
            'files_skipped': self.files_skipped,
            'files_failed': self.files_failed,
            'files_queued': self.files_queued,
            'bytes_transferred': self.bytes_transferred,
            'duration_seconds': self.duration_seconds,
            'errors': self.errors,
            'run_time': self.run_time.isoformat() if self.run_time else None,
            'summary': self.summary(),
        }


class Loader:
    """
    Orchestrates data loading by combining DataSource and FetchStrategy.
    
    Usage:
        loader = Loader(
            data_source=ECMWFAIFSDataSource(config),
            collection=my_collection,
        )
        
        result = loader.run()
    """
    
    def __init__(
            self,
            data_source,  # DataSource protocol
            collection,  # GeoRiva collection model
            *,
            data_feed=None,
            on_file_fetched: Optional[Callable] = None,  # Callback after each file
    ):
        self.data_source = data_source
        self.fetch_strategy = self.data_source.fetch_strategy()
        self.collection = collection
        self.on_file_fetched = on_file_fetched
        self.data_feed = data_feed
        
        self.logger = logging.getLogger(
            f"georiva.loader.{data_source.name.replace(' ', '_').lower()}"
        )
        self._temp_dir: Optional[str] = None

    # =========================================================================
    # Target tier routing
    # =========================================================================
    @property
    def _tier_bucket_type(self) -> str:
        """
        Storage bucket for this collection's auto-derived tier (ADR-0008).

        A collection lands in the STAGING bucket — held as a raw input for
        derivation, not auto-materialized — iff some enabled DerivedProduct of
        this feed consumes it at the staging tier. Otherwise it lands in SOURCES
        (the published path). Tier is computed from the product declarations, not
        a stored field, so "publish vs products" can no longer drift.
        """
        from georiva.core.storage import BucketType
        from georiva.sources.derivation_invocation import collection_routes_to_staging

        feed = self.data_feed
        collection = self.collection
        if (
            feed is not None and collection is not None
            and collection_routes_to_staging(feed, collection.slug)
        ):
            return BucketType.STAGING
        return BucketType.SOURCES

    @property
    def _tier_bucket(self):
        return storage.bucket(self._tier_bucket_type)

    # =========================================================================
    # Main Run Method
    # =========================================================================
    
    def run(
            self,
            *,
            dry_run: bool = False,
            max_files: Optional[int] = None,
            skip_existing: bool = True,
    ) -> LoaderRunResult:
        """
        Execute a loader run.
        
        Args:o
            dry_run: If True, generate requests but don't fetch
            max_files: Maximum files to fetch (useful for testing)
            skip_existing: Skip files already in storage (default: True)
            
        Returns:
            LoaderRunResult with statistics and details
        """
        from georiva.sources.models import FetchRun, FetchedFile

        result = LoaderRunResult()
        fetch_run = None

        if self.data_feed:
            fetch_run = FetchRun.objects.create(data_feed=self.data_feed, status='running')

        try:
            self.logger.info(f"Starting loader run for {self.collection.name}")

            # Connect fetch strategy
            self.fetch_strategy.connect()
            self.logger.debug("Fetch strategy connected")

            # Generate requests from data source
            requests = list(self.data_source.generate_requests_for_collection(self.collection))
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

            # Filter already-fetched files; copy from another collection if available
            requests_to_fetch = []
            for request in requests:
                storage_path = self._get_storage_path(request)

                if skip_existing and self._already_exists(request):
                    result.files_skipped += 1
                    self.logger.debug(f"Skipping (exists): {request.filename}")
                    if fetch_run:
                        ff = FetchedFile.objects.create(
                            fetch_run=fetch_run, file_path=storage_path)
                        ff.mark_skipped(reason="already exists")
                    continue

                if skip_existing:
                    existing_path = self._find_existing_catalog_path(request)
                    if existing_path:
                        dest_path = storage_path
                        try:
                            self._tier_bucket.copy(existing_path, dest_path)
                            result.files_fetched += 1
                            result.stored_paths.append(dest_path)
                            self.logger.info(
                                f"Copied (no re-download): {existing_path} → {dest_path}"
                            )
                            if fetch_run:
                                ff = FetchedFile.objects.create(
                                    fetch_run=fetch_run, file_path=dest_path)
                                ff.mark_fetching()
                                ff.mark_stored(bytes_transferred=0)
                        except Exception as e:
                            self.logger.warning(
                                f"Copy failed, will re-download: {e}"
                            )
                            requests_to_fetch.append(request)
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

                ff = None
                if fetch_run:
                    ff = FetchedFile.objects.create(
                        fetch_run=fetch_run,
                        file_path=self._get_storage_path(request),
                    )
                    ff.mark_fetching()

                fetch_result = self._fetch_and_store(request)
                result.fetch_results.append(fetch_result)

                if fetch_result.success:
                    result.files_fetched += 1
                    result.bytes_transferred += fetch_result.bytes_transferred
                    result.stored_paths.append(self._get_storage_path(request))
                    if ff:
                        ff.mark_stored(bytes_transferred=fetch_result.bytes_transferred or 0)

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
                    if ff:
                        ff.mark_failed(error=fetch_result.error or "")

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

            if fetch_run:
                fetch_run.mark_completed(
                    files_fetched=result.files_fetched,
                    files_skipped=result.files_skipped,
                    files_failed=result.files_failed,
                    bytes_transferred=result.bytes_transferred,
                )

            if self.data_feed:
                self.data_feed._update_run_stats(result, self.collection)

            self.logger.info(result.summary())

        return result
    
    # =========================================================================
    # File Operations
    # =========================================================================
    
    def _already_exists(self, request) -> bool:
        """Check if file already exists in storage for this collection."""
        storage_path = self._get_storage_path(request)
        return self._tier_bucket.exists(storage_path)
    
    def _find_existing_catalog_path(self, request) -> str | None:
        """
        Return the storage path of this file if it was already downloaded
        for another collection in the same DataFeed.

        Strategy:
        1. FileIngestion query (fast, no storage I/O) — only PENDING/PROCESSING
           because COMPLETED means the source file was already deleted by
           SourceFileManager.cleanup().
        2. Direct storage check on sibling collection paths — catches the case
           where the file exists in MinIO but has no FileIngestion entry
           (dropped event, manual upload, consumer restart, etc.).
        """
        from georiva.core.filename import build_filename
        from georiva.core.storage import BucketType
        from georiva.ingestion.models import FileIngestion

        filename = build_filename(
            original_filename=request.filename,
            reference_time=request.reference_time,
        )
        catalog_slug = self.collection.catalog.slug
        collection_slug = self.collection.slug

        # ── 1. FileIngestion check ─────────────────────────────────────────────
        log_path = (
            FileIngestion.objects
            .filter(
                bucket=self._tier_bucket_type,
                file_path__startswith=f"{catalog_slug}/",
                file_path__endswith=f"/{filename}",
                status__in=[
                    FileIngestion.Status.PENDING,
                    FileIngestion.Status.PROCESSING,
                ],
            )
            .exclude(file_path__contains=f"/{collection_slug}/")
            .values_list("file_path", flat=True)
            .first()
        )
        if log_path:
            return log_path

        # ── 2. Direct storage check on sibling collections ────────────────────
        # Handles files that exist in MinIO but have no FileIngestion entry
        # (dropped event, manual upload, consumer restart, etc.).
        if self.data_feed:
            for link in self.data_feed.collection_links.select_related('collection__catalog').exclude(
                collection=self.collection
            ):
                sibling = link.collection
                candidate = f"{sibling.catalog.slug}/{sibling.slug}/{filename}"
                if self._tier_bucket.exists(candidate):
                    return candidate

        return None
    
    def _fetch_and_store(self, request):
        """Fetch a file and store it."""
        
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
            
            # post process file
            processed_path, new_filename = self.data_source.post_process_fetched_file(request, temp_path)
            filename_to_store = new_filename or request.filename
            request.filename = filename_to_store
            
            storage_path = self._get_storage_path(request)
            
            # Store in permanent location
            self._store_file(processed_path, storage_path)
            
            self.logger.debug(f"Stored: {storage_path}")
            
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
        
        # Check minimum size (files should be at least a few KB)
        if size < 1000:
            self.logger.error(f"File too small ({size} bytes): {local_path}")
            return False
        
        # Check expected size if provided
        if request.expected_size and abs(size - request.expected_size) > 100:
            self.logger.warning(
                f"Size mismatch: expected {request.expected_size}, got {size}"
            )
            # Don't fail on size mismatch, just warn
        
        return True
    
    # =========================================================================
    # Storage Operations
    # =========================================================================
    def _get_storage_path(self, request) -> str:
        """
        Build storage path in georiva-sources bucket.
    
        Path: {catalog}/{collection}/{filename}
        
        If request has reference_time, filename gets GR-- prefix.
        """
        from georiva.core.filename import build_filename
        
        filename = build_filename(
            original_filename=request.filename,
            reference_time=request.reference_time,
        )
        
        # request.reference_time exists  → GR--20250115T0600--gfs_025.grib2
        # request.reference_time is None → sentinel2_ndvi.tif
        
        return f"{self.collection.catalog.slug}/{self.collection.slug}/{filename}"
    
    def _store_file(self, local_path: Path, storage_path: str):
        """Store file in permanent storage for this feed's target tier."""
        with open(local_path, 'rb') as f:
            self._tier_bucket.save(storage_path, f)
    
    # =========================================================================
    # Temp Directory Management
    # =========================================================================
    
    def _get_temp_path(self, filename: str) -> Path:
        """Get a temporary file path."""
        if self._temp_dir is None:
            self._temp_dir = tempfile.mkdtemp(prefix="georiva_loader_", dir=settings.GEORIVA_TEMP_DIR)
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
