"""
GeoRiva Loader

Orchestrates data loading by using DataSource.
"""

import logging
import shutil
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from georiva.core.storage import storage
from georiva.sources.fetch.base import FailureScope, FetchResult, FileRequest


@dataclass
class CandidateFile:
    """One file the source offers right now, classified against storage —
    the unit of the read-only "check for new files" dry run (PRD #217)."""

    filename: str
    storage_path: str
    exists: bool


@dataclass
class LoaderRunResult:
    """Result of a complete loader run."""

    started_at: datetime = field(default_factory=timezone.now)
    finished_at: datetime | None = None

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
    run_time: datetime | None = None  # For forecasts: which model run

    # Why the run stopped before walking its plan, if it did (#478)
    stop_reason: str | None = None
    # Set when it stopped because the source cannot serve us: the feed's other
    # collections are then not attempted in the same tick.
    source_unavailable: str | None = None

    @property
    def success(self) -> bool:
        return self.files_failed == 0 and self.files_fetched > 0

    @property
    def partial_success(self) -> bool:
        return self.files_fetched > 0 and self.files_failed > 0

    @property
    def status(self) -> str:
        if self.stop_reason:
            return "failed"
        if self.files_queued > 0:
            return "queued"
        if self.files_fetched == 0 and self.files_failed == 0:
            return "empty"
        elif self.success:
            return "success"
        elif self.partial_success:
            return "partial"
        return "failed"

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
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "files_requested": self.files_requested,
            "files_fetched": self.files_fetched,
            "files_skipped": self.files_skipped,
            "files_failed": self.files_failed,
            "files_queued": self.files_queued,
            "bytes_transferred": self.bytes_transferred,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
            "run_time": self.run_time.isoformat() if self.run_time else None,
            "stop_reason": self.stop_reason,
            "summary": self.summary(),
        }


def _format_duration(seconds: float) -> str:
    """A time limit as people write it: 2h, 15m, 90s."""
    for unit, size in (("h", 3600), ("m", 60)):
        if seconds >= size and seconds % size == 0:
            return f"{int(seconds // size)}{unit}"
    return f"{seconds:g}s"


class _RunStopped(Exception):
    """Raised inside a run when the source cannot serve it: the run ends early,
    FAILED, for `reason` (#478)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def plan_from_earlier_run(earlier, collection):
    """The requests an earlier run failed, rebuilt from its stored plan, or None
    when the source must be asked again: the run is not for this collection, has
    no failed rows (plan generation failed, a short-circuited run), or has one
    whose request was never stored (#480)."""
    if earlier is None or collection is None or earlier.collection_id != collection.pk:
        return None
    from georiva.sources.models import FetchedFile

    failed = list(earlier.fetched_files.filter(status=FetchedFile.Status.FAILED))
    if not failed or any(not row.request_payload for row in failed):
        return None
    try:
        return [FileRequest.from_dict(row.request_payload) for row in failed]
    except Exception:
        return None


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
        on_file_fetched: Callable | None = None,  # Callback after each file
        resumed_from=None,  # FetchRun this run retries; its failed requests are the plan
        clock: Callable[[], float] = time.monotonic,  # Seam for the run time limit
    ):
        self.data_source = data_source
        self.fetch_strategy = self.data_source.fetch_strategy()
        self.collection = collection
        self.on_file_fetched = on_file_fetched
        self.data_feed = data_feed
        self.resumed_from = resumed_from
        self.clock = clock

        self.logger = logging.getLogger(f"georiva.loader.{data_source.name.replace(' ', '_').lower()}")
        self._temp_dir: str | None = None

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
        if feed is not None and collection is not None and collection_routes_to_staging(feed, collection.slug):
            return BucketType.STAGING
        return BucketType.SOURCES

    @property
    def _tier_bucket(self):
        return storage.bucket(self._tier_bucket_type)

    # =========================================================================
    # Main Run Method
    # =========================================================================

    @staticmethod
    def _request_payload(request):
        """JSON-safe request dict for FetchedFile.request_payload, or None if
        the source's request can't be serialized — a null payload disables
        per-file retry for that record instead of failing the run."""
        try:
            payload = request.to_dict()
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _check_stop(fetch_result, consecutive_transient: int, threshold: int) -> int:
        """The consecutive-TRANSIENT count after this failed fetch, or raise
        _RunStopped when the source has shown it cannot serve the run: on a
        SOURCE failure, or at `threshold` TRANSIENT failures in a row (#478)."""
        scope = fetch_result.failure_scope or FailureScope.TRANSIENT
        if scope == FailureScope.SOURCE:
            raise _RunStopped(f"source unavailable — {fetch_result.error}")
        if scope != FailureScope.TRANSIENT:
            return 0
        consecutive_transient += 1
        if consecutive_transient >= threshold:
            raise _RunStopped(f"{consecutive_transient} consecutive transient failures — {fetch_result.error}")
        return consecutive_transient

    def _one_request_per_path(self, requests) -> list:
        """The source's requests with any that would store to an already-planned
        path dropped. Two requests for one path would overwrite each other in
        storage anyway, and a run holds one FetchedFile per path (#474); keeping
        the first turns a source's slip into a warning instead of a failed run."""
        unique, seen = [], set()
        for request in requests:
            path = self._get_storage_path(request)
            if path in seen:
                self.logger.warning(f"Dropping duplicate request for {path} ({request.filename})")
                continue
            seen.add(path)
            unique.append(request)
        return unique

    def _create_plan_rows(self, fetch_run, requests):
        """One PENDING FetchedFile per request, in request order, in one insert.
        Returned in the same order as `requests`."""
        from georiva.sources.models import FetchedFile

        return FetchedFile.objects.bulk_create(
            [
                FetchedFile(
                    fetch_run=fetch_run,
                    file_path=self._get_storage_path(request),
                    request_payload=self._request_payload(request),
                )
                for request in requests
            ]
        )

    def check_new_files(self) -> list[CandidateFile]:
        """
        Read-only dry run (PRD #217): ask the source what it would fetch for
        this collection and classify each candidate against storage. Persists
        nothing — no FetchRun/FetchedFile, no counters — and never connects
        the fetch strategy.
        """
        requests = self.data_source.generate_requests_for_collection(self.collection)
        return [
            CandidateFile(
                filename=request.filename,
                storage_path=self._get_storage_path(request),
                exists=self._already_exists(request),
            )
            for request in requests
        ]

    def fetch_one(self, request) -> FetchResult:
        """
        Fetch and store a single file, unconditionally (no skip-existing
        shortcut) — the execution primitive behind per-file retry (PRD #217).
        Owns strategy connect/disconnect and temp cleanup; records nothing.
        """
        self.fetch_strategy.connect()
        try:
            return self._fetch_and_store(request)
        finally:
            try:
                self.fetch_strategy.disconnect()
            except Exception:
                self.logger.debug("Fetch strategy disconnect failed", exc_info=True)
            self._cleanup_temp()

    def record_not_attempted(self, reason: str) -> LoaderRunResult:
        """
        End this collection's run before it starts: a FetchRun FAILED at once
        with `reason`, and no plan rows. Nothing is asked of the source, not even
        the plan, which may itself call it — the feed short-circuit after a
        sibling collection's run found the source unavailable (#478).
        """
        from georiva.sources.models import FetchRun

        result = LoaderRunResult(stop_reason=reason)
        result.add_error(reason)
        result.finish()
        if self.data_feed:
            fetch_run = FetchRun.objects.create(
                data_feed=self.data_feed,
                collection=self.collection,
                status="running",
                resumed_from=self.resumed_from,
            )
            fetch_run.mark_failed(error=reason)
            self.data_feed._update_run_stats(result, self.collection, started_at=fetch_run.started_at)
        self.logger.info(f"Not attempted: {reason}")
        return result

    def run(
        self,
        *,
        dry_run: bool = False,
        max_files: int | None = None,
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
        from georiva.sources.models import FetchedFile, FetchRun

        result = LoaderRunResult()
        fetch_run = None
        stop_reason = None
        # The row being fetched right now; failed in `finally` if the run stops
        # under it. Tracked rather than swept by status: a queued result also
        # leaves its row FETCHING, and that is not a failure.
        in_flight = None
        consecutive_transient = 0
        breaker_threshold = settings.GEORIVA_FETCH_CIRCUIT_BREAKER_THRESHOLD
        run_max_seconds = settings.GEORIVA_FETCH_RUN_MAX_SECONDS
        run_started = self.clock()
        began_at = timezone.now()
        retry_after = None

        if self.data_feed:
            fetch_run = FetchRun.objects.create(
                data_feed=self.data_feed,
                collection=self.collection,
                status="running",
                resumed_from=self.resumed_from,
            )
            began_at = fetch_run.started_at

        try:
            self.logger.info(f"Starting loader run for {self.collection.name}")

            # Connect and generate requests from the data source. Either failing
            # means the source cannot serve this run (a plugin's run detection
            # failing on DNS, say), not that there was nothing to fetch.
            try:
                self.fetch_strategy.connect()
                self.logger.debug("Fetch strategy connected")
                generated = plan_from_earlier_run(self.resumed_from, self.collection)
                if generated is None:
                    generated = self.data_source.generate_requests_for_collection(self.collection)
            except Exception as e:
                self.logger.exception(f"Could not plan the run: {e}")
                raise _RunStopped(f"{type(e).__name__}: {e}") from e
            requests = self._one_request_per_path(generated)
            result.files_requested = len(requests)

            # Persist the request count up front — the finish-time summary never
            # comes if the worker dies, and the run page should be able to say
            # "7 of 320" for a run that is live or was interrupted.
            if fetch_run:
                fetch_run.files_requested = len(requests)
                fetch_run.save(update_fields=["files_requested"])

            if not requests:
                self.logger.warning("No file requests generated")
                result.finish()
                return result

            # Extract run time from first request (for forecasts)
            if requests[0].reference_time:
                result.run_time = requests[0].reference_time
                self.logger.info(f"Processing forecast run: {result.run_time.isoformat()}")

            self.logger.info(f"Generated {len(requests)} file requests")

            if dry_run:
                self.logger.info("Dry run - skipping fetch")
                for req in requests:
                    self.logger.debug(f"  Would fetch: {req.filename}")
                result.finish()
                return result

            # Persist the whole plan before anything is fetched: one PENDING row
            # per request, in request order, so the run page can show what is
            # left and a run that stops still says which requests it meant (#474).
            rows = self._create_plan_rows(fetch_run, requests) if fetch_run else [None] * len(requests)

            # Filter already-fetched files; copy from another collection if available
            to_fetch = []
            considered = 0
            for request, ff in zip(requests, rows, strict=True):
                considered += 1
                storage_path = self._get_storage_path(request)

                if skip_existing and self._already_exists(request):
                    result.files_skipped += 1
                    self.logger.debug(f"Skipping (exists): {request.filename}")
                    if ff:
                        ff.mark_skipped(reason="already exists")
                    continue

                if skip_existing:
                    existing_path = self._find_existing_catalog_path(request)
                    if existing_path:
                        dest_path = storage_path
                        # The record is FETCHING *before* the object lands: the copy raises
                        # a Bucket notification, and the consumer finds this file's chain by
                        # looking up its FetchedFile. A record written afterwards can lose
                        # that race and leave the ingestion without its fetch.
                        if ff:
                            ff.mark_fetching()
                            in_flight = ff
                        try:
                            self._tier_bucket.copy(existing_path, dest_path)
                            # Counted only once its row says so: a row that cannot be
                            # marked goes to the re-download, which counts it there.
                            if ff:
                                ff.mark_stored(bytes_transferred=0)
                            result.files_fetched += 1
                            result.stored_paths.append(dest_path)
                            self.logger.info(f"Copied (no re-download): {existing_path} → {dest_path}")
                        except Exception as e:
                            self.logger.warning(f"Copy failed, will re-download: {e}")
                            # The re-download below walks this same row again.
                            to_fetch.append((request, ff))
                        in_flight = None
                        continue

                to_fetch.append((request, ff))

                if max_files and len(to_fetch) >= max_files:
                    self.logger.info(f"Reached max_files limit ({max_files})")
                    break

            if fetch_run and considered < len(requests):
                # max_files cut the plan short: drop the rows the run will never
                # reach, so requested = fetched + skipped + failed still holds.
                FetchedFile.objects.filter(pk__in=[ff.pk for ff in rows[considered:]]).delete()
                result.files_requested = considered
                fetch_run.files_requested = considered
                fetch_run.save(update_fields=["files_requested"])

            self.logger.info(f"{len(to_fetch)} to fetch, {result.files_skipped} skipped")

            # Fetch files
            for i, (request, ff) in enumerate(to_fetch, 1):
                # Checked between files only: a file in progress has its own limit.
                if self.clock() - run_started >= run_max_seconds:
                    raise _RunStopped(f"run exceeded its time limit ({_format_duration(run_max_seconds)})")

                self.logger.info(f"[{i}/{len(to_fetch)}] Fetching {request.filename}")

                if ff:
                    ff.mark_fetching()
                    in_flight = ff

                fetch_result = self._fetch_and_store(request)
                result.fetch_results.append(fetch_result)

                if fetch_result.success:
                    # Mark the row first: if that raises, `finally` fails it as
                    # interrupted, and it must not also be counted as fetched.
                    if ff:
                        ff.mark_stored(bytes_transferred=fetch_result.bytes_transferred or 0)
                    in_flight = None
                    result.files_fetched += 1
                    result.bytes_transferred += fetch_result.bytes_transferred
                    result.stored_paths.append(self._get_storage_path(request))

                    # Callback
                    if self.on_file_fetched:
                        try:
                            self.on_file_fetched(request, fetch_result)
                        except Exception as e:
                            self.logger.warning(f"on_file_fetched callback error: {e}")
                    consecutive_transient = 0

                elif fetch_result.status == "queued":
                    result.files_queued += 1
                else:
                    result.files_failed += 1
                    if fetch_result.retry_after:
                        retry_after = max(retry_after or 0, fetch_result.retry_after)
                    if fetch_result.error:
                        result.add_error(f"{request.filename}: {fetch_result.error}")
                    if ff:
                        ff.mark_failed(error=fetch_result.error or "", scope=fetch_result.failure_scope)
                    # Settled before the check: a stop must not fail this row twice.
                    in_flight = None
                    consecutive_transient = self._check_stop(fetch_result, consecutive_transient, breaker_threshold)
                in_flight = None

        except _RunStopped as stop:
            self.logger.warning(f"Loader run stopped: {stop.reason}")
            result.add_error(stop.reason)
            stop_reason = result.source_unavailable = stop.reason

        except Exception as e:
            self.logger.exception(f"Loader run failed: {e}")
            result.add_error(str(e))
            stop_reason = f"{type(e).__name__}: {e}"

        finally:
            # Cleanup
            try:
                self.fetch_strategy.disconnect()
            except Exception as e:
                self.logger.warning(f"Error disconnecting: {e}")

            self._cleanup_temp()
            result.stop_reason = stop_reason
            result.finish()

            if fetch_run:
                # No row stays Waiting once its run has ended, nor Fetching under a
                # run that stopped: a request the run never reached, or was in the
                # middle of, is a failure, and retryable like any other.
                reason = stop_reason or "the run ended"
                if in_flight:
                    in_flight.mark_failed(
                        error=f"interrupted: the run stopped while fetching ({reason})",
                        scope=FailureScope.TRANSIENT,
                    )
                    result.files_failed += 1
                result.files_failed += fetch_run.fetched_files.filter(status=FetchedFile.Status.PENDING).update(
                    status=FetchedFile.Status.FAILED,
                    error=f"not attempted: the run stopped ({reason})",
                    failure_scope=FailureScope.NOT_ATTEMPTED,
                    completed_at=timezone.now(),
                )
                counters = {
                    "files_fetched": result.files_fetched,
                    "files_skipped": result.files_skipped,
                    "files_failed": result.files_failed,
                    "bytes_transferred": result.bytes_transferred,
                }
                # A run that walked its whole plan completed, however many files
                # failed; one that stopped before the end failed (#478).
                if stop_reason:
                    fetch_run.mark_failed(
                        error=f"{FetchRun.STOPPED_PREFIX}{stop_reason}", retry_after=retry_after, **counters
                    )
                else:
                    fetch_run.mark_completed(retry_after=retry_after, **counters)

            if self.data_feed:
                self.data_feed._update_run_stats(result, self.collection, started_at=began_at)

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
        from georiva.core.storage.filename import build_filename
        from georiva.ingestion.models import FileIngestion

        filename = build_filename(
            original_filename=request.filename,
            reference_time=request.reference_time,
        )
        catalog_prefix = self.collection.catalog.storage_prefix
        collection_slug = self.collection.slug

        # ── 1. FileIngestion check ─────────────────────────────────────────────
        log_path = (
            FileIngestion.objects.filter(
                bucket=self._tier_bucket_type,
                file_path__startswith=f"{catalog_prefix}/",
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
            for link in self.data_feed.collection_links.select_related("collection__catalog__organisation").exclude(
                collection=self.collection
            ):
                sibling = link.collection
                candidate = f"{sibling.catalog.storage_prefix}/{sibling.slug}/{filename}"
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
            if fetch_result.status == "queued":
                self.logger.info(f"Request queued: {request.filename}")
                return fetch_result

            if not fetch_result.success:
                return fetch_result

            # Validate downloaded file
            if not self._validate_file(temp_path, request):
                return fetch_result.fail("File validation failed", FailureScope.FILE)

            # post process file
            processed_path, new_filename = self.data_source.post_process_fetched_file(request, temp_path)
            filename_to_store = new_filename or request.filename
            request.filename = filename_to_store

            storage_path = self._get_storage_path(request)

            # Store in permanent location. Our side refusing the file fails every
            # file after it too, so it is the source's scope, not this file's.
            try:
                self._store_file(processed_path, storage_path)
            except Exception as e:
                self.logger.exception(f"Failed to store {storage_path}")
                return fetch_result.fail(f"Storage write failed: {e}", FailureScope.SOURCE)

            self.logger.debug(f"Stored: {storage_path}")

            fetch_result.duration_seconds = time.time() - start_time

        except Exception as e:
            self.logger.exception(f"Failed to fetch {request.filename}")
            fetch_result = FetchResult(
                request=request,
                success=False,
                error=str(e),
                status="failed",
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
            self.logger.warning(f"Size mismatch: expected {request.expected_size}, got {size}")
            # Don't fail on size mismatch, just warn

        return True

    # =========================================================================
    # Storage Operations
    # =========================================================================
    def _get_storage_path(self, request) -> str:
        """
        Build storage path in georiva-sources bucket.

        Path: {org}/{catalog}/{collection}/{filename}

        If request has reference_time, filename gets GR-- prefix.

        The org segment comes from the DataFeed's catalog chain
        (collection → catalog → organisation), never from anything a source
        plugin or a remote server hands us: a crafted filename must not be able
        to land one institution's data under another's prefix.
        """
        from georiva.core.storage.filename import build_filename

        filename = build_filename(
            original_filename=request.filename,
            reference_time=request.reference_time,
        )

        # request.reference_time exists  → GR--20250115T0600--gfs_025.grib2
        # request.reference_time is None → sentinel2_ndvi.tif

        return f"{self.collection.catalog.storage_prefix}/{self.collection.slug}/{filename}"

    def _store_file(self, local_path: Path, storage_path: str):
        """Store file in permanent storage for this feed's target tier."""
        with open(local_path, "rb") as f:
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
