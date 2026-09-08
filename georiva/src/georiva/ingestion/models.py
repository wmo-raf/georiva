"""
GeoRiva Ingestion Log

Tracks every file that enters the system through any bucket.
Provides atomic locking to prevent duplicate processing and
handles crash recovery through stale lock detection.

Lifecycle:
    pending → processing (locked) → completed
                                  → failed → processing (retry) → ...

Crash recovery:
    processing (stale lock) → reclaimed by sweep/retry → processing → ...

Max retries exceeded:
    failed (retry_count >= max_retries) → manual intervention required
"""

import logging
import os
from datetime import timedelta

from django.db import models
from django.utils import timezone as dj_timezone
from wagtail.snippets.models import register_snippet

from georiva.organisations.lookups import NOT_ORM_SCOPABLE

logger = logging.getLogger(__name__)


@register_snippet
class FileIngestion(models.Model):
    """Per-file record of processing a single file from MinIO into STAC items and assets."""

    # Written before its collections are known, so there is no FK chain to an
    # organisation to follow. Its owner is the first segment of ``file_path`` —
    # the org slug, by the storage grammar — and the one listing that shows these
    # (the ingestion SSE snapshot) filters on that prefix directly.
    ORGANISATION_LOOKUP = NOT_ORM_SCOPABLE

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PROCESSING = "processing", "Processing"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"

    # =========================================================================
    # Identity — unique per file
    # =========================================================================

    bucket = models.CharField(
        max_length=50,
        help_text="Origin bucket type: 'incoming' or 'sources'",
    )
    file_path = models.CharField(
        max_length=500,
        help_text="Path relative to bucket root: {catalog}/{collection}/{filename}",
    )

    # =========================================================================
    # State
    # =========================================================================

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )

    # Lock management
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="Worker ID that holds the lock",
    )
    retry_count = models.IntegerField(default=0)

    # =========================================================================
    # Result
    # =========================================================================

    completed_at = models.DateTimeField(null=True, blank=True)
    archive_path = models.CharField(max_length=500, blank=True, default="")
    items_created = models.IntegerField(default=0)
    assets_created = models.IntegerField(default=0)
    error = models.TextField(blank=True, default="")

    # =========================================================================
    # Collections M2M — populated after _resolve_collections() succeeds,
    # before per-collection processing begins. Authoritative record of which
    # collections a file touched or attempted to touch.
    # =========================================================================

    collections = models.ManyToManyField(
        "georivacore.Collection",
        blank=True,
        related_name="file_ingestions",
    )

    # =========================================================================
    # Metadata
    # =========================================================================

    reference_time = models.DateTimeField(null=True, blank=True)
    file_size = models.BigIntegerField(null=True, blank=True)

    # =========================================================================
    # Timestamps
    # =========================================================================

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    force_reingest = models.BooleanField(default=False)

    # Processing summary — populated after ingestion completes
    variables_discovered = models.IntegerField(null=True)
    valid_time_start = models.DateTimeField(null=True)
    valid_time_end = models.DateTimeField(null=True)
    timestep_count = models.IntegerField(null=True)

    # =========================================================================
    # Configuration
    # =========================================================================

    MAX_RETRIES = 3
    LOCK_TIMEOUT = timedelta(minutes=30)

    class Meta:
        unique_together = ["bucket", "file_path"]
        indexes = [
            models.Index(
                fields=["status", "created_at"],
                name="idx_ingestion_status_created",
            ),
            models.Index(
                fields=["status", "locked_at"],
                name="idx_ingestion_status_locked",
            ),
            models.Index(
                fields=["bucket", "status"],
                name="idx_ingestion_bucket_status",
            ),
        ]
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.bucket}:{self.file_path} [{self.status}]"

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_stale(self) -> bool:
        """A lock is stale if processing exceeds LOCK_TIMEOUT."""
        if self.status != self.Status.PROCESSING:
            return False
        if not self.locked_at:
            return True
        return dj_timezone.now() - self.locked_at > self.LOCK_TIMEOUT

    @property
    def can_retry(self) -> bool:
        """Check if file hasn't exceeded max retries."""
        return self.retry_count < self.MAX_RETRIES

    @property
    def duration_seconds(self) -> float | None:
        """Processing duration if completed."""
        if self.completed_at and self.locked_at:
            return (self.completed_at - self.locked_at).total_seconds()
        return None

    # =========================================================================
    # Lock acquisition
    # =========================================================================

    @classmethod
    def register(cls, bucket: str, file_path: str, **kwargs) -> tuple["FileIngestion", bool]:
        """
        Register a file in the log. Returns (log, created).

        If the file is already registered, returns the existing record.
        Extra kwargs are passed to defaults (reference_time, etc).
        """
        defaults = {
            "status": cls.Status.PENDING,
        }
        defaults.update(kwargs)

        return cls.objects.get_or_create(
            bucket=bucket,
            file_path=file_path,
            defaults=defaults,
        )

    @classmethod
    def acquire(cls, bucket: str, file_path: str, worker_id: str = None) -> bool:
        """
        Atomically acquire a processing lock.

        Returns True if lock was acquired, False if file is already
        being processed or has completed.

        Handles:
            1. Pending files → lock
            2. Failed files under retry limit → lock and retry
            3. Stale processing locks → reclaim
        """
        if worker_id is None:
            worker_id = f"worker-{os.getpid()}"

        now = dj_timezone.now()
        stale_cutoff = now - cls.LOCK_TIMEOUT

        # Case 1 & 2: pending or retryable failed
        updated = cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
            status__in=[cls.Status.PENDING, cls.Status.FAILED],
            retry_count__lt=cls.MAX_RETRIES,
        ).update(
            status=cls.Status.PROCESSING,
            locked_at=now,
            locked_by=worker_id,
            retry_count=models.F("retry_count") + 1,
        )

        if updated:
            return True

        # Case 3: stale lock — worker probably crashed
        updated = cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
            status=cls.Status.PROCESSING,
            locked_at__lt=stale_cutoff,
            retry_count__lt=cls.MAX_RETRIES,
        ).update(
            locked_at=now,
            locked_by=worker_id,
            retry_count=models.F("retry_count") + 1,
        )

        return updated > 0

    # =========================================================================
    # State transitions
    # =========================================================================

    @classmethod
    def mark_completed(
        cls,
        bucket: str,
        file_path: str,
        archive_path: str = "",
        items_created: int = 0,
        assets_created: int = 0,
        variables_discovered: int = None,
        valid_time_start=None,
        valid_time_end=None,
        timestep_count: int = None,
    ):
        """Mark a file as successfully processed.

        The sole writer of COMPLETED, which is why the model-run hook hangs here
        rather than off a ``post_save`` receiver: this is a bulk ``update()`` and
        emits no ``post_save`` at all.
        """
        completed_at = dj_timezone.now()
        cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
        ).update(
            status=cls.Status.COMPLETED,
            completed_at=completed_at,
            archive_path=archive_path,
            items_created=items_created,
            assets_created=assets_created,
            error="",
            variables_discovered=variables_discovered,
            valid_time_start=valid_time_start,
            valid_time_end=valid_time_end,
            timestep_count=timestep_count,
        )
        cls._record_run_arrival(bucket, file_path, completed_at)

    @classmethod
    def _record_run_arrival(cls, bucket: str, file_path: str, completed_at):
        """Open (or reopen) the ``RunIngestion`` this file belongs to (ADR 0026).

        Runs after the row is COMPLETED so ``RunIngestion.file_count``, which
        counts completed files, already includes this one.

        Only forecast collections have run boundaries, and the collections M2M is
        written during ingestion — before this point, and only once resolution
        succeeded. A file that failed resolution has no collections and opens
        nothing. Never allowed to fail the ingestion it is bookkeeping for.
        """
        try:
            log = (
                cls.objects.filter(bucket=bucket, file_path=file_path)
                .prefetch_related("collections")
                .only("id", "reference_time")
                .first()
            )
            if log is None or log.reference_time is None:
                return
            from georiva.ingestion.run_closers import (
                close_if_declared_set_complete,
                close_if_upload_batches_complete,
            )

            for collection in log.collections.all():
                if not collection.is_forecast:
                    continue
                run, _reopened = RunIngestion.record_file(collection, log.reference_time, completed_at)
                if run is None:
                    continue
                # Both evidence-based closers are checked on arrival, not on a
                # timer: the last file to arrive is the only moment either
                # condition becomes true, and either is a stronger claim than the
                # quiet period that would otherwise close the run 20 minutes
                # later. Neither fires when its route did not produce this run —
                # both return False on no evidence.
                if not close_if_declared_set_complete(run):
                    close_if_upload_batches_complete(run)
        except Exception:
            logger.exception("RunIngestion bookkeeping failed for %s:%s", bucket, file_path)

    @classmethod
    def mark_failed(cls, bucket: str, file_path: str, error: str):
        """Mark a file as failed. Releases the lock for future retry."""
        cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
        ).update(
            status=cls.Status.FAILED,
            locked_at=None,
            locked_by="",
            error=error[:2000],
        )

    # =========================================================================
    # Queries
    # =========================================================================

    @classmethod
    def is_known(cls, bucket: str, file_path: str) -> bool:
        """Check if a file is already registered (any status)."""
        return cls.objects.filter(bucket=bucket, file_path=file_path).exists()

    @classmethod
    def is_done(cls, bucket: str, file_path: str) -> bool:
        """Check if a file has been successfully processed."""
        return cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
            status=cls.Status.COMPLETED,
        ).exists()

    @classmethod
    def reset_stale_locks(cls) -> int:
        """
        Reset locks that have exceeded LOCK_TIMEOUT.

        Returns the number of locks reset.
        """
        stale_cutoff = dj_timezone.now() - cls.LOCK_TIMEOUT

        return cls.objects.filter(
            status=cls.Status.PROCESSING,
            locked_at__lt=stale_cutoff,
        ).update(
            status=cls.Status.PENDING,
            locked_at=None,
            locked_by="",
        )

    @classmethod
    def get_retryable(cls, limit: int = 50):
        """Get failed files that can be retried."""
        return cls.objects.filter(
            status=cls.Status.FAILED,
            retry_count__lt=cls.MAX_RETRIES,
        ).order_by("created_at")[:limit]

    @classmethod
    def get_permanently_failed(cls):
        """Get files that have exceeded max retries."""
        return cls.objects.filter(
            status=cls.Status.FAILED,
            retry_count__gte=cls.MAX_RETRIES,
        ).order_by("created_at")

    @classmethod
    def prune_old_records(cls, max_age_days: int = 30) -> dict:
        cutoff = dj_timezone.now() - timedelta(days=max_age_days)

        completed = cls.objects.filter(
            status=cls.Status.COMPLETED,
            completed_at__lt=cutoff,
            archive_path="",  # archive already cleaned up
        ).delete()

        permanently_failed = cls.objects.filter(
            status=cls.Status.FAILED,
            retry_count__gte=cls.MAX_RETRIES,
            updated_at__lt=cutoff,
        ).delete()

        return {
            "completed_pruned": completed[0],
            "failed_pruned": permanently_failed[0],
        }

    @property
    def has_live_data(self) -> bool:
        """
        True if Item records exist that were produced from this file.
        A completed log with no live items means data was lost — re-ingest.
        """
        from georiva.core.models import Item

        source_file = f"{self.bucket}:{self.file_path}"
        return Item.objects.filter(source_file=source_file).exists()

    @classmethod
    def reset_for_reingest(cls, bucket: str, file_path: str) -> bool:
        updated = cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
            status__in=[cls.Status.COMPLETED, cls.Status.FAILED],
        ).update(
            status=cls.Status.PENDING,
            retry_count=0,
            locked_at=None,
            locked_by="",
            error="",
            completed_at=None,
            archive_path="",
            items_created=0,
            assets_created=0,
            force_reingest=False,
        )
        return updated > 0


# ---------------------------------------------------------------------------
# Model-run tracking
# ---------------------------------------------------------------------------


@register_snippet
class RunIngestion(models.Model):
    """One model run arriving into one collection.

    The sibling of ``FileIngestion``: that is one *file* arriving, this is one
    *model run* arriving — a single ``(collection, reference_time)`` pair, the
    unit operators and downstream consumers actually reason about ("has the
    00Z run landed for ecmwf-ifs-surface?").

    Only forecast collections have run boundaries, so a row exists only where
    ``reference_time`` is non-null.

    Lifecycle::

        open ──(closer fires)──> closed ──(a later file arrives)──> open
                                              revision += 1

    It is **reopenable on purpose.** Of the three arrival routes only the
    DataFeed one can name the run's full expected set up front; the other two
    close on evidence that a *batch* finished, which is not the same claim. So
    every closer closes optimistically and any later arrival reopens the row and
    bumps ``revision`` — which makes a consumer's "has it closed" an
    optimisation rather than a correctness guarantee, and gives a republishing
    consumer a monotonically increasing number to key on.
    """

    ORGANISATION_LOOKUP = "collection__catalog__organisation"

    class Status(models.TextChoices):
        OPEN = "open", "Open"
        CLOSED = "closed", "Closed"

    class Closer(models.TextChoices):
        """Which signal closed the run — recorded because the three differ in
        how much they actually prove, and an operator reading a closed row
        should be able to tell which claim they are looking at."""

        DECLARED_SET = "declared_set", "Declared expected set"
        UPLOAD_SESSION = "upload_session", "Upload session completed"
        QUIET_PERIOD = "quiet_period", "Quiet period"
        MANUAL = "manual", "Closed by an operator"

    # =========================================================================
    # Identity
    # =========================================================================

    collection = models.ForeignKey(
        "georivacore.Collection",
        on_delete=models.CASCADE,
        related_name="run_ingestions",
    )
    reference_time = models.DateTimeField(
        help_text="The model run this row tracks. Never null: a collection without "
        "reference times has no run boundary to track.",
    )

    # =========================================================================
    # State
    # =========================================================================

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.OPEN,
        db_index=True,
    )
    revision = models.PositiveIntegerField(
        default=0,
        help_text="Bumped every time a closed run reopens. Monotonic per row.",
    )
    closed_by = models.CharField(
        max_length=20,
        choices=Closer.choices,
        blank=True,
        default="",
    )

    # =========================================================================
    # Evidence
    # =========================================================================

    file_count = models.IntegerField(
        default=0,
        help_text="Distinct files that have completed into this collection for this reference time.",
    )
    expected_file_count = models.IntegerField(
        null=True,
        blank=True,
        help_text="Size of the declared expected set, when the arrival route declares one. Null on routes that cannot.",
    )
    last_file_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the most recent file completed into this run. The clock the quiet-period closer reads.",
    )

    # =========================================================================
    # Timestamps
    # =========================================================================

    opened_at = models.DateTimeField(auto_now_add=True)
    closed_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ["collection", "reference_time"]
        indexes = [
            models.Index(
                fields=["status", "last_file_at"],
                name="idx_runingestion_status_last",
            ),
            models.Index(
                fields=["collection", "-reference_time"],
                name="idx_runingestion_col_reftime",
            ),
        ]
        ordering = ["-reference_time"]
        verbose_name = "Run ingestion"

    def __str__(self):
        stamp = self.reference_time.strftime("%Y-%m-%dT%H:%MZ")
        return f"{self.collection.slug} @ {stamp} [{self.status}]"

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_open(self) -> bool:
        return self.status == self.Status.OPEN

    @property
    def version(self) -> int:
        """``ref_epoch_seconds * 100 + revision`` — a single sortable integer that
        orders runs by model time first and republishes of the same run second.

        Consumers that reload only on a strictly greater version need both
        halves: without the revision a corrected republish of the same run reuses
        the same number and is ignored; with the revision leading, a backfilled
        older run would outrank a newer one.
        """
        return int(self.reference_time.timestamp()) * 100 + self.revision

    # =========================================================================
    # Transitions
    # =========================================================================

    @classmethod
    def record_file(cls, collection, reference_time, completed_at=None):
        """Register that one file completed into ``(collection, reference_time)``.

        Opens the row if it is the first, reopens it (bumping ``revision``) if it
        arrives after a close. Returns ``(run, reopened)``.

        ``file_count`` is recomputed from ``FileIngestion`` rather than
        incremented, so a re-ingest of a file already counted does not inflate it.
        """
        from django.db import transaction

        if reference_time is None:
            return None, False

        completed_at = completed_at or dj_timezone.now()

        with transaction.atomic():
            run, created = cls.objects.select_for_update().get_or_create(
                collection=collection,
                reference_time=reference_time,
                defaults={"last_file_at": completed_at},
            )
            reopened = False
            if not created and run.status == cls.Status.CLOSED:
                run.status = cls.Status.OPEN
                run.revision += 1
                run.closed_at = None
                run.closed_by = ""
                reopened = True

            run.file_count = cls._completed_file_count(collection, reference_time)
            if run.last_file_at is None or completed_at > run.last_file_at:
                run.last_file_at = completed_at
            run.save(
                update_fields=[
                    "status",
                    "revision",
                    "closed_at",
                    "closed_by",
                    "file_count",
                    "last_file_at",
                    "updated_at",
                ]
            )

        if reopened:
            _emit_run_reopened(run)
        return run, reopened

    def close(self, closer) -> bool:
        """Close this run. Returns False if it was already closed.

        The conditional UPDATE is what makes concurrent closers safe: three
        closers can race on one row, and only the first one to land emits the
        signal.
        """
        updated = (
            type(self)
            .objects.filter(pk=self.pk, status=self.Status.OPEN)
            .update(
                status=self.Status.CLOSED,
                closed_at=dj_timezone.now(),
                closed_by=closer,
                updated_at=dj_timezone.now(),
            )
        )
        if not updated:
            return False
        self.refresh_from_db()
        _emit_run_closed(self)
        return True

    # =========================================================================
    # Queries
    # =========================================================================

    @classmethod
    def _completed_file_count(cls, collection, reference_time) -> int:
        return (
            FileIngestion.objects.filter(
                collections=collection,
                reference_time=reference_time,
                status=FileIngestion.Status.COMPLETED,
            )
            .distinct()
            .count()
        )

    @classmethod
    def latest_closed(cls, collection):
        """The most recent closed run for a collection, or None."""
        return cls.objects.filter(collection=collection, status=cls.Status.CLOSED).order_by("-reference_time").first()


def _emit_run_closed(run):
    from georiva.ingestion.domain_signals import run_ingestion_closed

    run_ingestion_closed.send(sender=RunIngestion, run=run)


def _emit_run_reopened(run):
    from georiva.ingestion.domain_signals import run_ingestion_reopened

    run_ingestion_reopened.send(sender=RunIngestion, run=run)


# ---------------------------------------------------------------------------
# Acquisition tracking models
# ---------------------------------------------------------------------------


class UploadSession(models.Model):
    """Tracks a batch of files uploaded through the manual upload UI."""

    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    ORGANISATION_LOOKUP = "catalog__organisation"

    catalog = models.ForeignKey(
        "georivacore.Catalog",
        on_delete=models.CASCADE,
        related_name="upload_sessions",
    )
    user = models.ForeignKey(
        "auth.User",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="upload_sessions",
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.ACTIVE)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        app_label = "georivaingestion"
        ordering = ["-started_at"]

    def _finish(self, status):
        from django.utils import timezone

        self.status = status
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "completed_at"])

    def mark_failed(self):
        self._finish(self.Status.FAILED)

    def mark_cancelled(self):
        self._finish(self.Status.CANCELLED)

    def _check_auto_complete(self):
        """Auto-complete when all files have reached a terminal state."""
        if self.status != self.Status.ACTIVE:
            return
        terminal = {UploadedFile.Status.STORED, UploadedFile.Status.FAILED}
        files = list(self.uploaded_files.values_list("status", flat=True))
        if files and all(s in terminal for s in files):
            self._finish(self.Status.COMPLETED)


class UploadedFile(models.Model):
    """Per-file record within an UploadSession."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        UPLOADING = "uploading", "Uploading"
        STORED = "stored", "Stored"
        FAILED = "failed", "Failed"

    ORGANISATION_LOOKUP = "session__catalog__organisation"

    session = models.ForeignKey(
        UploadSession,
        on_delete=models.CASCADE,
        related_name="uploaded_files",
    )
    original_filename = models.CharField(max_length=500)
    file_path = models.CharField(max_length=500, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    error = models.TextField(blank=True)
    bytes = models.BigIntegerField(default=0)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        app_label = "georivaingestion"
        ordering = ["id"]

    def mark_uploading(self):
        from django.utils import timezone

        self.status = self.Status.UPLOADING
        self.started_at = timezone.now()
        self.save(update_fields=["status", "started_at"])

    def mark_stored(self, file_path="", bytes=0):
        from django.utils import timezone

        self.status = self.Status.STORED
        self.file_path = file_path
        self.bytes = bytes
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "file_path", "bytes", "completed_at"])
        self.session._check_auto_complete()

    def mark_failed(self, error=""):
        from django.utils import timezone

        self.status = self.Status.FAILED
        self.error = error
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "error", "completed_at"])
        self.session._check_auto_complete()


# ---------------------------------------------------------------------------
# Manual upload configuration models
# ---------------------------------------------------------------------------


class ManualUploadConfig(models.Model):
    """Operator-created configuration enabling manual file uploads for a Catalog."""

    class ValidTimeFormat(models.TextChoices):
        YYYYMMDD = "YYYYMMDD", "YYYYMMDD"
        DDMMYYYY = "DDMMYYYY", "DDMMYYYY"
        YYYYMMDDHH = "YYYYMMDDHH", "YYYYMMDDHH"
        YYYYMMDDHHMM = "YYYYMMDDHHMM", "YYYYMMDDHHMM"
        DDMMYY = "DDMMYY", "DDMMYY"
        YYMMDD = "YYMMDD", "YYMMDD"
        CONTENT = "CONTENT", "From file content"

    ORGANISATION_LOOKUP = "catalog__organisation"

    catalog = models.ForeignKey(
        "georivacore.Catalog",
        on_delete=models.CASCADE,
        related_name="manual_upload_configs",
    )
    name = models.CharField(max_length=255)
    is_forecast = models.BooleanField(default=False)
    valid_time_format = models.CharField(max_length=20, choices=ValidTimeFormat.choices)

    class Meta:
        app_label = "georivaingestion"
        constraints = [
            models.UniqueConstraint(
                fields=["catalog", "name"],
                name="unique_manual_upload_config_name_per_catalog",
            ),
        ]

    def strptime_pattern(self) -> str | None:
        """Return the Python strptime pattern, or None for content-based formats."""
        from georiva.ingestion.time_extraction import _FORMAT_PATTERNS

        return _FORMAT_PATTERNS.get(self.valid_time_format)


class ManualUploadConfigVariable(models.Model):
    """Links a ManualUploadConfig to a Collection for one variable."""

    ORGANISATION_LOOKUP = "config__catalog__organisation"

    config = models.ForeignKey(
        ManualUploadConfig,
        on_delete=models.CASCADE,
        related_name="variables",
    )
    collection = models.ForeignKey(
        "georivacore.Collection",
        on_delete=models.CASCADE,
        related_name="manual_upload_variables",
    )
    variable_name = models.CharField(max_length=255)
    long_name = models.CharField(max_length=255, blank=True, default="")
    units = models.CharField(max_length=50, blank=True, default="")

    class Meta:
        app_label = "georivaingestion"
        unique_together = [("config", "variable_name")]


# ---------------------------------------------------------------------------
# Task-ferry Job models
# ---------------------------------------------------------------------------

from task_ferry.models import Job  # noqa: E402


@register_snippet
class FileIngestionJob(Job):
    # Same storage-key ownership as the FileIngestion it tracks.
    ORGANISATION_LOOKUP = NOT_ORM_SCOPABLE

    """
    Operator-visible record for a single file ingestion run.

    Lifecycle: pending → started → finished / failed / cancelled
    Progress and state are readable in real-time via GET /api/jobs/<id>/

    The companion FileIngestion handles distributed locking and retry logic
    independently. One FileIngestionJob is created per process_incoming_file
    invocation; when the run succeeds the FileIngestion FK is populated.
    """

    # Machine-written job/telemetry record — kept out of Wagtail's reference
    # index (see core/test_reference_index_exclusion.py).
    wagtail_reference_index_ignore = True

    file_path = models.CharField(
        max_length=500,
        help_text="Path relative to bucket root.",
    )
    bucket = models.CharField(
        max_length=50,
        help_text="Origin bucket: 'incoming' or 'sources'.",
    )
    # ForeignKey, not OneToOne: retries and re-ingests create a new job per
    # process_incoming_file invocation, all pointing at the same lock record.
    file_ingestion = models.ForeignKey(
        FileIngestion,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="jobs",
        help_text="Lock record for this file; set after the lock is acquired.",
    )
    items_created = models.IntegerField(default=0)
    assets_created = models.IntegerField(default=0)

    class Meta:
        app_label = "georivaingestion"


class LoaderJob(Job):
    ORGANISATION_LOOKUP = NOT_ORM_SCOPABLE

    """Per-run record for a Loader execution (data-source fetch + ingestion queue phase)."""

    data_feed = models.ForeignKey(
        "georivasources.DataFeed",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="loader_jobs",
    )
    collection = models.ForeignKey(
        "georivacore.Collection",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="loader_jobs",
    )

    files_total = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)

    resume_of_run = models.ForeignKey(
        "georivasources.FetchRun",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="resume_jobs",
        help_text="Set on jobs the stale-run sweep enqueued to resume an "
        "interrupted FetchRun; threads resume lineage to the new run.",
    )

    class Meta:
        app_label = "georivaingestion"
