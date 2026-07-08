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

import os
from datetime import timedelta

from django.db import models
from django.utils import timezone as dj_timezone
from wagtail.snippets.models import register_snippet




@register_snippet
class FileIngestion(models.Model):
    """Per-file record of processing a single file from MinIO into STAC items and assets."""
    
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        PROCESSING = 'processing', 'Processing'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'
    
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
        default='',
        help_text="Worker ID that holds the lock",
    )
    retry_count = models.IntegerField(default=0)
    
    # =========================================================================
    # Result
    # =========================================================================
    
    completed_at = models.DateTimeField(null=True, blank=True)
    archive_path = models.CharField(max_length=500, blank=True, default='')
    items_created = models.IntegerField(default=0)
    assets_created = models.IntegerField(default=0)
    error = models.TextField(blank=True, default='')
    
    # =========================================================================
    # Collections M2M — populated after _resolve_collections() succeeds,
    # before per-collection processing begins. Authoritative record of which
    # collections a file touched or attempted to touch.
    # =========================================================================

    collections = models.ManyToManyField(
        'georivacore.Collection',
        blank=True,
        related_name='file_ingestions',
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
    
    force_reingest = models.BooleanField(
        default=False
    )

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
        unique_together = ['bucket', 'file_path']
        indexes = [
            models.Index(
                fields=['status', 'created_at'],
                name='idx_ingestion_status_created',
            ),
            models.Index(
                fields=['status', 'locked_at'],
                name='idx_ingestion_status_locked',
            ),
            models.Index(
                fields=['bucket', 'status'],
                name='idx_ingestion_bucket_status',
            ),
        ]
        ordering = ['-created_at']
    
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
    def register(cls, bucket: str, file_path: str, **kwargs) -> tuple['FileIngestion', bool]:
        """
        Register a file in the log. Returns (log, created).

        If the file is already registered, returns the existing record.
        Extra kwargs are passed to defaults (reference_time, etc).
        """
        defaults = {
            'status': cls.Status.PENDING,
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
            retry_count=models.F('retry_count') + 1,
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
            retry_count=models.F('retry_count') + 1,
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
            archive_path: str = '',
            items_created: int = 0,
            assets_created: int = 0,
            variables_discovered: int = None,
            valid_time_start=None,
            valid_time_end=None,
            timestep_count: int = None,
    ):
        """Mark a file as successfully processed."""
        cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
        ).update(
            status=cls.Status.COMPLETED,
            completed_at=dj_timezone.now(),
            archive_path=archive_path,
            items_created=items_created,
            assets_created=assets_created,
            error='',
            variables_discovered=variables_discovered,
            valid_time_start=valid_time_start,
            valid_time_end=valid_time_end,
            timestep_count=timestep_count,
        )
    
    @classmethod
    def mark_failed(cls, bucket: str, file_path: str, error: str):
        """Mark a file as failed. Releases the lock for future retry."""
        cls.objects.filter(
            bucket=bucket,
            file_path=file_path,
        ).update(
            status=cls.Status.FAILED,
            locked_at=None,
            locked_by='',
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
            locked_by='',
        )
    
    @classmethod
    def get_retryable(cls, limit: int = 50):
        """Get failed files that can be retried."""
        return cls.objects.filter(
            status=cls.Status.FAILED,
            retry_count__lt=cls.MAX_RETRIES,
        ).order_by('created_at')[:limit]
    
    @classmethod
    def get_permanently_failed(cls):
        """Get files that have exceeded max retries."""
        return cls.objects.filter(
            status=cls.Status.FAILED,
            retry_count__gte=cls.MAX_RETRIES,
        ).order_by('created_at')
    
    @classmethod
    def prune_old_records(cls, max_age_days: int = 30) -> dict:
        cutoff = dj_timezone.now() - timedelta(days=max_age_days)
        
        completed = cls.objects.filter(
            status=cls.Status.COMPLETED,
            completed_at__lt=cutoff,
            archive_path='',  # archive already cleaned up
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
            locked_by='',
            error='',
            completed_at=None,
            archive_path='',
            items_created=0,
            assets_created=0,
            force_reingest=False,
        )
        return updated > 0


# ---------------------------------------------------------------------------
# Acquisition tracking models
# ---------------------------------------------------------------------------

class UploadSession(models.Model):
    """Tracks a batch of files uploaded through the manual upload UI."""

    class Status(models.TextChoices):
        ACTIVE = 'active', 'Active'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'
        CANCELLED = 'cancelled', 'Cancelled'

    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE,
        related_name='upload_sessions',
    )
    user = models.ForeignKey(
        'auth.User',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='upload_sessions',
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.ACTIVE)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        app_label = 'georivaingestion'
        ordering = ['-started_at']

    def _finish(self, status):
        from django.utils import timezone
        self.status = status
        self.completed_at = timezone.now()
        self.save(update_fields=['status', 'completed_at'])

    def mark_failed(self):
        self._finish(self.Status.FAILED)

    def mark_cancelled(self):
        self._finish(self.Status.CANCELLED)

    def _check_auto_complete(self):
        """Auto-complete when all files have reached a terminal state."""
        if self.status != self.Status.ACTIVE:
            return
        terminal = {UploadedFile.Status.STORED, UploadedFile.Status.FAILED}
        files = list(self.uploaded_files.values_list('status', flat=True))
        if files and all(s in terminal for s in files):
            self._finish(self.Status.COMPLETED)


class UploadedFile(models.Model):
    """Per-file record within an UploadSession."""

    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        UPLOADING = 'uploading', 'Uploading'
        STORED = 'stored', 'Stored'
        FAILED = 'failed', 'Failed'

    session = models.ForeignKey(
        UploadSession,
        on_delete=models.CASCADE,
        related_name='uploaded_files',
    )
    original_filename = models.CharField(max_length=500)
    file_path = models.CharField(max_length=500, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    error = models.TextField(blank=True)
    bytes = models.BigIntegerField(default=0)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        app_label = 'georivaingestion'
        ordering = ['id']

    def mark_uploading(self):
        from django.utils import timezone
        self.status = self.Status.UPLOADING
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at'])

    def mark_stored(self, file_path='', bytes=0):
        from django.utils import timezone
        self.status = self.Status.STORED
        self.file_path = file_path
        self.bytes = bytes
        self.completed_at = timezone.now()
        self.save(update_fields=['status', 'file_path', 'bytes', 'completed_at'])
        self.session._check_auto_complete()

    def mark_failed(self, error=''):
        from django.utils import timezone
        self.status = self.Status.FAILED
        self.error = error
        self.completed_at = timezone.now()
        self.save(update_fields=['status', 'error', 'completed_at'])
        self.session._check_auto_complete()


# ---------------------------------------------------------------------------
# Manual upload configuration models
# ---------------------------------------------------------------------------

class ManualUploadConfig(models.Model):
    """Operator-created configuration enabling manual file uploads for a Catalog."""

    class ValidTimeFormat(models.TextChoices):
        YYYYMMDD     = 'YYYYMMDD',     'YYYYMMDD'
        DDMMYYYY     = 'DDMMYYYY',     'DDMMYYYY'
        YYYYMMDDHH   = 'YYYYMMDDHH',   'YYYYMMDDHH'
        YYYYMMDDHHMM = 'YYYYMMDDHHMM', 'YYYYMMDDHHMM'
        DDMMYY       = 'DDMMYY',       'DDMMYY'
        YYMMDD       = 'YYMMDD',       'YYMMDD'
        CONTENT      = 'CONTENT',      'From file content'

    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE,
        related_name='manual_upload_configs',
    )
    name = models.CharField(max_length=255)
    is_forecast = models.BooleanField(default=False)
    valid_time_format = models.CharField(max_length=20, choices=ValidTimeFormat.choices)

    class Meta:
        app_label = 'georivaingestion'
        constraints = [
            models.UniqueConstraint(
                fields=['catalog', 'name'],
                name='unique_manual_upload_config_name_per_catalog',
            ),
        ]

    def strptime_pattern(self) -> str | None:
        """Return the Python strptime pattern, or None for content-based formats."""
        from georiva.ingestion.time_extraction import _FORMAT_PATTERNS
        return _FORMAT_PATTERNS.get(self.valid_time_format)


class ManualUploadConfigVariable(models.Model):
    """Links a ManualUploadConfig to a Collection for one variable."""

    config = models.ForeignKey(
        ManualUploadConfig,
        on_delete=models.CASCADE,
        related_name='variables',
    )
    collection = models.ForeignKey(
        'georivacore.Collection',
        on_delete=models.CASCADE,
        related_name='manual_upload_variables',
    )
    variable_name = models.CharField(max_length=255)
    long_name = models.CharField(max_length=255, blank=True, default='')
    units = models.CharField(max_length=50, blank=True, default='')

    class Meta:
        app_label = 'georivaingestion'
        unique_together = [('config', 'variable_name')]


# ---------------------------------------------------------------------------
# Task-ferry Job models
# ---------------------------------------------------------------------------

from task_ferry.models import Job  # noqa: E402


@register_snippet
class FileIngestionJob(Job):
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
    """Per-run record for a Loader execution (data-source fetch + ingestion queue phase)."""

    data_feed = models.ForeignKey(
        'georivasources.DataFeed',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="loader_jobs",
    )
    collection = models.ForeignKey(
        'georivacore.Collection',
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

    class Meta:
        app_label = "georivaingestion"
