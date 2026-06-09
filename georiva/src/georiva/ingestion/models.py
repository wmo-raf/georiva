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


class DataArrival(models.Model):
    """Top-level record for every batch of data entering MinIO, regardless of trigger."""

    class Trigger(models.TextChoices):
        SCHEDULED = 'scheduled', 'Scheduled'
        MANUAL_UPLOAD = 'manual_upload', 'Manual Upload'

    class Status(models.TextChoices):
        UPLOADING = 'uploading', 'Uploading'
        PENDING = 'pending', 'Pending'
        PROCESSING = 'processing', 'Processing'
        COMPLETED = 'completed', 'Completed'
        PARTIAL = 'partial', 'Partial'
        FAILED = 'failed', 'Failed'
        EMPTY = 'empty', 'Empty'

    trigger = models.CharField(max_length=20, choices=Trigger.choices)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING
    )
    file_path = models.CharField(max_length=500, blank=True, default='')

    data_feed = models.ForeignKey(
        'georivasources.DataFeed',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='arrivals',
    )
    collection = models.ForeignKey(
        'georivacore.Collection',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='data_arrivals',
    )

    error_message = models.TextField(blank=True, default='')

    files_requested = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    files_queued = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)

    started_at = models.DateTimeField(default=dj_timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'georivaingestion'
        ordering = ['-created_at']

    @property
    def duration_seconds(self):
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @classmethod
    def find_or_create(cls, file_path: str, trigger: str, **kwargs) -> tuple['DataArrival', bool]:
        """
        Find an existing DataArrival by file_path or create a new one.

        Matches by file_path so admin uploads (pre-created with status=uploading)
        are found when the bucket event fires, preventing duplicate records.
        Returns (arrival, created).
        """
        if file_path:
            existing = cls.objects.filter(file_path=file_path).first()
            if existing:
                return existing, False

        arrival = cls.objects.create(
            file_path=file_path,
            trigger=trigger,
            **kwargs,
        )
        return arrival, True


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
    # Metadata
    # =========================================================================
    
    catalog_slug = models.CharField(max_length=100, blank=True, default='')
    collection_slug = models.CharField(max_length=100, blank=True, null=True, default='')
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
    
    # db_constraint=False: TimescaleDB does not support FK constraints pointing
    # to hypertables. Django still handles on_delete=CASCADE at the ORM level.
    item = models.ForeignKey(
        'georivacore.Item',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name='file_ingestions',
        db_constraint=False,
    )

    data_arrival = models.ForeignKey(
        DataArrival,
        on_delete=models.CASCADE,
        related_name='file_ingestions',
    )
    
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
        Extra kwargs are passed to defaults (catalog_slug, reference_time, etc).
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
        
        qs = Item.objects.filter(
            collection__catalog__slug=self.catalog_slug,
            source_file__contains=source_file,
        )
        
        if self.collection_slug:
            qs = qs.filter(collection__slug=self.collection_slug)
        
        return qs.exists()
    
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

    def strptime_pattern(self) -> str:
        """Return the Python strptime pattern for the configured valid_time_format."""
        from georiva.ingestion.time_extraction import _FORMAT_PATTERNS
        return _FORMAT_PATTERNS[self.valid_time_format]


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

    file_path = models.CharField(
        max_length=500,
        help_text="Path relative to bucket root.",
    )
    bucket = models.CharField(
        max_length=50,
        help_text="Origin bucket: 'incoming' or 'sources'.",
    )
    file_ingestion = models.OneToOneField(
        FileIngestion,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="job",
        help_text="Lock record for this file; set after the lock is acquired.",
    )
    items_created = models.IntegerField(default=0)
    assets_created = models.IntegerField(default=0)

    class Meta:
        app_label = "georivaingestion"


class DataArrivalJob(Job):
    """
    Operator-visible record for a single DataArrival run (fetch + queue phase).

    Progress is available in real-time via GET /api/jobs/<id>/
    """

    data_feed = models.ForeignKey(
        'georivasources.DataFeed',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="arrival_jobs",
    )
    collection = models.ForeignKey(
        'georivacore.Collection',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="data_arrival_jobs",
    )
    data_arrival = models.ForeignKey(
        DataArrival,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="jobs",
    )

    files_total = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)

    class Meta:
        app_label = "georivaingestion"
