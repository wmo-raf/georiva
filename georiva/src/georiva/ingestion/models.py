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
from django.utils import timezone
from wagtail.snippets.models import register_snippet


@register_snippet
class IngestionLog(models.Model):
    """Tracks processing state for every file entering GeoRiva storage."""
    
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
        return timezone.now() - self.locked_at > self.LOCK_TIMEOUT
    
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
    def register(cls, bucket: str, file_path: str, **kwargs) -> tuple['IngestionLog', bool]:
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
        
        now = timezone.now()
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
            completed_at=timezone.now(),
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
        stale_cutoff = timezone.now() - cls.LOCK_TIMEOUT
        
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
