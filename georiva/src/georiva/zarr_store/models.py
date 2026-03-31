from datetime import timedelta

from django.db import models
from django.db.models import Exists, OuterRef
from django.utils import timezone as dj_timezone
from wagtail.snippets.models import register_snippet


@register_snippet
class ZarrSyncLog(models.Model):
    """
    Tracks Zarr store sync state for each (Item, Variable) pair.

    GeoTIFF is canonical; Zarr is a derived cache rebuilt from COG assets.
    Ingestion writes COG first, then enqueues a lower-priority zarr_sync_store
    task. This model provides atomic store-level locking to prevent concurrent
    writes to the same Zarr array, and crash recovery via stale lock detection.

    Lock granularity: store_path (one lock per variable.zarr, not per record).

    Lifecycle:
        pending → processing (store locked) → completed
                                            → failed → processing (retry) → ...

    Crash recovery:
        processing (stale lock) → acquired directly by acquire_store → ...
        processing (stale lock) → reset_stale_locks → pending → ...  (sweep fallback)

    Max retries exceeded:
        failed (retry_count >= MAX_RETRIES) → permanently failed

    retry_count semantics:
        Incremented only when a record transitions from PENDING/FAILED into
        PROCESSING (a genuine new attempt). Stale reclaim (crash recovery) does
        NOT increment retry_count — the previous attempt's outcome is unknown
        and should not penalise the record.
    """
    
    MAX_RETRIES = 3
    LOCK_TIMEOUT = timedelta(minutes=30)
    
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        PROCESSING = 'processing', 'Processing'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'
    
    # =========================================================================
    # Identity — unique per (item, variable)
    # =========================================================================
    
    item = models.ForeignKey(
        'georivacore.Item',
        on_delete=models.CASCADE,
        related_name='zarr_sync_logs',
        db_constraint=False,
    )
    variable = models.ForeignKey(
        'georivacore.Variable',
        on_delete=models.CASCADE,
        related_name='zarr_sync_logs',
    )
    
    # Relative path within the Zarr bucket: {catalog}/{collection}/{variable}.zarr
    store_path = models.CharField(max_length=500)
    
    # =========================================================================
    # State machine
    # =========================================================================
    
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    retry_count = models.IntegerField(default=0)
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=100, blank=True, default='')
    completed_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['item', 'variable']
        indexes = [
            models.Index(fields=['store_path', 'status'],
                         name='idx_zarrsync_store_status'),
            models.Index(fields=['status', 'created_at'],
                         name='idx_zarrsync_status_created'),
        ]
        ordering = ['-created_at']
    
    def __str__(self):
        return f"ZarrSyncLog({self.store_path}, item={self.item_id}, {self.status})"
    
    # =========================================================================
    # Store-level lock acquisition
    # =========================================================================
    
    @classmethod
    def acquire_store(cls, store_path: str, worker_id: str):
        """
        Atomically claim all processable records for store_path.

        Two categories of records are claimed:

          1. Fresh claim — PENDING or retryable FAILED records.
             retry_count is incremented: this is a genuine new attempt.

          2. Stale reclaim — PROCESSING records whose lock has expired
             (worker crashed or timed out).
             retry_count is NOT incremented: the previous attempt's outcome
             is unknown; ZarrWriter's duplicate-timestamp guard handles the
             case where the write already succeeded.

        Both UPDATE statements carry a NOT EXISTS guard against the same
        live-lock subquery, so concurrent workers racing on the same store
        are serialised by DB row-locking: exactly one wins, the other gets
        0 rows updated and returns None.

        Returns a queryset of claimed records ordered by item.time, or None
        if the store is already locked and there is nothing to process.
        """
        now = dj_timezone.now()
        stale_cutoff = now - cls.LOCK_TIMEOUT
        
        # Subquery: a live (non-stale) PROCESSING record exists for this store.
        # Used as a NOT EXISTS guard on both UPDATE statements below.
        live_lock = cls.objects.filter(
            store_path=OuterRef('store_path'),
            status=cls.Status.PROCESSING,
            locked_at__gte=stale_cutoff,
        )
        
        # 1. Fresh claim — genuine new attempt; increment retry_count.
        fresh_claimed = cls.objects.filter(
            store_path=store_path,
            status__in=[cls.Status.PENDING, cls.Status.FAILED],
            retry_count__lt=cls.MAX_RETRIES,
        ).exclude(
            Exists(live_lock)
        ).update(
            status=cls.Status.PROCESSING,
            locked_at=now,
            locked_by=worker_id,
            retry_count=models.F('retry_count') + 1,
        )
        
        # 2. Stale reclaim — crash recovery; do NOT increment retry_count.
        stale_reclaimed = cls.objects.filter(
            store_path=store_path,
            status=cls.Status.PROCESSING,
            locked_at__lt=stale_cutoff,
        ).exclude(
            Exists(live_lock)
        ).update(
            locked_at=now,
            locked_by=worker_id,
        )
        
        if not fresh_claimed and not stale_reclaimed:
            return None
        
        return (
            cls.objects
            .filter(store_path=store_path, status=cls.Status.PROCESSING, locked_by=worker_id)
            .select_related('item', 'variable', 'item__collection', 'item__collection__catalog')
            .order_by('item__time')
        )
    
    # =========================================================================
    # State transitions (instance methods)
    # =========================================================================
    
    def mark_completed(self):
        """Transition to COMPLETED and release the lock."""
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.COMPLETED,
            completed_at=dj_timezone.now(),
            locked_at=None,
            locked_by='',
            error='',
        )
    
    def mark_failed(self, error: str):
        """Transition to FAILED and release the lock."""
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.FAILED,
            locked_at=None,
            locked_by='',
            error=error[:2000],
        )
    
    def mark_permanently_failed(self, error: str):
        """Transition to FAILED with retry_count at MAX_RETRIES — will never be retried."""
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.FAILED,
            retry_count=self.MAX_RETRIES,
            locked_at=None,
            locked_by='',
            error=error[:2000],
        )
    
    # =========================================================================
    # Bulk helpers (classmethods)
    # =========================================================================
    
    @classmethod
    def reset_stale_locks(cls) -> int:
        """
        Sweep fallback: reset stale PROCESSING records back to PENDING.

        acquire_store() handles stale reclaim directly when a zarr_sync_store
        task fires. This method exists as a belt-and-suspenders fallback for
        records that become stale but never receive a new dispatch — e.g. if
        the Celery broker drops the task. Called by sweep_zarr_pending before
        get_pending_store_paths() so those records are visible for re-dispatch.

        Does NOT increment retry_count (same reasoning as stale reclaim in
        acquire_store — the previous attempt's outcome is unknown).

        Returns the number of records reset.
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
    def get_pending_store_paths(cls):
        """
        Return distinct store_paths that have PENDING or retryable FAILED records.
        Used by sweep_zarr_pending to find stores needing sync.
        """
        return (
            cls.objects
            .filter(
                status__in=[cls.Status.PENDING, cls.Status.FAILED],
                retry_count__lt=cls.MAX_RETRIES,
            )
            .values_list('store_path', flat=True)
            .distinct()
        )
