"""
DerivationRun — per-Production-Unit execution record for the engine.

The write-side analogue of FileIngestion. Serves three roles:
  1. distributed lock (prevents two workers computing the same unit),
  2. state machine (the only record of a failed/in-progress unit, since those
     produce no Published item),
  3. monitoring surface.

Lives in the processing (engine) app — engine bookkeeping, not catalog data.
See docs/adr/0005-generic-derivation-engine.md.
"""
from datetime import timedelta

from django.db import models
from django.utils import timezone as dj_timezone
from django_extensions.db.models import TimeStampedModel


class DerivationRun(TimeStampedModel):
    LOCK_TIMEOUT = timedelta(hours=2)

    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        RUNNING = 'running', 'Running'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'
        SKIPPED = 'skipped', 'Skipped (idempotent no-op)'
        NOT_READY = 'not_ready', 'Not ready'

    recipe_type = models.CharField(max_length=100, db_index=True)
    recipe_version = models.CharField(max_length=50)

    # Opaque ProductionUnit: the canonical JSON plus a stable hash used as the
    # lock/identity key (JSON itself is awkward to unique-index).
    unit_key = models.JSONField(default=dict)
    unit_hash = models.CharField(max_length=64, db_index=True)

    input_hash = models.CharField(max_length=64, blank=True)

    # Opaque, engine-uninterpreted grouping key (ADR-0008). The invocation layer
    # stamps it with the product/trigger identity; the tracking UI joins
    # product -> runs by it. NULL means "no product origin" (engine-internal or
    # manual run). The engine never reads or imports the feed layer to set this,
    # preserving the ADR-0005 layering.
    origin = models.CharField(max_length=255, null=True, blank=True, db_index=True)

    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING, db_index=True,
    )

    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=255, blank=True)

    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True)

    produced_item = models.ForeignKey(
        'georivacore.Item',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='derivation_runs',
        db_constraint=False,  # Item is a hypertable (see Asset.item)
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['recipe_type', 'unit_hash'],
                name='unique_run_per_recipe_unit',
            ),
        ]
        indexes = [
            models.Index(fields=['recipe_type', 'status']),
            models.Index(fields=['status', 'locked_at']),
        ]

    def __str__(self):
        return f"{self.recipe_type}[{self.unit_hash[:8]}] {self.status}"

    @property
    def is_stale(self) -> bool:
        if self.status != self.Status.RUNNING or not self.locked_at:
            return False
        return dj_timezone.now() - self.locked_at > self.LOCK_TIMEOUT

    @classmethod
    def acquire(cls, *, recipe_type, recipe_version, unit_key, unit_hash, worker_id="") -> "DerivationRun | None":
        """
        Atomically take the lock for (recipe_type, unit_hash).

        Returns the locked, RUNNING DerivationRun on success, or None if another
        worker already holds a live lock on this unit.
        """
        now = dj_timezone.now()
        run, _ = cls.objects.get_or_create(
            recipe_type=recipe_type,
            unit_hash=unit_hash,
            defaults={
                "recipe_version": recipe_version,
                "unit_key": unit_key,
                "status": cls.Status.PENDING,
            },
        )

        stale_cutoff = now - cls.LOCK_TIMEOUT

        # Lockable when pending/failed/idempotent-noop/not-ready, or a stale run.
        claimed = cls.objects.filter(
            pk=run.pk,
        ).filter(
            models.Q(status__in=[
                cls.Status.PENDING, cls.Status.FAILED,
                cls.Status.SKIPPED, cls.Status.NOT_READY, cls.Status.COMPLETED,
            ])
            | models.Q(status=cls.Status.RUNNING, locked_at__lt=stale_cutoff)
        ).update(
            status=cls.Status.RUNNING,
            recipe_version=recipe_version,
            unit_key=unit_key,
            locked_at=now,
            locked_by=worker_id,
            started_at=now,
            error="",
        )

        if not claimed:
            return None

        run.refresh_from_db()
        return run

    def mark_completed(self, *, produced_item=None, input_hash=""):
        self.status = self.Status.COMPLETED
        self.produced_item = produced_item
        self.input_hash = input_hash
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        self.save(update_fields=[
            "status", "produced_item", "input_hash", "completed_at",
            "locked_at", "locked_by", "modified",
        ])

    def mark_skipped(self, *, input_hash=""):
        self.status = self.Status.SKIPPED
        self.input_hash = input_hash
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        self.save(update_fields=[
            "status", "input_hash", "completed_at", "locked_at", "locked_by", "modified",
        ])

    def mark_not_ready(self):
        self.status = self.Status.NOT_READY
        self.locked_at = None
        self.locked_by = ""
        self.save(update_fields=["status", "locked_at", "locked_by", "modified"])

    def mark_failed(self, error: str):
        self.status = self.Status.FAILED
        self.error = error or ""
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        self.save(update_fields=[
            "status", "error", "completed_at", "locked_at", "locked_by", "modified",
        ])
