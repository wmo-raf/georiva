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

from django.db import models, transaction
from django.utils import timezone as dj_timezone
from django_extensions.db.models import TimeStampedModel

from georiva.events.context import ensure_context
from georiva.events.models import DomainEvent
from georiva.events.recording import record
from georiva.organisations.lookups import NOT_ORM_SCOPABLE
from georiva.processing.constants import DERIVATION_LOCK_TIMEOUT_SECONDS


class DerivationRun(TimeStampedModel):
    # Its only link to the data tree is ``produced_item``, which is null until a
    # run succeeds — so scoping on it would hide every pending run from its own
    # organisation. Runs are reached through an already-scoped product instead
    # (see access.get_via_scoped_parent_or_404).
    ORGANISATION_LOOKUP = NOT_ORM_SCOPABLE

    # Aligned with the run_unit_task hard time limit (see processing/constants):
    # the lock only becomes stealable strictly after Celery would have killed a
    # runaway task, so a live task's lock is never stolen, and a dead worker's
    # unit recovers in ~20 min rather than the old fixed 2h.
    LOCK_TIMEOUT = timedelta(seconds=DERIVATION_LOCK_TIMEOUT_SECONDS)

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"
        SKIPPED = "skipped", "Skipped (idempotent no-op)"
        NOT_READY = "not_ready", "Not ready"

    class RetryReason(models.TextChoices):
        # Why the most recent RUNNING transition fired. Recorded so an operator
        # can tell a code failure (celery_retry) from an infrastructure reclaim
        # (stale_running_reclaim) from a legitimate freshness recompute
        # (input_stale) from a hand-triggered rerun (manual_rerun).
        INITIAL = "initial", "Initial run"
        CELERY_RETRY = "celery_retry", "Celery auto-retry"
        STALE_RUNNING_RECLAIM = "stale_running_reclaim", "Stale RUNNING reclaimed"
        INPUT_STALE = "input_stale", "Input changed (recompute)"
        MANUAL_RERUN = "manual_rerun", "Manual re-run"
        NOT_READY_SWEEP = "not_ready_sweep", "Inputs ready (sweep revival)"
        INPUT_ARRIVED = "input_arrived", "Required input arrived"

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
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )

    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=255, blank=True)

    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error = models.TextField(blank=True)

    # Retry story (#209). `attempts` counts every RUNNING transition (bumped in
    # acquire's atomic claim, so a refused lock never increments); the reason
    # records why the latest attempt fired. No per-attempt history is kept — only
    # the count and the most recent trigger.
    attempts = models.PositiveIntegerField(default=0)
    last_retry_reason = models.CharField(
        max_length=32,
        choices=RetryReason.choices,
        default=RetryReason.INITIAL,
    )
    last_retry_at = models.DateTimeField(null=True, blank=True)

    produced_item = models.ForeignKey(
        "georivacore.Item",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="derivation_runs",
        db_constraint=False,  # Item is a hypertable (see Asset.item)
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["recipe_type", "unit_hash"],
                name="unique_run_per_recipe_unit",
            ),
        ]
        indexes = [
            models.Index(fields=["recipe_type", "status"]),
            models.Index(fields=["status", "locked_at"]),
        ]

    def __str__(self):
        return f"{self.recipe_type}[{self.unit_hash[:8]}] {self.status}"

    @property
    def is_stale(self) -> bool:
        if self.status != self.Status.RUNNING or not self.locked_at:
            return False
        return dj_timezone.now() - self.locked_at > self.LOCK_TIMEOUT

    @classmethod
    def acquire(
        cls, *, recipe_type, recipe_version, unit_key, unit_hash, worker_id="", origin=None, reason=RetryReason.INITIAL
    ) -> "DerivationRun | None":
        """
        Atomically take the lock for (recipe_type, unit_hash).

        Returns the locked, RUNNING DerivationRun on success, or None if another
        worker already holds a live lock on this unit.

        ``origin`` is an opaque grouping key (ADR-0008) supplied by the
        invocation layer. It is set when the run is first created and, on a
        re-acquire, updated only when a non-None ``origin`` is given — so an
        engine-internal re-run (origin=None) never clobbers a product's stamp.
        """
        now = dj_timezone.now()
        run, _ = cls.objects.get_or_create(
            recipe_type=recipe_type,
            unit_hash=unit_hash,
            defaults={
                "recipe_version": recipe_version,
                "unit_key": unit_key,
                "status": cls.Status.PENDING,
                "origin": origin,
            },
        )

        stale_cutoff = now - cls.LOCK_TIMEOUT

        # Lockable when pending/failed/idempotent-noop/not-ready, or a stale run.
        # The attempts bump and retry-reason stamp ride this same conditional
        # UPDATE, so they land iff the claim wins — a refused lock never counts.
        claim_values = dict(
            status=cls.Status.RUNNING,
            recipe_version=recipe_version,
            unit_key=unit_key,
            locked_at=now,
            locked_by=worker_id,
            started_at=now,
            error="",
            attempts=models.F("attempts") + 1,
            last_retry_reason=reason,
            last_retry_at=now,
        )
        # Only (re)stamp origin when the caller supplies one — an engine-internal
        # re-run (origin=None) must not erase the product origin set originally.
        if origin is not None:
            claim_values["origin"] = origin

        claimed = (
            cls.objects.filter(
                pk=run.pk,
            )
            .filter(
                models.Q(
                    status__in=[
                        cls.Status.PENDING,
                        cls.Status.FAILED,
                        cls.Status.SKIPPED,
                        cls.Status.NOT_READY,
                        cls.Status.COMPLETED,
                    ]
                )
                | models.Q(status=cls.Status.RUNNING, locked_at__lt=stale_cutoff)
            )
            .update(**claim_values)
        )

        if not claimed:
            return None

        run.refresh_from_db()
        return run

    # -- transitions ---------------------------------------------------------
    #
    # Each ending is one atomic block: the row update and the attempt event commit
    # together or not at all. Only the worker holding the lock reaches these, which
    # is what makes it the winner — a worker refused the lock in ``acquire`` never
    # gets here, so it writes nothing. The context is whatever the dispatcher
    # carried; the engine never resolves an organisation of its own.

    def _finish_attempt(self, outcome, update_fields, **extra):
        with ensure_context(actor="system:engine"):
            with transaction.atomic():
                self.save(update_fields=[*update_fields, "modified"])
                self.last_event = record(
                    DerivationAttemptFinished,
                    derivation_run=self,
                    outcome=outcome,
                    recipe_type=self.recipe_type,
                    recipe_version=self.recipe_version,
                    unit_hash=self.unit_hash,
                    unit_key=self.unit_key,
                    attempt_number=self.attempts,
                    retry_reason=self.last_retry_reason,
                    input_hash=self.input_hash,
                    origin=self.origin or "",
                    produced_item_id=self.produced_item_id,
                    error=self.error,
                )
        return self.last_event

    def mark_completed(self, *, produced_item=None, input_hash=""):
        self.status = self.Status.COMPLETED
        self.produced_item = produced_item
        self.input_hash = input_hash
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        return self._finish_attempt(
            DerivationAttemptFinished.Outcome.COMPLETED,
            ["status", "produced_item", "input_hash", "completed_at", "locked_at", "locked_by"],
        )

    def mark_skipped(self, *, input_hash=""):
        self.status = self.Status.SKIPPED
        self.input_hash = input_hash
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        return self._finish_attempt(
            DerivationAttemptFinished.Outcome.SKIPPED,
            ["status", "input_hash", "completed_at", "locked_at", "locked_by"],
        )

    def mark_not_ready(self):
        self.status = self.Status.NOT_READY
        self.locked_at = None
        self.locked_by = ""
        return self._finish_attempt(
            DerivationAttemptFinished.Outcome.NOT_READY,
            ["status", "locked_at", "locked_by"],
        )

    def mark_failed(self, error: str):
        self.status = self.Status.FAILED
        self.error = error or ""
        self.completed_at = dj_timezone.now()
        self.locked_at = None
        self.locked_by = ""
        return self._finish_attempt(
            DerivationAttemptFinished.Outcome.FAILED,
            ["status", "error", "completed_at", "locked_at", "locked_by"],
        )


class DerivationAttemptFinished(DomainEvent):
    """One attempt at a Derivation unit ended (ADR 0030).

    ``DerivationRun`` keeps an attempt count and the latest retry reason and
    nothing about the attempts before it; this is the record of each. The unit's
    identity is snapshotted, so the event stays true after the next attempt has
    overwritten the row. ``produced_item_id`` is a plain column: Item is a
    hypertable and is removed by retention.
    """

    label = "Derivation attempt"

    class Outcome(models.TextChoices):
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"
        SKIPPED = "skipped", "Skipped"
        NOT_READY = "not_ready", "Not ready"

    derivation_run = models.ForeignKey(
        DerivationRun, null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    outcome = models.CharField(max_length=10, choices=Outcome.choices)
    recipe_type = models.CharField(max_length=100)
    recipe_version = models.CharField(max_length=50)
    unit_hash = models.CharField(max_length=64)
    unit_key = models.JSONField(default=dict)
    attempt_number = models.PositiveIntegerField()
    retry_reason = models.CharField(max_length=32)
    input_hash = models.CharField(max_length=64, blank=True)
    origin = models.CharField(max_length=255, blank=True)
    produced_item_id = models.BigIntegerField(null=True, blank=True)
    error = models.TextField(blank=True, default="")

    class Meta(DomainEvent.Meta):
        indexes = [
            models.Index(fields=["derivation_run", "-occurred_at"], name="idx_derivatt_run"),
            models.Index(fields=["origin", "-occurred_at"], name="idx_derivatt_origin"),
        ]

    def subject_url(self):
        """The product's run list, when this attempt came from a product that still exists."""
        prefix = "derived_product:"
        if not self.origin.startswith(prefix) or not self.origin[len(prefix) :].isdigit():
            return None
        from django.urls import reverse

        return reverse("derived_product_runs", args=[int(self.origin[len(prefix) :])])

    def extra_links(self):
        """The lineage of inputs that went into the product — which did not trigger the attempt."""
        if self.produced_item_id is None:
            return []
        from django.urls import reverse

        return [("See inputs", reverse("derivation_attempt_inputs", args=[self.pk]))]

    @property
    def outcome_class(self):
        return "failed" if self.outcome == self.Outcome.FAILED else "ok"

    def summary(self):
        text = f"{self.recipe_type} v{self.recipe_version} attempt {self.attempt_number}: {self.get_outcome_display()}"
        if self.error:
            text += f" — {self.error.splitlines()[0][:150]}"
        return text
