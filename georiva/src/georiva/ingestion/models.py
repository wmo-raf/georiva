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
from functools import partial

from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models, transaction
from django.db.models import Max, Q
from django.utils import timezone as dj_timezone
from django.utils.translation import gettext_lazy as _
from wagtail.snippets.models import register_snippet

from georiva.events.context import ensure_context, update_context
from georiva.events.models import DomainEvent
from georiva.events.recording import record
from georiva.organisations.lookups import NOT_ORM_SCOPABLE

logger = logging.getLogger(__name__)


def record_sweep_intervention(sweep: str, reason: str, file_ingestion_id, bucket: str, file_path: str) -> None:
    """Write ``SweepIntervened`` for one file, under the chain the file already belongs to."""
    from georiva.events.context import use_context
    from georiva.ingestion.chains import context_for_revival

    with use_context(context_for_revival(bucket, file_path, sweep)):
        write_sweep_intervention(sweep, reason, file_ingestion_id, bucket, file_path)


def write_sweep_intervention(sweep: str, reason: str, file_ingestion_id, bucket: str, file_path: str) -> None:
    """The write itself, under whatever context is active — for a sweep that also dispatches inside it."""
    from georiva.events.models import SweepIntervened

    record(
        SweepIntervened,
        sweep_name=sweep,
        reason=reason,
        target_kind="file_ingestion",
        target_id=file_ingestion_id,
        target_label=f"{bucket}:{file_path}",
    )


def organisation_id_for_key(file_path: str):
    """The organisation a storage key belongs to: its first segment is the slug.

    ``None`` when the slug names no organisation — a misaddressed delivery, which
    is recorded as an unattributed event rather than guessed into someone's.
    """
    from georiva.organisations.models import Organisation

    slug = file_path.split("/", 1)[0]
    return Organisation.objects.filter(slug=slug).values_list("pk", flat=True).first()


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
    # A pending row older than this has lost its dispatched task (#401).
    PENDING_TIMEOUT = timedelta(minutes=45)

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
        with ensure_context(actor="system:ingestion", organisation_id=organisation_id_for_key(file_path)):
            with transaction.atomic():
                updated = cls.objects.filter(
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
                if updated:
                    cls._record_finished(bucket, file_path, FileIngestionFinished.Outcome.COMPLETED)
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
    def mark_failed(cls, bucket: str, file_path: str, error: str, *, outcome=None, unattributed=False):
        """Mark a file as failed. Releases the lock for future retry.

        ``outcome`` is the event's ending: ``failed`` by default, ``refused`` for a
        file turned away rather than one that broke. ``unattributed`` records the
        event under no organisation at all — for a key whose organisation or
        catalog cannot be trusted to exist, so it is nobody's to show.
        """
        outcome = outcome or FileIngestionFinished.Outcome.FAILED
        with ensure_context(actor="system:ingestion", organisation_id=organisation_id_for_key(file_path)):
            if unattributed:
                update_context(organisation_id=None)
            with transaction.atomic():
                updated = cls.objects.filter(
                    bucket=bucket,
                    file_path=file_path,
                ).update(
                    status=cls.Status.FAILED,
                    locked_at=None,
                    locked_by="",
                    error=error[:2000],
                )
                if updated:
                    cls._record_finished(bucket, file_path, outcome)

    @classmethod
    def fail_if_held_by(cls, bucket: str, file_path: str, locked_by: str, error: str) -> bool:
        """Fail a file still locked by ``locked_by`` — the job crashed holding it.

        The conditional UPDATE is what makes the winner: only the worker that
        still holds the lock marks the failure, and only it writes the event.
        """
        with ensure_context(actor="system:ingestion", organisation_id=organisation_id_for_key(file_path)):
            with transaction.atomic():
                updated = cls.objects.filter(
                    bucket=bucket,
                    file_path=file_path,
                    status=cls.Status.PROCESSING,
                    locked_by=locked_by,
                ).update(
                    status=cls.Status.FAILED,
                    locked_at=None,
                    locked_by="",
                    error=error[:2000],
                )
                if updated:
                    cls._record_finished(bucket, file_path, FileIngestionFinished.Outcome.FAILED)
        return bool(updated)

    @classmethod
    def _record_finished(cls, bucket: str, file_path: str, outcome: str):
        """Write ``FileIngestionFinished`` from the row as the winning UPDATE left it."""
        log = cls.objects.filter(bucket=bucket, file_path=file_path).first()
        if log is None:
            return None
        touched = [
            f"{catalog}/{collection}" for catalog, collection in log.collections.values_list("catalog__slug", "slug")
        ]
        return record(
            FileIngestionFinished,
            file_ingestion=log,
            outcome=outcome,
            bucket=bucket,
            file_path=file_path,
            collections_touched=sorted(touched),
            items_created=log.items_created,
            assets_created=log.assets_created,
            retry_count=log.retry_count,
            error=log.error,
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
    def reset_stale_locks(cls, sweep=None) -> int:
        """
        Reset locks that have exceeded LOCK_TIMEOUT.

        Returns the number of locks reset.
        """
        stale_cutoff = dj_timezone.now() - cls.LOCK_TIMEOUT

        stale = cls.objects.filter(status=cls.Status.PROCESSING, locked_at__lt=stale_cutoff)
        # Each reset file is a recorded intervention, inside the file's own chain.
        # Read before the update: afterwards nothing says which rows it touched.
        # The reset and its events commit together or not at all.
        with transaction.atomic():
            targets = list(stale.values_list("pk", "bucket", "file_path")) if sweep else []
            updated = stale.update(status=cls.Status.PENDING, locked_at=None, locked_by="")
            for pk, bucket, file_path in targets:
                record_sweep_intervention(sweep, "stale_lock_reclaimed", pk, bucket, file_path)
        return updated

    @classmethod
    def is_stale_pending(cls, bucket, file_path) -> bool:
        """True when the file is pending and has sat past PENDING_TIMEOUT."""
        cutoff = dj_timezone.now() - cls.PENDING_TIMEOUT
        return cls.objects.filter(
            bucket=bucket, file_path=file_path, status=cls.Status.PENDING, updated_at__lt=cutoff
        ).exists()

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
        if reference_time is None:
            return None, False

        completed_at = completed_at or dj_timezone.now()

        with ensure_context(actor="system:ingestion", organisation_id=collection.catalog.organisation_id):
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
                    event = record(
                        RunIngestionReopened,
                        run=run,
                        collection_slug=collection.slug,
                        catalog_slug=collection.catalog.slug,
                        reference_time=run.reference_time,
                        revision=run.revision,
                    )
                    transaction.on_commit(partial(_emit_run_reopened, run, event))
        return run, reopened

    def close(self, closer) -> bool:
        """Close this run. Returns False if it was already closed.

        The conditional UPDATE is what makes concurrent closers safe: three
        closers can race on one row, and only the first one to land emits the
        signal.
        """
        organisation_id = self.collection.catalog.organisation_id
        with ensure_context(actor="system:run_closer", organisation_id=organisation_id):
            with transaction.atomic():
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
                event = record(
                    RunIngestionClosed,
                    run=self,
                    collection_slug=self.collection.slug,
                    catalog_slug=self.collection.catalog.slug,
                    reference_time=self.reference_time,
                    revision=self.revision,
                    closer=closer,
                )
                transaction.on_commit(partial(_emit_run_closed, self, event))
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


def _emit_run_closed(run, event):
    from georiva.ingestion.domain_signals import run_ingestion_closed

    run_ingestion_closed.send(sender=RunIngestion, run=run, event=event)


def _emit_run_reopened(run, event):
    from georiva.ingestion.domain_signals import run_ingestion_reopened

    run_ingestion_reopened.send(sender=RunIngestion, run=run, event=event)


class FileIngestionFinished(DomainEvent):
    """A file ingestion reached a terminal state (ADR 0030).

    The run row is overwritten by every retry and re-ingest; this is the record
    of each ending. ``refused`` is a file turned away for a reason that is not a
    processing failure — an unknown organisation, or a time that cannot be
    extracted.
    """

    label = "File ingestion finished"

    class Outcome(models.TextChoices):
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"
        REFUSED = "refused", "Refused"

    file_ingestion = models.ForeignKey(
        FileIngestion, null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    outcome = models.CharField(max_length=10, choices=Outcome.choices)
    bucket = models.CharField(max_length=50)
    file_path = models.CharField(max_length=500)
    collections_touched = models.JSONField(default=list, help_text="``catalog/collection`` slugs the file touched.")
    items_created = models.IntegerField(default=0)
    assets_created = models.IntegerField(default=0)
    retry_count = models.IntegerField(default=0)
    error = models.TextField(blank=True, default="")

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["file_ingestion", "-occurred_at"], name="idx_fileingfin_file")]

    @property
    def outcome_class(self):
        return {"completed": "ok", "failed": "failed", "refused": "refused"}[self.outcome]

    def subject_url(self):
        if self.file_ingestion_id is None:
            return None
        from django.urls import reverse

        return reverse("file_ingestion_detail", args=[self.file_ingestion_id])

    def _collection_slug(self) -> str:
        """``catalog/collection`` — the first touched, else read off the key ``{org}/{catalog}/{collection}/…``."""
        if self.collections_touched:
            return sorted(self.collections_touched)[0]
        parts = self.file_path.split("/")
        return "/".join(parts[1:3]) if len(parts) >= 4 else ""

    def failure_group_key(self):
        slug = self._collection_slug()
        return f"collection:{slug}" if slug else ""

    def failure_group(self):
        from django.urls import reverse

        from georiva.core.models import Collection
        from georiva.events.models import FailureGroup

        slug = self._collection_slug()
        if not slug:
            return super().failure_group()
        catalog_slug, collection_slug = slug.split("/", 1)
        pk = (
            Collection.objects.filter(
                catalog__organisation_id=self.organisation_id, catalog__slug=catalog_slug, slug=collection_slug
            )
            .values_list("pk", flat=True)
            .first()
        )
        url = reverse("collection_items_list", args=[pk]) if pk else None
        return FailureGroup(key=self.failure_group_key(), label=slug, url=url)

    def summary(self):
        if self.outcome == self.Outcome.COMPLETED:
            return f"{self.file_path}: {self.items_created} items, {self.assets_created} assets"
        first_line = (self.error or "").splitlines()[0] if self.error else ""
        return f"{self.get_outcome_display()} {self.file_path}: {first_line}"[:300]


class RunIngestionClosed(DomainEvent):
    """A model run finished arriving into a collection (ADR 0026, ADR 0030)."""

    label = "Run closed"

    run = models.ForeignKey(RunIngestion, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    collection_slug = models.CharField(max_length=255)
    catalog_slug = models.CharField(max_length=255)
    reference_time = models.DateTimeField()
    revision = models.PositiveIntegerField()
    closer = models.CharField(max_length=20, choices=RunIngestion.Closer.choices)

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["run", "-occurred_at"], name="idx_runclosed_run")]

    def subject_url(self):
        if self.run_id is None:
            return None
        from django.urls import reverse

        return reverse("run_ingestion_detail", args=[self.run_id])

    def summary(self):
        return f"Closed at revision {self.revision} by {self.get_closer_display().lower()}"


class RunIngestionReopened(DomainEvent):
    """A late file reopened a closed run; ``revision`` is the bumped one."""

    label = "Run reopened"

    run = models.ForeignKey(RunIngestion, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    collection_slug = models.CharField(max_length=255)
    catalog_slug = models.CharField(max_length=255)
    reference_time = models.DateTimeField()
    revision = models.PositiveIntegerField()

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["run", "-occurred_at"], name="idx_runreopened_run")]

    def subject_url(self):
        if self.run_id is None:
            return None
        from django.urls import reverse

        return reverse("run_ingestion_detail", args=[self.run_id])

    def summary(self):
        return f"Reopened by a late file; now revision {self.revision}"


# ---------------------------------------------------------------------------
# Push feeds (ADR 0034)
# ---------------------------------------------------------------------------


class PushFeed(models.Model):
    """A catalog's single route in for files that are sent to GeoRiva.

    It describes one file shape. Routing stays on each Variable's ``sources``.
    """

    class ValidTimeFormat(models.TextChoices):
        YYYYMMDD = "YYYYMMDD", "YYYYMMDD"
        DDMMYYYY = "DDMMYYYY", "DDMMYYYY"
        YYYYMMDDHH = "YYYYMMDDHH", "YYYYMMDDHH"
        YYYYMMDDHHMM = "YYYYMMDDHHMM", "YYYYMMDDHHMM"
        DDMMYY = "DDMMYY", "DDMMYY"
        YYMMDD = "YYMMDD", "YYMMDD"
        CONTENT = "CONTENT", "From file content"

    ORGANISATION_LOOKUP = "catalog__organisation"

    catalog = models.OneToOneField(
        "georivacore.Catalog",
        on_delete=models.CASCADE,
        related_name="push_feed",
    )
    name = models.CharField(max_length=255)
    is_forecast = models.BooleanField(default=False)
    valid_time_format = models.CharField(max_length=20, choices=ValidTimeFormat.choices)
    files_per_run = models.PositiveIntegerField(
        default=1,
        validators=[MinValueValidator(1)],
        help_text="How many files make up one model run.",
    )
    expected_interval = models.DurationField(
        null=True,
        blank=True,
        help_text="How often a file is expected. Leave empty when there is no schedule to check.",
    )
    expected_since = models.DateTimeField(
        null=True,
        blank=True,
        editable=False,
        help_text="When the expected interval was set: the clock a feed with no delivery yet is measured from.",
    )

    class Meta:
        app_label = "georivaingestion"
        permissions = [("manage_push_senders", "Can add, rotate and revoke a push feed's machine senders")]

    def __str__(self):
        return self.name

    def clean(self):
        super().clean()
        self.refuse_pull_fed_catalog()

    def refuse_pull_fed_catalog(self):
        """A catalog has one route in (ADR 0034): not a push feed beside a pull feed."""
        if not self.catalog_id:
            return
        from georiva.sources.models import DataFeed

        pull_feed = DataFeed.objects.filter(catalog_id=self.catalog_id).first()
        if pull_feed:
            raise ValidationError(
                {
                    "catalog": _(
                        "This catalog already has a pull feed, \u201c%(name)s\u201d. A catalog has at most one feed."
                    )
                    % {"name": pull_feed.name}
                }
            )

    def save(self, *args, **kwargs):
        self.refuse_pull_fed_catalog()
        self._restart_overdue_clock()
        super().save(*args, **kwargs)

    def _restart_overdue_clock(self):
        """Stamp ``expected_since`` when the interval is set or changed, and clear it when removed."""
        if self.expected_interval is None:
            self.expected_since = None
            return
        previous = None
        if self.pk:
            previous = type(self).objects.filter(pk=self.pk).values_list("expected_interval", flat=True).first()
        if self.expected_since is None or previous != self.expected_interval:
            self.expected_since = dj_timezone.now()

    def last_delivery_at(self):
        """When the newest accepted delivery was accepted, or ``None``.

        Reads the ``last_accepted`` annotation when a list query supplied one.
        """
        if hasattr(self, "last_accepted"):
            return self.last_accepted
        return self.deliveries.filter(status=Delivery.Status.ACCEPTED).aggregate(m=Max("accepted_at"))["m"]

    def overdue_reference(self):
        """The instant the interval is counted from: the last accepted delivery, else when the interval was set."""
        return self.last_delivery_at() or self.expected_since

    def is_overdue(self, now=None) -> bool:
        """Derived on read (ADR 0034): the reference is older than the expected interval."""
        reference = self.overdue_reference()
        if self.expected_interval is None or reference is None:
            return False
        return (now or dj_timezone.now()) - reference > self.expected_interval

    def overdue_text(self, now=None) -> str | None:
        """The chip's words, or ``None`` when the feed is not overdue."""
        now = now or dj_timezone.now()
        if not self.is_overdue(now):
            return None
        last = self.last_delivery_at()
        if last is None:
            return _("Overdue · no deliveries yet")
        hours = int((now - last).total_seconds() // 3600)
        ago = f"{hours} h" if hours < 72 else f"{hours // 24} d"
        return _("Overdue · last delivery %(ago)s ago") % {"ago": ago}

    @classmethod
    def with_last_delivery(cls, queryset):
        """Annotate ``last_accepted`` so a list reads each feed's last delivery in one query."""
        return queryset.annotate(last_accepted=Max("deliveries__accepted_at", filter=Q(deliveries__status="accepted")))

    def strptime_pattern(self) -> str | None:
        """Return the Python strptime pattern, or None for content-based formats."""
        from georiva.ingestion.time_extraction import _FORMAT_PATTERNS

        return _FORMAT_PATTERNS.get(self.valid_time_format)

    def variables(self):
        """The active Variables of the feed's catalog: what an uploaded file is read for."""
        from georiva.core.models import Variable

        return Variable.objects.filter(collection__catalog=self.catalog_id, is_active=True)


class UploadSession(models.Model):
    """A person's batch of deliveries on a push feed's page (ADR 0034)."""

    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    ORGANISATION_LOOKUP = "push_feed__catalog__organisation"

    push_feed = models.ForeignKey(
        "georivaingestion.PushFeed",
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
        """Auto-complete when all deliveries have reached a terminal state."""
        if self.status != self.Status.ACTIVE:
            return
        terminal = {Delivery.Status.ACCEPTED}
        deliveries = list(self.deliveries.values_list("status", flat=True))
        if deliveries and all(s in terminal for s in deliveries):
            self._finish(self.Status.COMPLETED)


class Delivery(models.Model):
    """One file arriving into a push feed (ADR 0034).

    Today only a person's upload on the feed's page makes one. It starts its own
    Correlation; an accepted delivery hands off to ingestion, and the outcome
    stays on the ``FileIngestion`` for its key rather than being copied here.
    """

    class Status(models.TextChoices):
        RECEIVED = "received", "Received"
        ACCEPTED = "accepted", "Accepted"

    ORGANISATION_LOOKUP = "push_feed__catalog__organisation"

    push_feed = models.ForeignKey(PushFeed, on_delete=models.CASCADE, related_name="deliveries")
    session = models.ForeignKey(
        UploadSession,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    sent_by = models.ForeignKey(
        "auth.User",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="deliveries",
    )
    original_filename = models.CharField(max_length=500)
    file_path = models.CharField(max_length=500, blank=True)
    correlation_id = models.UUIDField(
        db_index=True,
        help_text="The chain this delivery started; the Bucket-notification consumer adopts it by key (ADR 0030).",
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RECEIVED)
    bytes = models.BigIntegerField(default=0)
    received_at = models.DateTimeField(auto_now_add=True)
    accepted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        app_label = "georivaingestion"
        ordering = ["-received_at", "-id"]
        verbose_name_plural = "deliveries"

    def save(self, *args, **kwargs):
        if self._state.adding and self.correlation_id is None:
            import uuid

            self.correlation_id = uuid.uuid4()
        super().save(*args, **kwargs)

    @classmethod
    def receive(cls, *, push_feed, filename, file_path, session=None, sent_by=None, correlation_id=None):
        """Record a delivery as received, with its ``DeliveryReceived`` event."""
        from georiva.events.context import new_context, use_context

        delivery = cls.objects.create(
            push_feed=push_feed,
            session=session,
            sent_by=sent_by,
            original_filename=filename,
            file_path=file_path,
            correlation_id=correlation_id,
        )
        context = new_context(
            actor=f"user:{sent_by.pk}" if sent_by else "system:upload",
            organisation_id=push_feed.catalog.organisation_id,
            correlation_id=delivery.correlation_id,
        )
        with use_context(context):
            record(
                DeliveryReceived,
                delivery=delivery,
                push_feed=push_feed,
                feed_name=push_feed.name,
                catalog_slug=push_feed.catalog.slug,
                filename=filename,
                file_path=file_path,
                sender=sent_by.get_username() if sent_by else "",
            )
        return delivery

    def mark_accepted(self, file_path="", bytes=0, *, check_session=True):
        """Accept the delivery. A caller still adding to the batch passes ``check_session=False``
        and checks once at the end, so the session cannot complete on a partial batch."""
        from django.utils import timezone

        self.status = self.Status.ACCEPTED
        self.file_path = file_path or self.file_path
        self.bytes = bytes
        self.accepted_at = timezone.now()
        self.save(update_fields=["status", "file_path", "bytes", "accepted_at"])
        if check_session and self.session_id:
            self.session._check_auto_complete()

    def timeline_url(self):
        from django.urls import reverse

        return reverse("events_timeline", args=[self.correlation_id])


class DeliveryReceived(DomainEvent):
    """A file arrived into a push feed; the first event of its chain (ADR 0034)."""

    label = "Delivery received"

    delivery = models.ForeignKey(Delivery, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    push_feed = models.ForeignKey(PushFeed, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    feed_name = models.CharField(max_length=255)
    catalog_slug = models.CharField(max_length=255)
    filename = models.CharField(max_length=500)
    file_path = models.CharField(max_length=500)
    sender = models.CharField(max_length=150, blank=True, help_text="Username of the person; blank when unknown.")

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["delivery", "-occurred_at"], name="idx_delivrecv_delivery")]

    def subject_url(self):
        if self.push_feed_id is None:
            return None
        from django.urls import reverse

        return reverse("push_feed_page", args=[self.push_feed_id])

    def summary(self):
        by = f" from {self.sender}" if self.sender else ""
        return f"{self.filename} received by {self.feed_name}{by}"


class PushFeedOverdue(DomainEvent):
    """A push feed first went overdue: nothing accepted within its expected interval (ADR 0034).

    Recorded once per overdue spell by the beat task; an accepted delivery ends the spell.
    """

    label = "Push feed overdue"

    push_feed = models.ForeignKey(PushFeed, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    feed_name = models.CharField(max_length=255)
    catalog_slug = models.CharField(max_length=255)
    expected_interval = models.DurationField()
    last_delivery_at = models.DateTimeField(null=True, blank=True)

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["push_feed", "-occurred_at"], name="idx_pushoverdue_feed")]

    @property
    def outcome_class(self):
        return "failed"

    def subject_url(self):
        if self.push_feed_id is None:
            return None
        from django.urls import reverse

        return reverse("push_feed_page", args=[self.push_feed_id])

    def failure_group_key(self):
        # The name stands in once the feed is gone, so its last failures still group together.
        return f"push-feed:{self.push_feed_id}" if self.push_feed_id else f"push-feed-name:{self.feed_name}"

    def failure_group(self):
        from georiva.events.models import FailureGroup

        # A rename shows at once (ADR 0033); the snapshot stands in once the feed is gone.
        label = self.push_feed.name if self.push_feed_id else self.feed_name
        return FailureGroup(key=self.failure_group_key(), label=label, url=self.subject_url())

    def summary(self):
        if self.last_delivery_at is None:
            return f"{self.feed_name}: no delivery yet, one expected every {self.expected_interval}"
        return f"{self.feed_name}: no delivery since {self.last_delivery_at:%Y-%m-%d %H:%M}"


class PushSender(models.Model):
    """A machine sender of a push feed: one MinIO user GeoRiva created for it (ADR 0034).

    Only the access key is kept; it is what a Bucket notification's
    ``principalId`` is matched against. The secret is shown once, when it is
    issued, and is never written down here.
    """

    ORGANISATION_LOOKUP = "push_feed__catalog__organisation"

    push_feed = models.ForeignKey(PushFeed, on_delete=models.CASCADE, related_name="senders")
    label = models.CharField(max_length=100, help_text="Who or what sends, e.g. the system's name.")
    access_key = models.CharField(max_length=100, unique=True)
    created_by = models.ForeignKey("auth.User", null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "georivaingestion"
        ordering = ["label", "id"]

    def __str__(self):
        return f"{self.label} ({self.access_key})"


class _PushSenderEvent(DomainEvent):
    """The shared snapshot of an event about a machine sender."""

    push_feed = models.ForeignKey(PushFeed, null=True, blank=True, on_delete=models.SET_NULL, related_name="+")
    feed_name = models.CharField(max_length=255)
    sender_label = models.CharField(max_length=100)
    access_key = models.CharField(max_length=100)

    class Meta:
        abstract = True

    def subject_url(self):
        if self.push_feed_id is None:
            return None
        from django.urls import reverse

        return reverse("push_feed_page", args=[self.push_feed_id])

    def summary(self):
        return f"Sender {self.sender_label} {self.verb} on {self.feed_name}"


class PushSenderAdded(_PushSenderEvent):
    """A machine sender was added to a push feed."""

    label = "Sender added"
    verb = "added"


class PushSenderRotated(_PushSenderEvent):
    """A machine sender's secret was replaced; the old one stopped working."""

    label = "Sender rotated"
    verb = "rotated"


class PushSenderRevoked(_PushSenderEvent):
    """A machine sender was removed; its credentials stopped working."""

    label = "Sender revoked"
    verb = "revoked"


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
