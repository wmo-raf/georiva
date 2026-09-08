"""Abstract bases: a record that is rebuilt, and the log of its attempts."""

from datetime import timedelta

from django.db import models
from django.utils import timezone
from django_extensions.db.models import TimeStampedModel


class BuildDisciplinedModel(TimeStampedModel):
    """A record whose content is rebuilt from data that keeps arriving.

    Subclasses add *what* is built and *from what* — this holds only the state
    machine, the lock, and the input fingerprint. It is deliberately grain-free:
    ``VirtualZarrManifest`` hangs one off a ``Variable``, a publication hangs
    one off a ``Collection``, and nothing here can tell.

    The one rule that is not obvious from the fields: **a build is claimed at
    dispatch, not when the task starts.** Claiming at start leaves a record
    PENDING for as long as it sits in the queue, so the next sweep dispatches it
    again — once per sweep for the depth of the backlog (#398). Use
    ``build_discipline.dispatch_build``; it is the only supported way to put a
    build on a queue.

    Subclassing::

        class ForecastPublication(BuildDisciplinedModel):
            collection = models.ForeignKey(...)

            def mark_ready(self, *, version, point_count):     # optional
                super().mark_ready(version=version, point_count=point_count)

    Every transition is a queryset ``update()`` on ``pk``, never a ``save()``:
    two workers may hold the same Python object, and only the database can
    decide between them. That also means the in-memory instance is stale
    afterwards — call ``refresh_from_db()`` if you need to read it back.

    What the base deliberately leaves to the subclass: coverage caches, storage
    size and object counts, and anything else derived from the artefact itself.
    Only the subclass knows what it wrote and where, and ``mark_ready`` takes
    those columns as keyword arguments.
    """

    #: How long a claim survives without progress before ``reset_stale_locks``
    #: takes it back. It has to cover the whole queued window as well as the
    #: build itself: the claim is taken at dispatch, so a backlog deeper than
    #: this produces a second dispatch (which the claim token then stands down).
    LOCK_TIMEOUT = timedelta(minutes=30)

    class Status(models.TextChoices):
        PENDING = "pending", "Pending build"
        BUILDING = "building", "Building"
        READY = "ready", "Ready"
        STALE = "stale", "Stale"
        FAILED = "failed", "Failed"
        # Terminal until data arrives: there is nothing to build from, so a
        # retry cannot succeed. The sweep skips NO_DATA; whatever notices data
        # arriving flips it back to STALE.
        NO_DATA = "no_data", "No data"

    #: Statuses a build may be claimed from. READY needs no build, BUILDING is
    #: already claimed, and NO_DATA is terminal until data arrives.
    BUILDABLE_STATUSES = (Status.PENDING, Status.STALE, Status.FAILED)

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    built_at = models.DateTimeField(null=True, blank=True, editable=False)
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="The claim token held by the copy currently building, not merely the worker.",
    )
    error = models.TextField(blank=True)
    input_fingerprint = models.CharField(
        max_length=200,
        blank=True,
        default="",
        editable=False,
        help_text=(
            "Identity of the inputs the last successful build read. A build whose "
            "inputs still fingerprint the same has nothing to do."
        ),
    )

    class Meta:
        abstract = True

    # =========================================================================
    # Lock arithmetic
    # =========================================================================

    @classmethod
    def stale_lock_cutoff(cls):
        """Locks stamped before this are abandoned."""
        return timezone.now() - cls.LOCK_TIMEOUT

    @property
    def lock_is_live(self) -> bool:
        """True while a claim on this row is still someone's to hold."""
        return (
            self.status == self.Status.BUILDING
            and self.locked_at is not None
            and self.locked_at >= self.stale_lock_cutoff()
        )

    def holds_claim(self, claim: str) -> bool:
        """Whether ``claim`` is still the claim this row is under.

        A blank ``claim`` means the caller was dispatched before claims existed
        and is trusted, matching ``virtual_zarr``'s reading of the same field.
        """
        return not claim or self.locked_by == claim

    # =========================================================================
    # Claiming
    # =========================================================================

    @classmethod
    def claim_for_build(cls, pk, claim: str = "", force: bool = False) -> bool:
        """Take the build lock for one record; return whether we got it.

        One conditional UPDATE, so two sweeps racing each other can only have
        one winner. ``claim`` is stored in ``locked_by`` and identifies *this
        claim*, not just the worker: the build task compares it before starting,
        so a copy whose claim was recycled by ``reset_stale_locks`` stands down
        instead of running alongside its replacement.

        ``force`` claims a record that does not *need* building — the operator's
        explicit rebuild, which may target a READY or NO_DATA row. It still
        refuses one another worker is actively holding.

        A claim whose task never runs is indistinguishable from a worker that
        died mid-build, and ``reset_stale_locks`` recovers both.
        """
        rows = cls.objects.filter(pk=pk)
        if force:
            rows = rows.exclude(status=cls.Status.BUILDING, locked_at__gte=cls.stale_lock_cutoff())
        else:
            rows = rows.filter(status__in=cls.BUILDABLE_STATUSES)

        return bool(
            rows.update(
                status=cls.Status.BUILDING,
                locked_at=timezone.now(),
                locked_by=claim,
                error="",
            )
        )

    def refresh_build_lock(self, worker_id: str = "") -> None:
        """Re-stamp a lock this worker already holds, now that its build is
        actually starting.

        Not a way to *take* the lock — that is ``claim_for_build``, which
        refuses a row someone else holds. This is unconditional by design: the
        caller is the copy that won the claim, and restarting the clock here
        stops a long queue wait eating into the window ``reset_stale_locks``
        allows the build itself.
        """
        type(self).objects.filter(pk=self.pk).update(
            status=self.Status.BUILDING,
            locked_at=timezone.now(),
            locked_by=worker_id,
            error="",
        )

    @classmethod
    def reset_stale_locks(cls) -> int:
        """Reset BUILDING records whose locks have expired (crash recovery).

        Back to PENDING, so the next sweep re-dispatches them.
        """
        return cls.objects.filter(
            status=cls.Status.BUILDING,
            locked_at__lt=cls.stale_lock_cutoff(),
        ).update(
            status=cls.Status.PENDING,
            locked_at=None,
            locked_by="",
        )

    # =========================================================================
    # Transitions
    # =========================================================================

    def mark_ready(self, **fields) -> None:
        """A build finished. ``fields`` are the subclass's own coverage caches.

        Pass ``input_fingerprint`` here — that is what lets the next build skip.
        """
        type(self).objects.filter(pk=self.pk).update(
            status=self.Status.READY,
            built_at=timezone.now(),
            locked_at=None,
            locked_by="",
            error="",
            **fields,
        )

    def mark_failed(self, error: str) -> None:
        type(self).objects.filter(pk=self.pk).update(
            status=self.Status.FAILED,
            locked_at=None,
            locked_by="",
            error=error[:2000],
        )

    def mark_no_data(self) -> None:
        """Park the record when there is nothing to build from at all.

        Unlike FAILED, NO_DATA is not retried by the sweep — retrying cannot
        succeed until data actually arrives, at which point whatever watches
        arrivals transitions it back to STALE.
        """
        type(self).objects.filter(pk=self.pk).update(
            status=self.Status.NO_DATA,
            locked_at=None,
            locked_by="",
            error="",
        )

    def mark_stale(self) -> None:
        """The inputs changed.

        Only READY or NO_DATA → STALE. A record already PENDING, BUILDING or
        FAILED is left alone — it will be rebuilt by the next sweep anyway, and
        overwriting BUILDING would drop a live claim.
        """
        type(self).objects.filter(
            pk=self.pk,
            status__in=[self.Status.READY, self.Status.NO_DATA],
        ).update(status=self.Status.STALE)

    def queue_rebuild(self) -> bool:
        """Flip to PENDING so the next sweep rebuilds — the operator's manual
        re-queue.

        Never dispatches a task itself: the sweep's ``get_buildable`` picks the
        row up on its normal cadence, so this path shares the sweep's locking
        exactly. The guard mirrors ``get_buildable``'s exclusion — a BUILDING
        row with a live lock is left to its worker (returns False); a stuck one
        (expired or missing lock stamp) is reset like ``reset_stale_locks``
        would. Single conditional UPDATE, so it cannot race a concurrent
        ``claim_for_build``.
        """
        updated = (
            type(self)
            .objects.filter(pk=self.pk)
            .exclude(
                status=self.Status.BUILDING,
                locked_at__gte=self.stale_lock_cutoff(),
            )
            .update(
                status=self.Status.PENDING,
                locked_at=None,
                locked_by="",
            )
        )
        return bool(updated)

    # =========================================================================
    # Queries
    # =========================================================================

    @classmethod
    def get_buildable(cls):
        """Records that need building: PENDING, STALE, or retryable FAILED.

        A record already in flight is BUILDING — claimed at dispatch — and so is
        excluded by the status filter itself. Subclasses override to add the
        ``select_related`` their build needs.
        """
        return cls.objects.filter(status__in=cls.BUILDABLE_STATUSES)

    def is_up_to_date(self, fingerprint: str) -> bool:
        """Whether a build over inputs fingerprinting as ``fingerprint`` would
        reproduce what is already there.

        A blank fingerprint is never up to date: it means either the caller
        cannot fingerprint its inputs or nothing has been built, and both have
        to build.
        """
        return bool(fingerprint) and self.status == self.Status.READY and self.input_fingerprint == fingerprint


class BuildAttemptLog(models.Model):
    """One attempt at building, or one retention pass, kept after the fact.

    The built record only ever holds the *latest* state — a failed build
    overwrites the previous error in place — so without these rows an operator
    cannot see that a build has been failing all week, or how long it used to
    take. Pruned by ``prune_expired`` on whatever daily task the subclass
    already runs.

    Subclasses supply the foreign key (its target and ``related_name`` differ per
    grain) and name it in ``TARGET_FIELD``, plus any columns describing what the
    build did — how it classified the work, how much it wrote, what snapshot it
    produced. Those vocabularies are the subclass's; only kind, outcome, timing
    and the error are universal.
    """

    RETENTION = timedelta(days=30)

    #: Name of the FK the subclass declares back to its built record.
    TARGET_FIELD = "target"

    class Kind(models.TextChoices):
        BUILD = "build", "Build"
        GC = "gc", "Garbage collection"

    class Outcome(models.TextChoices):
        SUCCESS = "success", "Success"
        FAILURE = "failure", "Failure"

    kind = models.CharField(max_length=10, choices=Kind.choices)
    outcome = models.CharField(max_length=10, choices=Outcome.choices)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField()
    error = models.TextField(blank=True)

    class Meta:
        abstract = True
        ordering = ["-started_at"]

    @property
    def duration(self) -> timedelta:
        return self.finished_at - self.started_at

    @classmethod
    def record(cls, target, kind, outcome, started_at, **fields) -> "BuildAttemptLog":
        """One attempt/run row, stamped finished now."""
        return cls.objects.create(
            **{cls.TARGET_FIELD: target},
            kind=kind,
            outcome=outcome,
            started_at=started_at,
            finished_at=timezone.now(),
            **fields,
        )

    @classmethod
    def prune_expired(cls) -> int:
        """Delete rows older than RETENTION; returns the number removed."""
        cutoff = timezone.now() - cls.RETENTION
        deleted, _ = cls.objects.filter(started_at__lt=cutoff).delete()
        return deleted
