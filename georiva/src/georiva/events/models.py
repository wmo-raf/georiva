"""The Domain event base and the index beside it (ADR 0030).

A Domain event is a durable, append-only, tenant-owned fact written in the same
transaction as the state change it describes. The base carries only what every
event shares; it has no generic status or payload field — each concrete event is
its own table with its own snapshot columns, so an event stays true after the
row it described has been overwritten by the next attempt.

The Python guard here (:class:`AppendOnlyQuerySet`, ``save`` and ``delete`` on
:class:`DomainEvent`) is the first of two layers. The second is a Postgres row
trigger installed by :class:`~georiva.events.operations.AppendOnlyTable`, which
the guard cannot replace: raw SQL and ``QuerySet._raw_delete`` bypass Python.
"""

import uuid
from dataclasses import dataclass

from django.db import models
from django.utils import timezone


class AppendOnlyError(RuntimeError):
    """Something tried to change or remove a Domain event."""


class OutcomeClass(models.TextChoices):
    OK = "ok", "OK"
    FAILED = "failed", "Failed"
    REFUSED = "refused", "Refused"


@dataclass(frozen=True)
class FailureGroup:
    """What a failed or refused event rolls up under on the dashboard (ADR 0033).

    ``key`` is stored on the index row; ``label`` and ``url`` are read from the
    group's newest event when the panel renders, so a rename shows at once.
    """

    key: str
    label: str
    url: str | None = None


class AppendOnlyQuerySet(models.QuerySet):
    def update(self, **kwargs):
        raise AppendOnlyError("Domain events are append-only: queryset update() is not allowed.")

    def delete(self):
        raise AppendOnlyError("Domain events are append-only: queryset delete() is not allowed.")

    def bulk_update(self, objs, fields, batch_size=None):
        raise AppendOnlyError("Domain events are append-only: bulk_update() is not allowed.")

    def update_or_create(self, defaults=None, **kwargs):
        raise AppendOnlyError("Domain events are append-only: update_or_create() is not allowed.")


class AppendOnlyModel(models.Model):
    """Rows that can be inserted and never changed or deleted through the ORM.

    Nulling a nullable reference when the row it points at is deleted is not an
    ORM ``save()`` — Django's collector issues it directly — so the guard does
    not stand in the way of ``SET_NULL``.
    """

    objects = AppendOnlyQuerySet.as_manager()

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if not self._state.adding:
            raise AppendOnlyError(f"{type(self).__name__} is append-only: an existing row cannot be saved.")
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise AppendOnlyError(f"{type(self).__name__} is append-only: rows cannot be deleted.")


class DomainEvent(AppendOnlyModel):
    """Abstract base of every concrete event type. Part of the supported plugin API.

    Tenancy is a plain organisation path with no global tier: a null organisation
    is *unattributed* — nobody's, visible to the instance administrator only.
    """

    ORGANISATION_LOOKUP = "organisation"

    #: Short past-tense noun phrase for the History panel, overridden per type.
    label = "Event"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    organisation = models.ForeignKey(
        "organisations.Organisation",
        null=True,
        blank=True,
        on_delete=models.PROTECT,  # the design's only PROTECT: an audit trail never loses its owner
        related_name="+",
        help_text="Null means unattributed: visible to the instance administrator only.",
    )
    occurred_at = models.DateTimeField(default=timezone.now, db_index=True)
    correlation_id = models.UUIDField(db_index=True)
    caused_by_id = models.UUIDField(null=True, blank=True)
    actor = models.CharField(max_length=100)

    class Meta:
        abstract = True
        ordering = ["-occurred_at"]

    @property
    def outcome_class(self) -> str:
        """``ok`` / ``failed`` / ``refused`` — what the failures panel filters on."""
        return OutcomeClass.OK

    def summary(self) -> str:
        """One line for a History row; concrete types override."""
        return self.label

    def subject_url(self) -> str | None:
        """Where the object this event describes lives now, or ``None`` once it is gone.

        The Timeline links to it when it exists and reads from the event's own
        snapshot fields when it does not.
        """
        return None

    def extra_links(self) -> list[tuple[str, str]]:
        """``(label, url)`` pairs beyond the subject, such as a Derivation attempt's inputs."""
        return []

    def failure_group_key(self) -> str:
        """What a failure of this event rolls up under on the dashboard (ADR 0033).

        A stable identifier — the thing an operator would fix, such as a feed or
        a collection — read from the event's own columns: ``record()`` calls it
        on every write, so it must not query. Blank, the default, groups by type.
        """
        return ""

    def failure_group(self) -> FailureGroup:
        """The key with a label and a link, for the dashboard line; may query."""
        return FailureGroup(key=self.failure_group_key(), label=self.label)

    def snapshot_items(self) -> list[tuple[str, object]]:
        """``(label, value)`` pairs for the expandable row: every snapshot column."""
        base = {"id", "organisation", "occurred_at", "correlation_id", "caused_by_id", "actor"}
        items = []
        for field in self._meta.concrete_fields:
            if field.name in base or field.is_relation:
                continue
            value = getattr(self, field.name)
            if value in (None, ""):
                continue
            items.append((field.verbose_name.capitalize(), value))
        return items


class EventIndex(AppendOnlyModel):
    """One row per Domain event, written by ``record()`` in the event's transaction.

    The Timeline and failures panel read this one table and fetch details per
    type, so there is no cross-table UNION whose shape would depend on which
    plugins are installed.
    """

    ORGANISATION_LOOKUP = "organisation"

    event_id = models.UUIDField(primary_key=True)
    event_type = models.CharField(max_length=100, help_text="``app_label.modelname`` of the concrete event.")
    correlation_id = models.UUIDField(db_index=True)
    occurred_at = models.DateTimeField(db_index=True)
    organisation = models.ForeignKey(
        "organisations.Organisation",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="+",
    )
    outcome_class = models.CharField(max_length=10, choices=OutcomeClass.choices, default=OutcomeClass.OK)
    failure_group_key = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The event's ``failure_group_key()``: what the failures panel groups on (ADR 0033).",
    )

    class Meta:
        ordering = ["occurred_at"]
        indexes = [
            models.Index(fields=["organisation", "outcome_class", "-occurred_at"], name="idx_evidx_org_outcome"),
        ]
        verbose_name = "Event index entry"


class EventsPurged(DomainEvent):
    """The purge command ran: who purged what, up to when, and how many of each type.

    Written by ``purge_events`` after its deletes, so removal is itself part of
    the record. Never purged.
    """

    label = "Events purged"

    cutoff = models.DateTimeField(help_text="Events that occurred before this instant were removed.")
    counts = models.JSONField(help_text="Rows removed, by event type.")

    def summary(self):
        total = sum(self.counts.values())
        return f"{total} events removed that occurred before {self.cutoff:%Y-%m-%d %H:%M}"


class SweepIntervened(DomainEvent):
    """A recovery sweep re-dispatched something (ADR 0030).

    The sweeps are the pipeline's reconciliation; this makes their interventions
    visible, so an operator can see when recovery machinery is doing the
    pipeline's job for it. The target is named by kind and id rather than a
    foreign key: this app sits beneath the ones whose rows it points at.
    """

    label = "Sweep intervened"

    sweep_name = models.CharField(max_length=100)
    reason = models.CharField(max_length=50)
    target_kind = models.CharField(max_length=30, help_text="``file_ingestion`` or ``derivation_run``.")
    target_id = models.BigIntegerField(null=True, blank=True)
    target_label = models.CharField(max_length=500, help_text="The file path or unit the sweep revived.")

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["target_kind", "target_id"], name="idx_sweepint_target")]

    def summary(self):
        return f"Sweep '{self.sweep_name}' revived {self.target_label}: {self.reason.replace('_', ' ')}"
