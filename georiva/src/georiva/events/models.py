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

from django.db import models
from django.utils import timezone


class AppendOnlyError(RuntimeError):
    """Something tried to change or remove a Domain event."""


class OutcomeClass(models.TextChoices):
    OK = "ok", "OK"
    FAILED = "failed", "Failed"
    REFUSED = "refused", "Refused"


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
