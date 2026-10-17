"""The one sanctioned way to remove Domain events (ADR 0030).

Scoped by organisation and cutoff; never scheduled. Within a single transaction
it suspends the append-only trigger on each affected table, deletes, restores
the trigger, and records an ``EventsPurged`` event. If anything fails the whole
transaction — including the suspension — rolls back, and the trigger stays on.
"""

from datetime import datetime, time

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone

from georiva.events.context import new_context, use_context
from georiva.events.models import EventIndex, EventsPurged
from georiva.events.operations import trigger_name
from georiva.events.recording import record
from georiva.events.registry import event_types, type_key
from georiva.organisations.models import Organisation


class Command(BaseCommand):
    help = "Delete one organisation's Domain events that occurred before a cutoff date, leaving an EventsPurged event."

    def add_arguments(self, parser):
        parser.add_argument("--organisation", required=True, help="Slug of the organisation whose events are purged.")
        parser.add_argument("--before", required=True, help="Cutoff date (YYYY-MM-DD); earlier events are removed.")
        parser.add_argument("--by", default="", help="Username of the person authorising the purge, for the record.")

    def handle(self, *args, organisation, before, by, **options):
        try:
            org = Organisation.objects.get(slug=organisation)
        except Organisation.DoesNotExist:
            raise CommandError(f"No organisation with slug {organisation!r}.") from None
        try:
            cutoff = timezone.make_aware(datetime.combine(datetime.strptime(before, "%Y-%m-%d").date(), time.min))
        except ValueError:
            raise CommandError("--before must be a date like 2026-01-31.") from None
        actor = "system:purge_events"
        if by:
            user = get_user_model().objects.filter(username=by).first()
            if user is None:
                raise CommandError(f"No user named {by!r}.")
            actor = f"user:{user.pk}"

        counts = self.purge(org, cutoff, actor)
        total = sum(counts.values())
        self.stdout.write(f"Purged {total} events for {org.slug} before {cutoff:%Y-%m-%d}: {counts}")

    @staticmethod
    def purge(org, cutoff, actor) -> dict:
        existing = set(connection.introspection.table_names())
        models = [m for m in event_types() if m is not EventsPurged and m._meta.db_table in existing]
        tables = [m._meta.db_table for m in models] + [EventIndex._meta.db_table]

        with transaction.atomic():
            with connection.cursor() as cursor:
                # ALTER TABLE refuses while deferred constraint checks are pending.
                cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
                for table in tables:
                    cursor.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER "{trigger_name(table)}"')
            counts = {}
            for model in models:
                removed = model._default_manager.filter(organisation=org, occurred_at__lt=cutoff)._raw_delete(
                    using=connection.alias
                )
                if removed:
                    counts[type_key(model)] = removed
            # EventsPurged is never purged, so its index rows stay with it.
            EventIndex._default_manager.filter(organisation=org, occurred_at__lt=cutoff).exclude(
                event_type=type_key(EventsPurged)
            )._raw_delete(using=connection.alias)
            with connection.cursor() as cursor:
                for table in tables:
                    cursor.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER "{trigger_name(table)}"')
            with use_context(new_context(actor=actor, organisation_id=org.pk)):
                record(EventsPurged, cutoff=cutoff, counts=counts)
        return counts
