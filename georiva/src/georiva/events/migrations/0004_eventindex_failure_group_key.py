"""Give each index row its failure group key (ADR 0033), and fill it for past failures.

Only failed and refused rows are backfilled — the failures panel never reads an
``ok`` row. The key comes from each event's own ``failure_group_key()``, so the
live model is read rather than the historical one, which has no methods; a type
whose table is gone or not yet in step with its model keeps the blank key, and
its old failures group by type alone. The append-only trigger is lifted for the
one column write, the way ``purge_events`` lifts it for its deletes.
"""

from django.db import DatabaseError, migrations, models, transaction

FAILING = ("failed", "refused")


def backfill(apps, schema_editor):
    from georiva.events.operations import trigger_name
    from georiva.events.registry import installed_tables, model_for, populate

    EventIndex = apps.get_model("georivaevents", "EventIndex")
    wanted: dict[str, list] = {}
    for event_type, event_id in EventIndex.objects.filter(outcome_class__in=FAILING).values_list(
        "event_type", "event_id"
    ):
        wanted.setdefault(event_type, []).append(event_id)
    if not wanted:
        return

    populate()
    existing = installed_tables()
    keys: dict[str, list] = {}
    for event_type, ids in wanted.items():
        model = model_for(event_type)
        if model is None or model._meta.db_table not in existing:
            continue
        try:
            with transaction.atomic():
                for event in model._default_manager.filter(pk__in=ids).iterator():
                    keys.setdefault(event.failure_group_key(), []).append(event.pk)
        except DatabaseError:
            continue
    keys.pop("", None)
    if not keys:
        return

    table = EventIndex._meta.db_table
    # ALTER TABLE refuses while deferred constraint checks are pending.
    schema_editor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    schema_editor.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER "{trigger_name(table)}"')
    for key, ids in keys.items():
        # The base manager: the live model's default one refuses update() by design.
        EventIndex._base_manager.filter(pk__in=ids).update(failure_group_key=key)
    schema_editor.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER "{trigger_name(table)}"')


class Migration(migrations.Migration):
    dependencies = [
        ("georivaevents", "0003_sweepintervened"),
    ]

    operations = [
        migrations.AddField(
            model_name="eventindex",
            name="failure_group_key",
            field=models.CharField(
                blank=True,
                default="",
                help_text="The event's ``failure_group_key()``: what the failures panel groups on (ADR 0033).",
                max_length=255,
            ),
        ),
        migrations.RunPython(backfill, migrations.RunPython.noop),
    ]
