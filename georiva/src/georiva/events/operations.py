"""``AppendOnlyTable``: the migration operation that makes append-only a property of Postgres.

The model guard stops the ORM; this stops everything else — raw SQL, a shell,
``QuerySet._raw_delete``. It installs a row trigger on one table that

* rejects every DELETE; and
* rejects every UPDATE unless each changed column is a nullable reference being
  set to NULL — the path Django's collector takes when an event's subject (a
  collection, a run) is deleted and its ``SET_NULL`` link is nulled.

The ``organisation`` reference is excluded from that exception: it is the one
``PROTECT`` reference and is never nulled by a cascade.

Usage, in a migration that creates a concrete event table (plugins use it too)::

    from georiva.events.operations import AppendOnlyTable

    operations = [
        migrations.CreateModel(name="PublicationWritten", ...),
        AppendOnlyTable("PublicationWritten"),
    ]

Part of the supported plugin API.
"""

import hashlib

from django.db import migrations

FUNCTION = "georiva_events_reject_change"

CREATE_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {FUNCTION}() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    changed text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION '% is append-only: rows cannot be deleted', TG_TABLE_NAME
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    FOR changed IN
        SELECT n.key
        FROM jsonb_each(to_jsonb(NEW)) AS n
        JOIN jsonb_each(to_jsonb(OLD)) AS o USING (key)
        WHERE n.value IS DISTINCT FROM o.value
    LOOP
        IF NOT (changed = ANY (TG_ARGV) AND (to_jsonb(NEW) -> changed) = 'null'::jsonb) THEN
            RAISE EXCEPTION '% is append-only: column % cannot be changed', TG_TABLE_NAME, changed
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;
"""


def trigger_name(table: str) -> str:
    """Stable and within Postgres' 63-character identifier limit."""
    name = f"{table}_append_only"
    if len(name) <= 63:
        return name
    return f"{table[:40]}_{hashlib.md5(table.encode()).hexdigest()[:8]}_ao"


def nullable_reference_columns(model) -> list[str]:
    return [
        field.column
        for field in model._meta.concrete_fields
        if field.is_relation and field.null and field.name != "organisation"
    ]


def install_sql(model) -> list[str]:
    table = model._meta.db_table
    columns = ", ".join("'" + column.replace("'", "''") + "'" for column in nullable_reference_columns(model))
    arguments = f"({columns})" if columns else "()"
    return [
        CREATE_FUNCTION,
        f'DROP TRIGGER IF EXISTS "{trigger_name(table)}" ON "{table}"',
        f'CREATE TRIGGER "{trigger_name(table)}" BEFORE UPDATE OR DELETE ON "{table}" '
        f"FOR EACH ROW EXECUTE FUNCTION {FUNCTION}{arguments}",
    ]


class AppendOnlyTable(migrations.operations.base.Operation):
    """Install the append-only trigger on ``model_name``'s table."""

    reversible = True
    reduces_to_sql = True

    def __init__(self, model_name):
        self.model_name = model_name

    def deconstruct(self):
        return (self.__class__.__qualname__, [self.model_name], {})

    def state_forwards(self, app_label, state):
        pass

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        for statement in install_sql(model):
            schema_editor.execute(statement, params=None)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        model = from_state.apps.get_model(app_label, self.model_name)
        table = model._meta.db_table
        schema_editor.execute(f'DROP TRIGGER IF EXISTS "{trigger_name(table)}" ON "{table}"')

    def describe(self):
        return f"Make {self.model_name} append-only in the database"
