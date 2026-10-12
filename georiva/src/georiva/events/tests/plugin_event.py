"""A plugin-shaped event type, declared outside the events app (#432).

Its table is created by the test runner at database setup — see
``config/test_runner.py`` — for the same reason ``StubPluginFeed``'s is: a model
declared in a test module joins the app registry for the whole run, so every
organisation deletion walks its table, including in suites that sort before the
plugin-contract tests.
"""

from django.db import connection, models

from georiva.events.models import DomainEvent
from georiva.events.operations import install_sql


class PluginThingHappened(DomainEvent):
    """What a plugin's own event looks like: the base, a label, snapshot columns."""

    label = "Plugin thing happened"
    note = models.CharField(max_length=100)

    class Meta(DomainEvent.Meta):
        app_label = "georivaevents"

    def summary(self):
        return f"thing: {self.note}"


def create_table():
    """What a plugin's migration does: create the table, then call the shared operation."""
    if PluginThingHappened._meta.db_table in connection.introspection.table_names():
        return  # idempotent, for --keepdb
    with connection.schema_editor() as editor:
        editor.create_model(PluginThingHappened)
        for statement in install_sql(PluginThingHappened):
            editor.execute(statement, params=None)
