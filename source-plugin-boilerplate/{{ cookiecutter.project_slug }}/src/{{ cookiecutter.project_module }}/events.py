"""A working example of a plugin recording its own Domain events (ADR 0030).

Delete this file (and the import in models.py) if the plugin does not need to
record anything. The full guide is docs/plugins/domain-events.md.

The four steps, all shown here:

1. define the event — a past-tense fact with its own snapshot columns;
2. install the append-only trigger in the migration that creates its table
   (see the note at the bottom of this file);
3. record it with ``record()``, inside the transaction of the change it describes;
4. continue the chain from a core Domain signal, with the event you were handed as
   the cause (``context_after``).

Tasks that record or enqueue should declare ``base=ChainedTask`` from
``georiva.events.tasks``: a plainly-declared task keeps working but silently drops
the chain.
"""

from django.db import models
from georiva.events.context import context_after, use_context
from georiva.events.models import DomainEvent
from georiva.events.recording import record


class {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}Ran(DomainEvent):
    """{{ cookiecutter.project_name }} did its work for a model run."""

    label = "{{ cookiecutter.project_name }} ran"

    # A snapshot: still true after the run row it describes has moved on.
    run_reference_time = models.DateTimeField()
    revision = models.PositiveIntegerField()

    class Meta(DomainEvent.Meta):
        pass

    def summary(self):
        return f"Ran for run {self.run_reference_time:%Y-%m-%d %H:%MZ} at revision {self.revision}"


def on_run_closed(sender, run, event=None, **kwargs):
    """Receiver for core's ``run_ingestion_closed`` signal (connect it in ``AppConfig.ready``).

    Core fires the signal after commit and hands over the event it just wrote, so
    this continues that event's chain with it as the cause. Receivers written as
    ``(sender, run, **kwargs)`` work unchanged if you do not need the event.
    """
    if event is None:
        return
    with use_context(context_after(event, actor="system:{{ cookiecutter.project_module }}")):
        record({{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}Ran, run_reference_time=run.reference_time, revision=run.revision)


# Wire-up, in apps.py:
#
#     def ready(self):
#         from georiva.ingestion.domain_signals import run_ingestion_closed
#         from .events import on_run_closed
#         run_ingestion_closed.connect(on_run_closed, dispatch_uid="{{ cookiecutter.project_module }}.on_run_closed")
#
# Migration: after `make dev-makemigrations` generates the CreateModel, add the
# database-level append-only protection to the operations list:
#
#     from georiva.events.operations import AppendOnlyTable
#     operations = [
#         migrations.CreateModel(...),
#         AppendOnlyTable("{{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}Ran"),
#     ]
