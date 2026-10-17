# Recording Domain Events from a Plugin

A **Domain event** is a durable, append-only, tenant-owned fact written in the same database transaction as the state
change it describes (ADR 0030). Core records them for fetches, ingestions, model runs, Derivation attempts and sweeps.
A plugin that does something worth tracing — a publisher writing to an external system, say — can record its own, and
they appear beside core's in every History panel, the Timeline and the failures panel.

> **Compatibility promise.** Everything under *The supported API* below is public. Changes to it will be announced and
> deprecated, not broken silently. Anything else under `georiva.events` is internal.

## The supported API

| Import                                                    | What it is                                                                                     |
|-----------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `georiva.events.models.DomainEvent`                       | The abstract base: id, organisation, occurred-at, Correlation, Caused-by, Actor.               |
| `georiva.events.recording.record`                         | Writes an event **and** its index row, in the caller's transaction.                            |
| `georiva.events.context`                                  | The context object, `use_context`, `ensure_context`, `context_after`, `caused_by`.             |
| `georiva.events.operations.AppendOnlyTable`               | The migration operation that installs the Postgres append-only trigger.                        |
| `georiva.events.registry`                                 | The event-type registry; filled at app-ready from every concrete `DomainEvent` subclass.       |
| `georiva.events.tasks`                                    | The task base classes: `ChainedTask`, `EntryTask`, `SweepTask`, `ReadSideTask`.                |
| `georiva.events.testing.event_context`                    | A context manager for tests.                                                                   |
| The `event=` keyword on core's Domain signals             | The event just written, handed to receivers (`run_ingestion_closed`, `unit_completed`, …).     |

## 1. Define the event

An event is a past-tense fact with its own **snapshot** of the identifying facts, so it stays true after the row it
described has been overwritten. Links to subjects are nullable `SET_NULL` references — never cascade, never protect — so
deleting a collection leaves its history readable.

```python
from django.db import models
from georiva.events.models import DomainEvent


class PublicationWritten(DomainEvent):
    label = "Publication written"          # shown in History rows and the Timeline

    run = models.ForeignKey("georivaingestion.RunIngestion", null=True, blank=True,
                            on_delete=models.SET_NULL, related_name="+")
    collection_slug = models.CharField(max_length=255)   # snapshot
    revision = models.PositiveIntegerField()             # snapshot

    class Meta(DomainEvent.Meta):
        pass

    def summary(self):                      # one line for a row
        return f"Published {self.collection_slug} at revision {self.revision}"

    def subject_url(self):                  # optional: where the subject lives now
        return None
```

Override `outcome_class` (`"ok"`, `"failed"` or `"refused"`) if the event can fail; the failures panel lists the last two.

## 2. Install the append-only trigger in the migration

`makemigrations` will generate the table. Add one operation after it, so the table is protected in the database and not
just by Python:

```python
from georiva.events.operations import AppendOnlyTable

operations = [
    migrations.CreateModel(name="PublicationWritten", ...),
    AppendOnlyTable("PublicationWritten"),
]
```

The trigger rejects every `DELETE`, and every `UPDATE` unless each changed column is a nullable reference being set to
`NULL` (the path taken when a subject is deleted). The `organisation` reference is never nullable by cascade.

## 3. Record it

`record()` fills the base fields from the **context** and writes the index row in the same transaction. It raises when no
context is set — a missing context is loud, a stale one would silently misattribute tenancy.

```python
from georiva.events.recording import record

with transaction.atomic():
    publication.save()
    record(PublicationWritten, run=run, collection_slug=..., revision=run.revision)
```

Record inside the state-changing transaction, after the conditional update you won. Never from a `post_save` receiver.

## 4. Continue the chain from a Domain signal

Core's Domain signals fire **after commit** and pass the event just written as the `event` keyword. A receiver typically
runs with no context of its own, so continue the event's chain, with that event as the cause:

```python
from georiva.events.context import context_after, use_context

def on_run_closed(sender, run, event=None, **kwargs):
    with use_context(context_after(event, actor="system:my_plugin")):
        dispatch_publish_task.delay(run.pk)      # carries the chain in its task headers
```

Receivers written as `(sender, run, **kwargs)` keep working unchanged.

## 5. Put your tasks on the base classes

Task context travels in Celery headers, written by every publish from inside a context and read by the task base
classes. A task declared with a plain `@app.task` keeps working — it never checks for context — **but it silently drops
the chain**: anything it records or enqueues starts from nothing. A plugin that adopts Domain events moves its own tasks
onto the base classes:

```python
from georiva.config.celery import app
from georiva.events.tasks import ChainedTask

@app.task(name="my_plugin.publish", base=ChainedTask, bind=True)
def publish(self, run_id): ...
```

| Base           | Use for                                                                                  |
|----------------|------------------------------------------------------------------------------------------|
| `ChainedTask`  | Work an arrival set in motion. The default. Reads the context from headers.              |
| `EntryTask`    | A scheduled or API-started task. Always starts a fresh chain.                            |
| `SweepTask`    | A recovery sweep (`sweep_name="…"`). Starts a `sweep:<name>` chain.                      |
| `ReadSideTask` | Artefact tasks. Inherit a context when present, never require one, write no events.      |

Every base resets the context when the task ends, however it ends. With `GEORIVA_TASK_CONTEXT_MODE=warn` (the production
default) a chained task with no headers logs an error, counts it and continues as `system:unpropagated`; with `strict`
(tests) it raises.

## 6. Test it

```python
from georiva.events.testing import event_context

with event_context(organisation_id=org.pk):
    record(PublicationWritten, ...)
```

Core's own tests prove this contract with a test-only event type (`georiva/events/tests/test_plugin_contract.py`) — they
never import a real plugin. The Timeline, History panels and failures panel tolerate a registered type whose table is
absent, so a deployment without your plugin is not broken by your plugin's missing tables.

## Cross-organisation work

A chain is single-organisation. If a future hop must cross organisations, swap the organisation in the context
explicitly (`dataclasses.replace(context, organisation_id=...)`) while keeping the Correlation. This is a requirement on
the caller, not something the framework does for you.
